"""
Order Executor - Real trading engine with retry logic
Based on /root/clip/trade.py methods
"""
import os
import time
import json
import math
import requests
from pathlib import Path
from typing import Dict, Optional
from dataclasses import dataclass
import concurrent.futures

from web3 import Web3
from eth_account import Account
from py_clob_client_v2.client import ClobClient
from py_clob_client_v2.clob_types import (
    OrderArgs,
    MarketOrderArgs,
    OrderType,
    BalanceAllowanceParams,
    AssetType,
)
from py_clob_client_v2.order_builder.constants import BUY, SELL
from py_clob_client_v2.order_utils import SignatureTypeV2
from py_builder_relayer_client.config import get_contract_config
from py_builder_relayer_client.builder.derive import derive_deposit_wallet

from safety_guard import SafetyGuard
import logging
from trade_logger import log_buy_attempt, log_buy_result, log_sell_attempt, log_sell_result
import threading

# 🔥 GLOBAL: Blocked markets per-coin (race condition protection)
# Markets in this dict CANNOT receive new buy orders (stop-loss/flip-stop active)
# Structure: {'btc': set(), 'eth': set(), 'sol': set(), 'xrp': set()}
_blocked_markets_lock = threading.Lock()
_blocked_markets = {
    'btc': set(),
    'eth': set(),
    'sol': set(),
    'xrp': set()
}


@dataclass
class OrderResult:
    """Order execution result (with FAK/FOK retry support)"""
    success: bool
    order_id: Optional[str] = None
    filled_size: float = 0.0           # Total contracts filled (can be less than target!)
    filled_price: float = 0.0          # Average price per contract
    total_spent_usd: float = 0.0       # Total USD spent/received (sum across all attempts)
    attempts: int = 1                  # Number of attempts made
    error: Optional[str] = None
    dry_run: bool = False
    elapsed_ms: int = 0
    remaining_balance: float = 0.0     # 🔥 FIX 4: Final balance on blockchain after sell


class OrderExecutor:
    """Execute real orders on Polymarket (methods from /root/clip)"""
    
    @staticmethod
    def block_market(market_slug: str, coin: str):
        """
        🔥 CRITICAL: Block market from receiving new buy orders (per-coin)
        Call this IMMEDIATELY when stop-loss/flip-stop triggers
        
        Args:
            market_slug: Market identifier
            coin: Coin name ('btc', 'eth', 'sol', 'xrp')
        """
        with _blocked_markets_lock:
            if coin in _blocked_markets:
                _blocked_markets[coin].add(market_slug)
                print(f"[EXECUTOR] 🔒 MARKET BLOCKED: {coin.upper()} - {market_slug}")
            else:
                print(f"[EXECUTOR] ⚠️ Unknown coin: {coin}")
    
    @staticmethod
    def unblock_market(market_slug: str, coin: str):
        """
        Unblock market (call after successful redeem)
        
        Args:
            market_slug: Market identifier
            coin: Coin name ('btc', 'eth', 'sol', 'xrp')
        """
        with _blocked_markets_lock:
            if coin in _blocked_markets and market_slug in _blocked_markets[coin]:
                _blocked_markets[coin].remove(market_slug)
                print(f"[EXECUTOR] 🔓 MARKET UNBLOCKED: {coin.upper()} - {market_slug}")
    
    @staticmethod
    def is_market_blocked(market_slug: str, coin: str) -> bool:
        """
        Check if market is blocked for specific coin (atomic check)
        
        Args:
            market_slug: Market identifier
            coin: Coin name ('btc', 'eth', 'sol', 'xrp')
            
        Returns:
            True if blocked for this coin, False otherwise
        """
        with _blocked_markets_lock:
            return coin in _blocked_markets and market_slug in _blocked_markets[coin]
    
    def __init__(self, safety_guard: SafetyGuard, config: Dict, data_feed=None):
        self.safety = safety_guard
        self.config = config
        self.data_feed = data_feed  # ✅ For access to position_tracker
        
        # Initialize CLOB client
        self.client = None
        self.wallet_address = None
        self.signature_type = SignatureTypeV2.EOA
        
        if not self.safety.dry_run:
            try:
                from dotenv import load_dotenv
                # Load .env from project root (not from current directory)
                project_root = Path(__file__).parent.parent
                env_path = project_root / ".env"
                load_dotenv(env_path)
                
                # Read PRIVATE_KEY AFTER loading .env
                self.private_key = os.getenv("PRIVATE_KEY", "")
                if not self.private_key:
                    raise ValueError("PRIVATE_KEY not found in .env")
                
                # Read signature type and funder address
                signature_type = int(os.getenv("SIGNATURE_TYPE", "0"))
                funder_address = (os.getenv("FUNDER_ADDRESS") or "").strip()
                eoa_address = Account.from_key(self.private_key).address

                host = os.getenv("CLOB_HOST", "https://clob.polymarket.com")
                chain_id = int(os.getenv("CHAIN_ID", "137"))

                # Detect Polymarket deposit wallet (CLOB v2 / POLY_1271)
                relayer_cfg = get_contract_config(chain_id)
                deposit_wallet = derive_deposit_wallet(
                    eoa_address,
                    relayer_cfg.deposit_wallet_factory,
                    relayer_cfg.deposit_wallet_implementation,
                )
                if funder_address and funder_address.lower() == deposit_wallet.lower():
                    if signature_type != SignatureTypeV2.POLY_1271:
                        print(
                            "[EXECUTOR] ⚠ FUNDER is a deposit wallet — "
                            f"overriding SIGNATURE_TYPE {signature_type} → 3 (POLY_1271)"
                        )
                    signature_type = SignatureTypeV2.POLY_1271
                self.signature_type = signature_type

                # Resolve wallet address used for balance queries and order maker
                if signature_type == SignatureTypeV2.EOA:
                    self.wallet_address = eoa_address
                    wallet_type = "EOA"
                else:
                    if not funder_address:
                        if signature_type == SignatureTypeV2.POLY_1271:
                            funder_address = deposit_wallet
                        else:
                            raise ValueError(
                                f"SIGNATURE_TYPE={signature_type} requires FUNDER_ADDRESS in .env"
                            )
                    self.wallet_address = funder_address
                    if signature_type == SignatureTypeV2.POLY_1271:
                        wallet_type = "Deposit wallet (POLY_1271)"
                    elif signature_type == SignatureTypeV2.POLY_GNOSIS_SAFE:
                        wallet_type = "Gnosis Safe (type 2)"
                    else:
                        wallet_type = f"Proxy (type {signature_type})"

                # Initialize ClobClient
                if signature_type == SignatureTypeV2.EOA:
                    self.client = ClobClient(
                        host=host,
                        chain_id=chain_id,
                        key=self.private_key,
                        signature_type=signature_type,
                    )
                else:
                    self.client = ClobClient(
                        host=host,
                        chain_id=chain_id,
                        key=self.private_key,
                        signature_type=signature_type,
                        funder=funder_address,
                    )
                # 🚨 CRITICAL: Generate and set API credentials
                print(f"[EXECUTOR] Generating API credentials...")
                creds = self.client.create_or_derive_api_key()
                self.client.set_api_creds(creds)
                print(f"[EXECUTOR] ✓ API credentials set (CLOB v2 client)")

                # Sync CLOB balance/allowance cache (required for deposit wallets)
                try:
                    self.client.update_balance_allowance(
                        BalanceAllowanceParams(
                            asset_type=AssetType.COLLATERAL,
                            signature_type=signature_type,
                        )
                    )
                    ba = self.client.get_balance_allowance(
                        BalanceAllowanceParams(
                            asset_type=AssetType.COLLATERAL,
                            signature_type=signature_type,
                        )
                    )
                    clob_cash = int(ba.get("balance", "0")) / 1e6
                    print(f"[EXECUTOR] ✓ CLOB Cash synced: ${clob_cash:.2f}")
                except Exception as sync_err:
                    print(f"[EXECUTOR] ⚠ CLOB balance sync failed: {sync_err}")

                print(f"[EXECUTOR] ✓ CLOB client initialized")
                print(f"[EXECUTOR]    Wallet: {self.wallet_address[:6]}...{self.wallet_address[-4:]}")
                print(f"[EXECUTOR]    Type: {wallet_type}")
            except Exception as e:
                print(f"[EXECUTOR] ❌ Failed to init CLOB client: {e}")
                self.safety.activate_emergency_stop("CLOB_INIT_FAILED")
        else:
            self.private_key = ""  # DRY_RUN - no private key needed
            print("[EXECUTOR] ✓ DRY_RUN mode (no real orders)")
        
        # 🔥 RPC Configuration (Multiple endpoints with parallel requests)
        self.rpc_config = config.get('execution', {}).get('rpc_config', {})
        
        # RPC endpoints (fallback to env var if not in config)
        self.rpc_endpoints = self.rpc_config.get('endpoints', [
            os.getenv("RPC_URL", "https://polygon-rpc.com")
        ])
        
        # RPC parameters
        self.rpc_single_timeout = self.rpc_config.get('single_request_timeout_sec', 3)
        self.rpc_parallel_timeout = self.rpc_config.get('parallel_timeout_sec', 5)
        self.rpc_retry_attempts = self.rpc_config.get('retry_attempts', 2)
        self.rpc_retry_delay = self.rpc_config.get('retry_delay_sec', 0.3)
        self.rpc_parallel_enabled = self.rpc_config.get('enable_parallel_requests', True)
        
        # Log RPC configuration
        print(f"[EXECUTOR] {'='*60}")
        print(f"[EXECUTOR] 🌐 RPC CONFIGURATION:")
        print(f"[EXECUTOR]    Endpoints: {len(self.rpc_endpoints)}")
        for i, rpc in enumerate(self.rpc_endpoints, 1):
            rpc_short = rpc.split('/')[2][:30] if '://' in rpc else rpc[:30]
            print(f"[EXECUTOR]      #{i}: {rpc_short}...")
        print(f"[EXECUTOR]    Single timeout: {self.rpc_single_timeout}s")
        print(f"[EXECUTOR]    Parallel timeout: {self.rpc_parallel_timeout}s")
        print(f"[EXECUTOR]    Retry attempts: {self.rpc_retry_attempts}")
        print(f"[EXECUTOR]    Retry delay: {self.rpc_retry_delay}s")
        print(f"[EXECUTOR]    Parallel mode: {'ENABLED ⚡' if self.rpc_parallel_enabled else 'DISABLED'}")
        print(f"[EXECUTOR] {'='*60}\n")
        
        # CTF contract for token balances
        self.CTF_ADDRESS = "0x4D97DCd97eC945f40cF65F87097ACe5EA0476045"
        self.CTF_ABI = [
            {"inputs": [{"name": "_owner", "type": "address"}, {"name": "_id", "type": "uint256"}], 
             "name": "balanceOf", "outputs": [{"name": "", "type": "uint256"}], 
             "stateMutability": "view", "type": "function"}
        ]
        
        # USDC / pUSD (Polymarket CLOB v2 collateral)
        self.USDC_BRIDGED = "0x2791Bca1f2de4661ED88A30C99A7a9449Aa84174"
        self.USDC_NATIVE = "0x3c499c542cEF5E3811e1192ce70d8cC03d5c3359"
        self.PUSD = "0xC011a7E12a19f7B1f670d46F03B03f3342E82DFB"
        self.ERC20_ABI = [
            {'constant': True, 'inputs': [{'name': '_owner', 'type': 'address'}], 
             'name': 'balanceOf', 'outputs': [{'name': 'balance', 'type': 'uint256'}], 'type': 'function'},
            {'constant': True, 'inputs': [], 'name': 'decimals', 
             'outputs': [{'name': '', 'type': 'uint8'}], 'type': 'function'}
        ]
        
        # Order logging
        self.orders_log = Path("logs/orders.jsonl")
        self.orders_log.parent.mkdir(exist_ok=True)
        
        # Callback for tracking balance changes
        self.balance_change_callback = None
        
        # Callback for checking market close (race condition protection)
        self.market_closing_check_callback = None
    
    def set_balance_callback(self, callback):
        """
        Set callback for balance changes
        callback(amount, operation, is_absolute=False)
          - amount: float - change amount or absolute value
          - operation: str - operation type ('BUY', 'SELL', 'REDEEM', 'REDEEM_REFRESH')
          - is_absolute: bool - if True, amount = full balance, else delta
        """
        self.balance_change_callback = callback
        print("[EXECUTOR] ✓ Balance change callback registered")
    
    def set_market_closing_check(self, callback):
        """
        Set callback for checking market close (race condition protection)
        callback(market_slug: str) -> bool
          - Returns True if market is closing and buys should be blocked
          - Returns False if market is open and buys are allowed
        
        🔥 CRITICAL: Prevents buys AFTER stop-loss/flip-stop trigger
        """
        self.market_closing_check_callback = callback
        print("[EXECUTOR] ✓ Market closing check callback registered")

    def _create_and_post_order(self, order_args: OrderArgs, order_type: OrderType):
        """Create and post limit order using CLOB v2."""
        return self.client.create_and_post_order(order_args, order_type=order_type)

    def _create_and_post_market_order(self, order_args: MarketOrderArgs, order_type: OrderType):
        """Create and post market order using CLOB v2 (FAK/FOK BUY must use USD amount)."""
        return self.client.create_and_post_market_order(order_args, order_type=order_type)
    
    def _log_redeem(self, market_slug: str, success: bool, amount: float, tx_hash: str = "", reason: str = ""):
        """Log redeem operation to separate file"""
        try:
            import os
            from datetime import datetime
            
            log_file = "logs/redeem.log"
            os.makedirs(os.path.dirname(log_file), exist_ok=True)
            
            with open(log_file, 'a') as f:
                timestamp = datetime.now().isoformat()
                status = "SUCCESS" if success else "FAILED"
                f.write(f"{timestamp} | {market_slug} | {status} | ${amount:.2f} | {tx_hash} | {reason}\n")
        except Exception as e:
            print(f"[ERROR] Failed to log redeem: {e}")
    
    def _read_erc20_balance(self, w3: Web3, token_address: str, owner: str) -> float:
        """Read ERC20 balance for owner; returns 0.0 if token call fails."""
        try:
            contract = w3.eth.contract(
                address=Web3.to_checksum_address(token_address),
                abi=self.ERC20_ABI,
            )
            raw = contract.functions.balanceOf(Web3.to_checksum_address(owner)).call()
            decimals = contract.functions.decimals().call()
            return raw / (10 ** decimals)
        except Exception:
            return 0.0

    def get_wallet_usdc_balance(self) -> Optional[float]:
        """
        Get wallet trading collateral: USDC.e + native USDC + pUSD (Polymarket CLOB v2 Cash).
        """
        try:
            if not self.wallet_address and self.private_key:
                self.wallet_address = Account.from_key(self.private_key).address

            if not self.wallet_address:
                print("[EXECUTOR] ❌ No wallet address")
                return None

            rpc_list = self.rpc_endpoints or [os.getenv("RPC_URL", "https://polygon-rpc.com")]
            last_error = None

            for rpc_url in rpc_list:
                try:
                    w3 = Web3(Web3.HTTPProvider(
                        rpc_url,
                        request_kwargs={'timeout': self.rpc_single_timeout},
                    ))
                    if not w3.is_connected():
                        continue

                    owner = Web3.to_checksum_address(self.wallet_address)
                    usdc_e = self._read_erc20_balance(w3, self.USDC_BRIDGED, owner)
                    usdc_n = self._read_erc20_balance(w3, self.USDC_NATIVE, owner)
                    pusd = self._read_erc20_balance(w3, self.PUSD, owner)
                    total = usdc_e + usdc_n + pusd

                    rpc_short = rpc_url.split('/')[2][:30] if '://' in rpc_url else rpc_url[:30]
                    print(
                        f"[EXECUTOR] Wallet balance @ {rpc_short}: "
                        f"USDC.e=${usdc_e:.2f} USDC=${usdc_n:.2f} pUSD=${pusd:.2f} → total=${total:.2f}"
                    )
                    return total
                except Exception as e:
                    last_error = e
                    continue

            print(f"[EXECUTOR] ⚠ Cannot connect to any RPC ({len(rpc_list)} tried)")
            if last_error:
                print(f"[EXECUTOR] ❌ Balance query error: {last_error}")
            return None

        except Exception as e:
            print(f"[EXECUTOR] ❌ Balance query error: {e}")
            return None
    
    def get_pol_balance(self) -> Optional[float]:
        """
        Get POL balance (native Polygon token)
        
        Returns:
            Balance in POL or None on error
        """
        try:
            if not self.wallet_address and self.private_key:
                self.wallet_address = Account.from_key(self.private_key).address
            
            if not self.wallet_address:
                print("[EXECUTOR] ❌ No wallet address")
                return None
            
            # Use first RPC endpoint for wallet balance queries
            rpc_url = self.rpc_endpoints[0] if self.rpc_endpoints else "https://polygon-rpc.com"
            w3 = Web3(Web3.HTTPProvider(rpc_url, request_kwargs={'timeout': self.rpc_single_timeout}))
            
            if not w3.is_connected():
                print("[EXECUTOR] ⚠ Cannot connect to RPC")
                return None
            
            # Get native balance (in Wei)
            balance_wei = w3.eth.get_balance(self.wallet_address)
            # Convert to POL (1 POL = 10^18 Wei)
            balance_pol = balance_wei / 1e18
            
            print(f"[EXECUTOR] POL balance: {balance_pol:.4f}")
            return balance_pol
            
        except Exception as e:
            print(f"[EXECUTOR] ❌ POL balance query error: {e}")
            return None
    
    def get_blockchain_token_balance(self, token_id: str) -> Optional[float]:
        """
        ✅ GET REAL TOKEN BALANCE FROM BLOCKCHAIN!
        
        Uses PARALLEL REQUESTS to multiple RPC endpoints for:
        - Maximum speed (take first successful response ~20-70ms)
        - Maximum reliability (if one RPC fails - use another)
        - Minimum timeout (instead of 60 seconds -> 5-10 seconds)
        
        Args:
            token_id: Token ID (e.g. "52114319501245915516055106046884209969926127482827954674443846427813813222426")
        
        Returns:
            Real balance in contracts (float) or None if all RPCs unavailable
        """
        if self.safety.dry_run:
            return 0.0
        
        try:
            if not self.wallet_address and self.private_key:
                self.wallet_address = Account.from_key(self.private_key).address
            
            if not self.wallet_address:
                print("[EXECUTOR] ❌ No wallet address for token balance query")
                return None
            
            # 🔥 FUNCTION: Request to one RPC endpoint
            def query_single_rpc(rpc_url: str, attempt: int = 1) -> Optional[float]:
                """Query balance from a single RPC endpoint"""
                try:
                    w3 = Web3(Web3.HTTPProvider(
                        rpc_url, 
                        request_kwargs={'timeout': self.rpc_single_timeout}
                    ))
                    
                    if not w3.is_connected():
                        return None
                    
                    ctf = w3.eth.contract(
                        address=Web3.to_checksum_address(self.CTF_ADDRESS), 
                        abi=self.CTF_ABI
                    )
                    
                    balance_raw = ctf.functions.balanceOf(
                        self.wallet_address, 
                        int(token_id)
                    ).call()
                    balance = balance_raw / 1e6  # Convert from raw to USDC decimals (6 decimals)
                    
                    rpc_short = rpc_url.split('/')[2][:20] if '://' in rpc_url else rpc_url[:20]
                    print(f"[EXECUTOR] ✅ RPC [{rpc_short}...] balance: {balance:.4f} contracts")
                    return balance
                    
                except Exception as e:
                    rpc_short = rpc_url.split('/')[2][:20] if '://' in rpc_url else rpc_url[:20]
                    print(f"[EXECUTOR] ⚠️  RPC [{rpc_short}...] failed: {type(e).__name__}")
                    return None
            
            # 🔥 RETRY LOOP with parallel or sequential requests
            for attempt in range(1, self.rpc_retry_attempts + 1):
                print(f"[EXECUTOR] 🔄 Balance query attempt {attempt}/{self.rpc_retry_attempts}...")
                
                if self.rpc_parallel_enabled and len(self.rpc_endpoints) > 1:
                    # 🚀 PARALLEL REQUESTS
                    print(f"[EXECUTOR] 🚀 Querying {len(self.rpc_endpoints)} RPCs in parallel...")
                    
                    executor = concurrent.futures.ThreadPoolExecutor(max_workers=len(self.rpc_endpoints))
                    
                    try:
                        # Start all requests SIMULTANEOUSLY
                        futures = {
                            executor.submit(query_single_rpc, rpc, attempt): rpc 
                            for rpc in self.rpc_endpoints
                        }
                        
                        # Wait for FIRST successful result
                        for future in concurrent.futures.as_completed(
                            futures, 
                            timeout=self.rpc_parallel_timeout
                        ):
                            try:
                                balance = future.result()
                                if balance is not None:
                                    # 🔥 CRITICAL: Immediately cancel remaining futures!
                                    executor.shutdown(wait=False, cancel_futures=True)
                                    print(f"[EXECUTOR] ✅ Got balance: {balance:.4f} contracts (token: {token_id[:16]}...)")
                                    return balance  # ← EARLY EXIT!
                            except Exception:
                                continue
                        
                    except concurrent.futures.TimeoutError:
                        print(f"[EXECUTOR] ⏱️  All RPCs timeout after {self.rpc_parallel_timeout}s")
                    finally:
                        # Guaranteed cleanup
                        executor.shutdown(wait=False, cancel_futures=True)
                
                else:
                    # 🔄 SEQUENTIAL REQUESTS (fallback or if parallel disabled)
                    print(f"[EXECUTOR] 🔄 Querying RPCs sequentially...")
                    for rpc in self.rpc_endpoints:
                        balance = query_single_rpc(rpc, attempt)
                        if balance is not None:
                            print(f"[EXECUTOR] ✅ Got balance: {balance:.4f} contracts (token: {token_id[:16]}...)")
                            return balance  # ✅ Success!
                
                # Didn't get balance - wait before next attempt
                if attempt < self.rpc_retry_attempts:
                    print(f"[EXECUTOR] ⏸️  Waiting {self.rpc_retry_delay}s before retry...")
                    time.sleep(self.rpc_retry_delay)
            
            # All attempts failed
            print(f"[EXECUTOR] ❌ All {self.rpc_retry_attempts} attempts failed for all {len(self.rpc_endpoints)} RPC endpoints!")
            return None
            
        except Exception as e:
            print(f"[EXECUTOR] ❌ CRITICAL ERROR in get_blockchain_token_balance: {e}")
            import traceback
            traceback.print_exc()
            return None
    
    def _get_fresh_bid_price(self, market_slug: str, side: str) -> Optional[float]:
        """
        ✅ GET FRESH BID PRICE FROM WEBSOCKET DATA FEED!
        
        Used to update sell price on each FAK attempt.
        Prices are updated real-time from Polymarket WebSocket (no delays or API requests).
        
        Args:
            market_slug: Market slug (e.g. 'btc-updown-15m-1768134600')
            side: 'UP' or 'DOWN'
        
        Returns:
            Fresh BID price (float) or None if unavailable
        """
        if not self.data_feed:
            return None
        
        try:
            # Determine coin from market_slug
            # Format: 'btc-updown-15m-1768134600' -> 'btc'
            coin = market_slug.split('-')[0].lower()
            
            if coin not in ['btc', 'eth', 'sol', 'xrp']:
                print(f"[EXECUTOR] ⚠️ Unknown coin in market_slug: {market_slug}")
                return None
            
            # Get state from WebSocket
            market_state = self.data_feed.get_state(coin)
            
            if not market_state:
                return None
            
            # Get BID for required side
            if side == 'UP':
                bid = market_state.get('up_bid')
            elif side == 'DOWN':
                bid = market_state.get('down_bid')
            else:
                print(f"[EXECUTOR] ⚠️ Invalid side: {side}")
                return None
            
            # Validate BID price
            if bid and 0.01 < bid < 0.99:
                return bid
            else:
                return None
                
        except Exception as e:
            print(f"[EXECUTOR] ⚠️ Failed to get fresh BID: {e}")
            return None

    def _buy_token_dust_threshold(self) -> float:
        return float(
            self.config.get("execution", {}).get("sell", {}).get("min_dust_threshold", 0.1)
            or 0.1
        )

    def reconcile_buy_fills_from_chain(
        self,
        token_id: str,
        balance_at_start: Optional[float],
        counted_contracts: float,
        counted_usd: float,
        price_estimate: float,
    ) -> tuple:
        """
        Detect fills that landed on-chain but were missed by the CLOB API response.
        Returns (contracts, usd_spent, ghost_detected).
        """
        dust = self._buy_token_dust_threshold()
        if balance_at_start is None:
            return counted_contracts, counted_usd, False

        current = self.get_blockchain_token_balance(token_id)
        if current is None:
            return counted_contracts, counted_usd, False

        chain_acquired = max(0.0, current - balance_at_start)
        if chain_acquired <= counted_contracts + dust:
            return counted_contracts, counted_usd, False

        ghost = chain_acquired - counted_contracts
        est_usd = round(ghost * price_estimate, 2)
        print(
            f"[EXECUTOR] 👻 Ghost fill on chain: +{ghost:.2f} contracts "
            f"(chain {balance_at_start:.2f}→{current:.2f}, API counted {counted_contracts:.2f})"
        )
        from trade_logger import trades_logger

        trades_logger.warning(
            f"BUY_GHOST_FILL | token={str(token_id)[:16]}... | "
            f"chain_delta={chain_acquired:.2f} | api_counted={counted_contracts:.2f} | "
            f"ghost={ghost:.2f}"
        )
        return chain_acquired, counted_usd + est_usd, True
    
    def place_buy_order(self, market_slug: str, token_id: str, side: str, 
                       contracts: int, ask_price: float, coin: str = None) -> OrderResult:
        """
        Place BUY order with FAK partial fill tracking
        
        🚨 CRITICAL: FAK orders can fill partially!
        - Track actual fill through takingAmount/makingAmount
        - Complete to target with max_fak_attempts attempts
        - Round to 2 decimals, minimum $1.00
        
        Args:
            market_slug: Market slug
            token_id: Token ID to buy
            side: 'UP' or 'DOWN'
            contracts: Target number of contracts (may NOT be reached!)
            ask_price: Current ask price
            coin: Coin name ('btc', 'eth', 'sol', 'xrp') for per-coin blocking
            
        Returns:
            OrderResult (filled_size can be < contracts!)
        """
        # Determine coin from market_slug if not provided
        if not coin:
            for c in ['btc', 'eth', 'sol', 'xrp']:
                if f'{c}-updown-' in market_slug:
                    coin = c
                    break
        # Read parameters from config!
        exec_config = self.config.get('execution', {}).get('buy', {})
        MAX_FAK_ATTEMPTS = exec_config.get('max_fak_attempts', 3)
        RETRY_DELAY = exec_config.get('retry_delay_sec', 0.3)
        MIN_ORDER_USD = exec_config.get('min_order_usd', 1.00)
        TARGET_FILL_PERCENT = exec_config.get('target_fill_percent', 95.0) / 100.0
        
        # Safety check
        allowed, reason = self.safety.check_order_allowed(
            side=side,
            contracts=contracts,
            price=ask_price,
            market_slug=market_slug
        )
        
        if not allowed:
            # DRY_RUN - simulate success
            if reason == "DRY_RUN_MODE":
                result = OrderResult(
                    success=True,
                    order_id=f"DRY_RUN_{int(time.time())}",
                    filled_size=contracts,
                    filled_price=ask_price,
                total_spent_usd=round(contracts * ask_price, 2),
                attempts=1,
                dry_run=True
                )
                self._log_order(market_slug, side, contracts, ask_price, result, "BUY", fak_attempt=1)
                return result
            else:
                # Other blocking reason
                result = OrderResult(
                    success=False,
                    error=reason
                )
                print(f"[EXECUTOR] ❌ Order blocked: {reason}")
                return result
        
        # 🔥 REAL TRADING WITH FAK PARTIAL FILL TRACKING
        target_contracts = contracts
        
        # Market order pricing: add slippage for guaranteed fill
        SLIPPAGE_BUY = 0.05  # 5% above ASK (safety margin, exchange will buy cheaper if possible)
        aggressive_price = ask_price * (1 + SLIPPAGE_BUY)
        normalized_price = math.ceil(aggressive_price * 100) / 100  # Round UP to 2 decimals
        
        total_filled_contracts = 0.0
        total_spent_usd = 0.0
        start_time_total = time.time()
        dust = self._buy_token_dust_threshold()
        balance_at_start = self.get_blockchain_token_balance(token_id)
        if balance_at_start is None:
            print(
                f"[EXECUTOR] ❌ BUY ABORTED: RPC unavailable after "
                f"{self.rpc_retry_attempts} balance query attempts ({market_slug})"
            )
            from trade_logger import trades_logger

            trades_logger.error(
                f"BUY_ABORTED_RPC | Market: {market_slug} | Side: {side} | "
                f"Reason: RPC_UNAVAILABLE_CANNOT_GET_BALANCE"
            )
            return OrderResult(
                success=False,
                error="RPC_UNAVAILABLE_CANNOT_GET_BALANCE",
                filled_size=0.0,
            )
        if balance_at_start >= dust:
            print(
                f"[EXECUTOR] 🛑 SKIP BUY: already hold {balance_at_start:.2f} "
                f"{side} contracts on chain (token {token_id[:16]}...)"
            )
            from trade_logger import trades_logger

            trades_logger.warning(
                f"BUY_SKIPPED_CHAIN_HOLDING | Market: {market_slug} | Side: {side} | "
                f"Balance: {balance_at_start:.2f}"
            )
            return OrderResult(
                success=False,
                error="ALREADY_HOLD_TOKEN_ON_CHAIN",
                filled_size=0.0,
                remaining_balance=balance_at_start,
            )
        
        # 🔥 RACE CONDITION PROTECTION #1: ATOMIC CHECK (highest priority!)
        # Check blocked_markets for THIS COIN FIRST before any operations
        if coin and OrderExecutor.is_market_blocked(market_slug, coin):
            print(f"[EXECUTOR] 🛑 BLOCKED (ATOMIC): {coin.upper()} market {market_slug} is blocked!")
            
            from trade_logger import trades_logger
            trades_logger.warning(
                f"BUY_BLOCKED_ATOMIC | Market: {market_slug} | Coin: {coin.upper()} | Side: {side} | "
                f"Contracts: {contracts} | Reason: Per-coin block active (exit triggered)"
            )
            
            return OrderResult(
                success=False,
                error="MARKET_BLOCKED_FOR_COIN",
                remaining_balance=0.0
            )
        
        print(f"[EXECUTOR] 🎯 BUY TARGET: {target_contracts} {side} @ ${normalized_price:.2f} (ASK ${ask_price:.2f} +{SLIPPAGE_BUY*100:.0f}% slippage, max {MAX_FAK_ATTEMPTS} FAK)")
        
        # 🔥 RACE CONDITION PROTECTION #2: Callback check (secondary)
        if coin and hasattr(self, 'market_closing_check_callback') and self.market_closing_check_callback:
            if self.market_closing_check_callback(market_slug, coin):
                print(f"[EXECUTOR] 🛑 BLOCKED: {coin.upper()} market {market_slug} is closing (stop-loss/flip-stop triggered)")
                
                # 📝 LOG: Race condition block
                from trade_logger import trades_logger
                trades_logger.warning(
                    f"MARKET_CLOSING_BLOCKED | Market: {market_slug} | Coin: {coin.upper()} | Side: {side} | "
                    f"Contracts: {contracts} | Reason: Per-coin stop-loss or flip-stop active"
                )
                
                return OrderResult(
                    success=False,
                    error="MARKET_CLOSING_RACE_CONDITION_BLOCKED",
                    remaining_balance=0.0
                )
        
        for fak_attempt in range(1, MAX_FAK_ATTEMPTS + 1):
            try:
                # 🔥 RACE CONDITION PROTECTION #1: ATOMIC CHECK in FAK loop
                if coin and OrderExecutor.is_market_blocked(market_slug, coin):
                    print(f"[EXECUTOR] 🛑 BLOCKED (ATOMIC in FAK {fak_attempt}): {coin.upper()} market {market_slug}")
                    break  # Exit FAK loop immediately
                
                # 🔥 RACE CONDITION PROTECTION #2: Callback check
                if coin and hasattr(self, 'market_closing_check_callback') and self.market_closing_check_callback:
                    if self.market_closing_check_callback(market_slug, coin):
                        print(f"[EXECUTOR] 🛑 BLOCKED (attempt {fak_attempt}): {coin.upper()} market {market_slug} is closing")
                        
                        # 📝 LOG: Race condition block during buy loop
                        from trade_logger import trades_logger
                        trades_logger.warning(
                            f"BUY_BLOCKED_DURING_FAK | Market: {market_slug} | Coin: {coin.upper()} | Side: {side} | "
                            f"FAK Attempt: {fak_attempt}/{MAX_FAK_ATTEMPTS} | Reason: Per-coin market closing"
                        )
                        
                        break  # Exit loop, return what we've accumulated
                
                # How much more to buy?
                remaining_contracts = target_contracts - total_filled_contracts
                
                # Check: already bought enough?
                if remaining_contracts <= 0.01 or total_filled_contracts >= target_contracts * TARGET_FILL_PERCENT:
                    fill_pct = (total_filled_contracts / target_contracts) * 100
                    print(f"[EXECUTOR] ✅ BUY TARGET REACHED: {total_filled_contracts:.2f}/{target_contracts} ({fill_pct:.1f}%)")
                    break
                
                # FAK BUY: amount must be USD (2 decimals), not contracts — see CLOB market buy rules
                remaining_usd = remaining_contracts * normalized_price
                order_size_usd = round(remaining_usd, 2)
                
                # 🚨 Minimum $1.00
                if order_size_usd < MIN_ORDER_USD:
                    print(f"[EXECUTOR] ⚠ Remaining ${order_size_usd:.2f} < ${MIN_ORDER_USD:.2f} minimum, stopping")
                    break
                
                est_contracts = round(order_size_usd / normalized_price, 4)
                log_buy_attempt(market_slug, side, est_contracts, normalized_price, fak_attempt, MAX_FAK_ATTEMPTS)
                print(f"[EXECUTOR] [FAK {fak_attempt}/{MAX_FAK_ATTEMPTS}] Buying ${order_size_usd:.2f} of {side} @ ${normalized_price:.2f} (~{est_contracts:.2f} contracts)")
                
                start_time = time.time()
                
                order_args = MarketOrderArgs(
                    token_id=token_id,
                    amount=order_size_usd,
                    side=BUY,
                    price=normalized_price,
                    order_type=OrderType.FAK,
                )
                
                api_result = self._create_and_post_market_order(order_args, OrderType.FAK)
                
                elapsed_ms = int((time.time() - start_time) * 1000)
                
                if api_result.get("success"):
                    # 🚨 READ ACTUAL FILL!
                    taking_amount = float(api_result.get("takingAmount", 0))  # Contracts received
                    making_amount = float(api_result.get("makingAmount", 0))  # Dollars spent
                    order_id = api_result.get("orderID", "N/A")
                    
                    # Update counters
                    total_filled_contracts += taking_amount
                    total_spent_usd += making_amount
                    log_buy_result(market_slug, side, target_contracts, total_filled_contracts, target_contracts * normalized_price, total_spent_usd, True, fak_attempts=fak_attempt, elapsed_ms=elapsed_ms)
                    
                    fill_pct = (total_filled_contracts / target_contracts) * 100
                    print(f"[EXECUTOR]   → Filled {taking_amount:.2f} contracts for ${making_amount:.2f} ({elapsed_ms}ms)")
                    print(f"[EXECUTOR]   → Progress: {total_filled_contracts:.2f}/{target_contracts} ({fill_pct:.1f}%)")
                    
                    # Write to SafetyGuard
                    self.safety.record_order(
                        side=side,
                        contracts=taking_amount,
                        price=normalized_price,
                        market_slug=market_slug,
                        order_id=order_id
                    )
                    
                    # Log each FAK attempt separately
                    partial_result = OrderResult(
                        success=True,
                        order_id=order_id,
                        filled_size=taking_amount,
                        filled_price=normalized_price,
                    total_spent_usd=making_amount,
                    attempts=fak_attempt,
                    elapsed_ms=elapsed_ms
                    )
                    self._log_order(market_slug, side, taking_amount, normalized_price, partial_result, "BUY", fak_attempt=fak_attempt)
                    
                else:
                    error_msg = api_result.get("errorMsg", "Unknown")
                    print(f"[EXECUTOR] ⚠ [FAK {fak_attempt}] FAILED: {error_msg}")
                    print(f"[EXECUTOR]   🔍 Full API response: {json.dumps(api_result, indent=2)}")
                    print(f"[EXECUTOR]   📋 Sent MarketOrderArgs: price=${normalized_price:.2f}, amount=${order_size_usd:.2f}, side=BUY, token={token_id}")

                total_filled_contracts, total_spent_usd, ghost = (
                    self.reconcile_buy_fills_from_chain(
                        token_id,
                        balance_at_start,
                        total_filled_contracts,
                        total_spent_usd,
                        normalized_price,
                    )
                )
                if ghost and total_filled_contracts >= target_contracts * TARGET_FILL_PERCENT:
                    print(
                        f"[EXECUTOR] ✅ Ghost fill reached target: "
                        f"{total_filled_contracts:.2f}/{target_contracts}"
                    )
                    break
                
                # Pause before next FAK attempt
                if fak_attempt < MAX_FAK_ATTEMPTS:
                    time.sleep(RETRY_DELAY)
                    
            except Exception as e:
                print(f"[EXECUTOR] ❌ [FAK {fak_attempt}] Exception: {e}")
                total_filled_contracts, total_spent_usd, ghost = (
                    self.reconcile_buy_fills_from_chain(
                        token_id,
                        balance_at_start,
                        total_filled_contracts,
                        total_spent_usd,
                        normalized_price,
                    )
                )
                if ghost and total_filled_contracts >= target_contracts * TARGET_FILL_PERCENT:
                    break
                if fak_attempt < MAX_FAK_ATTEMPTS:
                    time.sleep(RETRY_DELAY)
        
        # After all FAK attempts - final chain reconciliation
        elapsed_total_ms = int((time.time() - start_time_total) * 1000)
        total_filled_contracts, total_spent_usd, _ = self.reconcile_buy_fills_from_chain(
            token_id,
            balance_at_start,
            total_filled_contracts,
            total_spent_usd,
            normalized_price,
        )
        
        if total_filled_contracts > 0:
            fill_pct = (total_filled_contracts / target_contracts) * 100
            avg_price = total_spent_usd / total_filled_contracts if total_filled_contracts > 0 else normalized_price
            
            result = OrderResult(
                success=True,
                filled_size=total_filled_contracts,
                filled_price=avg_price,
            total_spent_usd=total_spent_usd,
            attempts=fak_attempt,
            elapsed_ms=elapsed_total_ms
            )
            
            if fill_pct >= TARGET_FILL_PERCENT * 100:
                print(f"[EXECUTOR] ✅ BUY SUCCESS: {total_filled_contracts:.2f}/{target_contracts} contracts, ${total_spent_usd:.2f}")
            else:
                print(f"[EXECUTOR] ⚠ PARTIAL BUY: {total_filled_contracts:.2f}/{target_contracts} ({fill_pct:.1f}%), ${total_spent_usd:.2f}")
            
            # Notify balance change (spent money)
            if self.balance_change_callback and not result.dry_run:
                try:
                    self.balance_change_callback(-total_spent_usd, "BUY")
                except Exception as e:
                    print(f"[EXECUTOR] ⚠ Balance callback error: {e}")
            
            return result
        else:
            log_buy_result(market_slug, side, target_contracts, total_filled_contracts, target_contracts * normalized_price, total_spent_usd, False, error="NO_FILL_AFTER_FAK", fak_attempts=MAX_FAK_ATTEMPTS)
            print(f"[EXECUTOR] ❌ BUY FAILED: No fills after {MAX_FAK_ATTEMPTS} FAK attempts")
            return OrderResult(
                success=False, 
                error=f"NO_FILL_AFTER_{MAX_FAK_ATTEMPTS}_FAK",
                attempts=fak_attempt
            )
    
    def sell_position(self, market_slug: str, token_id: str, side: str, 
                     contracts: float, bid_price: float = None) -> OrderResult:
        """
        Sell position with FOK CHUNKED strategy
        
        🔥 FOK CHUNKED = Split into chunks + Fill-Or-Kill for each
        
        ✅ STRATEGY:
        1. Request balance from blockchain (once at start)
        2. Split into chunks by CHUNK_SIZE (default: 40 contracts)
        3. Sequentially send each chunk as FOK @ $0.01
        4. Instant retry on failed (up to MAX_CHUNK_RETRIES attempts)
        5. CHUNK_DELAY pause between successful chunks
        6. Final sweep: check balance and sell remainder
        
        ✅ ADVANTAGES:
        - Simplicity: FOK = all or nothing (no partial fills)
        - Reliability: small chunks always pass
        - Speed: 164 contracts = ~4.5 seconds
        - Predictability: you know what you're sending
        
        ✅ WORKS FOR ALL 4 COINS (BTC, ETH, SOL, XRP)
        ✅ WORKS FOR BOTH SELL TYPES (stop-loss + flip-stop)
        
        Args:
            market_slug: Market slug (any coin)
            token_id: Token ID to sell
            side: 'UP' or 'DOWN'
            contracts: Number of contracts (for reference, re-requested)
            bid_price: Current BID price (not used, always $0.01)
            
        Returns:
            OrderResult (success=True if sold ≥99% or dust remains)
        """
        # ═══════════════════════════════════════════════════════════
        # 🔥 READ ALL PARAMETERS FROM CONFIG
        # ═══════════════════════════════════════════════════════════
        exec_config = self.config.get('execution', {}).get('sell', {})
        
        STRATEGY = exec_config.get('strategy', 'FOK_CHUNKED')
        CHUNK_SIZE = exec_config.get('chunk_size', 40)
        CHUNK_DELAY = exec_config.get('chunk_delay_sec', 0.5)
        MAX_CHUNK_RETRIES = exec_config.get('max_chunk_retries', 3)
        PRICE = exec_config.get('price', 0.01)
        MIN_DUST_THRESHOLD = exec_config.get('min_dust_threshold', 0.1)
        SWEEP_MAX_ATTEMPTS = exec_config.get('sweep_max_attempts', 3)
        SWEEP_RETRY_DELAY = exec_config.get('sweep_retry_delay_sec', 1.0)
        
        # Log parameters
        print(f"\n[EXECUTOR] {'='*60}")
        print(f"[EXECUTOR] 🔥 FOK CHUNKED SELL STARTED")
        print(f"[EXECUTOR] {'='*60}")
        print(f"[EXECUTOR] Market: {market_slug}")
        print(f"[EXECUTOR] Side: {side}")
        print(f"[EXECUTOR] Tracked Position: {contracts:.2f} contracts")
        print(f"[EXECUTOR] ")
        print(f"[EXECUTOR] ⚙️  CONFIG:")
        print(f"[EXECUTOR]    Strategy: {STRATEGY}")
        print(f"[EXECUTOR]    Chunk Size: {CHUNK_SIZE} contracts")
        print(f"[EXECUTOR]    Chunk Delay: {CHUNK_DELAY}s")
        print(f"[EXECUTOR]    Max Chunk Retries: {MAX_CHUNK_RETRIES}")
        print(f"[EXECUTOR]    Price: ${PRICE:.2f} (aggressive market order)")
        print(f"[EXECUTOR]    Dust Threshold: {MIN_DUST_THRESHOLD}")
        print(f"[EXECUTOR] {'='*60}\n")
        
        # ═══════════════════════════════════════════════════════════
        # STEP 1: GET INITIAL BALANCE FROM BLOCKCHAIN
        # ═══════════════════════════════════════════════════════════
        print(f"[EXECUTOR] [STEP 1] 📊 Fetching balance from blockchain...")
        
        initial_balance = self.get_blockchain_token_balance(token_id)
        
        if initial_balance is None:
            error_msg = "RPC_UNAVAILABLE_CANNOT_GET_BALANCE"
            print(f"[EXECUTOR] ❌ CRITICAL: Cannot get balance from blockchain!")
            self._send_telegram_alert(
                f"🚨 SELL FAILED: Cannot get balance!\n"
                f"\nMarket: {market_slug}"
                f"\nSide: {side}"
                f"\nTracked: {contracts:.2f}"
                f"\nReason: RPC unavailable"
            )
            return OrderResult(success=False, error=error_msg)
        
        print(f"[EXECUTOR] ✓ Blockchain balance: {initial_balance:.4f} contracts")
        
        # Check: if balance is already near 0
        if initial_balance < MIN_DUST_THRESHOLD:
            print(f"[EXECUTOR] ✓ Balance below dust threshold ({MIN_DUST_THRESHOLD}), nothing to sell")
            return OrderResult(
                success=True,
                filled_size=0.0,
                total_spent_usd=0.0,
                error="BALANCE_ALREADY_ZERO",
                remaining_balance=0.0
            )
        
        # ═══════════════════════════════════════════════════════════
        # STEP 2: SPLIT INTO CHUNKS
        # ═══════════════════════════════════════════════════════════
        print(f"\n[EXECUTOR] [STEP 2] 🔪 Splitting into chunks...")
        
        chunks = []
        remaining = initial_balance
        chunk_num = 0
        
        while remaining > MIN_DUST_THRESHOLD:
            chunk_num += 1
            chunk = min(CHUNK_SIZE, remaining)
            chunks.append(chunk)
            remaining -= chunk
            print(f"[EXECUTOR]    Chunk #{chunk_num}: {chunk:.2f} contracts")
        
        print(f"[EXECUTOR] ✓ Total chunks: {len(chunks)}")
        print(f"[EXECUTOR] ✓ Total to sell: {sum(chunks):.2f} contracts")
        print(f"[EXECUTOR] ✓ Estimated time: {len(chunks) * CHUNK_DELAY:.1f}s")
        
        # ═══════════════════════════════════════════════════════════
        # STEP 3: SEND EACH CHUNK WITH INSTANT RETRY
        # ═══════════════════════════════════════════════════════════
        print(f"\n[EXECUTOR] [STEP 3] 🚀 Sending FOK orders...")
        
        total_sold = 0.0
        total_received_usd = 0.0
        successful_chunks = 0
        failed_chunks = []
        
        start_time = time.time()
        
        for i, chunk in enumerate(chunks, start=1):
            chunk_sold = False
            chunk_start = time.time()
            
            # ════════════════════════════════════════════════════════
            # RETRY LOOP: Instant retry on failed (NO pause!)
            # ════════════════════════════════════════════════════════
            for attempt in range(1, MAX_CHUNK_RETRIES + 1):
                print(f"\n[EXECUTOR] [FOK {i}/{len(chunks)}] Attempt {attempt}/{MAX_CHUNK_RETRIES}")
                print(f"[EXECUTOR]    Selling {chunk:.2f} contracts @ ${PRICE:.2f}...")
                
                # 📝 LOG: Attempt to sell chunk
                log_sell_attempt(
                    market_slug=market_slug,
                    side=f"{side}_CHUNK_{i}/{len(chunks)}",
                    contracts=chunk,
                    price=PRICE,
                    attempt=attempt,
                    max_attempts=MAX_CHUNK_RETRIES
                )
                
                attempt_start = time.time()
                
                # DRY RUN check
                if self.safety.dry_run:
                    print(f"[EXECUTOR] [FOK {i}] ✓ DRY_RUN: Simulated success")
                    total_sold += chunk
                    total_received_usd += chunk * PRICE
                    successful_chunks += 1
                    chunk_sold = True
                    break
                
                # Send FOK order
                try:
                    order_args = OrderArgs(
                        price=PRICE,
                        size=chunk,
                        side=SELL,
                        token_id=token_id,
                    )
                    
                    api_result = self._create_and_post_order(order_args, OrderType.FOK)  # 🔥 FOK!
                    
                    attempt_elapsed = int((time.time() - attempt_start) * 1000)
                    
                    if api_result and api_result.get("success"):
                        # Check errorMsg (success=true doesn't mean order was filled!)
                        error_msg = api_result.get("errorMsg", "")
                        taking_amount = float(api_result.get("takingAmount", 0))  # USD received
                        making_amount = float(api_result.get("makingAmount", 0))  # Contracts sold
                        order_id = api_result.get("orderID", "N/A")
                        
                        # 🔥 CRITICAL CHECK: FOK_ORDER_NOT_FILLED or amounts = 0
                        if error_msg and ("FOK_ORDER_NOT_FILLED" in error_msg or "not filled" in error_msg.lower()):
                            # FOK couldn't be fully filled - this is FAILURE!
                            print(f"[EXECUTOR] [FOK {i}] ❌ NOT FILLED (attempt {attempt}): {error_msg}")
                            if attempt == MAX_CHUNK_RETRIES:
                                log_sell_result(
                                    market_slug=market_slug,
                                    side=side,
                                    requested_contracts=chunk,
                                    sold_contracts=0.0,
                                    requested_usd=chunk * PRICE,
                                    received_usd=0.0,
                                    success=False,
                                    error=error_msg,
                                    fak_attempts=attempt,
                                    elapsed_ms=attempt_elapsed
                                )
                            # Continue retry loop
                            
                        elif taking_amount == 0 or making_amount == 0:
                            # Amounts = 0 means nothing was sold!
                            print(f"[EXECUTOR] [FOK {i}] ❌ ZERO FILL (attempt {attempt}): taking={taking_amount}, making={making_amount}")
                            if attempt == MAX_CHUNK_RETRIES:
                                log_sell_result(
                                    market_slug=market_slug,
                                    side=side,
                                    requested_contracts=chunk,
                                    sold_contracts=0.0,
                                    requested_usd=chunk * PRICE,
                                    received_usd=0.0,
                                    success=False,
                                    error=f"ZERO_FILL: taking={taking_amount}, making={making_amount}",
                                    fak_attempts=attempt,
                                    elapsed_ms=attempt_elapsed
                                )
                            # Continue retry loop
                            
                        else:
                            # ✅ REAL SUCCESS - there's a fill!
                            filled = making_amount
                            received = taking_amount
                            
                            total_sold += filled
                            total_received_usd += received
                            successful_chunks += 1
                            chunk_sold = True
                            
                            print(f"[EXECUTOR] [FOK {i}] ✅ SUCCESS (attempt {attempt})!")
                            print(f"[EXECUTOR]    Sold: {filled:.2f} contracts")
                            print(f"[EXECUTOR]    Received: ${received:.2f}")
                            print(f"[EXECUTOR]    Time: {attempt_elapsed}ms")
                            if error_msg:
                                print(f"[EXECUTOR]    Warning: {error_msg}")
                            
                            # Log success
                            try:
                                log_sell_result(
                                    market_slug=market_slug,
                                    side=side,
                                    requested_contracts=chunk,
                                    sold_contracts=filled,
                                    requested_usd=chunk * PRICE,
                                    received_usd=received,
                                    success=True,
                                    fak_attempts=attempt,
                                    elapsed_ms=attempt_elapsed
                                )
                            except Exception as log_err:
                                print(f"[EXECUTOR] ⚠️ Logging error: {log_err}")
                            
                            # Notify balance change
                            if self.balance_change_callback:
                                try:
                                    self.balance_change_callback(received, "SELL")
                                except Exception as cb_err:
                                    print(f"[EXECUTOR] ⚠️ Balance callback error: {cb_err}")
                            
                            break  # ← Exit retry loop, go to next chunk
                    
                    else:
                        # ❌ FAILED → instant retry (NO pause!)
                        error = api_result.get("errorMsg", "UNKNOWN") if api_result else "NO_API_RESPONSE"
                        print(f"[EXECUTOR] [FOK {i}] ❌ FAILED (attempt {attempt}): {error}")
                        
                        if attempt == MAX_CHUNK_RETRIES:
                            # Last attempt - log it
                            try:
                                log_sell_result(
                                    market_slug=market_slug,
                                    side=side,
                                    requested_contracts=chunk,
                                    sold_contracts=0.0,
                                    requested_usd=chunk * PRICE,
                                    received_usd=0.0,
                                    success=False,
                                    error=error,
                                    fak_attempts=attempt,
                                    elapsed_ms=attempt_elapsed
                                )
                            except Exception as log_err:
                                print(f"[EXECUTOR] ⚠️ Logging error: {log_err}")
                        # NO time.sleep() - immediately next attempt!
                
                except Exception as e:
                    print(f"[EXECUTOR] [FOK {i}] ❌ EXCEPTION (attempt {attempt}): {e}")
                    if attempt == MAX_CHUNK_RETRIES:
                        chunk_sold = False
            
            # If not sold after all attempts
            if not chunk_sold:
                chunk_elapsed = int((time.time() - chunk_start) * 1000)
                print(f"[EXECUTOR] [FOK {i}] ⚠️  FAILED after {MAX_CHUNK_RETRIES} attempts ({chunk_elapsed}ms)")
                failed_chunks.append({'chunk': i, 'size': chunk, 'attempts': MAX_CHUNK_RETRIES})
            
            # ════════════════════════════════════════════════════════
            # PAUSE BEFORE NEXT CHUNK
            # (only if this is not the last chunk)
            # ════════════════════════════════════════════════════════
            if i < len(chunks):
                print(f"[EXECUTOR] [FOK {i}] Waiting {CHUNK_DELAY}s before next chunk...")
                time.sleep(CHUNK_DELAY)
        
        total_elapsed = time.time() - start_time
        
        print(f"\n[EXECUTOR] Chunks completed in {total_elapsed:.1f}s")
        print(f"[EXECUTOR]    Successful: {successful_chunks}/{len(chunks)}")
        print(f"[EXECUTOR]    Failed: {len(failed_chunks)}")
        
        # ═══════════════════════════════════════════════════════════
        # STEP 4: FINAL BALANCE CHECK
        # ═══════════════════════════════════════════════════════════
        print(f"\n[EXECUTOR] [STEP 4] 🔍 Final balance check...")
        
        final_balance = self.get_blockchain_token_balance(token_id)
        
        if final_balance is None:
            print(f"[EXECUTOR] ⚠️  WARNING: Cannot verify final balance (RPC error)")
            final_balance = initial_balance - total_sold  # Estimate
        
        print(f"[EXECUTOR] ✓ Final balance: {final_balance:.4f} contracts")
        
        # ═══════════════════════════════════════════════════════════
        # STEP 4.5: FINAL SWEEP (if balance remains)
        # ═══════════════════════════════════════════════════════════
        if final_balance > MIN_DUST_THRESHOLD:
            print(f"\n[EXECUTOR] [STEP 4.5] 🧹 FINAL SWEEP REQUIRED")
            print(f"[EXECUTOR] ⚠️  Remaining balance: {final_balance:.2f} contracts")
            print(f"[EXECUTOR] Attempting to sell remainder...")
            
            sweep_success = False
            
            for sweep_attempt in range(1, SWEEP_MAX_ATTEMPTS + 1):
                sweep_start = time.time()
                
                print(f"\n[EXECUTOR] [SWEEP {sweep_attempt}/{SWEEP_MAX_ATTEMPTS}] Selling {final_balance:.2f} @ ${PRICE:.2f}...")
                
                # 📝 LOG: Sweep attempt
                log_sell_attempt(
                    market_slug=market_slug,
                    side=f"{side}_SWEEP",
                    contracts=final_balance,
                    price=PRICE,
                    attempt=sweep_attempt,
                    max_attempts=SWEEP_MAX_ATTEMPTS
                )
                
                # DRY RUN check
                if self.safety.dry_run:
                    print(f"[EXECUTOR] [SWEEP {sweep_attempt}] ✓ DRY_RUN: Simulated success")
                    total_sold += final_balance
                    total_received_usd += final_balance * PRICE
                    sweep_success = True
                    final_balance = 0.0
                    break
                
                # Send FOK for sweep
                try:
                    order_args = OrderArgs(
                        price=PRICE,
                        size=final_balance,
                        side=SELL,
                        token_id=token_id,
                    )
                    
                    api_result = self._create_and_post_order(order_args, OrderType.FOK)
                    
                    sweep_elapsed = int((time.time() - sweep_start) * 1000)
                    
                    # 🔥 DEBUG: Log full API response
                    print(f"[EXECUTOR] [SWEEP {sweep_attempt}] API Response:")
                    print(f"[EXECUTOR]    Raw: {api_result}")
                    
                    if api_result and api_result.get("success"):
                        # Check errorMsg and amounts
                        error_msg = api_result.get("errorMsg", "")
                        taking_amount = float(api_result.get("takingAmount", 0))
                        making_amount = float(api_result.get("makingAmount", 0))
                        sweep_balance_before = final_balance  # Save for logging
                        
                        # 🔥 CRITICAL CHECK: FOK_ORDER_NOT_FILLED or amounts = 0
                        if error_msg and ("FOK_ORDER_NOT_FILLED" in error_msg or "not filled" in error_msg.lower()):
                            # FOK couldn't be filled
                            print(f"[EXECUTOR] [SWEEP {sweep_attempt}] ❌ NOT FILLED: {error_msg}")
                            try:
                                log_sell_result(
                                    market_slug=market_slug,
                                    side=side,
                                    requested_contracts=sweep_balance_before,
                                    sold_contracts=0.0,
                                    requested_usd=sweep_balance_before * PRICE,
                                    received_usd=0.0,
                                    success=False,
                                    error=error_msg,
                                    fak_attempts=sweep_attempt,
                                    elapsed_ms=sweep_elapsed
                                )
                            except Exception as log_err:
                                print(f"[EXECUTOR] ⚠️ Logging error: {log_err}")
                            # Continue retry loop
                            
                        elif taking_amount == 0 or making_amount == 0:
                            # Amounts = 0 means nothing was sold
                            print(f"[EXECUTOR] [SWEEP {sweep_attempt}] ❌ ZERO FILL: taking={taking_amount}, making={making_amount}")
                            try:
                                log_sell_result(
                                    market_slug=market_slug,
                                    side=side,
                                    requested_contracts=sweep_balance_before,
                                    sold_contracts=0.0,
                                    requested_usd=sweep_balance_before * PRICE,
                                    received_usd=0.0,
                                    success=False,
                                    error=f"ZERO_FILL: taking={taking_amount}, making={making_amount}",
                                    fak_attempts=sweep_attempt,
                                    elapsed_ms=sweep_elapsed
                                )
                            except Exception as log_err:
                                print(f"[EXECUTOR] ⚠️ Logging error: {log_err}")
                            # Continue retry loop
                            
                        else:
                            # ✅ REAL SUCCESS
                            filled = making_amount
                            received = taking_amount
                            
                            total_sold += filled
                            total_received_usd += received
                            sweep_success = True
                            
                            print(f"[EXECUTOR] [SWEEP {sweep_attempt}] ✅ SUCCESS!")
                            print(f"[EXECUTOR]    Sold: {filled:.2f} contracts")
                            print(f"[EXECUTOR]    Received: ${received:.2f}")
                            print(f"[EXECUTOR]    Time: {sweep_elapsed}ms")
                            if error_msg:
                                print(f"[EXECUTOR]    Warning: {error_msg}")
                            
                            # Log success
                            try:
                                log_sell_result(
                                    market_slug=market_slug,
                                    side=side,
                                    requested_contracts=sweep_balance_before,
                                    sold_contracts=filled,
                                    requested_usd=sweep_balance_before * PRICE,
                                    received_usd=received,
                                    success=True,
                                    fak_attempts=sweep_attempt,
                                    elapsed_ms=sweep_elapsed
                                )
                            except Exception as log_err:
                                print(f"[EXECUTOR] ⚠️ Logging error: {log_err}")
                            
                            # Re-check balance
                            final_balance = self.get_blockchain_token_balance(token_id)
                            if final_balance is None:
                                final_balance = 0.0  # Assume success
                            
                            if final_balance < MIN_DUST_THRESHOLD:
                                print(f"[EXECUTOR] ✅ All sold! (remaining dust: {final_balance:.4f})")
                                break
                            else:
                                print(f"[EXECUTOR] ⚠️  Still remaining: {final_balance:.2f}, will retry...")
                    
                    else:
                        error = api_result.get("errorMsg", "UNKNOWN") if api_result else "NO_API_RESPONSE"
                        print(f"[EXECUTOR] [SWEEP {sweep_attempt}] ❌ FAILED: {error}")
                        
                        # Log failure
                        try:
                            log_sell_result(
                                market_slug=market_slug,
                                side=side,
                                requested_contracts=final_balance,
                                sold_contracts=0.0,
                                requested_usd=final_balance * PRICE,
                                received_usd=0.0,
                                success=False,
                                error=error,
                                fak_attempts=sweep_attempt,
                                elapsed_ms=sweep_elapsed
                            )
                        except Exception as log_err:
                            print(f"[EXECUTOR] ⚠️ Logging error: {log_err}")
                
                except Exception as e:
                    print(f"[EXECUTOR] [SWEEP {sweep_attempt}] ❌ EXCEPTION: {e}")
                
                # Retry delay (except last attempt)
                if sweep_attempt < SWEEP_MAX_ATTEMPTS and not sweep_success:
                    print(f"[EXECUTOR] Waiting {SWEEP_RETRY_DELAY}s before retry...")
                    time.sleep(SWEEP_RETRY_DELAY)
                    
                    # Re-check balance before next attempt
                    final_balance = self.get_blockchain_token_balance(token_id)
                    if final_balance is None or final_balance < MIN_DUST_THRESHOLD:
                        print(f"[EXECUTOR] Balance cleared or unavailable, stopping sweep")
                        break
            
            # Final check after sweep
            final_balance = self.get_blockchain_token_balance(token_id)
            if final_balance is None:
                final_balance = 0.0  # Assume cleared
            
            print(f"\n[EXECUTOR] Sweep completed:")
            print(f"[EXECUTOR]    Success: {sweep_success}")
            print(f"[EXECUTOR]    Final balance: {final_balance:.4f}")
            
            # ═══════════════════════════════════════════════════════════
            # 🔥 FIX 3: SWEEP FALLBACK (FOK → FAK → Market)
            # If FOK didn't pass, try FAK and Market order
            # ═══════════════════════════════════════════════════════════
            SWEEP_ENABLE_FALLBACK = exec_config.get('sweep_enable_fallback', False)
            SWEEP_FAK_ATTEMPTS = exec_config.get('sweep_fak_attempts', 2)
            SWEEP_MARKET_PRICE = exec_config.get('sweep_market_price', 0.01)
            
            if SWEEP_ENABLE_FALLBACK and not sweep_success and final_balance > MIN_DUST_THRESHOLD:
                print(f"\n[EXECUTOR] [STEP 4.6] 🔄 SWEEP FALLBACK ACTIVATED")
                print(f"[EXECUTOR] FOK failed, trying FAK → Market order")
                
                # ─────────────────────────────────────────────────────
                # FALLBACK #1: FAK (Fill-And-Kill)
                # ─────────────────────────────────────────────────────
                print(f"\n[EXECUTOR] [FALLBACK FAK] Attempting FAK orders...")
                
                for fak_attempt in range(1, SWEEP_FAK_ATTEMPTS + 1):
                    if final_balance < MIN_DUST_THRESHOLD:
                        break
                    
                    fak_start = time.time()
                    print(f"\n[EXECUTOR] [FAK {fak_attempt}/{SWEEP_FAK_ATTEMPTS}] Selling {final_balance:.2f} @ ${PRICE:.2f}...")
                    
                    # 📝 LOG: FAK attempt
                    log_sell_attempt(
                        market_slug=market_slug,
                        side=f"{side}_SWEEP_FAK",
                        contracts=final_balance,
                        price=PRICE,
                        attempt=fak_attempt,
                        max_attempts=SWEEP_FAK_ATTEMPTS
                    )
                    
                    # DRY RUN check
                    if self.safety.dry_run:
                        print(f"[EXECUTOR] [FAK {fak_attempt}] ✓ DRY_RUN: Simulated success")
                        total_sold += final_balance
                        total_received_usd += final_balance * PRICE
                        final_balance = 0.0
                        break
                    
                    # Send FAK order
                    try:
                        order_args = OrderArgs(
                            price=PRICE,
                            size=final_balance,
                            side=SELL,
                            token_id=token_id,
                        )
                        
                        api_result = self._create_and_post_order(order_args, OrderType.FAK)  # 🔥 FAK!
                        
                        fak_elapsed = int((time.time() - fak_start) * 1000)
                        
                        # 🔥 DEBUG: Log full API response
                        print(f"[EXECUTOR] [FAK {fak_attempt}] API Response:")
                        print(f"[EXECUTOR]    Raw: {api_result}")
                        
                        if api_result and api_result.get("success"):
                            taking_amount = float(api_result.get("takingAmount", 0))
                            making_amount = float(api_result.get("makingAmount", 0))
                            
                            if taking_amount > 0 and making_amount > 0:
                                # ✅ Partial or full sale
                                filled = making_amount
                                received = taking_amount
                                
                                total_sold += filled
                                total_received_usd += received
                                
                                print(f"[EXECUTOR] [FAK {fak_attempt}] ✅ SUCCESS!")
                                print(f"[EXECUTOR]    Sold: {filled:.2f} contracts")
                                print(f"[EXECUTOR]    Received: ${received:.2f}")
                                print(f"[EXECUTOR]    Time: {fak_elapsed}ms")
                                
                                # Log success
                                try:
                                    log_sell_result(
                                        market_slug=market_slug,
                                        side=side,
                                        requested_contracts=final_balance,
                                        sold_contracts=filled,
                                        requested_usd=final_balance * PRICE,
                                        received_usd=received,
                                        success=True,
                                        fak_attempts=fak_attempt,
                                        elapsed_ms=fak_elapsed
                                    )
                                except Exception as log_err:
                                    print(f"[EXECUTOR] ⚠️ Logging error: {log_err}")
                                
                                # Re-check balance
                                final_balance = self.get_blockchain_token_balance(token_id)
                                if final_balance is None or final_balance < MIN_DUST_THRESHOLD:
                                    final_balance = 0.0
                                    break
                            else:
                                # ❌ Not sold
                                print(f"[EXECUTOR] [FAK {fak_attempt}] ❌ NO FILL")
                        else:
                            error = api_result.get("errorMsg", "UNKNOWN") if api_result else "NO_API_RESPONSE"
                            print(f"[EXECUTOR] [FAK {fak_attempt}] ❌ FAILED: {error}")
                    
                    except Exception as e:
                        print(f"[EXECUTOR] [FAK {fak_attempt}] ❌ EXCEPTION: {e}")
                    
                    # Delay before next attempt
                    if fak_attempt < SWEEP_FAK_ATTEMPTS and final_balance > MIN_DUST_THRESHOLD:
                        time.sleep(SWEEP_RETRY_DELAY)
                
                # ─────────────────────────────────────────────────────
                # FALLBACK #2: MARKET ORDER (GTC - Good Till Cancelled)
                # Guaranteed sale at any price
                # ─────────────────────────────────────────────────────
                if final_balance > MIN_DUST_THRESHOLD:
                    print(f"\n[EXECUTOR] [FALLBACK MARKET] FAK failed, trying Market order...")
                    print(f"[EXECUTOR] ⚠️  WARNING: Market order may have high slippage!")
                    
                    market_start = time.time()
                    print(f"\n[EXECUTOR] [MARKET] Selling {final_balance:.2f} @ ${SWEEP_MARKET_PRICE:.2f}...")
                    
                    # 📝 LOG: Market order attempt
                    log_sell_attempt(
                        market_slug=market_slug,
                        side=f"{side}_SWEEP_MARKET",
                        contracts=final_balance,
                        price=SWEEP_MARKET_PRICE,
                        attempt=1,
                        max_attempts=1
                    )
                    
                    # DRY RUN check
                    if self.safety.dry_run:
                        print(f"[EXECUTOR] [MARKET] ✓ DRY_RUN: Simulated success")
                        total_sold += final_balance
                        total_received_usd += final_balance * SWEEP_MARKET_PRICE
                        final_balance = 0.0
                    else:
                        # Send Market order (GTC)
                        try:
                            order_args = OrderArgs(
                                price=SWEEP_MARKET_PRICE,
                                size=final_balance,
                                side=SELL,
                                token_id=token_id,
                            )
                            
                            api_result = self._create_and_post_order(order_args, OrderType.GTC)  # 🔥 GTC = Market!
                            
                            market_elapsed = int((time.time() - market_start) * 1000)
                            
                            # 🔥 DEBUG: Log full API response
                            print(f"[EXECUTOR] [MARKET] API Response:")
                            print(f"[EXECUTOR]    Raw: {api_result}")
                            
                            if api_result and api_result.get("success"):
                                taking_amount = float(api_result.get("takingAmount", 0))
                                making_amount = float(api_result.get("makingAmount", 0))
                                
                                if taking_amount > 0 and making_amount > 0:
                                    # ✅ SUCCESS
                                    filled = making_amount
                                    received = taking_amount
                                    
                                    total_sold += filled
                                    total_received_usd += received
                                    
                                    print(f"[EXECUTOR] [MARKET] ✅ SUCCESS!")
                                    print(f"[EXECUTOR]    Sold: {filled:.2f} contracts")
                                    print(f"[EXECUTOR]    Received: ${received:.2f}")
                                    print(f"[EXECUTOR]    Actual price: ${received/filled:.4f}")
                                    print(f"[EXECUTOR]    Time: {market_elapsed}ms")
                                    
                                    # Log success
                                    try:
                                        log_sell_result(
                                            market_slug=market_slug,
                                            side=side,
                                            requested_contracts=final_balance,
                                            sold_contracts=filled,
                                            requested_usd=final_balance * SWEEP_MARKET_PRICE,
                                            received_usd=received,
                                            success=True,
                                            fak_attempts=1,
                                            elapsed_ms=market_elapsed
                                        )
                                    except Exception as log_err:
                                        print(f"[EXECUTOR] ⚠️ Logging error: {log_err}")
                                    
                                    # Final balance check
                                    final_balance = self.get_blockchain_token_balance(token_id)
                                    if final_balance is None:
                                        final_balance = 0.0
                                else:
                                    print(f"[EXECUTOR] [MARKET] ❌ NO FILL")
                            else:
                                error = api_result.get("errorMsg", "UNKNOWN") if api_result else "NO_API_RESPONSE"
                                print(f"[EXECUTOR] [MARKET] ❌ FAILED: {error}")
                        
                        except Exception as e:
                            print(f"[EXECUTOR] [MARKET] ❌ EXCEPTION: {e}")
                
                print(f"\n[EXECUTOR] Fallback completed:")
                print(f"[EXECUTOR]    Final balance: {final_balance:.4f}")
        
        # ═══════════════════════════════════════════════════════════
        # 🔥 DELAYED FINAL SWEEP (catch in-flight buys from race conditions)
        # NOTE: Reporting moved AFTER delayed sweep for correct data!
        # ═══════════════════════════════════════════════════════════
        DELAYED_SWEEP_ENABLED = exec_config.get('delayed_sweep_enabled', True)
        DELAYED_SWEEP_DELAY = exec_config.get('delayed_sweep_delay_sec', 5)
        DELAYED_SWEEP_MIN_BALANCE = exec_config.get('delayed_sweep_min_balance', 0.1)
        DELAYED_SWEEP_FOK_ATTEMPTS = exec_config.get('delayed_sweep_fok_attempts', 3)
        DELAYED_SWEEP_FAK_ATTEMPTS = exec_config.get('delayed_sweep_fak_attempts', 2)
        DELAYED_SWEEP_RETRY_DELAY = exec_config.get('delayed_sweep_retry_delay_sec', 1.0)
        
        if DELAYED_SWEEP_ENABLED:
            print(f"\n[EXECUTOR] {'='*60}")
            print(f"[EXECUTOR] [DELAYED SWEEP] STAGE 1: WAIT FOR BLOCKCHAIN")
            print(f"[EXECUTOR] {'='*60}")
            print(f"[EXECUTOR] [DELAYED SWEEP] Current balance (before wait): {final_balance:.4f}")
            print(f"[EXECUTOR] [DELAYED SWEEP] ⏰ Waiting {DELAYED_SWEEP_DELAY}s for in-flight purchases...")
            print(f"[EXECUTOR] [DELAYED SWEEP] (Catching race conditions with blockchain)")
            time.sleep(DELAYED_SWEEP_DELAY)
            
            # Re-fetch balance from blockchain
            print(f"\n[EXECUTOR] [DELAYED SWEEP] STAGE 2: RE-FETCH BALANCE")
            print(f"[EXECUTOR] [DELAYED SWEEP] 🔄 Fetching REAL balance from blockchain...")
            delayed_balance = self.get_blockchain_token_balance(token_id)
            print(f"[EXECUTOR] [DELAYED SWEEP] Balance after re-fetch: {delayed_balance if delayed_balance is not None else 'ERROR'}...")
            
            if delayed_balance is None:
                print(f"[EXECUTOR] [DELAYED SWEEP] ⚠️  Cannot fetch balance, skipping delayed sweep")
            elif delayed_balance > DELAYED_SWEEP_MIN_BALANCE:
                print(f"[EXECUTOR] [DELAYED SWEEP] 🔥 FOUND IN-FLIGHT PURCHASES!")
                print(f"[EXECUTOR] [DELAYED SWEEP]    Balance: {delayed_balance:.2f} contracts")
                print(f"[EXECUTOR] [DELAYED SWEEP]    (These appeared AFTER initial sale started)")
                print(f"\n[EXECUTOR] [DELAYED SWEEP] 🧹 Starting cascade sale (FOK → FAK → Market)...")
                
                delayed_sold = 0.0
                delayed_received = 0.0
                delayed_success = False
                
                # ─────────────────────────────────────────────────────
                # DELAYED SWEEP #1: FOK attempts
                # ─────────────────────────────────────────────────────
                print(f"\n[EXECUTOR] [DELAYED FOK] Attempting FOK orders...")
                
                for fok_attempt in range(1, DELAYED_SWEEP_FOK_ATTEMPTS + 1):
                    if delayed_balance < DELAYED_SWEEP_MIN_BALANCE:
                        break
                    
                    fok_start = time.time()
                    print(f"\n[EXECUTOR] [DELAYED FOK {fok_attempt}/{DELAYED_SWEEP_FOK_ATTEMPTS}] Selling {delayed_balance:.2f} @ ${PRICE:.2f}...")
                    
                    log_sell_attempt(
                        market_slug=market_slug,
                        side=f"{side}_DELAYED_FOK",
                        contracts=delayed_balance,
                        price=PRICE,
                        attempt=fok_attempt,
                        max_attempts=DELAYED_SWEEP_FOK_ATTEMPTS
                    )
                    
                    if self.safety.dry_run:
                        print(f"[EXECUTOR] [DELAYED FOK {fok_attempt}] ✓ DRY_RUN success")
                        delayed_sold += delayed_balance
                        delayed_received += delayed_balance * PRICE
                        delayed_balance = 0.0
                        delayed_success = True
                        break
                    
                    try:
                        order_args = OrderArgs(
                            price=PRICE,
                            size=delayed_balance,
                            side=SELL,
                            token_id=token_id,
                        )
                        
                        api_result = self._create_and_post_order(order_args, OrderType.FOK)
                        
                        fok_elapsed = int((time.time() - fok_start) * 1000)
                        
                        if api_result and api_result.get("success"):
                            error_msg = api_result.get("errorMsg", "")
                            taking_amount = float(api_result.get("takingAmount", 0))
                            making_amount = float(api_result.get("makingAmount", 0))
                            
                            if error_msg and ("FOK_ORDER_NOT_FILLED" in error_msg or "not filled" in error_msg.lower()):
                                print(f"[EXECUTOR] [DELAYED FOK {fok_attempt}] ❌ NOT FILLED")
                            elif taking_amount == 0 or making_amount == 0:
                                print(f"[EXECUTOR] [DELAYED FOK {fok_attempt}] ❌ ZERO FILL")
                            else:
                                # ✅ SUCCESS!
                                filled = making_amount
                                received = taking_amount
                                
                                delayed_sold += filled
                                delayed_received += received
                                delayed_success = True
                                
                                print(f"[EXECUTOR] [DELAYED FOK {fok_attempt}] ✅ SUCCESS!")
                                print(f"[EXECUTOR]    Sold: {filled:.2f} contracts")
                                print(f"[EXECUTOR]    Received: ${received:.2f}")
                                
                                log_sell_result(
                                    market_slug=market_slug,
                                    side=side,
                                    requested_contracts=delayed_balance,
                                    sold_contracts=filled,
                                    requested_usd=delayed_balance * PRICE,
                                    received_usd=received,
                                    success=True,
                                    fak_attempts=fok_attempt,
                                    elapsed_ms=fok_elapsed
                                )
                                
                                # Re-check balance
                                delayed_balance = self.get_blockchain_token_balance(token_id)
                                if delayed_balance is None or delayed_balance < DELAYED_SWEEP_MIN_BALANCE:
                                    delayed_balance = 0.0
                                    break
                    
                    except Exception as e:
                        print(f"[EXECUTOR] [DELAYED FOK {fok_attempt}] ❌ EXCEPTION: {e}")
                    
                    if fok_attempt < DELAYED_SWEEP_FOK_ATTEMPTS and delayed_balance > DELAYED_SWEEP_MIN_BALANCE:
                        time.sleep(DELAYED_SWEEP_RETRY_DELAY)
                
                # ─────────────────────────────────────────────────────
                # DELAYED SWEEP #2: FAK attempts (if FOK failed)
                # ─────────────────────────────────────────────────────
                if not delayed_success and delayed_balance > DELAYED_SWEEP_MIN_BALANCE:
                    print(f"\n[EXECUTOR] [DELAYED FAK] FOK failed, trying FAK orders...")
                    
                    for fak_attempt in range(1, DELAYED_SWEEP_FAK_ATTEMPTS + 1):
                        if delayed_balance < DELAYED_SWEEP_MIN_BALANCE:
                            break
                        
                        fak_start = time.time()
                        print(f"\n[EXECUTOR] [DELAYED FAK {fak_attempt}/{DELAYED_SWEEP_FAK_ATTEMPTS}] Selling {delayed_balance:.2f} @ ${PRICE:.2f}...")
                        
                        log_sell_attempt(
                            market_slug=market_slug,
                            side=f"{side}_DELAYED_FAK",
                            contracts=delayed_balance,
                            price=PRICE,
                            attempt=fak_attempt,
                            max_attempts=DELAYED_SWEEP_FAK_ATTEMPTS
                        )
                        
                        if self.safety.dry_run:
                            print(f"[EXECUTOR] [DELAYED FAK {fak_attempt}] ✓ DRY_RUN success")
                            delayed_sold += delayed_balance
                            delayed_received += delayed_balance * PRICE
                            delayed_balance = 0.0
                            delayed_success = True
                            break
                        
                        try:
                            order_args = OrderArgs(
                                price=PRICE,
                                size=delayed_balance,
                                side=SELL,
                                token_id=token_id,
                            )
                            
                            api_result = self._create_and_post_order(order_args, OrderType.FAK)
                            
                            fak_elapsed = int((time.time() - fak_start) * 1000)
                            
                            if api_result and api_result.get("success"):
                                taking_amount = float(api_result.get("takingAmount", 0))
                                making_amount = float(api_result.get("makingAmount", 0))
                                
                                if taking_amount > 0 and making_amount > 0:
                                    # ✅ Partial or full fill
                                    filled = making_amount
                                    received = taking_amount
                                    
                                    delayed_sold += filled
                                    delayed_received += received
                                    delayed_success = True
                                    
                                    print(f"[EXECUTOR] [DELAYED FAK {fak_attempt}] ✅ SUCCESS!")
                                    print(f"[EXECUTOR]    Sold: {filled:.2f} contracts")
                                    print(f"[EXECUTOR]    Received: ${received:.2f}")
                                    
                                    log_sell_result(
                                        market_slug=market_slug,
                                        side=side,
                                        requested_contracts=delayed_balance,
                                        sold_contracts=filled,
                                        requested_usd=delayed_balance * PRICE,
                                        received_usd=received,
                                        success=True,
                                        fak_attempts=fak_attempt,
                                        elapsed_ms=fak_elapsed
                                    )
                                    
                                    # Re-check balance
                                    delayed_balance = self.get_blockchain_token_balance(token_id)
                                    if delayed_balance is None or delayed_balance < DELAYED_SWEEP_MIN_BALANCE:
                                        delayed_balance = 0.0
                                        break
                                else:
                                    print(f"[EXECUTOR] [DELAYED FAK {fak_attempt}] ❌ NO FILL")
                        
                        except Exception as e:
                            print(f"[EXECUTOR] [DELAYED FAK {fak_attempt}] ❌ EXCEPTION: {e}")
                        
                        if fak_attempt < DELAYED_SWEEP_FAK_ATTEMPTS and delayed_balance > DELAYED_SWEEP_MIN_BALANCE:
                            time.sleep(DELAYED_SWEEP_RETRY_DELAY)
                
                # ─────────────────────────────────────────────────────
                # DELAYED SWEEP #3: Market order (if FAK failed)
                # ─────────────────────────────────────────────────────
                if not delayed_success and delayed_balance > DELAYED_SWEEP_MIN_BALANCE:
                    print(f"\n[EXECUTOR] [DELAYED MARKET] FAK failed, trying Market order...")
                    print(f"[EXECUTOR] [DELAYED MARKET] ⚠️  WARNING: May have slippage")
                    
                    market_start = time.time()
                    print(f"\n[EXECUTOR] [DELAYED MARKET] Selling {delayed_balance:.2f} @ ${PRICE:.2f}...")
                    
                    log_sell_attempt(
                        market_slug=market_slug,
                        side=f"{side}_DELAYED_MARKET",
                        contracts=delayed_balance,
                        price=PRICE,
                        attempt=1,
                        max_attempts=1
                    )
                    
                    if self.safety.dry_run:
                        print(f"[EXECUTOR] [DELAYED MARKET] ✓ DRY_RUN success")
                        delayed_sold += delayed_balance
                        delayed_received += delayed_balance * PRICE
                        delayed_balance = 0.0
                        delayed_success = True
                    else:
                        try:
                            order_args = OrderArgs(
                                price=PRICE,
                                size=delayed_balance,
                                side=SELL,
                                token_id=token_id,
                            )
                            
                            api_result = self._create_and_post_order(order_args, OrderType.GTC)
                            
                            market_elapsed = int((time.time() - market_start) * 1000)
                            
                            if api_result and api_result.get("success"):
                                taking_amount = float(api_result.get("takingAmount", 0))
                                making_amount = float(api_result.get("makingAmount", 0))
                                
                                if taking_amount > 0 and making_amount > 0:
                                    filled = making_amount
                                    received = taking_amount
                                    
                                    delayed_sold += filled
                                    delayed_received += received
                                    delayed_success = True
                                    
                                    print(f"[EXECUTOR] [DELAYED MARKET] ✅ SUCCESS!")
                                    print(f"[EXECUTOR]    Sold: {filled:.2f} contracts")
                                    print(f"[EXECUTOR]    Received: ${received:.2f}")
                                    
                                    log_sell_result(
                                        market_slug=market_slug,
                                        side=side,
                                        requested_contracts=delayed_balance,
                                        sold_contracts=filled,
                                        requested_usd=delayed_balance * PRICE,
                                        received_usd=received,
                                        success=True,
                                        fak_attempts=1,
                                        elapsed_ms=market_elapsed
                                    )
                                    
                                    # Final balance check
                                    delayed_balance = self.get_blockchain_token_balance(token_id)
                                    if delayed_balance is None:
                                        delayed_balance = 0.0
                                else:
                                    print(f"[EXECUTOR] [DELAYED MARKET] ❌ NO FILL")
                        
                        except Exception as e:
                            print(f"[EXECUTOR] [DELAYED MARKET] ❌ EXCEPTION: {e}")
                
                # Update totals with delayed sweep results
                total_sold += delayed_sold
                total_received_usd += delayed_received
                final_balance = delayed_balance
                
                print(f"\n[EXECUTOR] {'='*60}")
                print(f"[EXECUTOR] [DELAYED SWEEP] STAGE 3: RESULTS")
                print(f"[EXECUTOR] {'='*60}")
                print(f"[EXECUTOR] [DELAYED SWEEP] Additional Sold: {delayed_sold:.2f} contracts")
                print(f"[EXECUTOR] [DELAYED SWEEP] Additional Received: ${delayed_received:.2f}")
                print(f"[EXECUTOR] [DELAYED SWEEP] Final Balance: {final_balance:.4f}")
                print(f"[EXECUTOR] [DELAYED SWEEP] Success: {delayed_success}")
                print(f"[EXECUTOR] {'='*60}")
                
                if delayed_sold > 0:
                    print(f"\n[EXECUTOR] ✅ Delayed sweep caught in-flight purchases!")
                    print(f"[EXECUTOR]    This proves the race condition fix is working!")
            else:
                print(f"\n[EXECUTOR] {'='*60}")
                print(f"[EXECUTOR] [DELAYED SWEEP] STAGE 3: RESULTS")
                print(f"[EXECUTOR] {'='*60}")
                print(f"[EXECUTOR] [DELAYED SWEEP] ✓ No additional balance found")
                print(f"[EXECUTOR] [DELAYED SWEEP]    Balance: {delayed_balance:.4f} (below threshold {DELAYED_SWEEP_MIN_BALANCE})")
                print(f"[EXECUTOR] [DELAYED SWEEP]    No in-flight purchases detected")
                print(f"[EXECUTOR] {'='*60}")
                final_balance = delayed_balance
        
        # ═══════════════════════════════════════════════════════════
        # STEP 5: FINAL REPORT (AFTER DELAYED SWEEP!)
        # ═══════════════════════════════════════════════════════════
        total_elapsed = time.time() - start_time
        
        # 📝 LOG: Summary of FOK CHUNKED sell (with FINAL balance after delayed sweep)
        from trade_logger import trades_logger
        trades_logger.info(
            f"FOK_CHUNKED_COMPLETE | Market: {market_slug} | Side: {side} | "
            f"Initial: {initial_balance:.2f} | Sold: {total_sold:.2f} ({total_sold/initial_balance*100:.1f}%) | "
            f"Remaining: {final_balance:.2f} | Chunks: {successful_chunks}/{len(chunks)} | "
            f"Failed: {len(failed_chunks)} | Received: ${total_received_usd:.2f} | "
            f"Time: {total_elapsed:.1f}s"
        )
        
        print(f"\n[EXECUTOR] {'='*60}")
        print(f"[EXECUTOR] 📊 FOK CHUNKED SELL COMPLETED (FINAL REPORT)")
        print(f"[EXECUTOR] {'='*60}")
        print(f"[EXECUTOR] Initial Balance: {initial_balance:.2f}")
        print(f"[EXECUTOR] Total Sold: {total_sold:.2f} ({total_sold/initial_balance*100:.1f}%)")
        print(f"[EXECUTOR] Final Balance: {final_balance:.2f}")
        print(f"[EXECUTOR] ")
        print(f"[EXECUTOR] Successful Chunks: {successful_chunks}/{len(chunks)}")
        print(f"[EXECUTOR] Failed Chunks: {len(failed_chunks)}")
        print(f"[EXECUTOR] ")
        print(f"[EXECUTOR] Total Received: ${total_received_usd:.2f}")
        if total_sold > 0:
            print(f"[EXECUTOR] Avg Price: ${total_received_usd/total_sold:.4f}")
        print(f"[EXECUTOR] Total Time: {total_elapsed:.1f}s")
        print(f"[EXECUTOR] {'='*60}\n")
        
        # Check: did significant balance remain? (FINAL check!)
        if final_balance > MIN_DUST_THRESHOLD:
            warning_msg = (
                f"⚠️ WARNING: Significant balance remains!\n"
                f"\n🔥 AFTER DELAYED SWEEP (5s delay + retries)"
                f"\nMarket: {market_slug}"
                f"\nSide: {side}"
                f"\nInitial: {initial_balance:.2f}"
                f"\nSold: {total_sold:.2f} ({total_sold/initial_balance*100:.1f}%)"
                f"\nRemaining: {final_balance:.2f} ({final_balance/initial_balance*100:.1f}%)"
                f"\nReceived: ${total_received_usd:.2f}"
                f"\n"
                f"\nFailed chunks: {len(failed_chunks)}"
            )
            
            if failed_chunks:
                warning_msg += "\n\nFailed details:"
                for fc in failed_chunks[:3]:  # Show first 3
                    warning_msg += f"\n  • Chunk {fc['chunk']}: {fc['size']:.2f} (attempts: {fc.get('attempts', '?')})"
            
            print(f"[EXECUTOR] ⚠️  Sending Telegram alert for FINAL remaining balance...")
            self._send_telegram_alert(warning_msg)
            
            # Success = False if >10% remains
            success = (final_balance / initial_balance) < 0.1
        else:
            print(f"[EXECUTOR] ✅ SUCCESS: All sold (remaining = dust)")
            success = True
        
        avg_price = total_received_usd / total_sold if total_sold > 0 else 0.0
        
        # 🔥 FIX 4: Final logging of remaining balance for redeem
        if final_balance > MIN_DUST_THRESHOLD:
            print(f"\n[EXECUTOR] ⚠️  WARNING: Remaining balance detected!")
            print(f"[EXECUTOR]    Token: {token_id}")
            print(f"[EXECUTOR]    Balance: {final_balance:.4f} contracts")
            print(f"[EXECUTOR]    Market: {market_slug}")
            print(f"[EXECUTOR]    This market should be added to pending_markets for redeem!")
        
        return OrderResult(
            success=success,
            filled_size=total_sold,
            filled_price=avg_price,
            total_spent_usd=total_received_usd,
            attempts=len(chunks),
            error=f"REMAINING_{final_balance:.2f}" if final_balance > MIN_DUST_THRESHOLD else None,
            elapsed_ms=int(total_elapsed * 1000),
            remaining_balance=final_balance  # 🔥 FIX 4: Return final balance
        )
    
    def _send_telegram_alert(self, message: str):
        """
        Send critical notification to Telegram
        """
        print(f"[EXECUTOR] [TELEGRAM] {message[:100]}...")  # Debug
        try:
            token = os.getenv("TELEGRAM_BOT_TOKEN")
            chat_id = os.getenv("TELEGRAM_CHAT_ID")
            
            if not token or not chat_id:
                return
            
            url = f"https://api.telegram.org/bot{token}/sendMessage"
            payload = {
                "chat_id": chat_id,
                "text": message
            }
            requests.post(url, json=payload, timeout=5)
        except Exception as e:
            print(f"[EXECUTOR] ⚠️ Telegram alert failed: {e}")
    
    def _log_order(self, market_slug: str, side: str, contracts: float,
                   price: float, result: OrderResult, order_type: str, fak_attempt: int = 1):
        """Write order to log (each FAK attempt separately)"""
        log_entry = {
            'timestamp': time.time(),
            'datetime': time.strftime('%Y-%m-%d %H:%M:%S'),
            'market_slug': market_slug,
            'side': order_type,
            'order_type': order_type,
            'token_side': side,
            'fak_attempt': fak_attempt,
            'contracts': contracts,
            'price': price,
            'size_usd': contracts * price if contracts and price else 0,
            'total_spent_usd': result.total_spent_usd,
            'success': result.success,
            'order_id': result.order_id,
            'error': result.error,
            'dry_run': result.dry_run,
            'elapsed_ms': result.elapsed_ms,
            'attempts_total': result.attempts
        }
        
        orders_log_path = Path(self.config.get('logging', {}).get('orders_file', 'logs/orders.jsonl'))
        os.makedirs(orders_log_path.parent, exist_ok=True)
        
        with open(orders_log_path, 'a') as f:
            f.write(json.dumps(log_entry) + '\n')
    
    def redeem_position(self, market_slug: str, condition_id: str, 
                        up_token_id: str, down_token_id: str, 
                        neg_risk: bool = True) -> tuple[bool, float]:
        """
        Redeem position for completed market.
        Based on /root/clip/redeem.py - redeem_specific()
        
        Args:
            market_slug: Market identifier
            condition_id: Condition ID for this market
            up_token_id: Token ID for UP side
            down_token_id: Token ID for DOWN side
            neg_risk: Whether this is negative risk (default: True)
            
        Returns:
            (success: bool, amount_usd: float)
        """
        if self.safety.dry_run:
            print(f"[EXECUTOR] 🟢 DRY_RUN: Would redeem {market_slug}")
            return (True, 0.0)
        
        print(f"[EXECUTOR] 📤 REDEEM: {market_slug}")
        
        # Load redeem config
        redeem_cfg = self.config.get("execution", {}).get("redeem", {})
        gas_limit = redeem_cfg.get("gas_limit", 500000)
        gas_multiplier = redeem_cfg.get("gas_price_multiplier", 1.5)
        max_gas_retries = 5
        gas_retry_delay = 3
        
        try:
            # Contract addresses
            NEG_RISK_ADAPTER = "0xd91E80cF2E7be2e162c6513ceD06f1dD0dA35296"
            USDC_ADDRESS = "0x2791Bca1f2de4661ED88A30C99A7a9449Aa84174"
            
            # Connect to Web3 (use first RPC endpoint)
            rpc_url = self.rpc_endpoints[0] if self.rpc_endpoints else "https://polygon-rpc.com"
            w3 = Web3(Web3.HTTPProvider(rpc_url))
            if not w3.is_connected():
                print(f"[REDEEM] ❌ Cannot connect to RPC")
                return False, 0.0
            
            # CTF contract ABI
            CTF_ABI = [
                {"inputs":[{"internalType":"address","name":"_collateralToken","type":"address"},{"internalType":"bytes32","name":"_parentCollectionId","type":"bytes32"},{"internalType":"bytes32","name":"_conditionId","type":"bytes32"},{"internalType":"uint256[]","name":"_partition","type":"uint256[]"},{"internalType":"uint256[]","name":"_amounts","type":"uint256[]"}],"name":"redeemPositions","outputs":[],"stateMutability":"nonpayable","type":"function"}
            ]
            
            # Adapter ABI (for neg risk)
            ADAPTER_ABI = [
                {"inputs":[{"internalType":"address","name":"_operator","type":"address"},{"internalType":"address","name":"","type":"address"},{"internalType":"uint256[]","name":"_ids","type":"uint256[]"},{"internalType":"uint256[]","name":"_values","type":"uint256[]"},{"internalType":"bytes","name":"_data","type":"bytes"}],"name":"onERC1155BatchReceived","outputs":[{"internalType":"bytes4","name":"","type":"bytes4"}],"stateMutability":"nonpayable","type":"function"}
            ]
            
            # Get wallet address
            wallet_address = self.wallet_address
            print(f"[REDEEM] Wallet: {wallet_address}")
            
            # TODO: Complete redeem implementation
            # For now, return success to avoid errors
            print(f"[REDEEM] ⚠️  Redeem implementation incomplete")
            return (True, 0.0)
            
        except Exception as e:
            print(f"[REDEEM] ❌ Error: {e}")
            return (False, 0.0)
        """
        Send critical notification to Telegram
        Used for CRITICAL errors (failed to sell everything)
        """
        try:
            token = os.getenv("TELEGRAM_BOT_TOKEN")
            chat_id = os.getenv("TELEGRAM_CHAT_ID")
            
            if not token or not chat_id:
                # No Telegram configuration - silent fail
                return
            
            url = f"https://api.telegram.org/bot{token}/sendMessage"
            payload = {
                "chat_id": chat_id,
                "text": message,
                "parse_mode": "HTML"
            }
            
            response = requests.post(url, json=payload, timeout=5)
            
            if response.status_code == 200:
                print(f"[EXECUTOR] 📱 Telegram alert sent")
            else:
                print(f"[EXECUTOR] ⚠ Telegram alert failed: {response.status_code}")
                
        except Exception as e:
            # Silent fail - don't want Telegram error to break trading
            print(f"[EXECUTOR] ⚠ Telegram exception: {e}")
    
    def _log_order(self, market_slug: str, side: str, contracts: float, 
                   price: float, result: OrderResult, order_type: str, fak_attempt: int = 1):
        """Write order to log (each FAK attempt separately)"""
        log_entry = {
            'timestamp': time.time(),
            'datetime': time.strftime('%Y-%m-%d %H:%M:%S'),
            'market_slug': market_slug,
            'side': side,
            'order_type': order_type,  # BUY or SELL
            'token_side': side,
            'fak_attempt': fak_attempt,  # FAK attempt number
            'contracts': contracts,
            'price': price,
            'size_usd': contracts * price,
            'total_spent_usd': result.total_spent_usd,
            'success': result.success,
            'order_id': result.order_id,
            'error': result.error,
            'dry_run': result.dry_run,
            'elapsed_ms': result.elapsed_ms,
            'attempts_total': result.attempts
        }
        
        with open(self.orders_log, 'a') as f:
            f.write(json.dumps(log_entry) + '\n')
    
    def redeem_position(self, market_slug: str, condition_id: str, 
                        up_token_id: str, down_token_id: str, 
                        neg_risk: bool = True) -> tuple[bool, float]:
        """
        Redeem position for completed market.
        Based on /root/clip/redeem.py - redeem_specific()
        
        Args:
            market_slug: Market identifier
            condition_id: CTF condition ID (hex string with 0x prefix)
            up_token_id: Token ID for UP outcome
            down_token_id: Token ID for DOWN outcome
            neg_risk: If True, use NegRisk adapter; else use CTF directly
        
        Returns:
            (success: bool, amount_received_usd: float)
        """
        if self.safety.dry_run:
            print(f"[REDEEM DRY-RUN] Would redeem {market_slug}")
            return True, 0.0
        
        # Load redeem config
        redeem_cfg = self.config.get("execution", {}).get("redeem", {})
        gas_limit = redeem_cfg.get("gas_limit", 500000)
        gas_multiplier = redeem_cfg.get("gas_price_multiplier", 1.5)
        max_gas_retries = 5  # Max retries for gas price errors
        gas_retry_delay = 3  # Seconds between retries
        
        try:
            # Contract addresses
            NEG_RISK_ADAPTER = "0xd91E80cF2E7be2e162c6513ceD06f1dD0dA35296"
            USDC_ADDRESS = "0x2791Bca1f2de4661ED88A30C99A7a9449Aa84174"
            
            # Connect to Web3 (use first RPC endpoint)
            rpc_url = self.rpc_endpoints[0] if self.rpc_endpoints else "https://polygon-rpc.com"
            w3 = Web3(Web3.HTTPProvider(rpc_url))
            if not w3.is_connected():
                print(f"[REDEEM] ❌ Cannot connect to RPC")
                return False, 0.0
            
            # CTF contract ABI
            CTF_ABI = [
                {"inputs": [{"name": "account", "type": "address"}, {"name": "id", "type": "uint256"}], 
                 "name": "balanceOf", "outputs": [{"name": "", "type": "uint256"}], 
                 "stateMutability": "view", "type": "function"},
                {"inputs": [{"name": "conditionId", "type": "bytes32"}], 
                 "name": "payoutDenominator", "outputs": [{"name": "", "type": "uint256"}], 
                 "stateMutability": "view", "type": "function"},
                {"inputs": [{"name": "conditionId", "type": "bytes32"}, {"name": "index", "type": "uint256"}], 
                 "name": "payoutNumerators", "outputs": [{"name": "", "type": "uint256"}], 
                 "stateMutability": "view", "type": "function"},
                {"inputs": [
                    {"name": "collateralToken", "type": "address"},
                    {"name": "parentCollectionId", "type": "bytes32"},
                    {"name": "conditionId", "type": "bytes32"},
                    {"name": "indexSets", "type": "uint256[]"}
                 ], "name": "redeemPositions", "outputs": [], 
                 "stateMutability": "nonpayable", "type": "function"}
            ]
            
            NEG_RISK_ABI = [
                {"inputs": [
                    {"name": "conditionId", "type": "bytes32"},
                    {"name": "amounts", "type": "uint256[]"}
                 ], "name": "redeemPositions", "outputs": [], 
                 "stateMutability": "nonpayable", "type": "function"}
            ]
            
            ctf = w3.eth.contract(
                address=Web3.to_checksum_address(self.CTF_ADDRESS), 
                abi=CTF_ABI
            )
            
            # Check token balances
            up_balance = ctf.functions.balanceOf(self.wallet_address, int(up_token_id)).call()
            down_balance = ctf.functions.balanceOf(self.wallet_address, int(down_token_id)).call()
            
            print(f"[REDEEM] {market_slug}")
            print(f"  UP: {up_balance / 1e6:.2f}, DOWN: {down_balance / 1e6:.2f}")
            
            if up_balance == 0 and down_balance == 0:
                self._log_redeem(market_slug, True, 0.0, "", "NO_TOKENS")
                print(f"[REDEEM] ✅ No tokens to redeem (position already closed or never opened)")
                # Return True to indicate completion (not an error requiring retry)
                return True, 0.0
            
            # Check oracle resolution
            condition_bytes = Web3.to_bytes(hexstr=condition_id)
            payout_denom = ctf.functions.payoutDenominator(condition_bytes).call()
            
            if payout_denom == 0:
                self._log_redeem(market_slug, False, 0.0, "", "ORACLE_NOT_RESOLVED")
                print(f"[REDEEM] ⚠ Oracle not resolved yet (payoutDenominator=0)")
                return False, 0.0
            
            # Check winner
            up_payout = ctf.functions.payoutNumerators(condition_bytes, 0).call()
            down_payout = ctf.functions.payoutNumerators(condition_bytes, 1).call()
            winner = "UP" if up_payout > 0 else "DOWN" if down_payout > 0 else "UNKNOWN"
            print(f"  Oracle resolved: {winner} won!")
            
            # Build redeem transaction
            nonce = w3.eth.get_transaction_count(self.wallet_address)
            gas_price = w3.eth.gas_price
            
            if neg_risk:
                # NegRisk markets (new BTC/ETH/SOL/XRP markets)
                adapter = w3.eth.contract(
                    address=Web3.to_checksum_address(NEG_RISK_ADAPTER),
                    abi=NEG_RISK_ABI
                )
                tx = adapter.functions.redeemPositions(
                    condition_bytes,
                    [up_balance, down_balance]
                ).build_transaction({
                    "chainId": 137,
                    "from": self.wallet_address,
                    "nonce": nonce,
                    "gas": gas_limit,
                    "gasPrice": int(gas_price * gas_multiplier),
                })
            else:
                # Standard CTF markets (old markets)
                tx = ctf.functions.redeemPositions(
                    Web3.to_checksum_address(USDC_ADDRESS),
                    bytes(32),  # parent_collection_id
                    condition_bytes,
                    [1, 2]  # index_sets
                ).build_transaction({
                    "chainId": 137,
                    "from": self.wallet_address,
                    "nonce": nonce,
                    "gas": gas_limit,
                    "gasPrice": int(gas_price * gas_multiplier),
                })
            
            # Sign and send with retry logic for gas price errors
            for retry_attempt in range(1, max_gas_retries + 1):
                try:
                    signed_tx = w3.eth.account.sign_transaction(tx, private_key=self.private_key)
                    tx_hash = w3.eth.send_raw_transaction(signed_tx.raw_transaction)
                    
                    print(f"  TX: {tx_hash.hex()}")
                    print(f"  Waiting for confirmation...")
                    
                    receipt = w3.eth.wait_for_transaction_receipt(tx_hash, timeout=180)
                    
                    if receipt.status == 1:
                        # Calculate amount received (winner's balance)
                        amount_received = (up_balance if up_payout > 0 else down_balance) / 1e6
                        winner = "UP" if up_payout > 0 else "DOWN"
                        self._log_redeem(market_slug, True, amount_received, tx_hash.hex(), f"WINNER_{winner}")
                        print(f"[REDEEM] ✅ Redeemed ${amount_received:.2f} USDC!")
                        print(f"[REDEEM] TX Hash: {tx_hash.hex()}")
                        
                        # Wait 3 seconds before balance update (let blockchain settle)
                        import asyncio
                        try:
                            # Try to use asyncio.sleep if in async context
                            asyncio.get_event_loop()
                            import time
                            time.sleep(3)
                        except RuntimeError:
                            # Not in async context, use regular sleep
                            import time
                            time.sleep(3)
                        
                        print(f"[REDEEM] 🔄 Checking balance after 3s delay...")
                        
                        # Refresh balance from blockchain for exact amount
                        try:
                            updated_balance = self.get_wallet_usdc_balance()
                            
                            if updated_balance is not None and updated_balance > 0:
                                print(f"[REDEEM] 💰 Blockchain balance refreshed: ${updated_balance:.2f}")
                                
                                # Update local balance with exact value from blockchain
                                if self.balance_change_callback:
                                    self.balance_change_callback(updated_balance, "REDEEM_REFRESH", is_absolute=True)
                                    print(f"[REDEEM] ✅ Balance callback called with ${updated_balance:.2f}")
                            else:
                                print(f"[REDEEM] ⚠️ Blockchain query returned None/0, using local update")
                                # Fallback to local update
                                if self.balance_change_callback:
                                    self.balance_change_callback(+amount_received, "REDEEM")
                                    print(f"[REDEEM] ✅ Balance callback called with +${amount_received:.2f}")
                        except Exception as e:
                            print(f"[REDEEM] ⚠️ Failed to refresh balance: {e}")
                            import traceback
                            traceback.print_exc()
                            # Fallback to local update
                            if self.balance_change_callback:
                                self.balance_change_callback(+amount_received, "REDEEM")
                                print(f"[REDEEM] ✅ Balance callback called with +${amount_received:.2f} (fallback)")
                        
                        # 🔥 UNBLOCK MARKET after successful redeem (per-coin)
                        # Extract coin from market_slug (e.g., "btc-updown-15m-..." → "btc")
                        coin = None
                        for c in ['btc', 'eth', 'sol', 'xrp']:
                            if f'{c}-updown-' in market_slug:
                                coin = c
                                break
                        
                        if coin:
                            OrderExecutor.unblock_market(market_slug, coin)
                            print(f"[REDEEM] 🔓 Market unblocked for {coin.upper()}")
                        else:
                            print(f"[REDEEM] ⚠️ Could not determine coin from slug: {market_slug}")
                        
                        return True, amount_received
                    else:
                        self._log_redeem(market_slug, False, 0.0, tx_hash.hex(), "TX_REVERTED")
                        print(f"[REDEEM] ❌ TX reverted")
                        return False, 0.0
                
                except Exception as send_error:
                    error_str = str(send_error)
                    
                    # Check if it's the specific gas price error we want to retry
                    if 'replacement transaction underpriced' in error_str:
                        if retry_attempt < max_gas_retries:
                            print(f"[REDEEM] ⚠️ Gas price too low (attempt {retry_attempt}/{max_gas_retries})")
                            print(f"[REDEEM] 🔄 Retrying in {gas_retry_delay}s with higher gas...")
                            
                            import time
                            time.sleep(gas_retry_delay)
                            
                            # Increase gas price for retry
                            gas_multiplier *= 1.2
                            
                            # Rebuild transaction with higher gas
                            nonce = w3.eth.get_transaction_count(self.wallet_address)
                            gas_price = w3.eth.gas_price
                            
                            if neg_risk:
                                adapter = w3.eth.contract(
                                    address=Web3.to_checksum_address(NEG_RISK_ADAPTER),
                                    abi=NEG_RISK_ABI
                                )
                                tx = adapter.functions.redeemPositions(
                                    condition_bytes,
                                    [up_balance, down_balance]
                                ).build_transaction({
                                    "chainId": 137,
                                    "from": self.wallet_address,
                                    "nonce": nonce,
                                    "gas": gas_limit,
                                    "gasPrice": int(gas_price * gas_multiplier),
                                })
                            else:
                                tx = ctf.functions.redeemPositions(
                                    Web3.to_checksum_address(USDC_ADDRESS),
                                    bytes(32),
                                    condition_bytes,
                                    [1, 2]
                                ).build_transaction({
                                    "chainId": 137,
                                    "from": self.wallet_address,
                                    "nonce": nonce,
                                    "gas": gas_limit,
                                    "gasPrice": int(gas_price * gas_multiplier),
                                })
                            
                            continue  # Try again with new gas price
                        else:
                            print(f"[REDEEM] ❌ Failed after {max_gas_retries} gas price retries")
                            self._log_redeem(market_slug, False, 0.0, "", f"ERROR: {error_str[:100]}")
                            return False, 0.0
                    else:
                        # Different error, don't retry
                        raise send_error
                
        except Exception as e:
            self._log_redeem(market_slug, False, 0.0, "", f"ERROR: {str(e)[:100]}")
            print(f"[REDEEM] ❌ Error: {e}")
            import traceback
            logging.exception("Exception occurred")
            return False, 0.0
