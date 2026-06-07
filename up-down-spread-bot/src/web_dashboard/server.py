"""
Flask web dashboard: API + static UI.
Run inside the bot process (--web) or standalone (reads logs/bot_state.json).
"""
import json
import shutil
import threading
import time
from pathlib import Path
from typing import Any, Dict

from flask import Flask, jsonify, request

from market_config import apply_market_window_settings

# Project root: repository root (parent of /config, /src)
PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
CONFIG_PATH = PROJECT_ROOT / "config" / "config.json"
STATIC_DIR = Path(__file__).resolve().parent / "static"
TEMPLATE_DIR = Path(__file__).resolve().parent / "templates"


def create_app(project_root: Path | None = None) -> Flask:
    root = project_root or PROJECT_ROOT

    app = Flask(
        __name__,
        static_folder=str(STATIC_DIR),
        template_folder=str(TEMPLATE_DIR),
    )

    @app.route("/")
    def index():
        from flask import render_template

        return render_template("index.html")

    @app.route("/api/health")
    def health():
        import web_dashboard_state as wds

        snap = wds.get_snapshot()
        ts = snap.get("updated_at", 0)
        age = time.time() - ts if ts else 9999
        file_snap = wds.read_state_file(root)
        file_ts = file_snap.get("updated_at", 0) if file_snap else 0
        file_age = time.time() - file_ts if file_ts else 9999
        ctx = wds.get_bot_context()
        feed_ok = bool(ctx and ctx.get("data_feed"))
        bot_live = feed_ok or age < 15.0 or file_age < 15.0
        feed_status: Dict[str, Any] = {}
        if ctx and ctx.get("data_feed"):
            df = ctx["data_feed"]
            coins = list(ctx.get("coins") or [])
            if hasattr(df, "feed_connectivity_status"):
                feed_status = df.feed_connectivity_status(coins)
        return jsonify(
            {
                "ok": True,
                "bot_live": bot_live,
                "feed_direct": feed_ok,
                "snapshot_age_sec": round(min(age, file_age), 2),
                "feed_status": feed_status,
            }
        )

    @app.route("/api/status")
    def api_status():
        import web_dashboard_state as wds
        from trading_hours import dashboard_payload, load_from_config_path

        snap = wds.get_snapshot()
        file_snap = wds.read_state_file(root)
        snap_ts = float(snap.get("updated_at") or snap.get("snapshot_ts") or 0)
        snapshot_age_sec = round(time.time() - snap_ts, 2) if snap_ts else 9999.0
        live_ok = snap.get("status") == "running" and bool(snap.get("coins"))
        if not live_ok:
            if file_snap:
                snap = file_snap
            else:
                snap = dict(snap)
        else:
            snap = dict(snap)
            if not snap.get("recent_trades") and file_snap and file_snap.get(
                "recent_trades"
            ):
                snap["recent_trades"] = file_snap["recent_trades"]
        ctx = wds.get_bot_context()
        if ctx and ctx.get("data_feed"):
            from market_config import enabled_coins_from_config

            df = ctx["data_feed"]
            coins = list(ctx.get("coins") or [])
            if not coins and CONFIG_PATH.exists():
                try:
                    with open(CONFIG_PATH, "r", encoding="utf-8") as f:
                        coins = enabled_coins_from_config(json.load(f))
                except (OSError, json.JSONDecodeError):
                    coins = []
            # Read-only: clob_poll + WS write markets; we read via get_state.
            snap = wds.inject_live_from_data_feed(snap, df, coins)
            raw_age = snap.get("book_age_sec")
            book_age = 9999.0 if raw_age is None else float(raw_age)
            if snap.get("live_feed_ts"):
                snapshot_age_sec = book_age if book_age < 9000 else 0.0
                snap["feed_stale"] = book_age > 12.0
            else:
                snap["feed_stale"] = True
        else:
            snap["feed_stale"] = snapshot_age_sec > 8.0
        snap["snapshot_age_sec"] = snapshot_age_sec
        if "feed_stale" not in snap:
            snap["feed_stale"] = snapshot_age_sec > 8.0
        if CONFIG_PATH.exists():
            try:
                snap["trading_hours"] = dashboard_payload(
                    load_from_config_path(CONFIG_PATH)
                )
                from web_dashboard.snapshot_builder import apply_trading_status_to_coins

                snap["coins"] = apply_trading_status_to_coins(
                    snap.get("coins") or {}, CONFIG_PATH
                )
            except (OSError, json.JSONDecodeError, ValueError):
                pass
        return jsonify(snap)

    @app.route("/api/config", methods=["GET"])
    def get_config():
        if not CONFIG_PATH.exists():
            return jsonify({"error": "config.json not found"}), 404
        try:
            with open(CONFIG_PATH, "r", encoding="utf-8") as f:
                data = json.load(f)
            apply_market_window_settings(data)
            return jsonify(data)
        except (OSError, json.JSONDecodeError) as e:
            return jsonify({"error": str(e)}), 500

    @app.route("/api/config", methods=["POST"])
    def post_config():
        if not request.is_json:
            return jsonify({"error": "Expected JSON body"}), 400
        body = request.get_json()
        if not isinstance(body, dict):
            return jsonify({"error": "Invalid JSON"}), 400
        apply_market_window_settings(body)
        if not CONFIG_PATH.parent.is_dir():
            CONFIG_PATH.parent.mkdir(parents=True, exist_ok=True)
        backup = CONFIG_PATH.with_suffix(".json.bak")
        try:
            if CONFIG_PATH.exists():
                shutil.copy2(CONFIG_PATH, backup)
            with open(CONFIG_PATH, "w", encoding="utf-8") as f:
                json.dump(body, f, indent=2)
            return jsonify({"ok": True, "message": "Saved. Restart the bot to apply."})
        except OSError as e:
            return jsonify({"error": str(e)}), 500

    @app.route("/api/bot/stop", methods=["POST"])
    def bot_stop():
        import web_dashboard_state as wds

        wds.request_stop()
        return jsonify({"ok": True, "message": "Stop requested — bot will shut down gracefully."})

    @app.route("/api/trades")
    def api_trades():
        import web_dashboard_state as wds
        from web_dashboard.snapshot_builder import (
            filter_trades_by_entry_date,
            query_trades_from_db_paginated,
            query_trades_paginated,
        )
        from market_config import enabled_coins_from_config

        page = request.args.get("page", 1, type=int)
        page_size = request.args.get("page_size", 20, type=int)
        if page_size not in (10, 20, 50):
            page_size = 20
        date_from = (request.args.get("date_from") or "").strip() or None
        date_to = (request.args.get("date_to") or "").strip() or None

        ctx = wds.get_bot_context()
        if ctx and ctx.get("multi_trader"):
            payload = query_trades_paginated(
                multi_trader=ctx["multi_trader"],
                strategy_base=ctx["strategy_base"],
                coins=ctx["coins"],
                data_feed=ctx.get("data_feed"),
                market_windows=ctx.get("market_windows"),
                market_starts=ctx.get("market_starts"),
                page=page,
                page_size=page_size,
                date_from=date_from,
                date_to=date_to,
                read_trade_files=True,
            )
            return jsonify(payload)

        if CONFIG_PATH.exists():
            try:
                with open(CONFIG_PATH, "r", encoding="utf-8") as f:
                    cfg = json.load(f)
                apply_market_window_settings(cfg)
                coins = enabled_coins_from_config(cfg)
                db_payload = query_trades_from_db_paginated(
                    config=cfg,
                    strategy_base="late_v3",
                    coins=coins,
                    page=page,
                    page_size=page_size,
                    date_from=date_from,
                    date_to=date_to,
                )
                if db_payload:
                    return jsonify(db_payload)
            except (OSError, json.JSONDecodeError):
                pass

        snap = wds.get_snapshot()
        file_snap = wds.read_state_file(root)
        rows = list(snap.get("recent_trades") or [])
        if not rows and file_snap:
            rows = list(file_snap.get("recent_trades") or [])
        filtered = filter_trades_by_entry_date(rows, date_from, date_to)
        total = len(filtered)
        page = max(1, page)
        page_size = max(1, min(100, page_size))
        start = (page - 1) * page_size
        items = filtered[start : start + page_size]
        total_pages = max(1, (total + page_size - 1) // page_size)
        return jsonify(
            {
                "items": items,
                "total": total,
                "page": page,
                "page_size": page_size,
                "total_pages": total_pages,
                "pending_settlement_count": sum(
                    1
                    for t in rows
                    if t.get("is_open") or t.get("settlement_pending")
                ),
                "date_from": date_from or "",
                "date_to": date_to or "",
                "offline": True,
            }
        )

    @app.route("/api/trades/settle-pending", methods=["POST"])
    def api_settle_pending():
        import web_dashboard_state as wds
        from settlement_service import settle_all_pending

        ctx = wds.get_bot_context()
        if not ctx or not ctx.get("multi_trader"):
            return jsonify(
                {
                    "ok": False,
                    "error": "Bot not running with --web; start main.py --web first.",
                }
            ), 503

        body = request.get_json(silent=True) if request.is_json else {}
        if not isinstance(body, dict):
            body = {}
        limit_raw = body.get("limit")
        limit: int | None = None
        if limit_raw is not None and str(limit_raw).strip() != "":
            try:
                limit = int(limit_raw)
            except (TypeError, ValueError):
                return jsonify({"ok": False, "error": "limit must be a positive integer"}), 400
            if limit <= 0:
                return jsonify({"ok": False, "error": "limit must be >= 1"}), 400
            limit = min(limit, 100)

        try:
            result = settle_all_pending(
                multi_trader=ctx["multi_trader"],
                strategy_base=ctx["strategy_base"],
                coins=ctx["coins"],
                proxy_url=ctx.get("proxy_url"),
                lock_chainlink_window=ctx.get("lock_chainlink_window"),
                market_window_prices=ctx.get("market_windows"),
                delay_sec=0.0,
                limit=limit,
                strategies=ctx.get("strategies"),
                strategy_bases=ctx.get("strategy_bases"),
            )
            refresh = ctx.get("refresh_trades")
            if callable(refresh):
                refresh()
            return jsonify({"ok": True, **result})
        except Exception as exc:
            return jsonify({"ok": False, "error": str(exc)}), 500

    @app.route("/api/ask-profit")
    def api_ask_profit():
        from ask_profit_db import (
            DEFAULT_ASK_FROM,
            DEFAULT_ASK_TO,
            DEFAULT_ASK_STEP,
            DEFAULT_BET_AMOUNTS,
            parse_bet_amounts,
            query_ask_profit_grid,
        )

        ask_from = request.args.get("ask_from", DEFAULT_ASK_FROM, type=float)
        ask_to = request.args.get("ask_to", DEFAULT_ASK_TO, type=float)
        ask_step = request.args.get("ask_step", DEFAULT_ASK_STEP, type=float)
        amounts_raw = (request.args.get("amounts") or "").strip()
        custom_raw = (request.args.get("custom_usd") or "").strip()
        custom_usd = None
        if custom_raw:
            try:
                custom_usd = float(custom_raw)
            except (TypeError, ValueError):
                return jsonify({"ok": False, "error": "custom_usd 无效"}), 400

        if ask_from <= 0 or ask_to <= 0 or ask_step <= 0 or ask_from > ask_to:
            return jsonify({"ok": False, "error": "ask 范围参数无效"}), 400

        bet_amounts = parse_bet_amounts(
            amounts_raw or ",".join(str(x) for x in DEFAULT_BET_AMOUNTS),
            extra=custom_usd,
        )

        cfg = None
        if CONFIG_PATH.exists():
            try:
                with open(CONFIG_PATH, "r", encoding="utf-8") as f:
                    cfg = json.load(f)
            except (OSError, json.JSONDecodeError):
                cfg = None

        payload, err = query_ask_profit_grid(
            cfg,
            ask_from=ask_from,
            ask_to=ask_to,
            ask_step=ask_step,
            bet_amounts=bet_amounts,
        )
        if err:
            return jsonify({"ok": False, "error": err}), 503
        return jsonify({"ok": True, **payload})

    return app


def run_server_thread(
    host: str, port: int, project_root: Path | None = None
) -> None:
    """Start Flask in a daemon thread (used by main.py --web)."""
    app = create_app(project_root or PROJECT_ROOT)

    def run():
        # Werkzeug production warning suppressed for local dashboard
        import logging

        log = logging.getLogger("werkzeug")
        log.setLevel(logging.ERROR)
        app.run(host=host, port=port, threaded=True, use_reloader=False)

    t = threading.Thread(target=run, name="WebDashboard", daemon=True)
    t.start()


if __name__ == "__main__":
    # Standalone: UI only (status from bot_state.json when bot runs with --web)
    import logging

    logging.getLogger("werkzeug").setLevel(logging.ERROR)
    app = create_app()
    print(f"[WEB] Open http://127.0.0.1:5050 (dashboard)")
    app.run(host="127.0.0.1", port=5050, threaded=True)
