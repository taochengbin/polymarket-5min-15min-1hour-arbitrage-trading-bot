(function () {
  const EM = "\u2014";

  const summaryEl = document.getElementById("summary-stats");
  const coinsEl = document.getElementById("coins-container");
  const tbody = document.querySelector("#recent-trades tbody");
  const badge = document.getElementById("conn-badge");
  const configEditor = document.getElementById("config-editor");
  const configMsg = document.getElementById("config-message");
  const headerSubtitle = document.getElementById("header-subtitle");

  function labelFromIntervalSec(sec) {
    if (sec == null || Number.isNaN(sec)) return null;
    if (sec === 300) return "5m";
    if (sec === 900) return "15m";
    return `${sec}s`;
  }

  function updateHeaderSubtitle(data) {
    if (!headerSubtitle) return;
    let ml = data.market_label;
    if (!ml && data.market_interval_sec != null) {
      ml = labelFromIntervalSec(data.market_interval_sec);
    }
    const part = ml ? `${ml} ` : "";
    headerSubtitle.textContent = `Polymarket ${part}desk \u00b7 live status \u00b7 settings \u00b7 analytics`;
  }

  function updateHeaderSubtitleFromConfig(cfg) {
    if (!headerSubtitle || !cfg || typeof cfg !== "object") return;
    const pm = cfg.data_sources && cfg.data_sources.polymarket;
    if (!pm) return;
    let ml = pm.market_window;
    if (!ml && pm.market_interval_sec != null) {
      ml = labelFromIntervalSec(pm.market_interval_sec);
    }
    if (ml) {
      headerSubtitle.textContent = `Polymarket ${ml} desk \u00b7 live status \u00b7 settings \u00b7 analytics`;
    }
  }

  function fmtTime(sec) {
    sec = Math.floor(sec || 0);
    const h = Math.floor(sec / 3600);
    const m = Math.floor((sec % 3600) / 60);
    const s = sec % 60;
    if (h > 0) return `${h}h ${m}m ${s}s`;
    return `${m}m ${s}s`;
  }

  function fmtUsd(n) {
    if (n == null || Number.isNaN(n)) return EM;
    const sign = n >= 0 ? "+" : "";
    return sign + "$" + Number(n).toFixed(2);
  }

  function fmtSpot(n) {
    if (n == null || Number.isNaN(n)) return EM;
    return Number(n).toLocaleString(undefined, { maximumFractionDigits: 2 });
  }

  function fmtEntryTime(t) {
    const et = t.entry_time;
    if (et != null && et > 0) {
      const d = new Date(et * 1000);
      const pad = (n) => String(n).padStart(2, "0");
      return (
        d.getFullYear() +
        "-" +
        pad(d.getMonth() + 1) +
        "-" +
        pad(d.getDate()) +
        " " +
        pad(d.getHours()) +
        ":" +
        pad(d.getMinutes()) +
        ":" +
        pad(d.getSeconds())
      );
    }
    if (t.entry_timestamp) return t.entry_timestamp;
    return EM;
  }

  function renderSummary(data) {
    const p = data.portfolio || {};
    const dry = data.dry_run;
    const wl = `${p.total_wins ?? 0}W / ${p.total_losses ?? 0}L`;
    summaryEl.innerHTML = [
      card("Uptime", fmtTime(data.uptime_sec)),
      card("Market", data.market_label || EM),
      card("Mode", dry ? "DRY RUN" : "LIVE", dry ? "warn" : "ok"),
      card("Wallet", data.wallet_balance != null ? "$" + data.wallet_balance.toFixed(2) : EM),
      card("Total PnL", fmtUsd(p.total_pnl), (p.total_pnl || 0) >= 0 ? "pos" : "neg"),
      card("Trades", String(p.total_trades ?? "0")),
      card("W/L \u00b7 WR", `${wl} \u00b7 ${p.win_rate ?? 0}%`),
      card("ROI %", p.portfolio_roi != null ? p.portfolio_roi.toFixed(2) + "%" : EM),
    ].join("");
  }

  function card(label, value, valClass) {
    const vc = valClass ? ` ${valClass}` : "";
    return `<div class="stat-card"><div class="label">${label}</div><div class="value${vc}">${escapeHtml(
      String(value)
    )}</div></div>`;
  }

  function escapeHtml(s) {
    const d = document.createElement("div");
    d.textContent = s;
    return d.innerHTML;
  }

  function renderCoins(data) {
    const coins = data.coins || {};
    const names = ["btc", "eth", "sol", "xrp"];
    coinsEl.innerHTML = names
      .map((c) => {
        const x = coins[c];
        if (!x) return "";
        const en = x.trading_enabled !== false;
        const fav = x.favorite || EM;
        const conf = x.confidence != null ? x.confidence.toFixed(3) : EM;
        const slugShort = (x.market_slug || "").split("-").pop() || EM;
        const st = x.stats || {};
        let posHtml = '<div class="row"><span>Position</span><strong>None</strong></div>';
        if (x.position) {
          const p = x.position;
          posHtml = `
          <div class="pos-block">
            <div class="row"><span>Unrealized</span><strong class="${p.unrealized_pnl >= 0 ? "pnl-pos" : "pnl-neg"}">${fmtUsd(
            p.unrealized_pnl
          )}</strong></div>
            <div class="row"><span>Invested</span><strong>$${p.total_invested}</strong></div>
            <div class="row"><span>Side / entries</span><strong>${p.our_side} \u00b7 ${p.entries_count}</strong></div>
            <div class="row"><span>If UP wins</span><strong>${fmtUsd(p.if_up_wins)}</strong></div>
            <div class="row"><span>If DOWN wins</span><strong>${fmtUsd(p.if_down_wins)}</strong></div>
          </div>`;
        }
        return `
        <div class="coin-card">
          <h3>${c.toUpperCase()}
            ${en ? "" : '<span class="disabled-tag">disabled</span>'}
          </h3>
          <div class="row"><span>Market</span><strong>${escapeHtml(slugShort)}</strong></div>
          <div class="row"><span>Time left</span><strong>${fmtTime(x.seconds_till_end)}</strong></div>
          <div class="row"><span>UP / DN ask</span><strong>${x.up_ask?.toFixed(3) ?? EM} / ${x.down_ask?.toFixed(3) ?? EM}</strong></div>
          <div class="row"><span>Favorite \u00b7 Conf</span><strong>${fav} \u00b7 ${conf}</strong></div>
          <div class="row"><span>PnL (coin)</span><strong class="${(st.pnl || 0) >= 0 ? "pnl-pos" : "pnl-neg"}">${fmtUsd(
            st.pnl
          )}</strong></div>
          <div class="row"><span>W/L \u00b7 WR</span><strong>${st.wins ?? 0}W / ${st.losses ?? 0}L \u00b7 ${st.win_rate ?? 0}%</strong></div>
          ${posHtml}
        </div>`;
      })
      .join("");
  }

  function tradeRowSignature(t) {
    const pnl = t.pnl_usd != null ? t.pnl_usd : t.pnl;
    let pnlKey = pnl;
    if (t.is_open === true && pnl != null) {
      pnlKey = Math.round(Number(pnl) * 10) / 10;
    }
    return [
      t.market_slug,
      t.is_open,
      t.bet_side,
      t.entry_label,
      t.spot_at_entry,
      t.entry_timestamp || t.entry_time,
      t.spot_start,
      t.spot_end,
      t.exit_label,
      t.bet_result_label,
      pnlKey,
    ].join("\t");
  }

  function tradesSignature(rows) {
    return (rows || []).map(tradeRowSignature).join("\n");
  }

  let lastTradesSig = "";
  let tradePage = 1;
  let tradePageSize = 20;
  let tradeDateFrom = "";
  let tradeDateTo = "";
  let tradeTotalPages = 1;
  let tradesFetchInFlight = false;

  const settleStatusEl = document.getElementById("settle-status");
  const tradesPagerInfo = document.getElementById("trades-pager-info");
  const tradeDateFromEl = document.getElementById("trade-date-from");
  const tradeDateToEl = document.getElementById("trade-date-to");
  const tradePageSizeEl = document.getElementById("trade-page-size");

  function entrySortKey(t) {
    const et = t.entry_time;
    if (et != null && et > 0) return et;
    if (t.entry_timestamp) {
      const ms = Date.parse(String(t.entry_timestamp).replace(" ", "T"));
      if (!Number.isNaN(ms)) return ms / 1000;
    }
    return t.close_time || 0;
  }

  function renderTradeRows(rows) {
    const sig = tradesSignature(rows);
    if (sig === lastTradesSig) {
      return;
    }
    lastTradesSig = sig;
    tbody.innerHTML = rows
      .map((t) => {
        const pnl = t.pnl_usd != null ? t.pnl_usd : t.pnl;
        const cls = (pnl ?? 0) >= 0 ? "pnl-pos" : "pnl-neg";
        const m = (t.market_slug || "").split("-").pop() || t.market_slug;
        const sym = t.spot_label || (t.coin || "").toUpperCase() || EM;
        const bet =
          (t.bet_side || EM) +
          (t.entry_label ? ` · ${t.entry_label}` : "");
        const isOpen = t.is_open === true;
        const lbl = t.bet_result_label;
        const settledLbl =
          lbl === "押中" || lbl === "未中" || lbl === "待结算";
        const betRes = settledLbl ? lbl : isOpen ? "持仓" : lbl || EM;
        const exitLbl =
          isOpen && (!t.exit_label || t.exit_label === EM)
            ? "持仓中"
            : t.exit_label || EM;
        const rowCls = isOpen ? "row-open" : cls;
        return `<tr class="${isOpen ? "row-open" : ""}">
        <td>${escapeHtml(t.strategy || "")}</td>
        <td>${escapeHtml(sym)}</td>
        <td>${escapeHtml(m || "")}</td>
        <td>${escapeHtml(bet)}</td>
        <td class="num">${escapeHtml(fmtSpot(t.spot_at_entry))}</td>
        <td class="num">${escapeHtml(fmtEntryTime(t))}</td>
        <td class="num">${escapeHtml(fmtSpot(t.spot_start))}</td>
        <td class="num">${escapeHtml(fmtSpot(t.spot_end))}</td>
        <td>${escapeHtml(betRes)}</td>
        <td>${escapeHtml(exitLbl)}</td>
        <td class="${rowCls}">${isOpen ? fmtUsd(pnl) + " (浮)" : fmtUsd(pnl)}</td>
      </tr>`;
      })
      .join("");
    if (!rows.length) {
      tbody.innerHTML = '<tr><td colspan="11">暂无交易记录（下单后会显示持仓行）</td></tr>';
    }
  }

  function updatePagerUi(meta) {
    if (!tradesPagerInfo) return;
    const total = meta.total ?? 0;
    const page = meta.page ?? 1;
    const tp = meta.total_pages ?? 1;
    tradeTotalPages = tp;
    tradesPagerInfo.textContent = `共 ${total} 条 · 第 ${page}/${tp} 页`;
    const prevBtn = document.getElementById("btn-trade-prev");
    const nextBtn = document.getElementById("btn-trade-next");
    if (prevBtn) prevBtn.disabled = page <= 1;
    if (nextBtn) nextBtn.disabled = page >= tp;
    if (settleStatusEl && meta.pending_settlement_count != null) {
      const pending = meta.pending_settlement_count;
      if (pending > 0) {
        settleStatusEl.textContent = `待结算 ${pending} 条`;
      } else if (!settleStatusEl.dataset.busy) {
        settleStatusEl.textContent = "";
      }
    }
  }

  async function fetchTrades() {
    if (tradesFetchInFlight) return null;
    tradesFetchInFlight = true;
    try {
      const q = new URLSearchParams({
        page: String(tradePage),
        page_size: String(tradePageSize),
      });
      if (tradeDateFrom) q.set("date_from", tradeDateFrom);
      if (tradeDateTo) q.set("date_to", tradeDateTo);
      const r = await fetch("/api/trades?" + q.toString(), { cache: "no-store" });
      if (!r.ok) throw new Error("trades " + r.status);
      const data = await r.json();
      const rows = [...(data.items || [])].sort(
        (a, b) => entrySortKey(b) - entrySortKey(a)
      );
      renderTradeRows(rows);
      updatePagerUi(data);
      return data;
    } finally {
      tradesFetchInFlight = false;
    }
  }

  async function fetchStatus() {
    const r = await fetch("/api/status", { cache: "no-store" });
    if (!r.ok) throw new Error("status " + r.status);
    return r.json();
  }

  let healthPoll = 0;

  async function tick() {
    try {
      const data = await fetchStatus();
      updateHeaderSubtitle(data);
      renderSummary(data);
      renderCoins(data);
      healthPoll += 1;
      if (healthPoll % 4 === 1) {
        await fetchTrades();
      }
      if (healthPoll % 8 === 0) {
        const health = await fetch("/api/health", { cache: "no-store" }).then((x) => x.json());
        if (health.bot_live) {
          badge.textContent = "live";
          badge.className = "badge badge-ok";
        } else {
          badge.textContent = "no live bot";
          badge.className = "badge badge-off";
        }
      }
    } catch (e) {
      badge.textContent = "disconnected";
      badge.className = "badge badge-warn";
      console.error("[WEB]", e);
    }
  }

  async function loadConfig() {
    configMsg.textContent = "";
    configMsg.className = "message";
    try {
      const r = await fetch("/api/config");
      const j = await r.json();
      if (j.error) throw new Error(j.error);
      configEditor.value = JSON.stringify(j, null, 2);
      updateHeaderSubtitleFromConfig(j);
      configMsg.textContent = "Loaded.";
      configMsg.className = "message ok";
    } catch (e) {
      configMsg.textContent = String(e.message || e);
      configMsg.className = "message err";
    }
  }

  document.getElementById("btn-refresh").addEventListener("click", () => {
    tick();
    fetchTrades();
  });

  document.getElementById("btn-trade-query").addEventListener("click", () => {
    tradeDateFrom = tradeDateFromEl?.value || "";
    tradeDateTo = tradeDateToEl?.value || "";
    tradePageSize = parseInt(tradePageSizeEl?.value || "20", 10) || 20;
    tradePage = 1;
    lastTradesSig = "";
    fetchTrades();
  });

  document.getElementById("btn-trade-prev").addEventListener("click", () => {
    if (tradePage > 1) {
      tradePage -= 1;
      lastTradesSig = "";
      fetchTrades();
    }
  });

  document.getElementById("btn-trade-next").addEventListener("click", () => {
    if (tradePage < tradeTotalPages) {
      tradePage += 1;
      lastTradesSig = "";
      fetchTrades();
    }
  });

  document.getElementById("btn-settle-pending").addEventListener("click", async () => {
    const btn = document.getElementById("btn-settle-pending");
    const limitEl = document.getElementById("settle-limit");
    const rawLimit = limitEl ? String(limitEl.value || "").trim() : "";
    let limit = null;
    if (rawLimit !== "") {
      limit = parseInt(rawLimit, 10);
      if (Number.isNaN(limit) || limit < 1) {
        alert("拉取条数请输入 1–100 的整数");
        return;
      }
      if (limit > 100) limit = 100;
    }
    const limitMsg =
      limit != null
        ? `最近 ${limit} 条待结算记录`
        : "全部待结算记录";
    if (!confirm(`从 Gamma/Chainlink 拉取${limitMsg}？可能耗时较长。`)) return;
    btn.disabled = true;
    if (settleStatusEl) {
      settleStatusEl.dataset.busy = "1";
      settleStatusEl.textContent = "拉取中…";
    }
    try {
      const body = limit != null ? { limit } : {};
      const r = await fetch("/api/trades/settle-pending", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(body),
      });
      const j = await r.json();
      if (!r.ok || !j.ok) throw new Error(j.error || "settle failed");
      if (settleStatusEl) {
        const total = j.pending_total ?? j.pending_count ?? 0;
        const done = j.pending_count ?? 0;
        const cap =
          j.limit != null && total > done
            ? `（待结算共 ${total} 条，本次处理最近 ${done} 条）`
            : `（共 ${done} 条）`;
        settleStatusEl.textContent = `完成：${j.settled_ok ?? 0} 成功 / ${j.settled_fail ?? 0} 失败 ${cap}`;
        delete settleStatusEl.dataset.busy;
      }
      lastTradesSig = "";
      tradePage = 1;
      await fetchTrades();
      tick();
    } catch (e) {
      if (settleStatusEl) {
        settleStatusEl.textContent = String(e.message || e);
        delete settleStatusEl.dataset.busy;
      }
    } finally {
      btn.disabled = false;
    }
  });
  document.getElementById("btn-load-config").addEventListener("click", loadConfig);
  document.getElementById("btn-save-config").addEventListener("click", async () => {
    configMsg.textContent = "";
    try {
      const parsed = JSON.parse(configEditor.value);
      const r = await fetch("/api/config", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(parsed),
      });
      const j = await r.json();
      if (!r.ok) throw new Error(j.error || "save failed");
      configMsg.textContent = j.message || "Saved.";
      configMsg.className = "message ok";
    } catch (e) {
      configMsg.textContent = String(e.message || e);
      configMsg.className = "message err";
    }
  });

  document.getElementById("btn-stop").addEventListener("click", async () => {
    if (!confirm("Request graceful stop? The bot will exit (same as Ctrl+C).")) return;
    try {
      const r = await fetch("/api/bot/stop", { method: "POST" });
      const j = await r.json();
      alert(j.message || "OK");
    } catch (e) {
      alert(e);
    }
  });

  loadConfig();
  tick();
  fetchTrades();
  setInterval(tick, 250);
})();
