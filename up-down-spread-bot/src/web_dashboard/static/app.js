(function () {
  const EM = "\u2014";

  const summaryEl = document.getElementById("summary-stats");
  const coinsEl = document.getElementById("coins-container");
  const tradingHoursEl = document.getElementById("trading-hours-panel");
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

  function fmtUsdPlain(n) {
    if (n == null || Number.isNaN(n)) return EM;
    return "$" + Number(n).toFixed(2);
  }

  function fmtAsk(n) {
    if (n == null || Number.isNaN(n)) return EM;
    const v = Number(n);
    if (v <= 0) return EM;
    return v.toFixed(3);
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

  function renderTradingHours(data) {
    if (!tradingHoursEl) return;
    const th = data.trading_hours || {};
    if (!th.enabled) {
      tradingHoursEl.innerHTML = `
        <div class="trading-hours-head"><span class="label-muted">全天</span></div>
        <div class="trading-hours-status active">未限制（24h）</div>`;
      return;
    }
    const ranges = Array.isArray(th.ranges) ? th.ranges.filter(Boolean) : [];
    const rangeHtml = ranges.length
      ? ranges
          .map((r) => `<div class="range-line">${escapeHtml(String(r))}</div>`)
          .join("")
      : `<div class="range-line">${escapeHtml(th.summary || EM)}</div>`;
    const statusCls = th.allowed_now ? "active" : "paused";
    const statusTxt = th.allowed_now ? "当前可交易" : "当前暂停";
    const reason = (th.status_reason || "").trim();
    const localT = (th.local_time || "").trim();
    tradingHoursEl.innerHTML = `
      <div class="trading-hours-ranges">${rangeHtml}</div>
      <div class="trading-hours-status ${statusCls}">${escapeHtml(statusTxt)}</div>
      ${
        reason
          ? `<div class="trading-hours-hint">${escapeHtml(reason)}</div>`
          : ""
      }
      ${
        localT
          ? `<div class="trading-hours-hint label-muted">本机时间 ${escapeHtml(localT)}</div>`
          : ""
      }`;
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
        const tradeHint = (x.trading_reason || "").trim();
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
            ${en ? "" : '<span class="disabled-tag">暂停</span>'}
          </h3>
          ${
            tradeHint
              ? `<div class="row trading-hint"><span>交易状态</span><strong>${escapeHtml(tradeHint)}</strong></div>`
              : ""
          }
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
      t.entry_ask,
      t.size_usd,
      t.entry_label,
      t.spot_at_entry,
      t.window_range_high,
      t.window_range_low,
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

  function marketGroupKey(t) {
    return `${t.strategy || ""}\t${t.market_slug || ""}`;
  }

  function tradeCommonCells(t) {
    const m = (t.market_slug || "").split("-").pop() || t.market_slug;
    const sym = t.spot_label || (t.coin || "").toUpperCase() || EM;
    return {
      strategy: t.strategy || "",
      sym,
      market: m || "",
      rangeHigh: fmtSpot(t.window_range_high),
      rangeLow: fmtSpot(t.window_range_low),
    };
  }

  function renderTradeLegDetailCells(t) {
    const pnl = t.pnl_usd != null ? t.pnl_usd : t.pnl;
    const cls = (pnl ?? 0) >= 0 ? "pnl-pos" : "pnl-neg";
    const bet =
      (t.bet_side || EM) + (t.entry_label ? ` · ${t.entry_label}` : "");
    const entryAsk =
      t.entry_ask != null
        ? t.entry_ask
        : t.token_ask != null
          ? t.token_ask
          : null;
    const askMax =
      t.first_leg_ask_max != null ? t.first_leg_ask_max : null;
    const er = (t.entry_reason || "normal").trim();
    const secondTrigger =
      er === "second_entry" || er === "flip_reverse"
        ? EM
        : t.second_entry_would_trigger_ask === true
          ? "是"
          : t.second_entry_would_trigger_ask === false
            ? "否"
            : askMax != null
              ? "否"
              : EM;
    const orderUsd = t.size_usd != null ? t.size_usd : t.total_cost;
    const isOpen = t.is_open === true;
    const lbl = t.bet_result_label;
    const settledLbl = lbl === "押中" || lbl === "未中" || lbl === "待结算";
    const betRes = settledLbl ? lbl : isOpen ? "持仓" : lbl || EM;
    const exitLbl =
      isOpen && (!t.exit_label || t.exit_label === EM)
        ? "持仓中"
        : t.exit_label || EM;
    const rowCls = isOpen ? "row-open" : cls;
    return `
        <td>${escapeHtml(bet)}</td>
        <td class="num">${escapeHtml(fmtAsk(entryAsk))}</td>
        <td class="num">${escapeHtml(askMax != null ? fmtAsk(askMax) : EM)}</td>
        <td>${escapeHtml(secondTrigger)}</td>
        <td class="num">${escapeHtml(fmtUsdPlain(orderUsd))}</td>
        <td class="num">${escapeHtml(fmtSpot(t.spot_at_entry))}</td>
        <td class="num">${escapeHtml(fmtEntryTime(t))}</td>
        <td class="num">${escapeHtml(fmtSpot(t.spot_start))}</td>
        <td class="num">${escapeHtml(fmtSpot(t.spot_end))}</td>
        <td>${escapeHtml(betRes)}</td>
        <td>${escapeHtml(exitLbl)}</td>
        <td class="${rowCls}">${isOpen ? fmtUsd(pnl) + " (浮)" : fmtUsd(pnl)}</td>`;
  }

  function renderTradeGroupRows(group, isLastGroup) {
    const legs = group.legs;
    const multiLeg = legs.length > 1;
    const common = tradeCommonCells(legs[0]);
    const rs = legs.length;
    const groupEndCls = isLastGroup ? "" : " trade-market-group-end";
    const mergedEndCls = isLastGroup ? "" : " trade-market-group-end";
    const html = [];

    legs.forEach((t, i) => {
      const isOpen = t.is_open === true;
      const openCls = isOpen ? " row-open" : "";
      const detail = renderTradeLegDetailCells(t);
      const rowEndCls = i === legs.length - 1 ? groupEndCls : "";

      if (i === 0 && multiLeg) {
        html.push(`<tr class="trade-market-group${openCls}${rowEndCls}">
        <td rowspan="${rs}" class="trade-common-merged${mergedEndCls}">${escapeHtml(common.strategy)}</td>
        <td rowspan="${rs}" class="trade-common-merged${mergedEndCls}">${escapeHtml(common.sym)}</td>
        <td rowspan="${rs}" class="trade-common-merged${mergedEndCls}">${escapeHtml(common.market)}</td>
        <td rowspan="${rs}" class="trade-common-merged num${mergedEndCls}">${escapeHtml(common.rangeHigh)}</td>
        <td rowspan="${rs}" class="trade-common-merged num${mergedEndCls}">${escapeHtml(common.rangeLow)}</td>
        ${detail}
      </tr>`);
      } else if (i === 0) {
        html.push(`<tr class="${openCls}${rowEndCls}">
        <td>${escapeHtml(common.strategy)}</td>
        <td>${escapeHtml(common.sym)}</td>
        <td>${escapeHtml(common.market)}</td>
        <td class="num">${escapeHtml(common.rangeHigh)}</td>
        <td class="num">${escapeHtml(common.rangeLow)}</td>
        ${detail}
      </tr>`);
      } else {
        html.push(`<tr class="trade-market-group${openCls}${rowEndCls}">${detail}</tr>`);
      }
    });
    return html.join("");
  }

  function groupTradeRows(rows) {
    const groups = [];
    const indexByKey = new Map();
    for (const t of rows) {
      const key = marketGroupKey(t);
      let idx = indexByKey.get(key);
      if (idx == null) {
        idx = groups.length;
        indexByKey.set(key, idx);
        groups.push({ key, legs: [] });
      }
      groups[idx].legs.push(t);
    }
    for (const g of groups) {
      g.legs.sort((a, b) => entrySortKey(a) - entrySortKey(b));
    }
    return groups;
  }

  function renderTradeRows(rows) {
    const sig = tradesSignature(rows);
    if (sig === lastTradesSig) {
      return;
    }
    lastTradesSig = sig;
    if (!rows.length) {
      tbody.innerHTML =
        '<tr><td colspan="17">暂无交易记录（下单后会显示持仓行）</td></tr>';
      return;
    }

    const groups = groupTradeRows(rows);
    tbody.innerHTML = groups
      .map((group, gi) => renderTradeGroupRows(group, gi === groups.length - 1))
      .join("");
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
    const r = await fetch("/api/status", {
      cache: "no-store",
      signal: AbortSignal.timeout(8000),
    });
    if (!r.ok) throw new Error("status " + r.status);
    return r.json();
  }

  let healthPoll = 0;
  let lastGoodData = null;

  function updateConnBadge(data) {
    if (data.live_feed_ts && !data.feed_stale) {
      badge.textContent = "live";
      badge.className = "badge badge-ok";
    } else if (data.live_feed_ts && data.feed_stale) {
      badge.textContent = "ask stale";
      badge.className = "badge badge-warn";
    } else if (data.feed_stale) {
      badge.textContent = "snapshot stale";
      badge.className = "badge badge-warn";
    } else {
      badge.textContent = "connecting…";
      badge.className = "badge badge-warn";
    }
  }

  async function tick() {
    try {
      const data = await fetchStatus();
      lastGoodData = data;
      updateHeaderSubtitle(data);
      renderSummary(data);
      renderTradingHours(data);
      renderCoins(data);
      updateConnBadge(data);
      healthPoll += 1;
      if (healthPoll % 4 === 1) {
        await fetchTrades();
      }
      if (healthPoll % 8 === 0 && !data.live_feed_ts) {
        const health = await fetch("/api/health", { cache: "no-store" }).then((x) => x.json());
        if (health.feed_direct) {
          badge.textContent = "bot on (no ask)";
          badge.className = "badge badge-warn";
        } else if (health.bot_live) {
          badge.textContent = "file only";
          badge.className = "badge badge-off";
        } else {
          badge.textContent = "no live bot";
          badge.className = "badge badge-off";
        }
      }
    } catch (e) {
      badge.textContent = "disconnected";
      badge.className = "badge badge-warn";
      console.error("[WEB]", e);
      if (lastGoodData) {
        renderSummary(lastGoodData);
        renderTradingHours(lastGoodData);
        renderCoins(lastGoodData);
      }
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

  const askProfitHeadRow = document.getElementById("ask-profit-head-row");
  const askProfitTbody = document.querySelector("#ask-profit-table tbody");
  const askProfitStatus = document.getElementById("ask-profit-status");
  const askProfitCustomEl = document.getElementById("ask-profit-custom-usd");
  let askProfitFetchInFlight = false;

  function fmtProfitCell(n) {
    if (n == null || Number.isNaN(n)) return EM;
    const v = Number(n);
    const sign = v >= 0 ? "+" : "";
    return sign + "$" + v.toFixed(2);
  }

  function renderAskProfitTable(data) {
    if (!askProfitHeadRow || !askProfitTbody) return;
    const amounts = data.amounts || [5, 25];
    askProfitHeadRow.innerHTML =
      "<th>Ask</th>" +
      amounts
        .map((a) => `<th>$${Number(a).toFixed(2)} 押中利润</th>`)
        .join("");

    const rows = data.rows || [];
    askProfitTbody.innerHTML = rows
      .map((row) => {
        const ask = row.ask != null ? Number(row.ask).toFixed(2) : EM;
        const cells = amounts
          .map((a) => {
            const key = String(Number(a).toFixed(2));
            const altKey = String(a);
            const cell =
              (row.profits && (row.profits[key] || row.profits[altKey])) || null;
            const profit = cell ? cell.profit_if_win : null;
            const cls = profit != null && profit >= 0 ? "pnl-pos" : "pnl-neg";
            return `<td class="num ${cls}">${escapeHtml(fmtProfitCell(profit))}</td>`;
          })
          .join("");
        return `<tr><td class="num">${escapeHtml(ask)}</td>${cells}</tr>`;
      })
      .join("");

    if (!rows.length) {
      askProfitTbody.innerHTML = `<tr><td colspan="${amounts.length + 1}">暂无数据</td></tr>`;
    }
  }

  async function fetchAskProfit() {
    if (askProfitFetchInFlight) return;
    askProfitFetchInFlight = true;
    if (askProfitStatus) askProfitStatus.textContent = "加载中…";
    try {
      const q = new URLSearchParams({
        amounts: "5,25",
        ask_from: "0.65",
        ask_to: "1",
        ask_step: "0.01",
      });
      const custom = askProfitCustomEl?.value?.trim();
      if (custom) q.set("custom_usd", custom);
      const r = await fetch("/api/ask-profit?" + q.toString(), {
        cache: "no-store",
        signal: AbortSignal.timeout(15000),
      });
      const data = await r.json();
      if (!r.ok || !data.ok) throw new Error(data.error || "ask-profit " + r.status);
      renderAskProfitTable(data);
      if (askProfitStatus) {
        const hits = data.cache_hits ?? 0;
        const misses = data.cache_misses ?? 0;
        askProfitStatus.textContent =
          misses > 0
            ? `缓存命中 ${hits} · 新写入 ${misses}`
            : `全部来自缓存 (${hits})`;
      }
    } catch (e) {
      if (askProfitStatus) askProfitStatus.textContent = String(e.message || e);
      console.error("[ASK-PROFIT]", e);
    } finally {
      askProfitFetchInFlight = false;
    }
  }

  document.getElementById("btn-ask-profit-query")?.addEventListener("click", fetchAskProfit);
  askProfitCustomEl?.addEventListener("keydown", (e) => {
    if (e.key === "Enter") fetchAskProfit();
  });

  loadConfig();
  tick();
  fetchTrades();
  fetchAskProfit();
  setInterval(tick, 250);
})();
