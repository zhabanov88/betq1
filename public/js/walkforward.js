'use strict';
const walkForward = {
  charts:  {},
  running: false,

  async run() {
    if (this.running) return;
    this.running = true;
    const btn = document.getElementById('wfRunBtn') || document.querySelector('[onclick="walkForward.run()"]');
    if (btn) { btn.textContent = '⏳ Анализ...'; btn.style.pointerEvents = 'none'; }

    const activeStrat = typeof backtestEngine !== 'undefined'
      ? (backtestEngine.activeStrategies || []).find(s => s.enabled) : null;
    const sumEl = document.getElementById('wfSummary');

    if (!activeStrat) {
      if (sumEl) sumEl.innerHTML = '<div style="color:#ff9800;padding:8px">⚠️ Добавьте стратегию в Движке бэктеста</div>';
      this.running = false;
      if (btn) { btn.textContent = '▶ Run'; btn.style.pointerEvents = ''; }
      return;
    }

    if (sumEl) sumEl.innerHTML = '<div style="color:var(--text3);padding:8px">⏳ Walk-forward на реальных данных...</div>';

    try {
      const resp = await fetch('/api/bt/walkforward', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          strategy: { name: activeStrat.name, sport: activeStrat.sport, code: activeStrat.code },
          cfg: { bankroll: parseFloat(document.getElementById('wfBankroll')?.value) || 1000, dateFrom: '2019-01-01', dateTo: new Date().toISOString().slice(0,10) },
          wfCfg: {
            windowSize: parseInt(document.getElementById('wfWindowSize')?.value || document.getElementById('wfWindows')?.value) || 6,
            inSample: parseFloat(document.getElementById('wfInSample')?.value) || 70,
            anchored: document.getElementById('wfAnchored')?.checked || false,
          },
        }),
      });
      const d = await resp.json();
      if (d.error) throw new Error(d.error);
      this.displayResults({ windows: d.windows, overallPnL: 0 });
      this.renderCharts({ windows: d.windows, overallPnL: 0 });
    } catch(e) {
      if (sumEl) sumEl.innerHTML = `<div style="color:var(--red)">❌ ${e.message}</div>`;
    }
    this.running = false;
    if (btn) { btn.textContent = '▶ Run'; btn.style.pointerEvents = ''; }
  },

  simulate(cfg) {
    const windows = [];
    const totalDays = 730;
    const numWindows = Math.max(3, Math.min(20, cfg.windowSize));
    const winSize = Math.floor(totalDays / numWindows);

    let overallPnL = 0;

    for (let i = 0; i < numWindows; i++) {
      const start    = cfg.anchored ? 0 : i * winSize;
      const end      = (i + 1) * winSize;
      const trainEnd = start + Math.floor((end - start) * cfg.inSample);

      const bets    = Math.floor(15 + Math.random() * 50);
      const winRate = 0.42 + Math.random() * 0.22;
      const avgOdds = 1.7 + Math.random() * 1.1;
      const pnl     = (winRate * (avgOdds - 1) - (1 - winRate)) * bets * cfg.bankroll * 0.02;
      const roi     = bets > 0 ? (pnl / (bets * cfg.bankroll * 0.02) * 100) : 0;
      overallPnL   += pnl;

      windows.push({
        window:   i + 1,
        trainDays: `д.${start}–${trainEnd}`,
        testDays:  `д.${trainEnd}–${end}`,
        bets, winRate: (winRate * 100).toFixed(1),
        avgOdds: avgOdds.toFixed(2),
        roi: roi.toFixed(1),
        pnl: pnl.toFixed(0),
        stable: Math.abs(roi) < 30 && winRate > 0.4,
      });
    }
    return { windows, overallPnL };
  },

  displayResults(result) {
    const summary = document.getElementById('wfSummary');
    if (summary) {
      const profitable = result.windows.filter(w => parseFloat(w.pnl) > 0).length;
      const total      = result.windows.length;
      const avgROI     = result.windows.reduce((s,w) => s + parseFloat(w.roi), 0) / total;
      summary.innerHTML = `
        <div class="bt-strat-breakdown-row" style="flex-wrap:wrap;gap:12px">
          <span>Окон: <strong>${total}</strong></span>
          <span>Прибыльных: <strong class="positive">${profitable}/${total}</strong></span>
          <span>Средний ROI: <strong class="${avgROI>=0?'positive':'negative'}">${avgROI.toFixed(1)}%</strong></span>
          <span>Общий PnL: <strong class="${result.overallPnL>=0?'positive':'negative'}">${result.overallPnL>=0?'+':''}${result.overallPnL.toFixed(0)}</strong></span>
        </div>`;
    }

    const el = document.getElementById('wfResultsTable');
    if (!el) return;
    el.innerHTML = `<table class="data-table">
      <thead><tr>
        <th>Окно</th><th>Train период</th><th>Test период</th>
        <th>Ставок</th><th>Win%</th><th>Avg Odds</th><th>ROI%</th><th>PnL</th><th>Статус</th>
      </tr></thead>
      <tbody>${result.windows.map(w => `<tr>
        <td><strong>W${w.window}</strong></td>
        <td style="font-size:11px">${w.trainDays}</td>
        <td style="font-size:11px">${w.testDays}</td>
        <td>${w.bets}</td>
        <td>${w.winRate}%</td>
        <td>${w.avgOdds}</td>
        <td class="${parseFloat(w.roi)>=0?'positive':'negative'}">${parseFloat(w.roi)>=0?'+':''}${w.roi}%</td>
        <td class="${parseFloat(w.pnl)>=0?'positive':'negative'}">${parseFloat(w.pnl)>=0?'+':''}${w.pnl}</td>
        <td>${w.stable?'<span style="color:#00e676">✅ Стабильно</span>':'<span style="color:#f59e0b">⚠️ Нестабильно</span>'}</td>
      </tr>`).join('')}</tbody>
    </table>`;
  },

  renderCharts(result) {
    this.destroyCharts();
    const isDark = document.body.classList.contains('dark-mode');
    const tc = isDark ? '#8892a4' : '#4a5568';
    const gc = isDark ? 'rgba(255,255,255,0.05)' : 'rgba(0,0,0,0.07)';
    const base = {
      responsive: true, maintainAspectRatio: false,
      plugins: { legend: { display: false } },
      scales: { x:{ticks:{color:tc,font:{size:9}},grid:{color:gc}}, y:{ticks:{color:tc,font:{size:9}},grid:{color:gc}} },
    };

    const cvs = document.getElementById('chartWFWindows');
    if (cvs) {
      const rois = result.windows.map(w => parseFloat(w.roi));
      this.charts.windows = new Chart(cvs, {
        type: 'bar',
        data: {
          labels: result.windows.map(w => `W${w.window}`),
          datasets: [{
            data: rois,
            backgroundColor: rois.map(v => v >= 0 ? 'rgba(0,230,118,0.75)' : 'rgba(255,69,96,0.75)'),
            borderRadius: 4,
          }],
        },
        options: base,
      });
    }
  },

  destroyCharts() {
    Object.values(this.charts).forEach(c => { try { c.destroy(); } catch(e){} });
    this.charts = {};
  },
};