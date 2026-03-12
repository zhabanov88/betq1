'use strict';
// ═══════════════════════════════════════════════════════════════════
// Walk-Forward — полностью исправленная версия
// Исправления:
//   1. Сервер возвращает testBets, trainDates, testDates — displayResults
//      ожидал bets, trainDays, testDays → маппинг исправлен
//   2. Нет ClickHouse → сервер возвращает error → нужен fallback на локальную
//      симуляцию на основе трейдов из бэктеста
//   3. overallPnL считался как 0 вместо суммы по окнам
//   4. trainDays/testDays могут быть undefined → защита
// ═══════════════════════════════════════════════════════════════════
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
      if (sumEl) sumEl.innerHTML = '<div style="color:#ff9800;padding:10px">⚠️ Сначала добавьте и включите стратегию в Движке бэктеста, затем нажмите Run снова.</div>';
      this.running = false;
      if (btn) { btn.textContent = '▶ Run'; btn.style.pointerEvents = ''; }
      return;
    }

    // Показываем активную стратегию
    const stratInfoEl = document.getElementById('wfStratInfo');
    if (stratInfoEl) {
      stratInfoEl.innerHTML = `
        <div style="display:flex;align-items:center;gap:8px;padding:8px 0;border-bottom:1px solid rgba(255,255,255,0.06);margin-bottom:8px">
          <span style="width:10px;height:10px;border-radius:50%;background:${activeStrat.color||'#00d4ff'};flex-shrink:0"></span>
          <strong style="font-size:13px">${activeStrat.name}</strong>
          <span style="font-size:10px;color:var(--text3);background:rgba(255,255,255,0.07);padding:2px 6px;border-radius:4px">${activeStrat.sport}</span>
        </div>`;
    }

    if (sumEl) sumEl.innerHTML = '<div style="color:var(--text3);padding:8px">⏳ Запуск walk-forward анализа...</div>';

    const numWindows = parseInt(document.getElementById('wfWindowSize')?.value || document.getElementById('wfWindows')?.value) || 5;
    const inSample   = parseFloat(document.getElementById('wfInSample')?.value) || 70;
    const anchored   = document.getElementById('wfAnchored')?.checked || false;
    const bankroll   = parseFloat(document.getElementById('wfBankroll')?.value) || 1000;

    let usedLocal = false;

    try {
      const resp = await fetch('/api/bt/walkforward', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          strategy: { name: activeStrat.name, sport: activeStrat.sport, code: activeStrat.code },
          //cfg: { bankroll, dateFrom: '2019-01-01', dateTo: new Date().toISOString().slice(0,10) },
          cfg: {
            bankroll,
            staking: document.getElementById('btStaking')?.value || 'flat',
            maxStakePct: parseFloat(document.getElementById('btMaxStake')?.value) || 5,
            dateFrom: '2019-01-01',
            dateTo: new Date().toISOString().slice(0,10)
          },
          wfCfg: { windowSize: numWindows, inSample, anchored },
        }),
      });

      const d = await resp.json();

      // Сервер вернул ошибку или пустые данные → fallback
      if (d.error || !d.windows?.length) {
        console.warn('[WF] сервер:', d.error || 'no windows');
        usedLocal = true;
      } else {
        // ── ИСПРАВЛЕНИЕ: маппинг полей сервера → поля displayResults ──
        const windows = d.windows.map(w => ({
          window:    w.window,
          // Сервер возвращает trainDates/testDates, displayResults ожидает trainDays/testDays
          trainDays: w.trainDates  || w.trainDays  || '—',
          testDays:  w.testDates   || w.testDays   || '—',
          // Сервер возвращает testBets, displayResults ожидает bets
          bets:      w.testBets    ?? w.bets        ?? 0,
          winRate:   w.winRate     ?? 0,
          avgOdds:   w.avgOdds     ?? 0,
          roi:       w.roi         ?? 0,
          pnl:       w.pnl         ?? 0,
          stable:    w.stable      ?? false,
        }));
        const overallPnL = windows.reduce((s, w) => s + parseFloat(w.pnl), 0);
        this.displayResults({ windows, overallPnL }, activeStrat, false);
        this.renderCharts({ windows, overallPnL });
      }
    } catch(e) {
      console.warn('[WF] fetch error:', e.message);
      usedLocal = true;
    }

    if (usedLocal) {
      const result = this.simulateFromBacktest({ numWindows, inSample, anchored, bankroll }, activeStrat);
      this.displayResults(result, activeStrat, true);
      this.renderCharts(result);
    }

    this.running = false;
    if (btn) { btn.textContent = '▶ Run'; btn.style.pointerEvents = ''; }
  },

  // ── Локальная симуляция на основе реальных трейдов или demo ─────────────
  simulateFromBacktest({ numWindows, inSample, anchored, bankroll }, strat) {
    const lastResult = typeof backtestEngine !== 'undefined' ? backtestEngine._lastResult : null;
    const trades = lastResult?.trades || [];

    if (trades.length >= numWindows * 8) {
      return this._simulateFromTrades(trades, { numWindows, inSample, anchored, bankroll });
    } else {
      return this._simulateDemo({ numWindows, inSample, anchored, bankroll }, strat);
    }
  },

  _simulateFromTrades(trades, { numWindows, inSample, anchored, bankroll }) {
    const sorted = [...trades].sort((a, b) => (a.date || '').localeCompare(b.date || ''));
    const n = sorted.length;
    const winSize = Math.floor(n / numWindows);
    const windows = [];
    let overallPnL = 0;

    for (let i = 0; i < numWindows; i++) {
      const blockStart = anchored ? 0 : i * winSize;
      const blockEnd   = Math.min((i + 1) * winSize, n);
      const splitAt    = blockStart + Math.floor((blockEnd - blockStart) * (inSample / 100));

      const trainSet = sorted.slice(blockStart, splitAt);
      const testSet  = sorted.slice(splitAt, blockEnd);
      if (!testSet.length) continue;

      const bets    = testSet.length;
      const wins    = testSet.filter(t => t.won === 'W').length;
      const totPnL  = testSet.reduce((s, t) => s + parseFloat(t.pnl || 0), 0);
      const totStk  = testSet.reduce((s, t) => s + parseFloat(t.stake || 0), 0);
      const roi     = totStk > 0 ? (totPnL / totStk * 100) : 0;
      const winRate = bets > 0 ? (wins / bets * 100) : 0;
      const avgOdds = bets > 0 ? testSet.reduce((s, t) => s + parseFloat(t.odds || 2), 0) / bets : 0;
      overallPnL += totPnL;

      const trainFirst = trainSet[0]?.date || '—';
      const trainLast  = trainSet[trainSet.length-1]?.date || '—';
      const testFirst  = testSet[0]?.date || '—';
      const testLast   = testSet[testSet.length-1]?.date || '—';

      windows.push({
        window: i + 1,
        trainDays: `${trainFirst} — ${trainLast}`,
        testDays:  `${testFirst} — ${testLast}`,
        bets, winRate: winRate.toFixed(1),
        avgOdds: avgOdds.toFixed(2),
        roi: roi.toFixed(1),
        pnl: totPnL.toFixed(0),
        stable: Math.abs(roi) < 40 && winRate > 35,
      });
    }
    return { windows, overallPnL };
  },

  _simulateDemo({ numWindows, inSample, anchored, bankroll }, strat) {
    const windows = [];
    const totalDays = 730;
    const winSize   = Math.floor(totalDays / numWindows);
    let overallPnL  = 0;
    // Генерируем умеренно-позитивную базу
    const baseWR   = 0.50 + Math.random() * 0.05;
    const baseOdds = 1.90 + Math.random() * 0.3;

    for (let i = 0; i < numWindows; i++) {
      const start    = anchored ? 0 : i * winSize;
      const end      = (i + 1) * winSize;
      const trainEnd = start + Math.floor((end - start) * (inSample / 100));

      const bets    = Math.floor(25 + Math.random() * 35);
      const winRate = Math.max(0.38, Math.min(0.68, baseWR + (Math.random() - 0.5) * 0.10));
      const avgOdds = Math.max(1.55, Math.min(3.2,  baseOdds + (Math.random() - 0.5) * 0.25));
      const stake   = bankroll * 0.02;
      const pnl     = (winRate * (avgOdds - 1) - (1 - winRate)) * bets * stake;
      const roi     = (pnl / (bets * stake)) * 100;
      overallPnL += pnl;

      // Генерируем реалистичные даты
      const startDate = new Date(2023, 0, 1 + start);
      const trainDate = new Date(2023, 0, 1 + trainEnd);
      const endDate   = new Date(2023, 0, 1 + end);
      const fmt = d => d.toISOString().slice(0, 10);

      windows.push({
        window: i + 1,
        trainDays: `${fmt(startDate)} — ${fmt(trainDate)}`,
        testDays:  `${fmt(trainDate)} — ${fmt(endDate)}`,
        bets,
        winRate:   (winRate * 100).toFixed(1),
        avgOdds:   avgOdds.toFixed(2),
        roi:       roi.toFixed(1),
        pnl:       pnl.toFixed(0),
        stable:    Math.abs(roi) < 40 && winRate > 0.40,
      });
    }
    return { windows, overallPnL };
  },

  // ── Отображение результатов ──────────────────────────────────────────────
  displayResults(result, strat, isDemo) {
    const sumEl = document.getElementById('wfSummary');
    if (sumEl && result.windows.length) {
      const profitable = result.windows.filter(w => parseFloat(w.pnl) > 0).length;
      const total      = result.windows.length;
      const avgROI     = result.windows.reduce((s, w) => s + parseFloat(w.roi), 0) / total;
      const overallPnL = result.overallPnL ?? result.windows.reduce((s, w) => s + parseFloat(w.pnl), 0);

      let verdict = '', verdictColor = '';
      if (profitable / total >= 0.7 && avgROI > 5) {
        verdict = '✅ Стратегия стабильна — прибыльна в большинстве периодов';
        verdictColor = '#00e676';
      } else if (profitable / total >= 0.5) {
        verdict = '⚠️ Умеренная стабильность — прибыль в части периодов';
        verdictColor = '#f59e0b';
      } else {
        verdict = '❌ Нестабильная стратегия — убыточна в большинстве тестовых окон';
        verdictColor = '#ff4560';
      }

      const demoNote = isDemo
        ? `<div style="font-size:11px;color:#f59e0b;padding:6px 10px;background:rgba(245,158,11,0.08);border-radius:4px;margin-bottom:8px">⚠️ Demo-режим: данные из бэктеста или симуляция (подключите ClickHouse для реального WF-анализа)</div>`
        : '';

      sumEl.innerHTML = `
        ${demoNote}
        <div style="margin-bottom:10px;padding:8px 12px;border-radius:6px;background:rgba(255,255,255,0.04);border-left:3px solid ${verdictColor}">
          <strong style="color:${verdictColor};font-size:12px">${verdict}</strong>
        </div>
        <div style="display:grid;grid-template-columns:repeat(4,1fr);gap:8px;font-size:12px">
          <div><div style="color:var(--text3);font-size:10px">Всего окон</div><strong>${total}</strong></div>
          <div><div style="color:var(--text3);font-size:10px">Прибыльных</div><strong style="color:${profitable/total>=0.6?'#00e676':'#ff4560'}">${profitable}/${total}</strong></div>
          <div><div style="color:var(--text3);font-size:10px">Средний ROI</div><strong style="color:${avgROI>=0?'#00e676':'#ff4560'}">${avgROI>=0?'+':''}${avgROI.toFixed(1)}%</strong></div>
          <div><div style="color:var(--text3);font-size:10px">Общий PnL</div><strong style="color:${overallPnL>=0?'#00e676':'#ff4560'}">${overallPnL>=0?'+':''}${parseFloat(overallPnL).toFixed(0)}</strong></div>
        </div>`;
    }

    const el = document.getElementById('wfResultsTable');
    if (!el) return;

    if (!result.windows.length) {
      el.innerHTML = '<div style="color:var(--text3);padding:12px;text-align:center">Нет данных. Убедитесь что стратегия запущена в бэктесте.</div>';
      return;
    }

    el.innerHTML = `<table class="data-table">
      <thead><tr>
        <th>Окно</th>
        <th title="Период на котором стратегия обучалась (in-sample)">Train период ℹ</th>
        <th title="Период на котором проверялась стратегия (out-of-sample)">Test период ℹ</th>
        <th>Ставок</th><th>Win%</th><th>Avg Odds</th><th>ROI%</th><th>PnL</th><th>Статус</th>
      </tr></thead>
      <tbody>${result.windows.map(w => `<tr>
        <td><strong>W${w.window}</strong></td>
        <td style="font-size:11px;color:var(--text3)">${w.trainDays || '—'}</td>
        <td style="font-size:11px">${w.testDays || '—'}</td>
        <td>${w.bets ?? 0}</td>
        <td>${w.winRate ?? 0}%</td>
        <td>${w.avgOdds ?? 0}</td>
        <td class="${parseFloat(w.roi)>=0?'positive':'negative'}">${parseFloat(w.roi)>=0?'+':''}${w.roi}%</td>
        <td class="${parseFloat(w.pnl)>=0?'positive':'negative'}">${parseFloat(w.pnl)>=0?'+':''}${w.pnl}</td>
        <td>${w.stable
          ? '<span style="color:#00e676">✅ Стабильно</span>'
          : '<span style="color:#f59e0b">⚠️ Нестабильно</span>'}</td>
      </tr>`).join('')}</tbody>
    </table>`;
  },

  // ── График ───────────────────────────────────────────────────────────────
  renderCharts(result) {
    this.destroyCharts();
    if (!result.windows?.length) return;

    const tc = '#8892a4';
    const gc = 'rgba(255,255,255,0.05)';

    const cvs = document.getElementById('chartWFWindows');
    if (!cvs) return;

    const rois = result.windows.map(w => parseFloat(w.roi));

    this.charts.windows = new Chart(cvs, {
      type: 'bar',
      data: {
        labels: result.windows.map(w => `W${w.window}`),
        datasets: [{
          label: 'ROI %',
          data: rois,
          backgroundColor: rois.map(v => v >= 0 ? 'rgba(0,230,118,0.75)' : 'rgba(255,69,96,0.75)'),
          borderRadius: 4,
        }],
      },
      options: {
        responsive: true, maintainAspectRatio: false, animation: false,
        plugins: {
          legend: { display: false },
          tooltip: {
            callbacks: {
              label: ctx => `ROI: ${ctx.raw >= 0 ? '+' : ''}${ctx.raw.toFixed(1)}%`,
              afterLabel: ctx => {
                const w = result.windows[ctx.dataIndex];
                return [
                  `Ставок: ${w.bets ?? 0}`,
                  `Win%: ${w.winRate ?? 0}%`,
                  `Avg Odds: ${w.avgOdds ?? 0}`,
                  `PnL: ${parseFloat(w.pnl) >= 0 ? '+' : ''}${w.pnl}`,
                ];
              },
            },
          },
        },
        scales: {
          x: { ticks: { color: tc, font: { size: 9 } }, grid: { color: gc } },
          y: {
            ticks: { color: tc, font: { size: 9 }, callback: v => v + '%' },
            grid: { color: gc },
            // Нулевая линия
            border: { dash: [4, 4] },
          },
        },
      },
    });
  },

  destroyCharts() {
    Object.values(this.charts).forEach(c => { try { c.destroy(); } catch(e){} });
    this.charts = {};
  },
};