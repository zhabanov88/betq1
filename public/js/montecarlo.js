'use strict';
// ═══════════════════════════════════════════════════════════════════
// Monte Carlo — полностью исправленная версия
// Исправления:
//   1. chartMCDistrib (HTML) vs chartMCHist (предыдущий код) → используем chartMCDistrib
//   2. ruinByBet с сервера — массив чисел, не объектов → нормализуем
//   3. paths может отсутствовать или быть пустым → защита
//   4. betsPerRun с сервера = d.betsPerRun, но может не прийти → fallback
//   5. Полная клиентская симуляция при недоступности сервера
//   6. Карточка стратегии + текстовый вердикт
// ═══════════════════════════════════════════════════════════════════
const monteCarlo = {
  charts: {},

  async run() {
    const simCount      = parseInt(document.getElementById('mcSimCount')?.value)      || 5000;
    const betsPerRun    = parseInt(document.getElementById('mcBetsPerRun')?.value)    || 500;
    const startBankroll = parseFloat(document.getElementById('mcBankroll')?.value)    || 1000;
    const winRate       = parseFloat(document.getElementById('mcWinRate')?.value)     || 52;
    const avgOdds       = parseFloat(document.getElementById('mcAvgOdds')?.value)     || 2.0;
    const stakePct      = parseFloat(document.getElementById('mcStake')?.value)       || 2;
    const ruinThreshold = parseFloat(document.getElementById('mcRuinThreshold')?.value) || 10;

    const sumEl = document.getElementById('mcSummary');
    if (sumEl) sumEl.innerHTML = '<div style="color:var(--text3);padding:8px">⏳ Запуск симуляции...</div>';

    // Получаем активную стратегию
    const activeStrat = typeof backtestEngine !== 'undefined'
      ? (backtestEngine.activeStrategies || []).find(s => s.enabled) : null;

    // Берём параметры из последнего бэктеста если есть
    const lastResult  = typeof backtestEngine !== 'undefined' ? backtestEngine._lastResult : null;
    const lastTrades  = lastResult?.trades || [];
    const btStaking   = document.getElementById('btStaking')?.value || 'flat';
    const btMaxStake  = parseFloat(document.getElementById('btMaxStake')?.value) || 5;

    this._renderStrategyCard(activeStrat, { winRate, avgOdds, stakePct, startBankroll, lastTrades });

    let serverOk = false;

    try {
      const resp = await fetch('/api/bt/montecarlo', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          // Передаём реальные трейды из бэктеста если есть — это устраняет расхождение
          trades: lastTrades.length >= 20 ? lastTrades.slice(-500).map(t => ({
            odds: parseFloat(t.odds),
            stake: parseFloat(t.stake),
            won: t.won,
            pnl: parseFloat(t.pnl),
            prob: 0.5,
          })) : null,
          strategy: activeStrat ? { sport: activeStrat.sport, code: activeStrat.code } : null,
          cfg: { bankroll: startBankroll, staking: btStaking, maxStakePct: btMaxStake,
                 dateFrom: '2020-01-01', dateTo: new Date().toISOString().slice(0,10) },
          mcCfg: { simCount, betsPerRun, ruinThreshold: ruinThreshold / 100, winRate, avgOdds, stakePct },
        }),
      });

      if (!resp.ok) throw new Error('HTTP ' + resp.status);
      const d = await resp.json();
      if (d.error) throw new Error(d.error);

      const { paths, finals, ruinByBet, percentiles: pc, avg, ruinProbability, realStats } = d;
      if (!pc || !finals?.length) throw new Error('empty_response');

      const { p5, p25, p50, p75, p95 } = pc;
      const actualBetsPerRun = d.betsPerRun || betsPerRun;

      this._renderSummary({ p5, p25, p50, p75, p95, avg, ruinProbability, startBankroll, realStats, betsPerRun: actualBetsPerRun, simCount });
      this._renderCharts({ paths: paths || [], finals, ruinByBet: ruinByBet || [], betsPerRun: actualBetsPerRun, startBankroll, p5, p25, p50, p75, p95 });
      serverOk = true;

    } catch (e) {
      console.warn('[MC] сервер недоступен, клиентская симуляция:', e.message);
    }

    if (!serverOk) {
      // Клиентская симуляция
      const params = { simCount: Math.min(simCount, 3000), betsPerRun, startBankroll,
                       winRate: winRate / 100, avgOdds, stakePct: stakePct / 100,
                       ruinThreshold: ruinThreshold / 100 };

      // Если есть реальные трейды из бэктеста — использовать их статистику
      if (lastTrades.length >= 20) {
        const wins = lastTrades.filter(t => t.won === 'W').length;
        params.winRate = wins / lastTrades.length;
        params.avgOdds = lastTrades.reduce((s, t) => s + parseFloat(t.odds || 2), 0) / lastTrades.length;
        const avgStk = lastTrades.reduce((s, t) => s + parseFloat(t.stake || 0), 0) / lastTrades.length;
        params.stakePct = startBankroll > 0 ? avgStk / startBankroll : 0.02;
      }

      const result = this._simulateLocal(params);
      const realStats = lastTrades.length >= 20 ? {
        winRate: params.winRate,
        avgOdds: params.avgOdds,
        tradesUsed: lastTrades.length,
      } : null;

      this._renderSummary({ ...result.percentiles, avg: result.avg, ruinProbability: result.ruinProbability,
                             startBankroll, realStats, betsPerRun, simCount: params.simCount });
      this._renderCharts({ ...result, betsPerRun, startBankroll,
                           p5: result.percentiles.p5, p25: result.percentiles.p25,
                           p50: result.percentiles.p50, p75: result.percentiles.p75, p95: result.percentiles.p95 });
    }
  },

  // ── Клиентская симуляция ─────────────────────────────────────────────────
  _simulateLocal({ simCount, betsPerRun, startBankroll, winRate, avgOdds, stakePct, ruinThreshold }) {
    const paths = [];
    const finals = [];
    const ruinByBet = new Array(betsPerRun).fill(0);
    let ruinCount = 0;

    for (let s = 0; s < simCount; s++) {
      let bank = startBankroll;
      let ruined = false;
      const path = s < 400 ? [bank] : null;

      for (let b = 0; b < betsPerRun; b++) {
        if (bank <= startBankroll * ruinThreshold && !ruined) {
          ruined = true;
          ruinCount++;
          for (let rb = b; rb < betsPerRun; rb++) ruinByBet[rb]++;
          break;
        }
        const stake = Math.min(bank * stakePct, bank * 0.25);
        const won   = Math.random() < winRate;
        bank = Math.max(0, bank + (won ? stake * (avgOdds - 1) : -stake));
        if (path) path.push(bank);
      }
      if (path) paths.push(path);
      finals.push(bank);
    }

    finals.sort((a, b) => a - b);
    const n = finals.length;
    const pct = p => finals[Math.max(0, Math.min(n - 1, Math.floor(n * p)))];

    return {
      paths,
      finals,
      ruinByBet,
      percentiles: { p5: pct(0.05), p25: pct(0.25), p50: pct(0.50), p75: pct(0.75), p95: pct(0.95) },
      avg: finals.reduce((s, v) => s + v, 0) / n,
      ruinProbability: ruinCount / simCount * 100,
    };
  },

  // ── Карточка стратегии ───────────────────────────────────────────────────
  _renderStrategyCard(strat, { winRate, avgOdds, startBankroll, lastTrades }) {
    const el = document.getElementById('mcStrategyCard');
    if (!el) return;

    // Вычисляем реальные параметры если есть трейды
    let dispWR = winRate / 100;
    let dispOdds = avgOdds;
    let dataNote = '';

    if (lastTrades.length >= 20) {
      const wins = lastTrades.filter(t => t.won === 'W').length;
      dispWR = wins / lastTrades.length;
      dispOdds = lastTrades.reduce((s, t) => s + parseFloat(t.odds || 2), 0) / lastTrades.length;
      dataNote = `<div style="font-size:10px;color:#00e676;margin-top:4px">✓ Параметры взяты из последнего бэктеста (${lastTrades.length} ставок)</div>`;
    }

    const edge = (dispWR * (dispOdds - 1) - (1 - dispWR)) * 100;
    const kellyFull = edge > 0 ? edge / ((dispOdds - 1) * 100) * 100 : 0;
    const edgeColor = edge > 0 ? '#00e676' : '#ff4560';

    if (strat) {
      el.innerHTML = `
        <div style="padding:10px;border-radius:8px;background:rgba(255,255,255,0.04);border:1px solid rgba(255,255,255,0.08);margin-bottom:12px">
          <div style="display:flex;align-items:center;gap:8px;margin-bottom:6px">
            <span style="width:10px;height:10px;border-radius:50%;background:${strat.color||'#00d4ff'};flex-shrink:0"></span>
            <strong style="font-size:13px">${strat.name}</strong>
            <span style="font-size:10px;color:var(--text3);background:rgba(255,255,255,0.08);padding:2px 6px;border-radius:4px">${strat.sport}</span>
          </div>
          <div style="display:grid;grid-template-columns:1fr 1fr;gap:4px;font-size:12px">
            <span style="color:var(--text3)">WR:</span>
            <strong>${(dispWR*100).toFixed(1)}%</strong>
            <span style="color:var(--text3)">Avg odds:</span>
            <strong>${dispOdds.toFixed(2)}</strong>
            <span style="color:var(--text3)">Матожидание:</span>
            <strong style="color:${edgeColor}">${edge >= 0 ? '+' : ''}${edge.toFixed(2)}%</strong>
            <span style="color:var(--text3)">Полный Келли:</span>
            <strong>${kellyFull.toFixed(1)}% банка</strong>
          </div>
          ${dataNote}
        </div>`;
    } else {
      el.innerHTML = `
        <div style="padding:10px;border-radius:8px;background:rgba(255,152,0,0.08);border:1px solid rgba(255,152,0,0.25);margin-bottom:12px">
          <div style="color:#f59e0b;font-size:11px;margin-bottom:6px">⚠️ Нет активной стратегии — используются ручные параметры ниже</div>
          <div style="display:grid;grid-template-columns:1fr 1fr;gap:4px;font-size:12px">
            <span style="color:var(--text3)">Матожидание:</span>
            <strong style="color:${edgeColor}">${edge >= 0 ? '+' : ''}${edge.toFixed(2)}%</strong>
            <span style="color:var(--text3)">Полный Келли:</span>
            <strong>${kellyFull.toFixed(1)}%</strong>
          </div>
        </div>`;
    }
  },

  // ── Текстовое резюме ─────────────────────────────────────────────────────
  _renderSummary({ p5, p25, p50, p75, p95, avg, ruinProbability, startBankroll, realStats, betsPerRun, simCount }) {
    const sumEl = document.getElementById('mcSummary');
    if (!sumEl) return;

    const roi    = ((avg - startBankroll) / startBankroll * 100).toFixed(1);
    const roiP50 = ((p50 - startBankroll) / startBankroll * 100).toFixed(1);

    let verdict = '', verdictColor = '';
    if (ruinProbability < 5 && parseFloat(roi) > 15) {
      verdict = '✅ Отличные параметры: высокий ROI при низком риске краха';
      verdictColor = '#00e676';
    } else if (ruinProbability < 15 && parseFloat(roi) > 0) {
      verdict = '📈 Хорошие параметры: положительное ожидание с управляемым риском';
      verdictColor = '#00d4ff';
    } else if (ruinProbability >= 15 && ruinProbability < 40) {
      verdict = '⚠️ Высокий риск: снизьте размер ставки или улучшите win rate';
      verdictColor = '#f59e0b';
    } else if (ruinProbability >= 40) {
      verdict = '🚨 Критический риск краха! Срочно снизьте размер ставки';
      verdictColor = '#ff4560';
    } else {
      verdict = '📊 Стратегия около безубыточности — нужно улучшить параметры';
      verdictColor = '#f59e0b';
    }

    const srcNote = realStats?.tradesUsed >= 10
      ? `<div style="font-size:11px;color:var(--text3);margin-bottom:8px">📊 На основе ${realStats.tradesUsed} реальных ставок (WR ${(realStats.winRate*100).toFixed(1)}%, avg odds ${realStats.avgOdds.toFixed(2)})</div>`
      : `<div style="font-size:11px;color:var(--text3);margin-bottom:8px">🔢 Симуляция: ${simCount.toLocaleString()} запусков × ${betsPerRun} ставок</div>`;

    sumEl.innerHTML = `
      ${srcNote}
      <div style="margin-bottom:10px;padding:8px 12px;border-radius:6px;background:rgba(255,255,255,0.04);border-left:3px solid ${verdictColor}">
        <strong style="color:${verdictColor};font-size:12px">${verdict}</strong>
      </div>
      <div style="display:grid;grid-template-columns:1fr 1fr;gap:8px;font-size:12px">
        <div>
          <div style="color:var(--text3);font-size:10px">Медиана (P50)</div>
          <strong style="color:var(--accent)">${Math.round(p50)}</strong>
          <span style="color:var(--text3);font-size:10px"> (${roiP50 >= 0 ? '+' : ''}${roiP50}%)</span>
        </div>
        <div>
          <div style="color:var(--text3);font-size:10px">Среднее</div>
          <strong>${Math.round(avg)}</strong>
          <span style="color:var(--text3);font-size:10px"> (${roi >= 0 ? '+' : ''}${roi}%)</span>
        </div>
        <div>
          <div style="color:var(--text3);font-size:10px">P5 (худшие 5%)</div>
          <strong style="color:var(--red)">${Math.round(p5)}</strong>
        </div>
        <div>
          <div style="color:var(--text3);font-size:10px">P95 (лучшие 5%)</div>
          <strong style="color:var(--green)">${Math.round(p95)}</strong>
        </div>
        <div>
          <div style="color:var(--text3);font-size:10px">Вер. руина</div>
          <strong style="color:${ruinProbability > 20 ? 'var(--red)' : 'var(--green)'}">${ruinProbability.toFixed(1)}%</strong>
        </div>
        <div>
          <div style="color:var(--text3);font-size:10px">ROI (среднее)</div>
          <strong style="color:${parseFloat(roi) >= 0 ? 'var(--green)' : 'var(--red)'}">${roi >= 0 ? '+' : ''}${roi}%</strong>
        </div>
      </div>`;
  },

  // ── Графики ──────────────────────────────────────────────────────────────
  _renderCharts({ paths, finals, ruinByBet, betsPerRun, startBankroll, p5, p25, p50, p75, p95 }) {
    this.destroyCharts();
    const tc = '#8892a4';
    const gc = 'rgba(255,255,255,0.05)';
    const baseOpts = {
      responsive: true, maintainAspectRatio: false, animation: false,
      plugins: { legend: { display: false } },
      scales: {
        x: { ticks: { color: tc, font: { size: 8 }, maxTicksLimit: 12 }, grid: { color: gc } },
        y: { ticks: { color: tc, font: { size: 9 } }, grid: { color: gc } },
      },
    };

    // ── 1. Кривые капитала ─────────────────────────────────────────────────
    const cvsPaths = document.getElementById('chartMCPaths');
    if (cvsPaths && Array.isArray(paths) && paths.length > 0) {
      const len = paths[0]?.length || (betsPerRun + 1);
      const labels = Array.from({ length: len }, (_, i) => i);

      // Фоновые пути (до 200)
      const bgDatasets = paths.slice(0, 200).map(p => ({
        data: p, borderColor: 'rgba(0,212,255,0.07)', borderWidth: 0.8,
        pointRadius: 0, tension: 0, fill: false,
      }));

      // Перцентильные линии — находим ближайшие реальные пути к каждому перцентилю
      const targets = [
        { label: 'P5',     val: p5,  color: '#ff4560' },
        { label: 'P25',    val: p25, color: '#f59e0b' },
        { label: 'Median', val: p50, color: '#00d4ff' },
        { label: 'P75',    val: p75, color: '#f0e060' },
        { label: 'P95',    val: p95, color: '#00e676' },
      ];

      const percentileDatasets = targets.map(t => {
        const closest = paths.reduce((best, p) => {
          const lastVal = p[p.length - 1] ?? 0;
          return Math.abs(lastVal - t.val) < Math.abs((best[best.length - 1] ?? 0) - t.val) ? p : best;
        }, paths[0]);
        return {
          label: t.label, data: closest,
          borderColor: t.color, borderWidth: 2.5,
          pointRadius: 0, tension: 0.2, fill: false,
        };
      });

      this.charts.paths = new Chart(cvsPaths, {
        type: 'line',
        data: { labels, datasets: [...bgDatasets, ...percentileDatasets] },
        options: {
          ...baseOpts,
          plugins: {
            legend: {
              display: true,
              labels: {
                color: tc, font: { size: 10 }, boxWidth: 20,
                // Показывать только перцентильные линии, не все 200 фоновых
                filter: item => item.datasetIndex >= Math.min(paths.length, 200),
              },
            },
          },
        },
      });
    }

    // ── 2. Гистограмма итогового банкролла ────────────────────────────────
    // ВАЖНО: используем ID 'chartMCDistrib' как в index.html
    const cvsHist = document.getElementById('chartMCDistrib');
    if (cvsHist && Array.isArray(finals) && finals.length > 0) {
      const sorted  = [...finals].sort((a, b) => a - b);
      const minVal  = sorted[0];
      const maxVal  = sorted[sorted.length - 1];
      const range   = maxVal - minVal;
      const buckets = Math.min(40, Math.max(10, Math.floor(Math.sqrt(finals.length))));
      const step    = range > 0 ? range / buckets : 1;

      const counts = new Array(buckets).fill(0);
      finals.forEach(v => {
        const i = Math.min(buckets - 1, Math.floor((v - minVal) / step));
        counts[i]++;
      });
      const labels = counts.map((_, i) => Math.round(minVal + i * step).toLocaleString());
      const colors = counts.map((_, i) => {
        const val = minVal + i * step;
        return val < startBankroll ? 'rgba(255,69,96,0.75)' : 'rgba(0,212,255,0.65)';
      });

      this.charts.distrib = new Chart(cvsHist, {
        type: 'bar',
        data: { labels, datasets: [{ data: counts, backgroundColor: colors, borderRadius: 2 }] },
        options: {
          ...baseOpts,
          plugins: {
            legend: { display: false },
            tooltip: {
              callbacks: {
                title: ctx => `Банкролл: ${ctx[0].label}`,
                label: ctx => `Симуляций: ${ctx.raw}`,
              },
            },
          },
        },
      });
    }

    // ── 3. Кривая вероятности краха ───────────────────────────────────────
    const cvsRuin = document.getElementById('chartMCRuin');
    if (cvsRuin && Array.isArray(ruinByBet) && ruinByBet.length > 0) {
      // ruinByBet с сервера — массив чисел (кол-во разорений к каждой ставке)
      // или массив объектов {bet, cumRuin} — нормализуем оба варианта
      const totalSims = finals.length || 1;
      const ruinData = ruinByBet.map(v => {
        if (typeof v === 'object' && v !== null) return v.cumRuin ?? 0;
        return (v / totalSims) * 100;
      });

      // Прореживаем до разумного числа точек
      const maxPoints = 200;
      const step2 = Math.max(1, Math.floor(ruinData.length / maxPoints));
      const thinned = ruinData.filter((_, i) => i % step2 === 0);
      const thinLabels = thinned.map((_, i) => i * step2);

      this.charts.ruin = new Chart(cvsRuin, {
        type: 'line',
        data: {
          labels: thinLabels,
          datasets: [{
            data: thinned,
            borderColor: '#ff4560',
            backgroundColor: 'rgba(255,69,96,0.1)',
            borderWidth: 2,
            pointRadius: 0,
            fill: true,
            tension: 0.4,
          }],
        },
        options: {
          ...baseOpts,
          scales: {
            x: { ...baseOpts.scales.x, title: { display: true, text: 'Ставка №', color: tc, font: { size: 9 } } },
            y: {
              ...baseOpts.scales.y,
              min: 0,
              ticks: { ...baseOpts.scales.y.ticks, callback: v => v.toFixed(1) + '%' },
            },
          },
          plugins: {
            legend: { display: false },
            tooltip: { callbacks: { label: ctx => `Вер. краха: ${ctx.raw.toFixed(2)}%` } },
          },
        },
      });
    }
  },

  destroyCharts() {
    Object.values(this.charts).forEach(c => { try { c.destroy(); } catch(e){} });
    this.charts = {};
  },
};