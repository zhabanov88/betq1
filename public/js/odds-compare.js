'use strict';
// ═══════════════════════════════════════════════════════════════════════════
//  BetQuant Pro — Odds Comparison
//  Сравнение коэффициентов по 8 букмекерам, арбитраж, лучшие линии
// ═══════════════════════════════════════════════════════════════════════════
const oddsCompare = {
  fixtures: [],
  charts:   {},
  selected: null,
  viewMode: 'grid',    // 'grid' | 'arb'

  // ── lifecycle ─────────────────────────────────────────────────────────────
  async init() {
    await this.load();
  },

  async load() {
    this._setLoading(true);
    try {
      const d = await this._fetch('/api/odds-compare/fixtures');
      this.fixtures = d?.fixtures || [];
      this.render();
    } catch(e) {
      console.warn('[oddsCompare]', e);
    } finally {
      this._setLoading(false);
    }
  },

  async loadArb() {
    this._setLoading(true);
    try {
      const d = await this._fetch('/api/odds-compare/arbitrage');
      this._renderArbPanel(d?.opportunities || []);
    } catch(e) {
      console.warn('[oddsCompare] arb', e);
    } finally {
      this._setLoading(false);
    }
  },

  // ── main render ───────────────────────────────────────────────────────────
  render() {
    this._renderTabs();
    if (this.viewMode === 'grid') this._renderGrid();
    if (this.viewMode === 'arb')  this.loadArb();
  },

  _renderTabs() {
    const el = document.getElementById('oc-tabs');
    if (!el) return;
    el.innerHTML = `
      <button class="ctrl-btn ${this.viewMode==='grid' ? 'primary' : ''}" onclick="oddsCompare.setView('grid')">📊 Матчи</button>
      <button class="ctrl-btn ${this.viewMode==='arb'  ? 'primary' : ''}" onclick="oddsCompare.setView('arb')">🎯 Арбитраж</button>
      <button class="ctrl-btn" onclick="oddsCompare.load()">🔄 Обновить</button>
      <select class="ctrl-select" id="ocLeague" onchange="oddsCompare.filterLeague(this.value)">
        <option value="">Все лиги</option>
        <option value="Premier League">Premier League</option>
        <option value="La Liga">La Liga</option>
        <option value="Bundesliga">Bundesliga</option>
        <option value="Serie A">Serie A</option>
        <option value="Champions League">Champions League</option>
      </select>`;
  },

  _renderGrid() {
    const el = document.getElementById('oc-content');
    if (!el) return;
    const list = this._filtered();
    if (!list.length) { el.innerHTML = '<div class="lm-empty">Нет данных</div>'; return; }

    el.innerHTML = `
      <div class="oc-grid">
        ${list.map(f => this._fixtureCard(f)).join('')}
      </div>`;
  },

  _fixtureCard(f) {
    const bms    = Object.keys(f.bookmakers || {});
    const mkts   = ['home', 'draw', 'away'];
    const labels = ['1', 'X', '2'];

    // Лучший коэф по каждому исходу
    const bestOdds = {};
    for (const m of mkts) {
      let best = 0, bestBm = '';
      for (const bm of bms) {
        const o = f.bookmakers[bm]?.[m] || 0;
        if (o > best) { best = o; bestBm = bm; }
      }
      bestOdds[m] = { odds: best, bm: bestBm };
    }

    // Процент разброса (max - min) / min
    const spread = m => {
      const vals = bms.map(bm => f.bookmakers[bm]?.[m] || 0).filter(o => o > 1);
      if (vals.length < 2) return 0;
      return +((Math.max(...vals) - Math.min(...vals)) / Math.min(...vals) * 100).toFixed(1);
    };

    const arbBadge = f.arb?.possible
      ? `<span class="oc-arb-badge">🎯 ARB +${f.arb.profit}%</span>`
      : '';

    return `
      <div class="oc-card ${this.selected === f.id ? 'selected' : ''}" onclick="oddsCompare.selectFixture('${f.id}')">
        <div class="oc-card-head">
          <span class="lm-league-tag">${f.league}</span>
          <span class="oc-time">${this._fmtTime(f.startTime)}</span>
          ${arbBadge}
        </div>
        <div class="oc-match-name">
          <span class="oc-team">${f.home}</span>
          <span style="color:var(--text3);margin:0 6px">vs</span>
          <span class="oc-team">${f.away}</span>
        </div>
        <div class="oc-best-row">
          ${mkts.map((m, i) => {
            const b = bestOdds[m];
            const sp = spread(m);
            return `<div class="oc-best-cell">
              <span class="oc-mkt-label">${labels[i]}</span>
              <span class="oc-best-odds">${b.odds > 0 ? b.odds.toFixed(2) : '—'}</span>
              <span class="oc-bm-label">${BOOKMAKER_LABELS[b.bm] || b.bm}</span>
              ${sp > 3 ? `<span class="oc-spread positive">Δ${sp}%</span>` : ''}
            </div>`;
          }).join('')}
        </div>
      </div>`;
  },

  // ── detail panel ──────────────────────────────────────────────────────────
  async selectFixture(id) {
    this.selected = id;
    document.querySelectorAll('.oc-card').forEach(c => c.classList.toggle('selected', c.onclick?.toString().includes(id)));
    const el = document.getElementById('oc-detail');
    if (!el) return;

    const f = this.fixtures.find(x => x.id === id);
    if (!f) return;

    const bms  = Object.entries(f.bookmakers || {});
    const mkts = ['home', 'draw', 'away', 'over25', 'under25'];
    const mktLabels = { home: '1 (Хозяева)', draw: 'X (Ничья)', away: '2 (Гости)', over25: 'Over 2.5', under25: 'Under 2.5' };

    // Build comparison table
    const headerRow = `<tr><th>Букмекер</th>${mkts.map(m => `<th>${mktLabels[m]}</th>`).join('')}<th>Маржа %</th></tr>`;

    // Find best per market for highlighting
    const bestPerMkt = {};
    for (const m of mkts) {
      let best = 0;
      for (const [, o] of bms) { if ((o[m]||0) > best) best = o[m]; }
      bestPerMkt[m] = best;
    }

    const rows = bms.map(([bm, o]) => {
      const sum = (o.home > 0 ? 1/o.home : 0) + (o.draw > 1 ? 1/o.draw : 0) + (o.away > 0 ? 1/o.away : 0);
      const margin = +((sum - 1) * 100).toFixed(2);
      const cells = mkts.map(m => {
        const v = o[m] || 0;
        const isBest = v > 0 && v === bestPerMkt[m];
        return `<td class="${isBest ? 'oc-best-cell-hi' : ''}">${v > 0 ? v.toFixed(2) : '—'}</td>`;
      }).join('');
      const sharp = bm === 'pinnacle' ? '<span class="oc-sharp-tag">Sharp</span>' : '';
      return `<tr>
        <td><span class="oc-bm-icon">${BOOKMAKER_ICONS[bm] || '📚'}</span> ${BOOKMAKER_LABELS[bm] || bm} ${sharp}</td>
        ${cells}
        <td class="${margin > 6 ? 'negative' : margin < 3 ? 'positive' : ''}">${margin > 0 ? margin + '%' : '—'}</td>
      </tr>`;
    }).join('');

    el.innerHTML = `
      <div class="oc-det-header">
        <div>
          <div class="oc-det-title">${f.home} vs ${f.away}</div>
          <div class="oc-det-sub">${f.league} · ${this._fmtTime(f.startTime)}</div>
        </div>
        ${f.arb?.possible ? `<div class="oc-arb-alert">🎯 Арбитраж +${f.arb.profit}% возможен!</div>` : ''}
      </div>
      <div style="overflow-x:auto">
        <table class="data-table oc-compare-table">
          <thead>${headerRow}</thead>
          <tbody>${rows}</tbody>
        </table>
      </div>
      ${f.arb?.possible ? this._arbDetails(f) : ''}
      <div style="margin-top:16px">
        <div class="lm-sec-title">📈 Разброс коэффициентов</div>
        <div style="position:relative;height:200px"><canvas id="oc-spread-chart"></canvas></div>
      </div>`;

    setTimeout(() => this._renderSpreadChart(f), 40);
  },

  _arbDetails(f) {
    const legs = f.arb?.legs || [];
    if (!legs.length) return '';
    const stake = 1000;
    const invSum = legs.reduce((s, l) => s + 1/l.odds, 0);
    const legStakes = legs.map(l => ({
      ...l,
      stake: +(stake / invSum / l.odds * l.odds).toFixed(2),
    }));
    return `
      <div class="oc-arb-panel">
        <div class="lm-sec-title">🎯 Арбитражный расчёт (ставка ${stake} ед.)</div>
        <div style="display:flex;gap:12px;flex-wrap:wrap">
          ${legStakes.map(l => `
            <div class="oc-arb-leg">
              <div class="oc-bm-label">${BOOKMAKER_LABELS[l.bm] || l.bm}</div>
              <div class="oc-best-odds">${l.odds}</div>
              <div>Ставка: <strong>${l.stake}</strong></div>
            </div>`).join('')}
        </div>
        <div class="oc-arb-profit">
          Гарантированная прибыль: <strong class="positive">+${(stake * f.arb.profit / 100).toFixed(2)} ед. (+${f.arb.profit}%)</strong>
        </div>
      </div>`;
  },

  _renderSpreadChart(f) {
    if (this.charts.spread) { try { this.charts.spread.destroy(); } catch(e){} }
    const cvs = document.getElementById('oc-spread-chart');
    if (!cvs) return;
    const bms   = Object.keys(f.bookmakers || {});
    const mkts  = ['home', 'draw', 'away'];
    const labels= ['1 (Хозяева)', 'X (Ничья)', '2 (Гости)'];
    const dk    = document.body.classList.contains('dark-mode');
    const tc    = dk ? '#8892a4' : '#4a5568', gc = dk ? 'rgba(255,255,255,.05)' : 'rgba(0,0,0,.07)';
    const COLORS = ['#00d4ff','#ffd740','#ff4560','#00e676','#c084fc','#fb923c','#a78bfa','#f472b6'];

    this.charts.spread = new Chart(cvs, {
      type: 'bar',
      data: {
        labels,
        datasets: bms.map((bm, i) => ({
          label: BOOKMAKER_LABELS[bm] || bm,
          data:  mkts.map(m => f.bookmakers[bm]?.[m] || null),
          backgroundColor: COLORS[i % COLORS.length] + 'cc',
          borderRadius: 3,
        })),
      },
      options: {
        responsive: true, maintainAspectRatio: false,
        plugins: { legend: { labels: { color: tc, font: { size: 10 } }, position: 'bottom' } },
        scales: {
          x: { ticks:{ color:tc, font:{size:10} }, grid:{color:gc} },
          y: { ticks:{ color:tc, font:{size:10} }, grid:{color:gc} },
        },
      },
    });
  },

  _renderArbPanel(opps) {
    const el = document.getElementById('oc-content');
    if (!el) return;
    if (!opps.length) {
      el.innerHTML = `<div class="lm-empty" style="padding:48px;text-align:center">
        🎯 Арбитражных ситуаций не найдено<br>
        <span style="font-size:11px;color:var(--text3);margin-top:6px;display:block">Данные обновляются каждые 5 минут</span>
      </div>`;
      return;
    }
    el.innerHTML = `
      <div style="margin-bottom:12px;font-size:13px;color:var(--text3)">
        Найдено: <strong class="positive">${opps.length}</strong> арбитражных ситуаций
      </div>
      ${opps.map(a => `
        <div class="oc-arb-full-card">
          <div class="oc-card-head">
            <span class="lm-league-tag">${a.league}</span>
            <span class="oc-time">${this._fmtTime(a.startTime)}</span>
            <span class="oc-arb-badge">🎯 +${a.profit}%</span>
          </div>
          <div class="oc-match-name">${a.match}</div>
          <div style="display:flex;gap:10px;flex-wrap:wrap;margin-top:8px">
            ${(a.legs || []).map(l => `
              <div class="oc-arb-leg">
                <span class="oc-bm-label">${BOOKMAKER_LABELS[l.bm] || l.bm}</span>
                <span class="oc-best-odds">${l.odds}</span>
              </div>`).join('')}
          </div>
        </div>`).join('')}`;
  },

  // ── helpers ───────────────────────────────────────────────────────────────
  setView(v)   { this.viewMode = v; this.render(); },
  filterLeague(v) {
    const el = document.getElementById('oc-content');
    if (!el) return;
    this._appliedLeague = v;
    this._renderGrid();
  },
  _filtered() {
    const l = this._appliedLeague;
    return l ? this.fixtures.filter(f => f.league.includes(l)) : this.fixtures;
  },
  _fmtTime(iso) {
    if (!iso) return '—';
    return new Date(iso).toLocaleString('ru', { month: 'short', day: 'numeric', hour: '2-digit', minute: '2-digit' });
  },
  _setLoading(on) {
    const el = document.getElementById('oc-loading');
    if (el) el.style.display = on ? 'flex' : 'none';
  },
  async _fetch(url) {
    const r = await fetch(url, { headers: { 'x-auth-token': localStorage.getItem('bq_token') || 'demo' } });
    if (!r.ok) throw new Error(r.status);
    return r.json();
  },
};

// ─── Bookmaker label/icon maps ─────────────────────────────────────────────
const BOOKMAKER_LABELS = {
  pinnacle:'Pinnacle', bet365:'Bet365', betfair:'Betfair',
  unibet:'Unibet', williamhill:'William Hill', bwin:'Bwin',
  '1xbet':'1xBet', betway:'Betway',
};
const BOOKMAKER_ICONS = {
  pinnacle:'🔷', bet365:'🟢', betfair:'🟠', unibet:'🟤',
  williamhill:'⚫', bwin:'🔴', '1xbet':'🔵', betway:'🟡',
};