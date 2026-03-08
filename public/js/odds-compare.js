'use strict';
// ═══════════════════════════════════════════════════════════════════════════
//  BetQuant Pro — Odds Comparison
//  Данные из ODDS_API или ClickHouse. Демо — только при bq_demo_mode=true.
// ═══════════════════════════════════════════════════════════════════════════
const oddsCompare = {
  fixtures: [],
  charts:   {},
  selected: null,
  viewMode: 'grid',

  async init() { await this.load(); },

  async load() {
    this._setLoading(true);
    try {
      const d = await this._fetch('/api/odds-compare/fixtures');
      if (d?.fixtures?.length) {
        this.fixtures = d.fixtures;
        this._setSource(d.source || 'api');
        this.render();
      } else {
        this._showNoData();
      }
    } catch(e) {
      console.warn('[oddsCompare]', e.message);
      this._showNoData();
    } finally {
      this._setLoading(false);
    }
  },

  _showNoData() {
    const el = document.getElementById('oc-content');
    if (el) el.innerHTML = `
      <div style="padding:64px;text-align:center">
        <div style="font-size:40px;margin-bottom:16px">📭</div>
        <div style="color:var(--text2);font-size:16px;margin-bottom:8px">Нет данных коэффициентов</div>
        <div style="color:var(--text3);font-size:12px;line-height:1.6">
          Для получения реальных данных добавьте в <strong>.env</strong>:<br>
          <code style="background:var(--bg2);padding:2px 6px;border-radius:4px">ODDS_API_KEY=ваш_ключ</code><br><br>
          Бесплатный ключ: <a href="https://the-odds-api.com" target="_blank" style="color:var(--accent)">the-odds-api.com</a>
        </div>
      </div>`;
    this._renderTabs();
  },

  async loadArb() {
    this._setLoading(true);
    try {
      const d = await this._fetch('/api/odds-compare/arbitrage');
      this._renderArbPanel(d?.opportunities || []);
    } catch(e) {
      this._renderArbPanel([]);
    } finally {
      this._setLoading(false);
    }
  },

  render() {
    this._renderTabs();
    if (this.viewMode === 'grid') this._renderGrid();
    if (this.viewMode === 'arb')  this.loadArb();
  },

  _renderTabs() {
    const el = document.getElementById('oc-tabs');
    if (!el) return;
    el.innerHTML = `
      <button class="ctrl-btn ${this.viewMode==='grid'?'primary':''}" onclick="oddsCompare.setView('grid')">📊 Матчи</button>
      <button class="ctrl-btn ${this.viewMode==='arb' ?'primary':''}" onclick="oddsCompare.setView('arb')">🎯 Арбитраж</button>
      <button class="ctrl-btn" onclick="oddsCompare.load()">🔄 Обновить</button>
      <select class="ctrl-select" id="ocLeague" onchange="oddsCompare.filterLeague(this.value)">
        <option value="">Все лиги</option>
        <option>Premier League</option><option>La Liga</option>
        <option>Bundesliga</option><option>Serie A</option>
        <option>Champions League</option><option>Ligue 1</option>
      </select>
      <span id="oc-source" style="font-size:11px;color:var(--text3);margin-left:8px"></span>`;
  },

  setView(m) { this.viewMode = m; this.render(); },
  filterLeague(v) { this._league = v; this._renderGrid(); },
  _filtered() { return this._league ? this.fixtures.filter(f => f.league?.toLowerCase().includes(this._league.toLowerCase())) : this.fixtures; },

  _renderGrid() {
    const el = document.getElementById('oc-content');
    if (!el) return;
    const list = this._filtered();
    if (!list.length) { el.innerHTML = '<div class="lm-empty" style="padding:40px;text-align:center">Нет матчей по фильтру</div>'; return; }
    el.innerHTML = `<div class="oc-grid">${list.map(f => this._fixtureCard(f)).join('')}</div>`;
  },

  _fixtureCard(f) {
    const bms  = Object.keys(f.bookmakers || {});
    const mkts = ['home','draw','away'];
    const bestOdds = {};
    for (const m of mkts) {
      let best = 0, bestBm = '';
      for (const bm of bms) { const o = f.bookmakers[bm]?.[m]; if (o > best) { best = o; bestBm = bm; } }
      bestOdds[m] = { odds: best, bm: bestBm };
    }
    const margin = bms.length ? (() => {
      const avgs = mkts.map(m => bms.reduce((s,bm)=>s+(f.bookmakers[bm]?.[m]||0),0)/bms.length);
      return ((avgs.reduce((s,o)=>s+(o>0?1/o:0),0)-1)*100).toFixed(1);
    })() : '—';

    const rows = bms.slice(0,8).map(bm => {
      const b = f.bookmakers[bm];
      if (!b) return '';
      const cells = mkts.map(m => {
        const isBest = bestOdds[m].bm === bm;
        return `<td class="${isBest?'positive':''}" style="font-weight:${isBest?'600':'400'}">${b[m]?.toFixed(2)||'—'}</td>`;
      }).join('');
      return `<tr><td style="font-size:11px;color:var(--text3)">${bm}</td>${cells}</tr>`;
    }).join('');

    const time = f.startTime ? new Date(f.startTime).toLocaleString('ru',{month:'short',day:'numeric',hour:'2-digit',minute:'2-digit'}) : '';

    return `<div class="oc-card" style="margin-bottom:16px;background:var(--bg2);border-radius:10px;padding:16px;border:1px solid var(--border)">
      <div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:10px">
        <div>
          <span class="lm-league-tag">${f.league||'Футбол'}</span>
          ${time?`<span style="font-size:11px;color:var(--text3);margin-left:8px">${time}</span>`:''}
        </div>
        <span style="font-size:11px;color:var(--text3)">Маржа: ${margin}%</span>
      </div>
      <div style="text-align:center;font-size:15px;font-weight:600;margin-bottom:12px;color:var(--text1)">
        ${f.home} <span style="color:var(--text3);font-weight:400">vs</span> ${f.away}
      </div>
      <div style="display:flex;gap:12px;margin-bottom:10px">
        ${mkts.map(m => `<div style="flex:1;text-align:center;background:var(--bg3);border-radius:6px;padding:6px">
          <div style="font-size:10px;color:var(--text3);margin-bottom:2px">${{home:'1 (Хозяева)',draw:'X',away:'2 (Гости)'}[m]}</div>
          <div style="font-size:16px;font-weight:700;color:var(--accent)">${bestOdds[m].odds?.toFixed(2)||'—'}</div>
          <div style="font-size:10px;color:var(--text3)">${bestOdds[m].bm}</div>
        </div>`).join('')}
      </div>
      ${bms.length>1?`<details style="font-size:12px"><summary style="cursor:pointer;color:var(--text3);font-size:11px">Все букмекеры (${bms.length})</summary>
        <table class="data-table" style="margin-top:6px"><thead><tr><th>Букмекер</th><th>1</th><th>X</th><th>2</th></tr></thead><tbody>${rows}</tbody></table>
      </details>`:''}
    </div>`;
  },

  _renderArbPanel(opps) {
    const el = document.getElementById('oc-content');
    if (!el) return;
    if (!opps.length) {
      el.innerHTML = `<div style="padding:48px;text-align:center">
        <div style="font-size:36px;margin-bottom:12px">🔍</div>
        <div style="color:var(--text2);margin-bottom:6px">Арбитражных ситуаций не найдено</div>
        <div style="color:var(--text3);font-size:12px">Рынки эффективны или данных мало.<br>Подключите больше букмекеров через ODDS_API_KEY.</div>
      </div>`;
      return;
    }
    el.innerHTML = `<div style="display:flex;flex-direction:column;gap:12px">
      ${opps.map(o => `<div style="background:var(--bg2);border-radius:10px;padding:14px;border:1px solid var(--green)">
        <div style="display:flex;justify-content:space-between;margin-bottom:8px">
          <strong>${o.match}</strong>
          <span class="positive">+${o.profit?.toFixed(2)||'?'}%</span>
        </div>
        <div style="font-size:12px;color:var(--text3)">${o.legs?.map(l=>`${l.bm}: ${l.market} @ ${l.odds}`).join(' | ')||''}</div>
      </div>`).join('')}
    </div>`;
  },

  _setSource(src) {
    const el = document.getElementById('oc-source');
    if (el) el.textContent = src === 'demo' ? '⚠️ демо-данные' : src === 'api' ? '✅ Odds API' : `✅ ${src}`;
  },

  _setLoading(on) { const el = document.getElementById('oc-loading'); if (el) el.style.display = on ? 'flex' : 'none'; },
  async _fetch(url) {
    const r = await fetch(url, { headers: { 'x-auth-token': localStorage.getItem('bq_token')||'demo' } });
    if (!r.ok) throw new Error(r.status);
    return r.json();
  },
};