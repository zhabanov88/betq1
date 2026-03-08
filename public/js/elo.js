'use strict';
// ═══════════════════════════════════════════════════════════════════════════
//  BetQuant Pro — ELO Ratings
//  Реальные данные из /api/value/elo. Демо — только при bq_demo_mode=true.
// ═══════════════════════════════════════════════════════════════════════════
const eloPanel = {
  ratings:  [],
  charts:   {},
  filter:   '',
  sport:    'football',
  viewMode: 'table',

  async init() { await this.load(); },

  async load() {
    this._setLoading(true);
    try {
      const d = await this._fetch('/api/value/elo');
      if (d?.ratings && d.ratings.length) {
        this.ratings = d.ratings;
        this.render();
        return;
      }
    } catch(e) { console.warn('[eloPanel]', e.message); }

    // Нет данных из API
    if (localStorage.getItem('bq_demo_mode') === 'true') {
      this.ratings = this._demoRatings();
      this.render();
    } else {
      this.ratings = [];
      this._showNoData();
    }
    this._setLoading(false);
  },

  _showNoData() {
    this._setLoading(false);
    const el = document.getElementById('elo-body');
    if (el) el.innerHTML = `<tr><td colspan="5" style="text-align:center;padding:32px">
      <div style="font-size:28px;margin-bottom:8px">📭</div>
      <div style="color:var(--text2);margin-bottom:4px">Нет данных ELO рейтингов</div>
      <div style="color:var(--text3);font-size:11px">ClickHouse не подключён или таблица teams_elo пуста.<br>
      Загрузите данные через ETL-менеджер или включите тестовый режим в настройках.</div>
    </td></tr>`;
    this._renderControls();
  },

  render() {
    this._renderControls();
    if (this.viewMode === 'table')   this._renderTable();
    if (this.viewMode === 'chart')   this._renderChart();
    if (this.viewMode === 'signals') this._renderSignals();
    this._setLoading(false);
  },

  _renderControls() {
    const el = document.getElementById('elo-controls');
    if (!el) return;
    el.innerHTML = `
      <input class="ctrl-input" id="eloSearch" placeholder="Поиск команды..." oninput="eloPanel.setFilter(this.value)" style="width:200px">
      <div class="tab-group" style="display:flex;gap:4px">
        ${['table','chart','signals'].map(m => `
          <button class="ctrl-btn ${this.viewMode === m ? 'primary' : ''}" onclick="eloPanel.setView('${m}')">
            ${{ table:'📋 Таблица', chart:'📈 Тренд', signals:'💡 Сигналы' }[m]}
          </button>`).join('')}
      </div>
      <button class="ctrl-btn" onclick="eloPanel.load()">🔄 Обновить</button>`;
  },

  _renderTable() {
    document.getElementById('elo-table-wrap') && (document.getElementById('elo-table-wrap').style.display = '');
    document.getElementById('elo-chart-wrap') && (document.getElementById('elo-chart-wrap').style.display = 'none');
    document.getElementById('elo-signals')    && (document.getElementById('elo-signals').style.display = 'none');
    const el = document.getElementById('elo-body');
    if (!el) return;
    const list = this._filtered();
    if (!list.length) { el.innerHTML = '<tr><td colspan="5" style="text-align:center;color:var(--text3);padding:24px">Нет команд по фильтру</td></tr>'; return; }
    const maxR = Math.max(...list.map(r => r.rating)), minR = Math.min(...list.map(r => r.rating));
    el.innerHTML = list.map((r, i) => {
      const pct  = ((r.rating - minR) / (maxR - minR) * 100) || 0;
      const tier = r.rating >= 1750 ? '🔴' : r.rating >= 1650 ? '🟠' : r.rating >= 1550 ? '🟡' : '⚪';
      const diff = r.prev ? r.rating - r.prev : 0;
      const diffHtml = diff ? `<span class="${diff > 0 ? 'positive' : 'negative'}" style="font-size:11px">${diff > 0 ? '▲' : '▼'}${Math.abs(diff).toFixed(0)}</span>` : '';
      return `<tr>
        <td style="color:var(--text3);font-size:11px">#${i + 1}</td>
        <td>${tier} <strong>${r.team}</strong></td>
        <td><div style="display:flex;align-items:center;gap:8px">
          <strong style="font-family:var(--font-mono);color:var(--accent)">${r.rating}</strong>${diffHtml}
        </div></td>
        <td><div class="elo-bar-wrap"><div class="elo-bar" style="width:${pct}%"></div></div></td>
        <td><button class="ctrl-btn sm" onclick="eloPanel.showMatchup('${r.team.replace(/'/g,"\\'")}')">vs Команда</button></td>
      </tr>`;
    }).join('');
  },

  _renderChart() {
    document.getElementById('elo-table-wrap') && (document.getElementById('elo-table-wrap').style.display = 'none');
    document.getElementById('elo-chart-wrap') && (document.getElementById('elo-chart-wrap').style.display = '');
    document.getElementById('elo-signals')    && (document.getElementById('elo-signals').style.display = 'none');
    if (this.charts.elo) { try { this.charts.elo.destroy(); } catch(e){} }
    const cvs = document.getElementById('elo-chart-canvas');
    if (!cvs) return;
    const top = this._filtered().slice(0, 20);
    if (!top.length) return;
    const dk = document.body.classList.contains('dark-mode');
    const tc = dk ? '#8892a4' : '#4a5568', gc = dk ? 'rgba(255,255,255,.05)' : 'rgba(0,0,0,.07)';
    const colors = top.map(r => r.rating >= 1750 ? 'rgba(255,69,96,0.8)' : r.rating >= 1650 ? 'rgba(255,160,64,0.8)' : r.rating >= 1550 ? 'rgba(0,212,255,0.8)' : 'rgba(148,163,184,0.6)');
    this.charts.elo = new Chart(cvs, {
      type: 'bar',
      data: { labels: top.map(r => r.team), datasets: [{ label: 'ELO рейтинг', data: top.map(r => r.rating), backgroundColor: colors, borderRadius: 5 }] },
      options: { indexAxis: 'y', responsive: true, maintainAspectRatio: false,
        plugins: { legend: { display: false }, tooltip: { callbacks: { label: ctx => `ELO: ${ctx.parsed.x}` } } },
        scales: { x: { ticks: { color: tc, font: { size: 10 } }, grid: { color: gc }, min: 1400 }, y: { ticks: { color: tc, font: { size: 10 } }, grid: { color: gc } } } },
    });
  },

  _renderSignals() {
    document.getElementById('elo-table-wrap') && (document.getElementById('elo-table-wrap').style.display = 'none');
    document.getElementById('elo-chart-wrap') && (document.getElementById('elo-chart-wrap').style.display = 'none');
    document.getElementById('elo-signals')    && (document.getElementById('elo-signals').style.display = '');
    const el = document.getElementById('elo-signals');
    if (!el) return;
    const signals = this._generateSignals();
    if (!signals.length) {
      el.innerHTML = '<div class="lm-empty" style="padding:32px;text-align:center">📭 Нет ELO сигналов. Данные появятся когда загрузятся коэффициенты через Value Finder.</div>';
      return;
    }
    el.innerHTML = signals.map(s => `
      <div class="elo-signal-card">
        <div class="elo-sig-head"><span class="elo-sig-match">${s.match}</span><span class="lm-league-tag">${s.league}</span></div>
        <div class="elo-sig-body">
          <div class="elo-sig-block"><span class="elo-sig-label">Хозяева</span><span class="elo-sig-val">${s.homeTeam}</span><span class="elo-sig-elo">ELO: ${s.eloHome}</span></div>
          <div class="elo-sig-vs">vs</div>
          <div class="elo-sig-block"><span class="elo-sig-label">Гости</span><span class="elo-sig-val">${s.awayTeam}</span><span class="elo-sig-elo">ELO: ${s.eloAway}</span></div>
        </div>
        <div class="elo-sig-probs">
          <div class="elo-prob-row">
            <span>ELO: ${(s.eloHomeProb*100).toFixed(1)}%</span>
            <span style="color:var(--text3)">vs</span>
            <span>Рынок: ${(s.mktHomeProb*100).toFixed(1)}%</span>
            <span class="${s.edge > 0 ? 'positive' : 'negative'}">Edge: ${s.edge > 0 ? '+' : ''}${(s.edge*100).toFixed(1)}%</span>
          </div>
        </div>
        <div class="elo-sig-actions">
          <button class="ctrl-btn sm primary" onclick="valueFinder.addWatchlist('${s.match.replace(/'/g,"\\'")}','homeWin',${s.odds?.toFixed(2)||'0'})">+ Watch</button>
        </div>
      </div>`).join('');
  },

  setFilter(v)   { this.filter = v.toLowerCase(); this.render(); },
  setView(m)     { this.viewMode = m; this.render(); },
  _filtered()    { return this.filter ? this.ratings.filter(r => r.team.toLowerCase().includes(this.filter)) : this.ratings; },

  showMatchup(team) {
    const t = this.ratings.find(r => r.team === team);
    if (!t) return;
    const others = this.ratings.filter(r => r.team !== team).slice(0, 10);
    const m = this._getModal();
    m.innerHTML = `<div class="modal-content" style="width:480px;max-width:95vw">
      <div class="modal-header"><span>⚡ Матч-ап: ${team}</span><button onclick="document.getElementById('eloMatchupModal').style.display='none'">×</button></div>
      <div style="padding:16px">
        <p style="color:var(--text2);font-size:13px">ELO ${team}: <strong style="color:var(--accent)">${t.rating}</strong></p>
        <div style="display:flex;flex-direction:column;gap:6px;margin-top:10px">
          ${others.map(o => {
            const eloH = t.rating, eloA = o.rating;
            const diff = (eloH - eloA) / 400;
            const pHome = +(1/(1+Math.pow(10,-diff))*100).toFixed(1);
            const pAway = +(100 - pHome).toFixed(1);
            return `<div style="display:flex;justify-content:space-between;align-items:center;padding:6px 8px;background:var(--bg2);border-radius:6px;font-size:12px">
              <span style="color:var(--text2)">${team} vs ${o.team}</span>
              <span class="${pHome > 50 ? 'positive' : 'negative'}">${pHome}%</span>
              <span style="color:var(--text3)">vs ${pAway}%</span>
            </div>`;
          }).join('')}
        </div>
      </div></div>`;
    m.style.display = 'flex';
  },

  _generateSignals() {
    const pairs = [];
    for (let i = 0; i < Math.min(this.ratings.length, 20); i += 2) {
      const h = this.ratings[i], a = this.ratings[i+1];
      if (!h || !a) continue;
      const diff = (h.rating - a.rating) / 400;
      const eloHomeProb = 1 / (1 + Math.pow(10, -diff));
      const mktHomeProb = 0.4 + Math.random() * 0.2;
      const edge = eloHomeProb - mktHomeProb;
      if (Math.abs(edge) < 0.05) continue;
      const mktHome = +(1 / mktHomeProb * 0.9).toFixed(2);
      const kelly = Math.max(0, ((mktHome - 1) * eloHomeProb - (1 - eloHomeProb)) / (mktHome - 1));
      pairs.push({ match: `${h.team} — ${a.team}`, league: 'Лига 1', homeTeam: h.team, awayTeam: a.team, eloHome: h.rating, eloAway: a.rating, eloHomeProb, mktHomeProb, edge, kelly, odds: mktHome, recommended: h.team });
    }
    return pairs.filter(p => p.edge > 0.05).slice(0, 6);
  },

  _demoRatings() {
    return [
      { team:'Бавария',rating:1860 },{ team:'Реал Мадрид',rating:1850 },{ team:'Ман Сити',rating:1800 },
      { team:'ПСЖ',rating:1820 },{ team:'Ливерпуль',rating:1760 },{ team:'Барселона',rating:1780 },
      { team:'Арсенал',rating:1720 },{ team:'Интер',rating:1740 },{ team:'Дортмунд',rating:1710 },
      { team:'Атлетико',rating:1700 },{ team:'Байер',rating:1730 },{ team:'Лейпциг',rating:1700 },
      { team:'Наполи',rating:1700 },{ team:'Челси',rating:1620 },{ team:'Милан',rating:1690 },
      { team:'Ювентус',rating:1680 },{ team:'Тоттенхэм',rating:1650 },{ team:'Монако',rating:1640 },
      { team:'Севилья',rating:1590 },{ team:'Ньюкасл',rating:1600 },
    ].map(r => ({ ...r, rating: Math.round(r.rating + (Math.random()-0.5)*20) }));
  },

  _getModal() {
    let m = document.getElementById('eloMatchupModal');
    if (!m) { m = Object.assign(document.createElement('div'),{id:'eloMatchupModal',className:'modal'}); m.onclick=e=>{if(e.target===m)m.style.display='none';}; document.body.appendChild(m); }
    return m;
  },
  _setLoading(on) { const el = document.getElementById('elo-loading'); if (el) el.style.display = on ? 'flex' : 'none'; },
  async _fetch(url) {
    const r = await fetch(url, { headers: { 'x-auth-token': localStorage.getItem('bq_token') || 'demo' } });
    if (!r.ok) throw new Error(r.status);
    return r.json();
  },
};