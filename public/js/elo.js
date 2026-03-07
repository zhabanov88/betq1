'use strict';
// ═══════════════════════════════════════════════════════════════════════════
//  BetQuant Pro — ELO Ratings System
//  Рейтинги команд, тренды, сигналы расхождения с рынком
// ═══════════════════════════════════════════════════════════════════════════
const eloPanel = {
  ratings:  [],
  charts:   {},
  filter:   '',
  sport:    'football',
  viewMode: 'table',   // 'table' | 'chart' | 'signals'

  // ── lifecycle ─────────────────────────────────────────────────────────────
  async init() {
    await this.load();
  },

  async load() {
    this._setLoading(true);
    try {
      const d = await this._fetch('/api/value/elo');
      if (d?.ratings) {
        this.ratings = d.ratings;
        this.render();
      }
    } catch(e) {
      // если данных нет — показываем встроенные демо
      this.ratings = this._demoRatings();
      this.render();
    } finally {
      this._setLoading(false);
    }
  },

  // ── render ────────────────────────────────────────────────────────────────
  render() {
    this._renderControls();
    if (this.viewMode === 'table')   this._renderTable();
    if (this.viewMode === 'chart')   this._renderChart();
    if (this.viewMode === 'signals') this._renderSignals();
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
    document.getElementById('elo-table-wrap')?.style && (document.getElementById('elo-table-wrap').style.display = '');
    document.getElementById('elo-chart-wrap')?.style && (document.getElementById('elo-chart-wrap').style.display = 'none');
    document.getElementById('elo-signals')?.style    && (document.getElementById('elo-signals').style.display = 'none');

    const el = document.getElementById('elo-body');
    if (!el) return;

    const list = this._filtered();
    if (!list.length) { el.innerHTML = '<tr><td colspan="5" style="text-align:center;color:var(--text3);padding:24px">Нет данных</td></tr>'; return; }

    const maxR = Math.max(...list.map(r => r.rating));
    const minR = Math.min(...list.map(r => r.rating));

    el.innerHTML = list.map((r, i) => {
      const pct  = ((r.rating - minR) / (maxR - minR) * 100) || 0;
      const tier = r.rating >= 1750 ? '🔴' : r.rating >= 1650 ? '🟠' : r.rating >= 1550 ? '🟡' : '⚪';
      const diff = r.prev ? r.rating - r.prev : 0;
      const diffHtml = diff ? `<span class="${diff > 0 ? 'positive' : 'negative'}" style="font-size:11px">${diff > 0 ? '▲' : '▼'}${Math.abs(diff).toFixed(0)}</span>` : '';
      return `<tr>
        <td style="color:var(--text3);font-size:11px">#${i + 1}</td>
        <td>${tier} <strong>${r.team}</strong></td>
        <td>
          <div style="display:flex;align-items:center;gap:8px">
            <strong style="font-family:var(--font-mono);color:var(--accent)">${r.rating}</strong>
            ${diffHtml}
          </div>
        </td>
        <td>
          <div class="elo-bar-wrap">
            <div class="elo-bar" style="width:${pct}%"></div>
          </div>
        </td>
        <td>
          <button class="ctrl-btn sm" onclick="eloPanel.showMatchup('${r.team.replace(/'/g,"\\'")}')">vs Команда</button>
        </td>
      </tr>`;
    }).join('');
  },

  _renderChart() {
    document.getElementById('elo-table-wrap')?.style && (document.getElementById('elo-table-wrap').style.display = 'none');
    document.getElementById('elo-chart-wrap')?.style && (document.getElementById('elo-chart-wrap').style.display = '');
    document.getElementById('elo-signals')?.style    && (document.getElementById('elo-signals').style.display = 'none');


    if (this.charts.elo) { try { this.charts.elo.destroy(); } catch(e){} }
    const cvs = document.getElementById('elo-chart-canvas');
    if (!cvs) return;

    const top = this._filtered().slice(0, 20);
    const dk  = document.body.classList.contains('dark-mode');
    const tc  = dk ? '#8892a4' : '#4a5568', gc = dk ? 'rgba(255,255,255,.05)' : 'rgba(0,0,0,.07)';

    // Цвет по рейтингу
    const colors = top.map(r =>
      r.rating >= 1750 ? 'rgba(255,69,96,0.8)'
      : r.rating >= 1650 ? 'rgba(255,160,64,0.8)'
      : r.rating >= 1550 ? 'rgba(0,212,255,0.8)'
      : 'rgba(148,163,184,0.6)'
    );

    this.charts.elo = new Chart(cvs, {
      type: 'bar',
      data: {
        labels: top.map(r => r.team),
        datasets: [{
          label: 'ELO Rating',
          data:  top.map(r => r.rating),
          backgroundColor: colors,
          borderRadius: 5,
        }],
      },
      options: {
        indexAxis: 'y',
        responsive: true, maintainAspectRatio: false,
        plugins: {
          legend: { display: false },
          tooltip: { callbacks: { label: ctx => `ELO: ${ctx.parsed.x}` } },
        },
        scales: {
          x: { ticks:{ color:tc, font:{size:10} }, grid:{color:gc}, min: 1400 },
          y: { ticks:{ color:tc, font:{size:10} }, grid:{color:gc} },
        },
      },
    });
  },

  _renderSignals() {
    document.getElementById('elo-table-wrap')?.style && (document.getElementById('elo-table-wrap').style.display = 'none');
    document.getElementById('elo-chart-wrap')?.style && (document.getElementById('elo-chart-wrap').style.display = 'none');
    document.getElementById('elo-signals')?.style    && (document.getElementById('elo-signals').style.display = '');

    const el = document.getElementById('elo-signals');
    if (!el) return;

    // Генерируем примеры сигналов расхождения ELO vs рынок
    const signals = this._generateSignals();
    if (!signals.length) {
      el.innerHTML = '<div class="lm-empty">Загрузи Value Finder для получения ELO-сигналов</div>';
      return;
    }
    el.innerHTML = signals.map(s => `
      <div class="elo-signal-card">
        <div class="elo-sig-head">
          <span class="elo-sig-match">${s.match}</span>
          <span class="lm-league-tag">${s.league}</span>
        </div>
        <div class="elo-sig-body">
          <div class="elo-sig-block">
            <span class="elo-sig-label">Хозяева</span>
            <span class="elo-sig-val">${s.homeTeam}</span>
            <span class="elo-sig-elo">ELO: ${s.eloHome}</span>
          </div>
          <div class="elo-sig-vs">vs</div>
          <div class="elo-sig-block">
            <span class="elo-sig-label">Гости</span>
            <span class="elo-sig-val">${s.awayTeam}</span>
            <span class="elo-sig-elo">ELO: ${s.eloAway}</span>
          </div>
        </div>
        <div class="elo-sig-probs">
          <div class="elo-prob-row">
            <span>ELO prob:</span>
            <span>${(s.eloHomeProb*100).toFixed(1)}%</span>
            <span style="color:var(--text3)">vs</span>
            <span>Рынок: ${(s.mktHomeProb*100).toFixed(1)}%</span>
            <span class="${s.edge > 0 ? 'positive' : 'negative'}">Edge: ${s.edge > 0 ? '+' : ''}${(s.edge*100).toFixed(1)}%</span>
          </div>
          ${s.edge > 0.04 ? `
            <div class="elo-bet-hint">
              ✅ ELO говорит: <strong>Ставка на ${s.recommended}</strong> — коэф <strong>${s.odds}</strong>
            </div>` : ''}
        </div>
      </div>`).join('');
  },

  // ── Matchup calculator ────────────────────────────────────────────────────
  showMatchup(team) {
    const modal = this._getModal();
    const teams = this._filtered().map(r => r.team);

    modal.innerHTML = `
      <div class="modal-content" style="max-width:480px">
        <div class="modal-header">
          <span>⚡ ELO Matchup Calculator</span>
          <button onclick="this.closest('.modal').style.display='none'">×</button>
        </div>
        <div class="config-section">
          <div class="config-row">
            <label>Хозяева</label>
            <select class="ctrl-select" id="mcHome" onchange="eloPanel.calcMatchup()">
              ${teams.map(t => `<option value="${t}" ${t === team ? 'selected' : ''}>${t}</option>`).join('')}
            </select>
          </div>
          <div class="config-row">
            <label>Гости</label>
            <select class="ctrl-select" id="mcAway" onchange="eloPanel.calcMatchup()">
              ${teams.map((t, i) => `<option value="${t}" ${i === 1 ? 'selected' : ''}>${t}</option>`).join('')}
            </select>
          </div>
          <div class="config-row">
            <label>Рыночный коэф (1)</label>
            <input type="number" class="ctrl-input" id="mcMktHome" step="0.01" placeholder="2.10" oninput="eloPanel.calcMatchup()">
          </div>
        </div>
        <div id="mc-result" style="padding:12px 0"></div>
      </div>`;

    modal.style.display = 'flex';
    this.calcMatchup();
  },

  calcMatchup() {
    const homeEl = document.getElementById('mcHome');
    const awayEl = document.getElementById('mcAway');
    const mktEl  = document.getElementById('mcMktHome');
    const resEl  = document.getElementById('mc-result');
    if (!homeEl || !awayEl || !resEl) return;

    const homeTeam = homeEl.value;
    const awayTeam = awayEl.value;
    const rH = this.ratings.find(r => r.team === homeTeam)?.rating || 1500;
    const rA = this.ratings.find(r => r.team === awayTeam)?.rating || 1500;

    // ELO с домашним преимуществом +65
    const expH = 1 / (1 + Math.pow(10, (rA - (rH + 65)) / 400));
    const draw  = 0.22;
    const pHome = expH * (1 - draw);
    const pAway = (1 - expH) * (1 - draw);
    const pDraw = draw;

    const mktOdds = parseFloat(mktEl?.value) || 0;
    const mktImpl  = mktOdds > 1 ? 1 / mktOdds : 0;
    const edge     = mktImpl ? pHome - mktImpl : null;
    const kelly    = (edge > 0 && mktOdds > 1)
      ? ((mktOdds - 1) * pHome - (1 - pHome)) / (mktOdds - 1)
      : 0;

    resEl.innerHTML = `
      <div class="elo-matchup-result">
        <div class="kpi-row">
          <div class="kpi-card"><span>ELO</span><strong>${homeTeam}</strong><em>${rH}</em></div>
          <div class="kpi-card" style="font-size:18px">vs</div>
          <div class="kpi-card"><span>ELO</span><strong>${awayTeam}</strong><em>${rA}</em></div>
        </div>
        <div class="kpi-row" style="margin-top:10px">
          <div class="kpi-card"><span>Победа хозяев</span><strong class="positive">${(pHome*100).toFixed(1)}%</strong></div>
          <div class="kpi-card"><span>Ничья</span><strong>${(pDraw*100).toFixed(1)}%</strong></div>
          <div class="kpi-card"><span>Победа гостей</span><strong class="negative">${(pAway*100).toFixed(1)}%</strong></div>
        </div>
        ${edge !== null ? `
          <div class="kpi-row" style="margin-top:10px">
            <div class="kpi-card"><span>Рыночная вер.</span><strong>${(mktImpl*100).toFixed(1)}%</strong></div>
            <div class="kpi-card"><span>Edge ELO</span><strong class="${edge > 0 ? 'positive' : 'negative'}">${edge > 0 ? '+' : ''}${(edge*100).toFixed(2)}%</strong></div>
            <div class="kpi-card"><span>Kelly %</span><strong>${(kelly*100).toFixed(1)}%</strong></div>
          </div>
          ${edge > 0.04 ? '<div class="elo-bet-hint">✅ ELO видит положительный edge — рассмотри ставку</div>' : ''}` : ''}
      </div>`;
  },

  // ── helpers ───────────────────────────────────────────────────────────────
  setFilter(v) { this.filter = v.toLowerCase(); this.render(); },
  setView(v)   { this.viewMode = v; this.render(); },

  _filtered() {
    return this.filter
      ? this.ratings.filter(r => r.team.toLowerCase().includes(this.filter))
      : this.ratings;
  },

  _generateSignals() {
    // Симулируем сигналы расхождения ELO vs рынок
    const fixtures = [
      { match:'Arsenal vs Man City', league:'PL', homeTeam:'Arsenal', awayTeam:'Man City', eloHome:1720, eloAway:1800, mktHome:3.10 },
      { match:'Real Madrid vs Barcelona', league:'La Liga', homeTeam:'Real Madrid', awayTeam:'Barcelona', eloHome:1850, eloAway:1780, mktHome:2.05 },
      { match:'Bayern vs Leverkusen', league:'BL', homeTeam:'Bayern', awayTeam:'Leverkusen', eloHome:1860, eloAway:1730, mktHome:1.68 },
      { match:'Inter vs Juventus', league:'SA', homeTeam:'Inter', awayTeam:'Juventus', eloHome:1740, eloAway:1680, mktHome:2.10 },
    ];
    return fixtures.map(f => {
      const rH = f.eloHome + 65;
      const rA = f.eloAway;
      const eH = 1 / (1 + Math.pow(10, (rA - rH) / 400));
      const draw = 0.22;
      const eloHomeProb = eH * (1 - draw);
      const mktHomeProb = 1 / f.mktHome;
      const edge = eloHomeProb - mktHomeProb;
      const kelly = edge > 0 ? ((f.mktHome - 1) * eloHomeProb - (1 - eloHomeProb)) / (f.mktHome - 1) : 0;
      return { ...f, eloHomeProb, mktHomeProb, edge, kelly, odds: f.mktHome, recommended: f.homeTeam };
    });
  },

  _demoRatings() {
    return [
      { team:'Bayern Munich', rating:1860 }, { team:'Real Madrid', rating:1850 },
      { team:'Manchester City', rating:1800 }, { team:'PSG', rating:1820 },
      { team:'Liverpool', rating:1760 }, { team:'Barcelona', rating:1780 },
      { team:'Arsenal', rating:1720 }, { team:'Inter Milan', rating:1740 },
      { team:'Borussia Dortmund', rating:1710 }, { team:'Atletico Madrid', rating:1700 },
      { team:'Leverkusen', rating:1730 }, { team:'Leipzig', rating:1700 },
      { team:'Napoli', rating:1700 }, { team:'Chelsea', rating:1620 },
      { team:'AC Milan', rating:1690 }, { team:'Juventus', rating:1680 },
      { team:'Tottenham', rating:1650 }, { team:'Monaco', rating:1640 },
      { team:'Sevilla', rating:1590 }, { team:'Newcastle', rating:1600 },
    ].map(r => ({ ...r, rating: Math.round(r.rating + (Math.random() - 0.5) * 20) }));
  },

  _getModal() {
    let m = document.getElementById('eloMatchupModal');
    if (!m) {
      m = Object.assign(document.createElement('div'), { id:'eloMatchupModal', className:'modal' });
      m.onclick = e => { if (e.target === m) m.style.display = 'none'; };
      document.body.appendChild(m);
    }
    return m;
  },

  _setLoading(on) {
    const el = document.getElementById('elo-loading');
    if (el) el.style.display = on ? 'flex' : 'none';
  },

  async _fetch(url) {
    const r = await fetch(url, { headers: { 'x-auth-token': localStorage.getItem('bq_token') || 'demo' } });
    if (!r.ok) throw new Error(r.status);
    return r.json();
  },
};