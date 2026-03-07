'use strict';
// ═══════════════════════════════════════════════════════════════════════════
//  BetQuant Pro — Live Monitor
//  Лайв-матчи · in-play коэффициенты · нейросетевые сигналы
// ═══════════════════════════════════════════════════════════════════════════
const liveMonitor = {
  matches:    [],
  selectedId: null,
  oddsChart:  null,
  timer:      null,
  POLL: 30000,

  // ── lifecycle ─────────────────────────────────────────────────────────────
  async init() {
    await this.loadMatches();
    this.startPoll();
  },
  destroy() {
    this.stopPoll();
    if (this.oddsChart) { try { this.oddsChart.destroy(); } catch(e){} this.oddsChart = null; }
  },

  startPoll() { this.stopPoll(); this.timer = setInterval(() => this.loadMatches(true), this.POLL); },
  stopPoll()  { clearInterval(this.timer); this.timer = null; },

  // ── data ──────────────────────────────────────────────────────────────────
  async loadMatches(silent = false) {
    if (!silent) this._loading('lm-grid-loading', true);
    try {
      const d = await this._fetch('/api/live/matches');
      if (!d) return;
      this.matches = d.matches || [];
      this.renderGrid();
      this._liveBadge(d.liveCount || 0);
      if (this.selectedId) this.selectMatch(this.selectedId, true);
    } catch(e) { console.warn('[liveMonitor]', e); }
    finally    { this._loading('lm-grid-loading', false); }
  },

  async selectMatch(id, silent = false) {
    this.selectedId = id;
    document.querySelectorAll('.lm-card').forEach(c => c.classList.toggle('selected', c.dataset.id === id));
    if (!silent) this._loading('lm-detail-loading', true);
    try {
      const [det, hist] = await Promise.all([
        this._fetch(`/api/live/match/${id}`),
        this._fetch(`/api/live/odds/${id}`),
      ]);
      if (det) this.renderDetail(det, hist?.history || []);
    } catch(e) { console.warn('[liveMonitor] detail', e); }
    finally    { this._loading('lm-detail-loading', false); }
  },

  // ── grid ──────────────────────────────────────────────────────────────────
  renderGrid() {
    const live  = this.matches.filter(m => m.status === 'live');
    const sched = this.matches.filter(m => m.status === 'scheduled');
    const lEl   = document.getElementById('lm-live-grid');
    const sEl   = document.getElementById('lm-sched-grid');
    if (lEl) lEl.innerHTML = live.length  ? live.map(m  => this._card(m)).join('')  : '<div class="lm-empty">Нет активных матчей</div>';
    if (sEl) sEl.innerHTML = sched.length ? sched.map(m => this._card(m)).join('') : '<div class="lm-empty">Нет запланированных</div>';
  },

  _card(m) {
    const live = m.status === 'live';
    const sel  = m.id === this.selectedId ? 'selected' : '';
    const oddH = m.odds?.home  ? `<span>${(+m.odds.home).toFixed(2)}</span>` : '';
    const oddD = m.odds?.draw  ? `<span>${(+m.odds.draw).toFixed(2)}</span>` : '';
    const oddA = m.odds?.away  ? `<span>${(+m.odds.away).toFixed(2)}</span>` : '';
    return `
    <div class="lm-card ${sel}" data-id="${m.id}" onclick="liveMonitor.selectMatch('${m.id}')">
      <div class="lm-card-head">
        <span class="lm-league-tag">${m.league}</span>
        ${live
          ? `<span class="lm-min-badge live">${m.minute}'</span><span class="lm-live-dot"></span>`
          : `<span class="lm-min-badge">${this._fmtTime(m.startTime)}</span>`}
      </div>
      <div class="lm-card-teams">
        <span class="lm-team">${m.home}</span>
        <span class="lm-score">${live ? `${m.homeScore ?? 0} : ${m.awayScore ?? 0}` : 'vs'}</span>
        <span class="lm-team right">${m.away}</span>
      </div>
      ${oddH ? `<div class="lm-card-odds">${oddH}${oddD}${oddA}</div>` : ''}
    </div>`;
  },

  // ── detail ────────────────────────────────────────────────────────────────
  renderDetail(m, oddsHistory) {
    const el = document.getElementById('lm-detail');
    if (!el) return;
    const live = m.status === 'live';
    const s = m.stats || {};

    el.innerHTML = `
      <div class="lm-det-header">
        <div class="lm-det-meta">
          <span class="lm-league-tag">${m.league}</span>
          ${live ? `<span class="lm-min-badge live">${m.minute}'</span>` : `<span class="lm-min-badge">${this._fmtTime(m.startTime)}</span>`}
        </div>
        <div class="lm-det-score">
          <span class="lm-det-team">${m.home}</span>
          <span class="lm-det-num">${live ? `${m.homeScore} : ${m.awayScore}` : 'vs'}</span>
          <span class="lm-det-team right">${m.away}</span>
        </div>
        <div class="lm-det-odds">${this._oddsRow(m.odds, m.openOdds)}</div>
      </div>

      ${live ? this._statsSection(s) : ''}

      <div class="lm-section">
        <div class="lm-sec-title">📈 Движение коэффициентов</div>
        <div style="position:relative;height:180px"><canvas id="lm-odds-cvs"></canvas></div>
      </div>

      ${m.signal?.signals?.length ? this._signalSection(m.signal) : ''}

      <div class="lm-section">
        <div class="lm-sec-title">📋 События</div>
        <div class="lm-events">${this._events(m.events || [])}</div>
      </div>`;

    setTimeout(() => {
      this._initChart();
      this._updateChart(oddsHistory);
    }, 40);
  },

  _oddsRow(odds, open) {
    if (!odds) return '';
    return [
      { k: 'home', l: '1' }, { k: 'draw', l: 'X' }, { k: 'away', l: '2' }
    ].filter(x => odds[x.k]).map(x => {
      const cur  = +odds[x.k], op = open?.[x.k] ? +open[x.k] : null;
      const diff = op ? cur - op : 0;
      const cls  = diff < -.1 ? 'down' : diff > .1 ? 'up' : '';
      const arr  = diff < -.1 ? '▼' : diff > .1 ? '▲' : '';
      return `<div class="lm-odds-chip ${cls}">
        <span class="lm-odds-lbl">${x.l}</span>
        <span class="lm-odds-val">${cur.toFixed(2)}</span>
        ${arr ? `<span class="lm-odds-arr">${arr}</span>` : ''}
      </div>`;
    }).join('');
  },

  _statsSection(s) {
    const bar = (hv, av, lbl) => {
      const tot = ((+hv || 0) + (+av || 0)) || 1;
      const hp  = Math.round((+hv || 0) / tot * 100);
      return `<div class="lm-stat-row">
        <span class="lm-sval home">${hv ?? '—'}</span>
        <div class="lm-sbar"><div class="lm-sfill h" style="width:${hp}%"></div><div class="lm-sfill a" style="width:${100-hp}%"></div></div>
        <span class="lm-slbl">${lbl}</span>
        <span class="lm-sval away">${av ?? '—'}</span>
      </div>`;
    };
    return `<div class="lm-section">
      <div class="lm-sec-title">📊 Статистика</div>
      <div class="lm-stats">
        ${bar(s.home_sot,     s.away_sot,     'Удары в цель')}
        ${bar(s.home_shots,   s.away_shots,   'Удары всего')}
        ${bar(s.home_poss,    s.away_poss,    'Владение %')}
        ${bar(s.home_corners, s.away_corners, 'Угловые')}
        ${bar(s.home_da,      s.away_da,      'Опасные атаки')}
        ${bar((s.home_xg||0).toFixed(2), (s.away_xg||0).toFixed(2), 'xG')}
      </div>
    </div>`;
  },

  _signalSection(sig) {
    return `<div class="lm-section lm-signals">
      <div class="lm-sec-title">🧠 In-Play Сигналы</div>
      ${sig.signals.map((s, i) => `
        <div class="lm-signal-card ${i === 0 ? 'top' : ''}">
          <div class="lm-sig-head">
            <span class="lm-sig-label">${s.label}</span>
            <span class="lm-conf ${s.confidence >= 70 ? 'hi' : s.confidence >= 55 ? 'md' : 'lo'}">${s.confidence.toFixed(0)}%</span>
          </div>
          <div class="lm-sig-rat">${s.rationale}</div>
          ${s.odds ? `<div class="lm-sig-act">
            <span class="lm-sig-mkt">${s.market}</span>
            <span class="lm-sig-odds">@ ${(+s.odds).toFixed(2)}</span>
            <button class="ctrl-btn sm" onclick="liveMonitor.addCLV('${sig.matchId}','${s.market}',${s.odds})">+ CLV</button>
          </div>` : ''}
        </div>`).join('')}
      <div class="lm-sig-meta">
        Прогноз: <strong>${sig.projectedGoals} гол.</strong> &nbsp;|&nbsp;
        Осталось: <strong>${sig.remaining}'</strong> &nbsp;|&nbsp;
        Риск: <span class="lm-risk-${sig.riskLevel}">${{low:'Низкий',medium:'Средний',high:'Высокий'}[sig.riskLevel]}</span>
      </div>
    </div>`;
  },

  _events(events) {
    if (!events.length) return '<div class="lm-empty">Событий нет</div>';
    const ICON = { goal:'⚽', yellow:'🟡', red:'🔴', penalty:'⚽🅿', sub:'🔄' };
    return events.slice().reverse().map(e => `
      <div class="lm-event ${e.team}">
        <span class="lm-ev-min">${e.minute}'</span>
        <span class="lm-ev-ico">${ICON[e.type] || '•'}</span>
        <span class="lm-ev-txt">
          <strong>${e.player || ''}</strong>
          ${e.assist ? `<span class="lm-assist">(${e.assist})</span>` : ''}
          ${e.score  ? `<span class="lm-ev-score">${e.score}</span>` : ''}
        </span>
      </div>`).join('');
  },

  // ── odds chart ────────────────────────────────────────────────────────────
  _initChart() {
    const cvs = document.getElementById('lm-odds-cvs');
    if (!cvs) return;
    if (this.oddsChart) { try { this.oddsChart.destroy(); } catch(e){} }
    const dk = document.body.classList.contains('dark-mode');
    const tc = dk ? '#94a3b8' : '#475569', gc = dk ? 'rgba(255,255,255,.05)' : 'rgba(0,0,0,.06)';
    this.oddsChart = new Chart(cvs, {
      type: 'line',
      data: { labels: [], datasets: [
        { label:'1 (Хозяева)', data:[], borderColor:'#00d4ff', borderWidth:2, pointRadius:2, tension:.3, fill:false },
        { label:'X (Ничья)',   data:[], borderColor:'#ffd740', borderWidth:2, pointRadius:2, tension:.3, fill:false },
        { label:'2 (Гости)',   data:[], borderColor:'#ff4560', borderWidth:2, pointRadius:2, tension:.3, fill:false },
      ]},
      options: {
        responsive:true, maintainAspectRatio:false, animation:false,
        plugins:{ legend:{ labels:{ color:tc, font:{size:11} }}, tooltip:{ mode:'index', intersect:false }},
        scales:{ x:{ ticks:{color:tc,font:{size:10},maxTicksLimit:10}, grid:{color:gc} },
                 y:{ ticks:{color:tc,font:{size:10}}, grid:{color:gc} }},
      },
    });
  },

  _updateChart(history) {
    if (!this.oddsChart || !history?.length) return;
    this.oddsChart.data.labels           = history.map(h => h.phase === 'live' ? `${h.minute}'` : this._fmtTime(h.t, true));
    this.oddsChart.data.datasets[0].data = history.map(h => h.home);
    this.oddsChart.data.datasets[1].data = history.map(h => h.draw);
    this.oddsChart.data.datasets[2].data = history.map(h => h.away);
    this.oddsChart.update('none');
  },

  // ── CLV quick-add ─────────────────────────────────────────────────────────
  async addCLV(matchId, market, odds) {
    const m = this.matches.find(x => x.id === matchId);
    if (!m) return;
    await this._fetch('/api/clv/bet', 'POST', {
      matchName: `${m.home} vs ${m.away}`, market, betOdds: odds, stake: 10,
    });
    this._toast(`✅ Добавлено в CLV: ${market} @ ${(+odds).toFixed(2)}`);
  },

  // ── utils ─────────────────────────────────────────────────────────────────
  _fmtTime(iso, short = false) {
    if (!iso) return '—';
    return new Date(iso).toLocaleTimeString('ru', { hour:'2-digit', minute:'2-digit' });
  },
  _liveBadge(n) {
    const b = document.querySelector('[data-panel="live"] .lm-badge');
    if (b) { b.textContent = n || ''; b.style.display = n ? '' : 'none'; }
  },
  _loading(id, on) {
    const el = document.getElementById(id);
    if (el) el.style.display = on ? 'flex' : 'none';
  },
  _toast(msg) {
    let t = document.getElementById('bq-toast');
    if (!t) { t = Object.assign(document.createElement('div'), { id:'bq-toast', className:'bq-toast' }); document.body.append(t); }
    t.textContent = msg; t.classList.add('show');
    clearTimeout(t._tm); t._tm = setTimeout(() => t.classList.remove('show'), 3000);
  },
  async _fetch(url, method = 'GET', body = null) {
    const opts = { method, headers: { 'Content-Type': 'application/json', 'x-auth-token': localStorage.getItem('bq_token') || 'demo' } };
    if (body) opts.body = JSON.stringify(body);
    const r = await fetch(url, opts);
    if (!r.ok) throw new Error(`${r.status}`);
    return r.json();
  },
};