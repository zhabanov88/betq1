'use strict';
// ═══════════════════════════════════════════════════════════════════════════
//  BetQuant Pro — Live Monitor
//  Реальные данные из API-Football. Демо — только при bq_demo_mode=true.
// ═══════════════════════════════════════════════════════════════════════════
const liveMonitor = {
  matches:    [],
  selectedId: null,
  oddsChart:  null,
  timer:      null,
  _hint:      null,
  _source:    'none',
  POLL: 30000,

  async init() { await this.loadMatches(); this.startPoll(); },
  destroy()    { this.stopPoll(); if (this.oddsChart) { try { this.oddsChart.destroy(); } catch(e){} this.oddsChart = null; } },
  startPoll()  { this.stopPoll(); this.timer = setInterval(() => this.loadMatches(true), this.POLL); },
  stopPoll()   { clearInterval(this.timer); this.timer = null; },

  async loadMatches(silent = false) {
    if (!silent) this._loading('lm-grid-loading', true);
    try {
      const isDemoMode = localStorage.getItem('bq_demo_mode') === 'true';
      const url = isDemoMode ? '/api/live/matches?demo=true' : '/api/live/matches';
      const d   = await this._fetch(url);
      if (!d) return;
      this.matches = d.matches || [];
      this._hint   = d.hint   || null;
      this._source = d.source || 'none';
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

  renderGrid() {
    const live  = this.matches.filter(m => m.status === 'live');
    const sched = this.matches.filter(m => m.status === 'scheduled');
    const lEl   = document.getElementById('lm-live-grid');
    const sEl   = document.getElementById('lm-sched-grid');

    if (!this.matches.length) {
      const noDataHtml = `<div style="padding:48px;text-align:center">
        <div style="font-size:36px;margin-bottom:12px">📭</div>
        <div style="color:var(--text2);margin-bottom:8px">Нет данных о матчах</div>
        <div style="color:var(--text3);font-size:12px;line-height:1.7">
          ${this._hint || 'Добавьте <strong>API_FOOTBALL_KEY</strong> в .env для получения лайв-данных.<br>Или включите <strong>Тестовые данные</strong> в настройках.'}
        </div>
        <div style="display:flex;gap:8px;justify-content:center;margin-top:16px">
          <button class="ctrl-btn" onclick="liveMonitor.loadMatches()">🔄 Обновить</button>
          <a href="https://www.api-football.com" target="_blank" class="ctrl-btn">🔑 Получить ключ</a>
        </div>
      </div>`;
      if (lEl) lEl.innerHTML = noDataHtml;
      if (sEl) sEl.innerHTML = '';
      return;
    }

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
        ${live ? `<span class="lm-min-badge live">${m.minute}'</span><span class="lm-live-dot"></span>` : `<span class="lm-min-badge">${this._fmtTime(m.startTime)}</span>`}
      </div>
      <div class="lm-card-teams">
        <span class="lm-team">${m.home}</span>
        <span class="lm-score">${live ? `${m.homeScore??0} : ${m.awayScore??0}` : 'vs'}</span>
        <span class="lm-team right">${m.away}</span>
      </div>
      ${oddH ? `<div class="lm-card-odds">${oddH}${oddD}${oddA}</div>` : ''}
    </div>`;
  },

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
      ${m.events?.length ? `<div class="lm-section"><div class="lm-sec-title">📋 События</div><div class="lm-events">${this._events(m.events)}</div></div>` : ''}
    `;
    this._initChart();
    if (oddsHistory?.length) this._updateChart(oddsHistory);
  },

  _oddsRow(odds, openOdds) {
    if (!odds) return '';
    const mkts = [['home','1'],['draw','X'],['away','2']];
    return `<div style="display:flex;gap:12px;justify-content:center">
      ${mkts.map(([k,l]) => !odds[k] ? '' : `<div style="text-align:center">
        <div style="font-size:10px;color:var(--text3)">${l}</div>
        <div style="font-size:18px;font-weight:700;color:var(--accent)">${(+odds[k]).toFixed(2)}</div>
        ${openOdds?.[k] ? `<div style="font-size:10px;color:var(--text3)">Откр: ${(+openOdds[k]).toFixed(2)}</div>` : ''}
      </div>`).join('')}
    </div>`;
  },

  _statsSection(s) {
    if (!Object.keys(s).length) return '';
    const row = (label, h, a) => `<div class="lm-stat-row">
      <span class="lm-stat-val home">${h}</span>
      <span class="lm-stat-label">${label}</span>
      <span class="lm-stat-val away">${a}</span>
    </div>`;
    return `<div class="lm-section lm-stats">
      <div class="lm-sec-title">📊 Статистика матча</div>
      ${row('Удары', s.home_shots||0, s.away_shots||0)}
      ${row('В створ', s.home_sot||0, s.away_sot||0)}
      ${row('Владение %', s.home_poss||0, s.away_poss||0)}
      ${row('Угловые', s.home_corners||0, s.away_corners||0)}
      ${row('xG', (s.home_xg||0).toFixed(2), (s.away_xg||0).toFixed(2))}
      ${row('Опасные атаки', s.home_da||0, s.away_da||0)}
    </div>`;
  },

  _signalSection(sig) {
    if (!sig?.signals?.length) return '';
    return `<div class="lm-section lm-signals">
      <div class="lm-sec-title">💡 Сигналы (${sig.signals.length})</div>
      ${sig.signals.slice(0,3).map(s => `
        <div class="lm-signal-card ${s.confidence >= 70 ? 'top' : ''}">
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
        <span class="lm-ev-ico">${ICON[e.type]||'•'}</span>
        <span class="lm-ev-txt"><strong>${e.player||''}</strong>
          ${e.assist ? `<span class="lm-assist">(${e.assist})</span>` : ''}
          ${e.score  ? `<span class="lm-ev-score">${e.score}</span>` : ''}
        </span>
      </div>`).join('');
  },

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
      options: { responsive:true, maintainAspectRatio:false, animation:false,
        plugins:{ legend:{labels:{color:tc,font:{size:11}}}, tooltip:{mode:'index',intersect:false} },
        scales:{ x:{ticks:{color:tc,font:{size:10},maxTicksLimit:10},grid:{color:gc}}, y:{ticks:{color:tc,font:{size:10}},grid:{color:gc}} } },
    });
  },

  _updateChart(history) {
    if (!this.oddsChart || !history?.length) return;
    this.oddsChart.data.labels           = history.map(h => h.phase==='live' ? `${h.minute}'` : this._fmtTime(h.t, true));
    this.oddsChart.data.datasets[0].data = history.map(h => h.home);
    this.oddsChart.data.datasets[1].data = history.map(h => h.draw);
    this.oddsChart.data.datasets[2].data = history.map(h => h.away);
    this.oddsChart.update('none');
  },

  async addCLV(matchId, market, odds) {
    const m = this.matches.find(x => x.id === matchId);
    if (!m) return;
    await this._fetch('/api/clv/bet', 'POST', { matchName:`${m.home} vs ${m.away}`, market, betOdds:odds, stake:10 });
    this._toast(`✅ Добавлено в CLV: ${market} @ ${(+odds).toFixed(2)}`);
  },

  _fmtTime(iso, short = false) {
    if (!iso) return '—';
    return new Date(iso).toLocaleTimeString('ru', { hour:'2-digit', minute:'2-digit' });
  },
  _liveBadge(n) {
    const b = document.querySelector('[data-panel="live"] .lm-badge');
    if (b) { b.textContent = n||''; b.style.display = n ? '' : 'none'; }
  },
  _loading(id, on) { const el = document.getElementById(id); if (el) el.style.display = on ? 'flex' : 'none'; },
  _toast(msg) {
    let t = document.getElementById('bq-toast');
    if (!t) { t = Object.assign(document.createElement('div'),{id:'bq-toast',className:'bq-toast'}); document.body.append(t); }
    t.textContent=msg; t.classList.add('show'); clearTimeout(t._tm); t._tm=setTimeout(()=>t.classList.remove('show'),3000);
  },
  async _fetch(url, method='GET', body=null) {
    const opts = { method, headers:{'Content-Type':'application/json','x-auth-token':localStorage.getItem('bq_token')||'demo'} };
    if (body) opts.body = JSON.stringify(body);
    const r = await fetch(url, opts);
    if (!r.ok) throw new Error(`${r.status}`);
    return r.json();
  },
};