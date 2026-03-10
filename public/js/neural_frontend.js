'use strict';
// ═══════════════════════════════════════════════════════════════════════════
//  BetQuant Neural Networks Panel  v5
//  public/js/neural_frontend.js
//
//  НОВОЕ в v5:
//  • 11 видов спорта (было 4)
//  • Панель "Анализ матча" — POST /api/neural/markets/:sport
//    → value bets, все рынки сгруппированно, тренды команды
//  • Форма предикта с вводом коэффициентов
//  • Фильтр рынков по группе / min probability / только value
//  • Экспорт анализа матча в JSON/CSV
//  • Расширенные группы признаков (25+)
//  • Полная поддержка всех targets v5
// ═══════════════════════════════════════════════════════════════════════════

const neuralPanel = {
  activeSport:    'football',
  activeTab:      'status',  // status | weights | strategies | match
  statusData:     {},
  weightsData:    {},
  strategiesData: {},
  matchData:      {},
  _trainLoading:  {},
  _matchLoading:  false,

  ALL_SPORTS: ['football','hockey','tennis','basketball','volleyball','nfl','rugby','cricket','waterpolo','esports','baseball'],

  GROUP_META: {
    elo:        { icon:'⚡', label:'ELO рейтинг',          color:'#f59e0b' },
    poisson:    { icon:'📐', label:'Poisson (λ-голы)',      color:'#8b5cf6' },
    form_venue: { icon:'🏟️', label:'Дом/Выезд форма',      color:'#fb923c' },
    form10:     { icon:'📊', label:'Форма L-10',            color:'#22d3ee' },
    momentum:   { icon:'📈', label:'Моментум / Серия',      color:'#4ade80' },
    h2h:        { icon:'🤜', label:'H2H история',           color:'#e879f9' },
    fatigue:    { icon:'😮‍💨', label:'Усталость / B2B',     color:'#94a3b8' },
    xg:         { icon:'🎯', label:'xG / удары',            color:'#34d399' },
    market:     { icon:'💰', label:'Рынок / коэф.',         color:'#60a5fa' },
    season:     { icon:'📅', label:'Сезон / позиция',       color:'#f97316' },
    psych:      { icon:'🧠', label:'Психология',            color:'#a78bfa' },
    clash:      { icon:'⚔️', label:'Матч стилей',           color:'#f43f5e' },
    style:      { icon:'🎨', label:'Стиль игры',            color:'#06b6d4' },
    oppsplit:   { icon:'🎭', label:'Класс соперников',      color:'#84cc16' },
    league:     { icon:'🏆', label:'ДНК лиги',              color:'#fbbf24' },
    halftime:   { icon:'⏱️', label:'Тайм / Четверть',       color:'#ec4899' },
    indiv_tot:  { icon:'🎯', label:'Инд. тоталы',           color:'#14b8a6' },
    corners:    { icon:'📐', label:'Угловые',               color:'#64748b' },
    cards:      { icon:'🟨', label:'Карточки',              color:'#ef4444' },
    surface:    { icon:'🌿', label:'Покрытие',              color:'#84cc16' },
    serve:      { icon:'🎾', label:'Подача',                color:'#4ade80' },
    special:    { icon:'⚡', label:'PP/PK',                  color:'#a78bfa' },
    rank:       { icon:'🏅', label:'Рейтинг',               color:'#f59e0b' },
    maps:       { icon:'🗺️', label:'Карты (киберспорт)',     color:'#818cf8' },
  },

  MARKET_GROUP_LABELS: {
    outcome:'Исход матча', totals:'Тоталы', btts:'Обе забьют (BTTS)',
    handicap:'Форы / Спред', indiv_totals:'Индивидуальные тоталы',
    clean_sheet:'Сухой матч', halftime:'Таймовые рынки',
    specials:'Спецрынки', combo:'Комбинированные',
    overtime:'Овертайм', sets:'Сеты / Карты', maps:'Карты (киберспорт)',
    serve:'Подача', other:'Прочее',
  },

  MARKET_ICONS: {
    outcome:'⚖️', totals:'📊', btts:'⚽⚽', handicap:'📏',
    indiv_totals:'🎯', clean_sheet:'🧤', halftime:'⏱️',
    specials:'🟨', combo:'🔗', overtime:'⏰', sets:'🏸',
    maps:'🗺️', serve:'🎾', other:'•',
  },

  // ────────────────────────────────────────────────────────────────────────
  async init() {
    await this.loadStatus();
    this.render();
    this.bindEvents();
  },

  bindEvents() {
    document.addEventListener('click', e => {
      const tab = e.target.closest('[data-neural-sport]');
      if (tab) { this.setActiveSport(tab.dataset.neuralSport); return; }
      const pt = e.target.closest('[data-neural-tab]');
      if (pt) { this.setActiveTab(pt.dataset.neuralTab); return; }
    });
  },

  async setActiveSport(sport) {
    this.activeSport = sport;
    document.querySelectorAll('[data-neural-sport]').forEach(el =>
      el.classList.toggle('nn-sport-active', el.dataset.neuralSport === sport));
    this._refreshActiveTab();
  },

  async setActiveTab(tab) {
    this.activeTab = tab;
    document.querySelectorAll('[data-neural-tab]').forEach(el =>
      el.classList.toggle('nn-tab-active', el.dataset.neuralTab === tab));
    this._refreshActiveTab();
  },

  async _refreshActiveTab() {
    const sport = this.activeSport;
    if (this.activeTab === 'weights') {
      if (!this.weightsData[sport]) await this.loadWeights(sport);
      this.renderWeights(sport);
    } else if (this.activeTab === 'strategies') {
      if (!this.strategiesData[sport]) await this.loadStrategies(sport);
      this.renderStrategies(sport);
    } else if (this.activeTab === 'match') {
      this.renderMatchPanel(sport);
    } else {
      this.renderStatus();
    }
  },

  // ── API ──────────────────────────────────────────────────────────────────
  async api(path, opts = {}) {
    const r = await fetch('/api/neural' + path, {
      credentials: 'include',
      headers: { 'Content-Type': 'application/json' },
      ...opts,
    });
    if (!r.ok) throw new Error(`HTTP ${r.status}`);
    return r.json();
  },

  async loadStatus() {
    try {
      const d = await this.api('/status');
      // Поддержка обоих форматов ответа
      const models = d.models || d.status || [];
      this.statusData = {};
      (Array.isArray(models) ? models : Object.entries(models).map(([key,v])=>({key,...v}))).forEach(m => {
        this.statusData[m.key || m.sport] = m;
      });
    } catch(e) { this.statusData = {}; }
  },

  async loadWeights(sport) {
    try { this.weightsData[sport] = await this.api(`/weights/${sport}`); }
    catch(e) { this.weightsData[sport] = null; }
  },

  async loadStrategies(sport) {
    try { this.strategiesData[sport] = await this.api(`/strategy/${sport}`); }
    catch(e) { this.strategiesData[sport] = null; }
  },

  // ── Train ────────────────────────────────────────────────────────────────
  async trainSport(sport) {
    if (this._trainLoading[sport]) return;
    this._trainLoading[sport] = true;
    document.querySelectorAll(`[data-train-sport="${sport}"]`).forEach(b => {
      b.disabled = true; b.innerHTML = `<span class="nn-spin">⏳</span> Обучение...`;
    });
    this.showToast(`🧠 Обучение ${sport === 'all' ? 'всех моделей' : sport}...`, 'info');
    try {
      const d = await this.api(`/train/${sport}`, { method: 'POST' });
      if (d.ok) {
        const acc = d.results
          ? Object.entries(d.results).map(([s, r]) => `${s}: ${r.accuracy||'—'}%`).join(' | ')
          : `${d.accuracy}%`;
        this.showToast(`✅ Готово — ${acc}`, 'success');
        await this.loadStatus();
        // invalidate cache
        delete this.weightsData[sport]; delete this.strategiesData[sport];
        this.render();
      } else {
        this.showToast(`⚠️ Ошибка: ${d.error || 'неизвестная'}`, 'error');
      }
    } catch(e) {
      this.showToast(`❌ ${e.message}`, 'error');
    } finally {
      this._trainLoading[sport] = false;
      document.querySelectorAll(`[data-train-sport="${sport}"]`).forEach(b => {
        b.disabled = false;
        const info = this.statusData[sport];
        b.innerHTML = info?.trained ? '🔄 Переобучить' : '▶ Обучить';
      });
    }
  },

  // ── MAIN RENDER ──────────────────────────────────────────────────────────
  render() {
    const root = document.getElementById('nn-panel-root');
    if (!root) return;

    root.innerHTML = `
      <div class="nn-shell">

        <!-- ═══ HEADER ═══ -->
        <div class="nn-header">
          <div class="nn-header-left">
            <div class="nn-logo">🧠 Neural v5</div>
            <div class="nn-header-sub">Полное покрытие рынков · ${Object.keys(this.statusData).length || 11} видов спорта</div>
          </div>
          <div class="nn-header-actions">
            <button class="nn-btn sm outline" onclick="neuralPanel.trainAll()">⚡ Обучить все</button>
            <button class="nn-btn sm outline" onclick="neuralPanel.reloadStatus()">🔄 Обновить</button>
          </div>
        </div>

        <!-- ═══ SPORT SELECTOR ═══ -->
        <div class="nn-sport-bar" id="nn-sport-bar">
          ${this.ALL_SPORTS.map(s => {
            const info = this.statusData[s] || {};
            return `
              <button class="nn-sport-pill ${s === this.activeSport ? 'nn-sport-active':''} ${info.trained ? 'trained':''}"
                      data-neural-sport="${s}" title="${this.sportName(s)}">
                <span class="nn-sport-emoji">${this.sportIcon(s)}</span>
                <span class="nn-sport-label">${this.sportName(s)}</span>
                ${info.trained ? `<span class="nn-sport-acc">${info.accuracy}%</span>` : '<span class="nn-sport-dot red"></span>'}
              </button>`;
          }).join('')}
        </div>

        <!-- ═══ TABS ═══ -->
        <div class="nn-tabs" id="nn-tabs">
          ${[
            ['status',     '📋', 'Модели'],
            ['match',      '🔍', 'Анализ матча'],
            ['strategies', '🚀', 'Стратегии'],
            ['weights',    '⚖️', 'Веса / Признаки'],
          ].map(([t,ic,lb]) => `
            <button class="nn-tab ${t === this.activeTab ? 'nn-tab-active':''}" data-neural-tab="${t}">
              ${ic} ${lb}
            </button>`).join('')}
        </div>

        <!-- ═══ CONTENT ═══ -->
        <div class="nn-content" id="nn-content">
          ${this._renderTabContent()}
        </div>

      </div>
    `;

    this._postRender();
  },

  _renderTabContent() {
    const sport = this.activeSport;
    if (this.activeTab === 'status')     return this._renderStatusTab();
    if (this.activeTab === 'match')      return this._renderMatchTabStub(sport);
    if (this.activeTab === 'strategies') return this._renderStrategiesTabStub(sport);
    if (this.activeTab === 'weights')    return this._renderWeightsTabStub(sport);
    return '';
  },

  _postRender() {
    const sport = this.activeSport;
    if (this.activeTab === 'status') {
      Object.entries(this.statusData).forEach(([s, info]) => {
        if (info.trained && info.lossHistory?.length)
          setTimeout(() => this.drawMiniLoss(`loss-mini-${s}`, info.lossHistory), 80);
      });
    }
    if (this.activeTab === 'weights' && this.weightsData[sport]) {
      setTimeout(() => {
        this.drawArchitecture('nn-arch-canvas', this.weightsData[sport].architecture || []);
      }, 100);
    }
  },

  // ── STATUS TAB ───────────────────────────────────────────────────────────
  renderStatus() {
    const c = document.getElementById('nn-content');
    if (c) { c.innerHTML = this._renderStatusTab(); this._postRender(); }
  },

  _renderStatusTab() {
    const sports = this.ALL_SPORTS;
    const trained = sports.filter(s => this.statusData[s]?.trained).length;
    const totalMarkets = Object.values(this.statusData).reduce((s,m) => s+(m.targets||0),0);

    return `
      <div class="nn-status-summary">
        <div class="nn-sum-card">
          <div class="nn-sum-num">${trained}/${sports.length}</div>
          <div class="nn-sum-label">Обучено моделей</div>
        </div>
        <div class="nn-sum-card accent">
          <div class="nn-sum-num">${totalMarkets || '~170'}</div>
          <div class="nn-sum-label">Рынков суммарно</div>
        </div>
        <div class="nn-sum-card">
          <div class="nn-sum-num">${Object.values(this.statusData).reduce((s,m)=>s+(m.rowsUsed||0),0).toLocaleString()}</div>
          <div class="nn-sum-label">Матчей в обучении</div>
        </div>
        <div class="nn-sum-card">
          <div class="nn-sum-num">${Object.values(this.statusData).filter(m=>m.accuracy).reduce((s,m,_,a)=>s+m.accuracy/a.length,0).toFixed(1)||'—'}%</div>
          <div class="nn-sum-label">Средняя точность</div>
        </div>
      </div>

      <div class="nn-status-grid">
        ${sports.map(sport => {
          const info = this.statusData[sport] || {};
          const trained = info.trained;
          return `
            <div class="nn-status-card ${trained ? 'trained':''}"
                 onclick="neuralPanel.setActiveSport('${sport}'); neuralPanel.setActiveTab('match')">
              <div class="nn-sc-header">
                <span class="nn-sc-icon">${this.sportIcon(sport)}</span>
                <span class="nn-sc-name">${this.sportName(sport)}</span>
                ${trained ? `<span class="nn-badge green">✓</span>` : `<span class="nn-badge red">—</span>`}
              </div>
              <div class="nn-sc-stats">
                ${trained ? `
                  <div class="nn-sc-stat"><span>${info.accuracy}%</span><small>точность</small></div>
                  <div class="nn-sc-stat"><span>${(info.rowsUsed||0).toLocaleString()}</span><small>матчей</small></div>
                  <div class="nn-sc-stat"><span>${info.features||'—'}</span><small>признаков</small></div>
                  <div class="nn-sc-stat accent"><span>${info.targets||'?'}</span><small>рынков</small></div>
                ` : `<div class="nn-sc-nodata">Нет данных обучения</div>`}
              </div>
              ${trained && info.lossHistory?.length
                ? `<div class="nn-loss-mini" id="loss-mini-${sport}"></div>` : ''}
              <div class="nn-sc-footer">
                <button class="nn-btn xs ${trained ? 'ghost' : 'primary'}"
                        data-train-sport="${sport}"
                        onclick="event.stopPropagation(); neuralPanel.trainSport('${sport}')">
                  ${trained ? '🔄 Переобучить' : '▶ Обучить'}
                </button>
                ${trained ? `
                  <button class="nn-btn xs outline"
                          onclick="event.stopPropagation(); neuralPanel.setActiveSport('${sport}'); neuralPanel.setActiveTab('match')">
                    🔍 Анализ матча
                  </button>` : ''}
              </div>
            </div>`;
        }).join('')}
      </div>
    `;
  },

  // ── MATCH ANALYSIS TAB ───────────────────────────────────────────────────
  _renderMatchTabStub(sport) {
    const info = this.statusData[sport] || {};
    if (!info.trained) {
      return `
        <div class="nn-not-trained">
          <div class="nn-nt-icon">${this.sportIcon(sport)}</div>
          <div class="nn-nt-title">Модель ${this.sportName(sport)} не обучена</div>
          <div class="nn-nt-sub">Требуется минимум 100 матчей в ClickHouse</div>
          <button class="nn-btn primary" onclick="neuralPanel.trainSport('${sport}')">▶ Обучить</button>
        </div>`;
    }
    return this._buildMatchForm(sport) + `<div id="nn-match-result"></div>`;
  },

  renderMatchPanel(sport) {
    const c = document.getElementById('nn-content');
    if (c) c.innerHTML = this._renderMatchTabStub(sport);
  },

  _buildMatchForm(sport) {
    const oddsFields = this._oddsFieldsForSport(sport);
    return `
      <div class="nn-match-form">
        <div class="nn-mf-title">🔍 Анализ матча — ${this.sportName(sport)}</div>
        <div class="nn-mf-sub">Введите данные матча. Коэффициенты необязательны, но улучшают value-расчёт.</div>

        <div class="nn-mf-row">
          <div class="nn-mf-field">
            <label>🏠 ${sport==='tennis'?'Игрок 1 (Фаворит)':'Хозяева'}</label>
            <input type="text" id="mf-home" placeholder="${sport==='tennis'?'Djokovic':'Arsenal'}" class="nn-input">
          </div>
          <div class="nn-mf-vs">VS</div>
          <div class="nn-mf-field">
            <label>✈️ ${sport==='tennis'?'Игрок 2 (Андердог)':'Гости'}</label>
            <input type="text" id="mf-away" placeholder="${sport==='tennis'?'Medvedev':'Chelsea'}" class="nn-input">
          </div>
          <div class="nn-mf-field" style="max-width:160px">
            <label>📅 Дата</label>
            <input type="date" id="mf-date" value="${new Date().toISOString().slice(0,10)}" class="nn-input">
          </div>
        </div>

        ${sport === 'football' ? `
        <div class="nn-mf-row">
          <div class="nn-mf-field"><label>Лига</label>
            <input type="text" id="mf-league" placeholder="E0 / SP1 / D1" class="nn-input"></div>
        </div>` : ''}

        ${oddsFields.length ? `
        <div class="nn-mf-section">Коэффициенты (необязательно)</div>
        <div class="nn-mf-odds">
          ${oddsFields.map(f => `
            <div class="nn-mf-field sm">
              <label>${f.label}</label>
              <input type="number" step="0.01" min="1" id="${f.id}" placeholder="${f.placeholder||'1.85'}" class="nn-input sm">
            </div>`).join('')}
        </div>` : ''}

        <div class="nn-mf-actions">
          <button class="nn-btn primary lg" id="nn-analyze-btn"
                  onclick="neuralPanel.analyzeMatch('${sport}')">
            🔍 Анализировать все рынки
          </button>
          <button class="nn-btn outline" onclick="neuralPanel._clearMatchResult()">✕ Очистить</button>
        </div>

        <!-- Filter bar (скрыт до результата) -->
        <div id="nn-market-filters" style="display:none" class="nn-filter-bar">
          <span class="nn-filter-label">Фильтры:</span>
          <label class="nn-filter-check">
            <input type="checkbox" id="ff-value-only" onchange="neuralPanel._applyFilters()">
            💰 Только value
          </label>
          <label class="nn-filter-check">
            <input type="checkbox" id="ff-signal-only" onchange="neuralPanel._applyFilters()">
            📡 Сигнал (>55%)
          </label>
          <select id="ff-group" onchange="neuralPanel._applyFilters()" class="nn-select sm">
            <option value="">Все группы</option>
            ${Object.entries(this.MARKET_GROUP_LABELS).map(([k,v])=>`<option value="${k}">${v}</option>`).join('')}
          </select>
          <input type="range" id="ff-min-prob" min="0" max="90" value="0" step="5"
                 oninput="document.getElementById('ff-prob-val').textContent=this.value+'%'; neuralPanel._applyFilters()"
                 style="width:100px">
          <span id="ff-prob-val" style="font-size:12px;color:var(--text3)">0%</span>
          <button class="nn-btn xs outline" onclick="neuralPanel.exportMatchAnalysis()">📥 Экспорт</button>
        </div>
      </div>
    `;
  },

  _oddsFieldsForSport(sport) {
    const map = {
      football: [
        {id:'mf-b365h',label:'П1',placeholder:'2.10'},{id:'mf-b365d',label:'X',placeholder:'3.40'},
        {id:'mf-b365a',label:'П2',placeholder:'3.50'},{id:'mf-b365o25',label:'Over 2.5',placeholder:'1.85'},
        {id:'mf-b365u25',label:'Under 2.5',placeholder:'2.00'},{id:'mf-b365btts',label:'BTTS',placeholder:'1.75'},
      ],
      hockey: [
        {id:'mf-b365h',label:'П1',placeholder:'1.95'},{id:'mf-b365a',label:'П2',placeholder:'1.90'},
        {id:'mf-b365o55',label:'Over 5.5',placeholder:'1.85'},{id:'mf-b365ot',label:'OT',placeholder:'3.20'},
      ],
      tennis: [
        {id:'mf-b365h',label:'Фаворит',placeholder:'1.65'},{id:'mf-b365a',label:'Андердог',placeholder:'2.30'},
        {id:'mf-b365sets',label:'Over сетов',placeholder:'1.80'},
      ],
      basketball: [
        {id:'mf-b365h',label:'П1',placeholder:'1.85'},{id:'mf-b365a',label:'П2',placeholder:'1.95'},
        {id:'mf-b365o220',label:'Over 220',placeholder:'1.85'},{id:'mf-b365spread',label:'Spread',placeholder:'1.90'},
      ],
      esports: [
        {id:'mf-b365h',label:'Фаворит',placeholder:'1.60'},{id:'mf-b365a',label:'Андердог',placeholder:'2.40'},
        {id:'mf-b365maps',label:'Over 2.5 карт',placeholder:'2.10'},
      ],
    };
    return map[sport] || [
      {id:'mf-b365h',label:'П1',placeholder:'1.90'},{id:'mf-b365a',label:'П2',placeholder:'1.90'},
    ];
  },

  _getMatchFormData(sport) {
    const g = id => { const el = document.getElementById(id); return el ? el.value.trim() : ''; };
    const gn = id => { const v = parseFloat(g(id)); return isNaN(v)||v<=1 ? undefined : v; };

    const data = {
      home_team: g('mf-home') || 'TeamA',
      away_team: g('mf-away') || 'TeamB',
      date:      g('mf-date') || new Date().toISOString().slice(0,10),
    };
    if (sport === 'football') {
      data.league_code = g('mf-league') || 'UNK';
      data.b365_home   = gn('mf-b365h');
      data.b365_draw   = gn('mf-b365d');
      data.b365_away   = gn('mf-b365a');
      data.b365_over25 = gn('mf-b365o25');
      data.b365_under25= gn('mf-b365u25');
      data.b365_btts   = gn('mf-b365btts');
    } else if (sport === 'hockey') {
      data.b365_home  = gn('mf-b365h');
      data.b365_away  = gn('mf-b365a');
      data.b365_over55= gn('mf-b365o55');
      data.b365_ot    = gn('mf-b365ot');
    } else if (sport === 'tennis') {
      data.b365w = gn('mf-b365h');
      data.b365l = gn('mf-b365a');
    } else if (sport === 'basketball') {
      data.b365_home    = gn('mf-b365h');
      data.b365_away    = gn('mf-b365a');
      data.b365_over220 = gn('mf-b365o220');
    } else if (sport === 'esports') {
      data.b365_home  = gn('mf-b365h');
      data.b365_away  = gn('mf-b365a');
      data.b365_over_maps = gn('mf-b365maps');
    } else {
      data.b365_home = gn('mf-b365h');
      data.b365_away = gn('mf-b365a');
    }
    return data;
  },

  async analyzeMatch(sport) {
    if (this._matchLoading) return;
    this._matchLoading = true;
    const btn = document.getElementById('nn-analyze-btn');
    if (btn) { btn.disabled = true; btn.innerHTML = '<span class="nn-spin">⏳</span> Анализ...'; }

    const result = document.getElementById('nn-match-result');
    if (result) result.innerHTML = `
      <div class="nn-loading-block">
        <div class="nn-loading-spinner"></div>
        <div>Нейросеть анализирует ${this.statusData[sport]?.targets || '?'} рынков...</div>
      </div>`;

    try {
      const body = this._getMatchFormData(sport);
      const data = await this.api(`/markets/${sport}`, {
        method: 'POST',
        body: JSON.stringify(body),
      });
      this.matchData[sport] = data;
      this._renderMatchResult(sport, data);
      const filters = document.getElementById('nn-market-filters');
      if (filters) filters.style.display = 'flex';
    } catch(e) {
      if (result) result.innerHTML = `<div class="nn-error-block">❌ ${e.message}</div>`;
    } finally {
      this._matchLoading = false;
      if (btn) { btn.disabled = false; btn.innerHTML = '🔍 Анализировать все рынки'; }
    }
  },

  _renderMatchResult(sport, data) {
    const r = document.getElementById('nn-match-result');
    if (!r) return;

    r.innerHTML = `
      <!-- ── SUMMARY ── -->
      <div class="nn-mr-summary">
        <div class="nn-mr-match">
          <span class="nn-mr-home">${data.match?.home || '?'}</span>
          <span class="nn-mr-vs">VS</span>
          <span class="nn-mr-away">${data.match?.away || '?'}</span>
          <span class="nn-mr-date">${data.match?.date || ''}</span>
        </div>
        <div class="nn-mr-stats">
          <div class="nn-mr-stat"><span>${data.summary?.totalMarkets||0}</span><small>Рынков</small></div>
          <div class="nn-mr-stat green"><span>${data.summary?.valueBetsFound||0}</span><small>Value bets</small></div>
          <div class="nn-mr-stat yellow"><span>${data.summary?.strongSignals||0}</span><small>Сигналов</small></div>
        </div>
      </div>

      <!-- ── TEAM TRENDS ── -->
      ${data.teamTrends?.length ? `
        <div class="nn-section-header">📈 Тренды команд</div>
        <div class="nn-trends-row">
          ${data.teamTrends.map(t => `
            <div class="nn-trend-pill">
              <span>${t.icon||'📊'}</span> ${t.label}
              <span class="nn-trend-team">${t.team==='home'?'Хозяева':t.team==='away'?'Гости':'Обе'}</span>
            </div>`).join('')}
        </div>` : ''}

      <!-- ── VALUE BETS ── -->
      ${data.valueBets?.length ? `
        <div class="nn-section-header">💰 Value Bets <span class="nn-badge yellow">${data.valueBets.length}</span></div>
        <div class="nn-value-bets" id="nn-value-bets-list">
          ${data.valueBets.map(b => this._renderValueBet(b)).join('')}
        </div>` : ''}

      <!-- ── TOP SIGNALS ── -->
      ${data.topSignals?.length ? `
        <div class="nn-section-header">📡 Сильные сигналы <span class="nn-badge blue">${data.topSignals.length}</span></div>
        <div class="nn-signals-grid" id="nn-signals-list">
          ${data.topSignals.map(s => this._renderSignal(s)).join('')}
        </div>` : ''}

      <!-- ── ALL MARKETS GROUPED ── -->
      <div class="nn-section-header">📊 Все рынки по группам</div>
      <div id="nn-all-markets">
        ${this._renderGroupedMarkets(data.grouped || {})}
      </div>
    `;
  },

  _renderValueBet(b) {
    const confColors = { very_high:'#4ade80', high:'#22d3ee', medium:'#f59e0b', low:'#94a3b8' };
    const cc = confColors[b.confidence] || '#94a3b8';
    return `
      <div class="nn-vbet">
        <div class="nn-vbet-top">
          <span class="nn-vbet-label">${b.label}</span>
          <span class="nn-vbet-edge" style="color:#4ade80">+${((b.edge||0)*100).toFixed(1)}% edge</span>
        </div>
        <div class="nn-vbet-row">
          <div class="nn-vbet-prob">
            <span class="nn-vbet-pnum" style="color:${cc}">${b.prob}%</span>
            <small>Модель</small>
          </div>
          ${b.impliedProb ? `
          <div class="nn-vbet-prob">
            <span class="nn-vbet-pnum" style="color:var(--text2)">${b.impliedProb}%</span>
            <small>Букмекер</small>
          </div>` : ''}
          ${b.odds ? `
          <div class="nn-vbet-odds">
            <span class="nn-vbet-onum">× ${b.odds}</span>
            <small>Коэф.</small>
          </div>` : ''}
        </div>
        <div class="nn-vbet-bar">
          <div class="nn-vbet-bar-fill" style="width:${b.prob}%;background:${cc}"></div>
          ${b.impliedProb ? `<div class="nn-vbet-bar-implied" style="width:${b.impliedProb}%"></div>` : ''}
        </div>
        <div class="nn-vbet-factors">
          ${(b.topFactors||[]).map(f => `<span class="nn-feat-pill">${f}</span>`).join('')}
        </div>
        <div class="nn-vbet-rec">${b.recommendation||''}</div>
      </div>`;
  },

  _renderSignal(s) {
    const confIcons = { very_high:'🔥', high:'✅', medium:'⚡', low:'👀' };
    const confColors = { very_high:'#4ade80', high:'#22d3ee', medium:'#f59e0b', low:'#94a3b8' };
    const ci = confIcons[s.confidence]||'📡';
    const cc = confColors[s.confidence]||'#94a3b8';
    return `
      <div class="nn-signal-card" data-market-group="${s.group}">
        <div class="nn-sig-top">
          <span class="nn-sig-icon">${ci}</span>
          <span class="nn-sig-label">${s.label}</span>
          <span class="nn-sig-group-tag">${this.MARKET_ICONS[s.group]||'•'}</span>
        </div>
        <div class="nn-sig-prob" style="color:${cc}">${s.prob}%</div>
        <div class="nn-sig-bar">
          <div class="nn-sig-bar-fill" style="width:${s.prob}%;background:${cc}"></div>
        </div>
        ${s.value ? `<div class="nn-sig-value">💰 Value +${((s.edge||0)*100).toFixed(1)}%</div>` : ''}
      </div>`;
  },

  _renderGroupedMarkets(grouped) {
    if (!Object.keys(grouped).length) return '<div class="nn-empty">Нет данных</div>';
    return Object.entries(grouped).map(([group, markets]) => {
      if (!markets?.length) return '';
      const icon = this.MARKET_ICONS[group] || '•';
      const label = this.MARKET_GROUP_LABELS[group] || group;
      const signals = markets.filter(m => m.signal).length;
      const valueBets = markets.filter(m => m.value).length;
      return `
        <div class="nn-market-group" data-group="${group}">
          <div class="nn-mg-header" onclick="this.closest('.nn-market-group').classList.toggle('collapsed')">
            <span class="nn-mg-icon">${icon}</span>
            <span class="nn-mg-label">${label}</span>
            <span class="nn-mg-count">${markets.length} рынков</span>
            ${signals ? `<span class="nn-badge blue">${signals} сигнал</span>` : ''}
            ${valueBets ? `<span class="nn-badge yellow">${valueBets} value</span>` : ''}
            <span class="nn-mg-toggle">▼</span>
          </div>
          <div class="nn-mg-body">
            <div class="nn-mg-table">
              <div class="nn-mg-thead">
                <span>Рынок</span><span>Вероятность</span><span>Коэф.</span><span>Edge</span><span>Уровень</span>
              </div>
              ${markets.map(m => {
                const confColors = { very_high:'#4ade80', high:'#22d3ee', medium:'#f59e0b', low:'#94a3b8' };
                const cc = confColors[m.confidence] || '#94a3b8';
                return `
                  <div class="nn-mg-row ${m.value?'value':''} ${m.strongSignal?'strong':''}"
                       data-market-group="${group}" data-prob="${m.prob}" data-value="${m.value?1:0}">
                    <span class="nn-mg-market-name">
                      ${m.strongSignal ? '🔥 ' : m.signal ? '✅ ' : ''}${m.label}
                    </span>
                    <span style="color:${cc};font-weight:700">${m.prob}%</span>
                    <span style="color:var(--text2)">${m.odds ? '× '+m.odds : '—'}</span>
                    <span style="color:${m.edge>0?'#4ade80':'var(--text3)'}">
                      ${m.edge !== null ? (m.edge>0?'+':'')+((m.edge||0)*100).toFixed(1)+'%' : '—'}
                    </span>
                    <span class="nn-conf-badge" style="border-color:${cc};color:${cc}">
                      ${this._confLabel(m.confidence)}
                    </span>
                  </div>`;
              }).join('')}
            </div>
          </div>
        </div>`;
    }).join('');
  },

  _confLabel(c) {
    return { very_high:'🔥 Очень высокий', high:'✅ Высокий', medium:'⚡ Средний', low:'👀 Слабый' }[c] || c;
  },

  _applyFilters() {
    const valueOnly  = document.getElementById('ff-value-only')?.checked;
    const signalOnly = document.getElementById('ff-signal-only')?.checked;
    const groupFilter= document.getElementById('ff-group')?.value;
    const minProb    = parseFloat(document.getElementById('ff-min-prob')?.value || 0);

    document.querySelectorAll('.nn-mg-row').forEach(row => {
      const group = row.dataset.marketGroup;
      const prob  = parseFloat(row.dataset.prob || 0);
      const value = row.dataset.value === '1';
      const signal= row.classList.contains('strong') || row.classList.contains('value');

      let show = true;
      if (valueOnly && !value) show = false;
      if (signalOnly && !signal) show = false;
      if (groupFilter && group !== groupFilter) show = false;
      if (prob < minProb) show = false;

      row.style.display = show ? '' : 'none';
    });

    // Скрываем пустые группы
    document.querySelectorAll('.nn-market-group').forEach(grp => {
      if (!groupFilter) { grp.style.display = ''; return; }
      grp.style.display = grp.dataset.group === groupFilter ? '' : 'none';
    });
  },

  _clearMatchResult() {
    const r = document.getElementById('nn-match-result');
    if (r) r.innerHTML = '';
    const f = document.getElementById('nn-market-filters');
    if (f) f.style.display = 'none';
  },

  exportMatchAnalysis() {
    const sport = this.activeSport;
    const data = this.matchData[sport];
    if (!data) return;
    const rows = [['Рынок','Группа','Вероятность %','Коэф','Edge %','Уверенность','Value','Топ-факторы']];
    (data.allMarkets || []).forEach(m => {
      rows.push([
        m.label, m.group, m.prob, m.odds||'', m.edge!=null?(m.edge*100).toFixed(2):'',
        m.confidence, m.value?'YES':'', (m.topFactors||[]).join('; ')
      ]);
    });
    const csv = rows.map(r => r.map(v => `"${String(v).replace(/"/g,'""')}"`).join(',')).join('\n');
    const blob = new Blob(['\uFEFF'+csv], { type:'text/csv;charset=utf-8' });
    Object.assign(document.createElement('a'),{
      href:URL.createObjectURL(blob),
      download:`match_analysis_${sport}_${data.match?.home||''}_${Date.now()}.csv`
    }).click();
  },

  // ── WEIGHTS TAB ──────────────────────────────────────────────────────────
  _renderWeightsTabStub(sport) {
    const data = this.weightsData[sport];
    if (!data) return `
      <div class="nn-not-trained">
        <div class="nn-nt-icon">${this.sportIcon(sport)}</div>
        <div class="nn-nt-title">Загрузка весов ${this.sportName(sport)}...</div>
        <div class="nn-loading-spinner"></div>
      </div>`;
    return this._buildWeightsContent(sport, data);
  },

  renderWeights(sport) {
    const c = document.getElementById('nn-content');
    const data = this.weightsData[sport];
    if (!data || data.error) {
      if (c) c.innerHTML = `
        <div class="nn-not-trained">
          <div class="nn-nt-icon">${this.sportIcon(sport)}</div>
          <div class="nn-nt-title">Модель ${this.sportName(sport)} не обучена</div>
          <button class="nn-btn primary" onclick="neuralPanel.trainSport('${sport}')">▶ Обучить</button>
        </div>`;
      return;
    }
    if (c) { c.innerHTML = this._buildWeightsContent(sport, data); this._postRender(); }
  },

  _buildWeightsContent(sport, data) {
    const features = data.features || data.inputImportance || [];
    const maxW = Math.max(...features.map(f => f.weight||0), 0.001);
    const groupSummary = {};
    features.forEach(f => {
      const g = f.group || 'other';
      if (!groupSummary[g]) groupSummary[g] = { sum:0, count:0, topFeat:f.feature||f.label||'' };
      groupSummary[g].sum   += (f.weight||0);
      groupSummary[g].count += 1;
    });
    const groupsSorted = Object.entries(groupSummary)
      .map(([g,v]) => ({group:g, total:v.sum, topFeat:v.topFeat}))
      .sort((a,b) => b.total-a.total);
    const maxGroup = groupsSorted[0]?.total || 1;

    return `
      <div class="nn-weights-header">
        <div class="nn-weights-title">
          ${this.sportIcon(sport)} ${data.label||this.sportName(sport)} — Анализ весов
          ${data.accuracy ? `<span class="nn-badge green">${data.accuracy}% acc</span>` : ''}
          ${data.rowsUsed ? `<span class="nn-badge blue">${data.rowsUsed.toLocaleString()} матчей</span>` : ''}
          ${data.architecture ? `<span class="nn-badge grey">[${data.architecture.join('→')}]</span>` : ''}
        </div>
        ${data.trainedAt ? `<div class="nn-trained-at">Обучена: ${new Date(data.trainedAt).toLocaleString('ru-RU')}</div>` : ''}
      </div>

      <div class="nn-weights-layout">
        <!-- Топ признаков -->
        <div class="nn-section">
          <div class="nn-section-title">📊 Важность признаков (top-20)</div>
          <div class="nn-section-sub">L2-норма весов первого слоя</div>
          <div class="nn-feature-list">
            ${features.slice(0,20).map((f,i) => {
              const gm = this.GROUP_META[f.group] || {icon:'•',label:f.group||'',color:'#818cf8'};
              const name = f.feature || f.label || f.key || '';
              const w = f.weight || 0;
              return `
                <div class="nn-feature-row">
                  <div class="nn-feature-rank">${i+1}</div>
                  <div class="nn-feature-label">
                    <span class="nn-feat-group-dot" style="background:${gm.color}" title="${gm.label}">${gm.icon}</span>
                    <span class="nn-feature-name">${name}</span>
                  </div>
                  <div class="nn-feature-bar-wrap">
                    <div class="nn-feature-bar" style="width:${(w/maxW*100).toFixed(1)}%;background:${gm.color}88"></div>
                  </div>
                  <div class="nn-feature-val">${w.toFixed(4)}</div>
                </div>`;
            }).join('')}
          </div>
        </div>

        <!-- Группы -->
        <div class="nn-section">
          <div class="nn-section-title">🏷️ Важность по группам</div>
          <div class="nn-groups">
            ${groupsSorted.map(g => {
              const gm = this.GROUP_META[g.group] || {icon:'•',label:g.group,color:'#818cf8'};
              return `
                <div class="nn-group-item">
                  <span class="nn-group-icon">${gm.icon}</span>
                  <div style="flex:1;min-width:0">
                    <div style="display:flex;justify-content:space-between;margin-bottom:3px">
                      <span class="nn-group-name">${gm.label}</span>
                      <span class="nn-group-total">${g.total.toFixed(3)}</span>
                    </div>
                    <div class="nn-group-bar-wrap">
                      <div class="nn-group-bar-fill" style="width:${(g.total/maxGroup*100).toFixed(1)}%;background:${gm.color}"></div>
                    </div>
                    <div class="nn-group-top-feat">${g.topFeat}</div>
                  </div>
                </div>`;
            }).join('')}
          </div>

          <div class="nn-section-title" style="margin-top:20px">🔗 Архитектура</div>
          <canvas id="nn-arch-canvas" width="320" height="160" style="display:block;margin-top:8px"></canvas>
        </div>

        <!-- Целевые переменные -->
        <div class="nn-section nn-full-width">
          <div class="nn-section-title">🎯 Анализ по целевым рынкам</div>
          <div class="nn-targets-grid">
            ${(data.targetExplanations || []).map(t => `
              <div class="nn-target-card">
                <div class="nn-target-label">${t.label}</div>
                <div class="nn-target-features">
                  ${(t.topFeatures||[]).slice(0,5).map(f => {
                    const imp = typeof f.importance==='number'?`<span class="nn-feat-imp">${(f.importance*100).toFixed(1)}%</span>`:'';
                    return `<span class="nn-feat-pill">${f.name||f}${imp}</span>`;
                  }).join('')}
                </div>
                ${t.explanation ? `<div class="nn-target-explanation">${t.explanation}</div>` : ''}
              </div>`).join('')}
          </div>
        </div>
      </div>
    `;
  },

  // ── STRATEGIES TAB ───────────────────────────────────────────────────────
  _renderStrategiesTabStub(sport) {
    const data = this.strategiesData[sport];
    if (!data) return `<div class="nn-loading-block"><div class="nn-loading-spinner"></div><div>Загрузка...</div></div>`;
    return this._buildStrategiesContent(sport, data);
  },

  renderStrategies(sport) {
    const c = document.getElementById('nn-content');
    const data = this.strategiesData[sport];
    if (!data || data.error || !data.strategies?.length) {
      if (c) c.innerHTML = `
        <div class="nn-not-trained">
          <div class="nn-nt-icon">🚀</div>
          <div class="nn-nt-title">Обучите модель для генерации стратегий</div>
          <button class="nn-btn primary" onclick="neuralPanel.trainSport('${sport}')">▶ Обучить ${this.sportName(sport)}</button>
        </div>`;
      return;
    }
    if (c) c.innerHTML = this._buildStrategiesContent(sport, data);
  },

  _buildStrategiesContent(sport, data) {
    const strategies = data.strategies || [];
    // Группируем по целевому рынку для фильтрации
    const groups = [...new Set(strategies.map(s => {
      if (['home_win','draw','away_win'].includes(s.target)) return 'Исход';
      if (s.target.startsWith('over') || s.target.startsWith('under')) return 'Тоталы';
      if (s.target.startsWith('btts')) return 'BTTS';
      if (s.target.startsWith('ah_') || s.target.includes('spread') || s.target.includes('line')) return 'Фора';
      if (s.target.startsWith('home_ov') || s.target.startsWith('away_ov')) return 'Инд. тоталы';
      if (s.target.startsWith('ht_') || s.target.startsWith('h2_') || s.target.startsWith('q1')) return 'Тайм';
      return 'Другое';
    }))];

    return `
      <div class="nn-strat-header">
        <div class="nn-strat-title">🚀 Стратегии — ${data.label}</div>
        <div class="nn-strat-actions">
          <div class="nn-strat-filter">
            ${groups.map(g => `<button class="nn-filter-btn" onclick="neuralPanel._filterStrats(this,'${g}')">${g}</button>`).join('')}
            <button class="nn-filter-btn active" onclick="neuralPanel._filterStrats(this,'all')">Все</button>
          </div>
          <button class="nn-btn sm outline" onclick="neuralPanel.exportStrategies('${sport}')">📥 JSON</button>
          <button class="nn-btn sm primary" onclick="neuralPanel.sendAllToBacktest('${sport}')">📤 Все → Бэктест</button>
        </div>
      </div>
      <div class="nn-strat-grid" id="nn-strat-grid">
        ${strategies.map((s,idx) => this._renderStratCard(s,idx,sport)).join('')}
      </div>
    `;
  },

  _filterStrats(btn, group) {
    document.querySelectorAll('.nn-filter-btn').forEach(b => b.classList.remove('active'));
    btn.classList.add('active');
    document.querySelectorAll('.nn-strat-card').forEach(card => {
      card.style.display = group === 'all' || card.dataset.group === group ? '' : 'none';
    });
  },

  _renderStratCard(s, idx, sport) {
    const roi = s.roi || '+5-10%';
    const roiColor = roi.startsWith('+') ? '#4ade80' : '#f87171';
    const confColor = s.confidence>70?'#4ade80':s.confidence>50?'#f59e0b':'#94a3b8';
    const groupLabel = (() => {
      if (['home_win','draw','away_win'].includes(s.target)) return 'Исход';
      if (s.target.startsWith('over')||s.target.startsWith('under')) return 'Тоталы';
      if (s.target.startsWith('btts')) return 'BTTS';
      if (s.target.startsWith('ah_')||s.target.includes('spread')||s.target.includes('line')) return 'Фора';
      if (s.target.startsWith('home_ov')||s.target.startsWith('away_ov')) return 'Инд. тоталы';
      if (s.target.startsWith('ht_')||s.target.startsWith('h2_')||s.target.startsWith('q1')) return 'Тайм';
      return 'Другое';
    })();
    return `
      <div class="nn-strat-card" id="nn-strat-${idx}" data-group="${groupLabel}">
        <div class="nn-strat-top">
          <span class="nn-strat-name">${s.label||s.target}</span>
          <span class="nn-strat-roi" style="color:${roiColor}">ROI ${roi}</span>
        </div>
        <div class="nn-strat-meta">
          <span class="nn-strat-group-tag">${groupLabel}</span>
          <span class="nn-strat-conf" style="color:${confColor}">${s.confidence}% уверенность</span>
        </div>
        <div class="nn-conf-wrap">
          <div class="nn-conf-bar">
            <div class="nn-conf-fill" style="width:${s.confidence}%;background:${confColor}"></div>
          </div>
        </div>
        ${(s.topFeatures||[]).length ? `
        <div class="nn-strat-features">
          <span style="font-size:11px;color:var(--text3)">Ключевые признаки:</span>
          ${(s.topFeatures||[]).slice(0,4).map(f => {
            const g = this._guessGroup(f);
            const gm = this.GROUP_META[g] || {icon:'•',color:'#818cf8'};
            return `<span class="nn-feat-pill" style="border-color:${gm.color}55">${gm.icon} ${f}</span>`;
          }).join('')}
        </div>` : ''}
        ${s.explanation ? `<div class="nn-strat-explanation">${s.explanation}</div>` : ''}
        <div class="nn-strat-footer">
          <button class="nn-btn xs preview" onclick="neuralPanel.previewCode('${sport}',${idx})">👁 Код</button>
          <button class="nn-btn xs primary" onclick="neuralPanel.sendToBacktest('${sport}',${idx})">→ Бэктест</button>
        </div>
      </div>`;
  },

  _guessGroup(f) {
    const n = (f||'').toLowerCase();
    if (n.includes('elo')) return 'elo';
    if (n.includes('poisson')||n.includes('λ')||n.includes('ожид')) return 'poisson';
    if (n.includes('h2h')) return 'h2h';
    if (n.includes('l5')||n.includes('l-5')) return 'form_venue';
    if (n.includes('l10')||n.includes('l-10')) return 'form10';
    if (n.includes('дома')||n.includes('гостях')||n.includes('venue')) return 'form_venue';
    if (n.includes('усталость')||n.includes('отдых')||n.includes('b2b')) return 'fatigue';
    if (n.includes('xg')||n.includes('удар')) return 'xg';
    if (n.includes('коэф')||n.includes('implied')||n.includes('маржа')) return 'market';
    if (n.includes('подача')||n.includes('serve')||n.includes('эйс')) return 'serve';
    if (n.includes('покрытие')||n.includes('hard')||n.includes('clay')) return 'surface';
    if (n.includes('рейтинг')||n.includes('rank')) return 'rank';
    if (n.includes('момент')||n.includes('серия')||n.includes('тренд')) return 'momentum';
    if (n.includes('психо')||n.includes('bounce')||n.includes('comeback')) return 'psych';
    if (n.includes('стиль')||n.includes('атака')||n.includes('нападение')) return 'style';
    if (n.includes('матч стил')||n.includes('clash')) return 'clash';
    if (n.includes('лига')||n.includes('ДНК')) return 'league';
    if (n.includes('сезон')||n.includes('позиция')) return 'season';
    return 'form10';
  },

  // ── CODE PREVIEW ─────────────────────────────────────────────────────────
  previewCode(sport, idx) {
    const data = this.strategiesData[sport];
    if (!data?.strategies?.[idx]) return;
    const s = data.strategies[idx];
    const modal = document.createElement('div');
    modal.className = 'nn-modal-overlay';
    modal.innerHTML = `
      <div class="nn-modal">
        <div class="nn-modal-header">
          <div class="nn-modal-title">${s.label} — код стратегии</div>
          <button onclick="this.closest('.nn-modal-overlay').remove()" class="nn-modal-close">✕</button>
        </div>
        <pre class="nn-code-block">${(s.code||'// Код не сгенерирован').replace(/</g,'&lt;').replace(/>/g,'&gt;')}</pre>
        <div class="nn-modal-footer">
          <button class="nn-btn sm" onclick="navigator.clipboard.writeText(${JSON.stringify(s.code||'')}).then(()=>neuralPanel.showToast('✅ Скопировано','success'))">📋 Скопировать</button>
          <button class="nn-btn sm primary" onclick="neuralPanel.sendToBacktest('${sport}',${idx}); this.closest('.nn-modal-overlay').remove()">→ Бэктест</button>
        </div>
      </div>`;
    document.body.appendChild(modal);
    modal.addEventListener('click', e => { if (e.target === modal) modal.remove(); });
  },

  // ── SEND TO BACKTEST ─────────────────────────────────────────────────────
  sendToBacktest(sport, idx) {
    const data = this.strategiesData[sport];
    if (!data?.strategies?.[idx]) return;
    const s = data.strategies[idx];
    if (!s.code) { this.showToast('⚠️ Обучите модель заново — нет кода','warning'); return; }
    if (typeof backtestEngine === 'undefined') { this.showToast('❌ backtestEngine не найден','error'); return; }
    const strat = {
      id:`nn_${sport}_${s.target}_${Date.now()}`, name:`🧠 NN: ${s.label}`,
      sport, code:s.code, color:'#00d4ff', enabled:true,
    };
    const ex = backtestEngine.activeStrategies.findIndex(x => x.id.startsWith(`nn_${sport}_${s.target}`));
    if (ex >= 0) Object.assign(backtestEngine.activeStrategies[ex], strat);
    else backtestEngine.activeStrategies.push(strat);
    backtestEngine.saveActiveStrategies?.();
    backtestEngine.renderStrategySlots?.();
    this.showToast(`✅ "${strat.name}" → Бэктест`, 'success');
    setTimeout(() => { if (typeof app !== 'undefined' && app.showPanel) app.showPanel('backtest'); }, 600);
  },

  sendAllToBacktest(sport) {
    const data = this.strategiesData[sport];
    if (!data?.strategies?.length) return;
    data.strategies.forEach((_,idx) => this.sendToBacktest(sport,idx));
    this.showToast(`✅ ${data.strategies.length} стратегий → Бэктест`, 'success');
    setTimeout(() => { if (typeof app !== 'undefined' && app.showPanel) app.showPanel('backtest'); }, 800);
  },

  exportStrategies(sport) {
    const data = this.strategiesData[sport];
    if (!data) return;
    const blob = new Blob([JSON.stringify(data.strategies,null,2)],{type:'application/json'});
    Object.assign(document.createElement('a'),{href:URL.createObjectURL(blob),download:`nn_strategies_${sport}_${Date.now()}.json`}).click();
  },

  // ── TRAIN ALL ────────────────────────────────────────────────────────────
  async trainAll() {
    this.showToast('🧠 Последовательное обучение всех моделей...','info');
    for (const sport of this.ALL_SPORTS) {
      try { await this.api(`/train/${sport}`,{method:'POST'}); }
      catch(e) { console.warn(`Train ${sport}:`,e.message); }
    }
    await this.loadStatus();
    this.render();
    this.showToast('✅ Все модели обучены','success');
  },

  async reloadStatus() {
    await this.loadStatus();
    this.render();
    this.showToast('🔄 Статус обновлён','info');
  },

  // ── CHARTS ───────────────────────────────────────────────────────────────
  drawMiniLoss(containerId, history) {
    const el = document.getElementById(containerId); if (!el||!history?.length) return;
    el.innerHTML = `<canvas width="160" height="28"></canvas>`;
    const ctx = el.querySelector('canvas').getContext('2d');
    const mn=Math.min(...history),mx=Math.max(...history),range=mx-mn||1;
    const pts=history.map((v,i)=>({x:(i/(history.length-1))*158+1,y:26-((v-mn)/range)*24}));
    ctx.strokeStyle='#4ade80'; ctx.lineWidth=1.5; ctx.beginPath();
    pts.forEach((p,i)=>i===0?ctx.moveTo(p.x,p.y):ctx.lineTo(p.x,p.y)); ctx.stroke();
    [pts[0],pts[pts.length-1]].forEach((p,i)=>{
      ctx.fillStyle=i===0?'#f59e0b':'#4ade80';
      ctx.beginPath();ctx.arc(p.x,p.y,3,0,Math.PI*2);ctx.fill();
    });
  },

  drawArchitecture(canvasId, layers) {
    const canvas=document.getElementById(canvasId); if(!canvas||!layers?.length) return;
    const ctx=canvas.getContext('2d'),W=canvas.width,H=canvas.height;
    ctx.clearRect(0,0,W,H);
    const cols=layers.length,colW=W/cols,nodeR=6;
    const colors=['#4ade80','#818cf8','#818cf8','#8b5cf6','#f59e0b'];
    const positions=layers.map((n,col)=>{
      const visible=Math.min(n,8),spacing=(H-40)/(visible+1);
      return Array.from({length:visible},(_,row)=>({x:col*colW+colW/2,y:20+spacing*(row+1)}));
    });
    ctx.globalAlpha=0.08; ctx.strokeStyle='#818cf8'; ctx.lineWidth=0.7;
    for(let l=0;l<positions.length-1;l++)
      positions[l].slice(0,5).forEach(f=>positions[l+1].slice(0,5).forEach(t=>{
        ctx.beginPath();ctx.moveTo(f.x,f.y);ctx.lineTo(t.x,t.y);ctx.stroke();
      }));
    ctx.globalAlpha=1;
    positions.forEach((col,ci)=>{
      col.forEach(({x,y})=>{
        ctx.beginPath();ctx.arc(x,y,nodeR,0,Math.PI*2);
        ctx.fillStyle=colors[Math.min(ci,colors.length-1)];ctx.fill();
      });
      ctx.fillStyle='#94a3b8';ctx.font='10px monospace';ctx.textAlign='center';
      const lbl=ci===0?`In\n${layers[ci]}`:ci===layers.length-1?`Out\n${layers[ci]}`:`H${ci}\n${layers[ci]}`;
      lbl.split('\n').forEach((line,li)=>ctx.fillText(line,col[0]?.x||0,H-18+li*12));
    });
  },

  // ── HELPERS ──────────────────────────────────────────────────────────────
  sportIcon(s) {
    return {football:'⚽',hockey:'🏒',tennis:'🎾',basketball:'🏀',volleyball:'🏐',
            nfl:'🏈',rugby:'🏉',cricket:'🏏',waterpolo:'🤽',esports:'🎮',baseball:'⚾'}[s]||'🎯';
  },
  sportName(s) {
    return {football:'Футбол',hockey:'Хоккей',tennis:'Теннис',basketball:'Баскетбол',
            volleyball:'Волейбол',nfl:'NFL',rugby:'Регби',cricket:'Крикет',
            waterpolo:'Водное поло',esports:'Киберспорт',baseball:'Бейсбол'}[s]||s;
  },
  showToast(msg, type='info') {
    const colors={info:'#818cf8',success:'#4ade80',error:'#f87171',warning:'#f59e0b'};
    const t=Object.assign(document.createElement('div'),{
      textContent:msg,
      style:`position:fixed;top:20px;right:20px;z-index:9999;padding:10px 18px;
             border-radius:8px;font-size:13px;font-weight:600;pointer-events:none;
             background:${colors[type]||colors.info};color:#0f172a;
             box-shadow:0 4px 20px rgba(0,0,0,0.4);transition:opacity 0.3s`,
    });
    document.body.appendChild(t);
    setTimeout(()=>{t.style.opacity='0';setTimeout(()=>t.remove(),350)},3000);
  },
};

// Auto-init
if (document.readyState==='loading') {
  document.addEventListener('DOMContentLoaded',()=>{if(document.getElementById('nn-panel-root'))neuralPanel.init();});
} else {
  if (document.getElementById('nn-panel-root')) neuralPanel.init();
}