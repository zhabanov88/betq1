'use strict';
// ═══════════════════════════════════════════════════════════════════════════
//  BetQuant Pro — Библиотека стратегий v2.0
//  С поддержкой выбора стратегии и привязки к live данным
// ═══════════════════════════════════════════════════════════════════════════
const library = {
  _meta: null,                    // спорты, турниры, статистики из БД
  _strategies: [],                // все стратегии
  _selectedStrategyId: null,      // выбранная для мониторинга
  _liveInterval: null,
  _tab: 'all',                    // all | mine | signals

  async init() {
    await this.loadMeta();
    await this.load();
    this.render();
  },

  // ── Загрузка справочных данных ────────────────────────────────────────────
  async loadMeta() {
    try {
      const r = await this._fetch('/api/matching/mappings/meta');
      if (r) this._meta = r;
    } catch (e) {
      console.warn('[library] meta load failed:', e.message);
    }
  },

  // ── Загрузка стратегий ────────────────────────────────────────────────────
  async load() {
    // 1. Стратегии из PostgreSQL
    let dbStrategies = [];
    try {
      const r = await this._fetch('/api/matching/strategies/list');
      dbStrategies = r?.strategies || [];
    } catch (e) {
      console.warn('[library] db strategies failed:', e.message);
    }

    // 2. Стратегии из localStorage (локально сохранённые)
    const localStrats = JSON.parse(localStorage.getItem('bq_strategies') || '[]');

    // 3. Встроенные стратегии
    const builtins = this._getBuiltins();

    // Мерджим: DB > local > builtin (приоритет DB)
    const dbIds = new Set(dbStrategies.map(s => s.id));
    const localFiltered = localStrats.filter(s => !dbIds.has(s.id));

    this._strategies = [
      ...dbStrategies.map(s => ({ ...s, source: 'db', roi: s.roi || '—', bets: s.total_signals || 0 })),
      ...localFiltered.map(s => ({ ...s, source: 'local', roi: '—', bets: 0 })),
      ...builtins,
    ];
  },

  // ── Рендер панели ─────────────────────────────────────────────────────────
  render() {
    const container = document.getElementById('libraryGrid');
    if (!container) return;

    // Если родительский контейнер — перерисуем всю панель
    const panel = document.getElementById('panel-library');
    if (panel) {
      panel.innerHTML = this._buildPanelHTML();
      this._bindEvents();
    }

    this._renderCards();
    this._renderSignalPanel();
  },

  _buildPanelHTML() {
    return `
    <div class="panel-header">
      <h2>📚 Библиотека стратегий</h2>
      <div class="panel-controls">
        <button class="ctrl-btn" onclick="library.load().then(()=>library.render())">↻ Обновить</button>
        <button class="ctrl-btn primary" onclick="app.showPanel('strategy')">+ Создать</button>
        <button class="ctrl-btn primary" onclick="app.showPanel('ai-strategy')">🤖 AI стратегия</button>
      </div>
    </div>

    <!-- Вкладки -->
    <div class="lib-tabs" style="display:flex;gap:4px;padding:0 20px 12px">
      <button class="lib-tab ${this._tab==='all'?'active':''}" onclick="library.setTab('all')">Все</button>
      <button class="lib-tab ${this._tab==='mine'?'active':''}" onclick="library.setTab('mine')">Мои</button>
      <button class="lib-tab ${this._tab==='signals'?'active':''}" onclick="library.setTab('signals')">🔴 Сигналы</button>
    </div>

    <!-- Фильтры -->
    <div class="lib-filters" style="padding:0 20px 16px;display:flex;gap:10px;flex-wrap:wrap;align-items:center">
      <input id="libSearch" type="text" class="ctrl-input" placeholder="🔍 Поиск..." onInput="library._renderCards()" style="width:200px">
      <select id="libSportFilter" class="ctrl-select" onchange="library._renderCards()">
        <option value="">Все виды спорта</option>
        ${(this._meta?.sports || []).map(s => `<option value="${s.id}">${s.icon || '🏆'} ${s.name}</option>`).join('')}
      </select>
      <select id="libSortFilter" class="ctrl-select" onchange="library._renderCards()">
        <option value="date">По дате</option>
        <option value="signals">По сигналам</option>
        <option value="name">По названию</option>
      </select>
    </div>

    <!-- Сетка карточек -->
    <div class="lib-layout" style="display:flex;gap:16px;padding:0 20px;height:calc(100vh - 280px);min-height:400px">
      
      <!-- Карточки -->
      <div style="flex:1;overflow-y:auto">
        <div id="libraryGrid" class="strategy-grid" style="display:grid;grid-template-columns:repeat(auto-fill,minmax(320px,1fr));gap:14px;padding-right:4px"></div>
      </div>

      <!-- Панель сигналов (правая) -->
      <div id="lib-signal-panel" style="width:360px;min-width:300px;overflow-y:auto;display:${this._selectedStrategyId?'flex':'none'};flex-direction:column;gap:12px">
        <div class="lib-signal-header">
          <span style="font-weight:700;font-size:14px">🎯 Live мониторинг</span>
          <button class="ctrl-btn sm" onclick="library.clearSelection()">✕ Закрыть</button>
        </div>
        <div id="lib-signal-content">
          <div style="color:var(--text3);text-align:center;padding:40px 0">
            Выберите стратегию для мониторинга
          </div>
        </div>
      </div>
    </div>
    `;
  },

  setTab(tab) {
    this._tab = tab;
    document.querySelectorAll('.lib-tab').forEach(b => b.classList.toggle('active', b.textContent.toLowerCase().includes(tab) || (tab==='all' && b.textContent==='Все')));
    this._renderCards();
  },

  _renderCards() {
    const container = document.getElementById('libraryGrid');
    if (!container) return;

    const search = (document.getElementById('libSearch')?.value || '').toLowerCase();
    const sportFilter = document.getElementById('libSportFilter')?.value || '';
    const sort = document.getElementById('libSortFilter')?.value || 'date';

    let list = [...this._strategies];

    // Фильтр по вкладке
    if (this._tab === 'mine') {
      list = list.filter(s => s.source === 'db' || s.source === 'local');
    } else if (this._tab === 'signals') {
      list = list.filter(s => s.last_signal_at || (s.bets && s.bets > 0));
    }

    // Фильтр по поиску
    if (search) list = list.filter(s =>
      (s.name || '').toLowerCase().includes(search) ||
      (s.description || s.desc || '').toLowerCase().includes(search) ||
      (s.tags || []).some(t => t.toLowerCase().includes(search))
    );

    // Фильтр по спорту
    if (sportFilter) list = list.filter(s => String(s.sport_id) === sportFilter);

    // Сортировка
    if (sort === 'signals') list.sort((a, b) => (b.bets || 0) - (a.bets || 0));
    else if (sort === 'name') list.sort((a, b) => (a.name || '').localeCompare(b.name || ''));
    else list.sort((a, b) => new Date(b.created_at || 0) - new Date(a.created_at || 0));

    if (!list.length) {
      container.innerHTML = `
        <div class="empty-state" style="grid-column:1/-1">
          <div class="empty-state-icon">📭</div>
          <div>Стратегий не найдено</div>
          <div style="font-size:12px;color:var(--text3);margin-top:8px">Создайте стратегию в Конструкторе или AI стратегиях</div>
          <button class="ctrl-btn primary" style="margin-top:12px" onclick="app.showPanel('ai-strategy')">🤖 Создать с AI</button>
        </div>`;
      return;
    }

    container.innerHTML = list.map(s => this._cardHTML(s)).join('');
  },

  _cardHTML(s) {
    const isSelected = this._selectedStrategyId === s.id;
    const roiColor = typeof s.roi === 'string' && s.roi.startsWith('+') ? 'var(--green)' :
                     typeof s.roi === 'string' && s.roi.startsWith('-') ? 'var(--red)' : 'var(--text2)';
    const sportIcon = s.sport_icon || (this._meta?.sports?.find(sp => sp.id === s.sport_id)?.icon) || '🏆';
    const sportName = s.sport_name || s.sport || 'Спорт';
    const lastSignal = s.last_signal_at ? `<span style="color:var(--green);font-size:11px">⚡ ${this._relTime(s.last_signal_at)}</span>` : '';
    const sourceChip = s.source === 'db' ? '<span class="chip" style="background:rgba(99,102,241,.2);color:#818cf8">БД</span>' :
                       s.source === 'local' ? '<span class="chip">Локальная</span>' :
                       '<span class="chip">Встроенная</span>';

    const statCodes = (s.stat_event_codes || []).slice(0, 3).map(c =>
      `<span class="lib-stat-chip">${this._getStatName(c)}</span>`
    ).join('');

    return `
    <div class="library-card ${isSelected ? 'selected-strategy' : ''}" onclick="library.selectStrategy('${s.id}')" style="${isSelected ? 'border-color:var(--accent);' : ''}">
      <div class="library-card-title">
        <span>${sportIcon} ${s.name}</span>
        <div style="display:flex;gap:4px;align-items:center">${sourceChip} ${lastSignal}</div>
      </div>
      <div class="library-card-desc">${s.description || s.desc || ''}</div>
      
      ${statCodes ? `<div style="margin:6px 0;display:flex;flex-wrap:wrap;gap:4px">${statCodes}</div>` : ''}
      
      <div class="library-card-stats">
        <span style="color:${roiColor}">ROI: ${s.roi || '—'}</span>
        <span style="color:var(--text2)">Сигналов: ${(s.bets || 0).toLocaleString()}</span>
        <span style="color:var(--text2)">${sportName}</span>
      </div>
      
      <div style="margin:8px 0">${(s.tags || []).map(t => `<span class="library-card-tag">${t}</span>`).join('')}</div>
      
      <div class="library-card-actions">
        <button class="ctrl-btn sm" onclick="event.stopPropagation();library.loadStrategy('${s.id}')">✎ Редактировать</button>
        <button class="ctrl-btn sm" onclick="event.stopPropagation();library.backtestStrategy('${s.id}')">▶ Бэктест</button>
        <button class="ctrl-btn sm ${isSelected ? 'primary' : ''}" onclick="event.stopPropagation();library.toggleMonitor('${s.id}')">
          ${isSelected ? '🔴 Мониторинг' : '📡 Мониторить'}
        </button>
      </div>
    </div>`;
  },

  // ── Выбор стратегии для мониторинга ───────────────────────────────────────
  async selectStrategy(id) {
    if (this._selectedStrategyId === id) {
      this.clearSelection();
      return;
    }
    this._selectedStrategyId = id;
    this._renderCards();

    const panel = document.getElementById('lib-signal-panel');
    if (panel) panel.style.display = 'flex';

    await this._renderSignalPanel();
    this._startLivePolling();
  },

  clearSelection() {
    this._selectedStrategyId = null;
    this._stopLivePolling();
    const panel = document.getElementById('lib-signal-panel');
    if (panel) panel.style.display = 'none';
    this._renderCards();
  },

  toggleMonitor(id) {
    if (this._selectedStrategyId === id) this.clearSelection();
    else this.selectStrategy(id);
  },

  // ── Панель live сигналов ──────────────────────────────────────────────────
  async _renderSignalPanel() {
    const content = document.getElementById('lib-signal-content');
    if (!content || !this._selectedStrategyId) return;

    const strategy = this._strategies.find(s => s.id === this._selectedStrategyId);
    if (!strategy) return;

    content.innerHTML = `<div style="color:var(--text3);text-align:center;padding:20px">⏳ Загрузка матчей...</div>`;

    try {
      const data = await this._fetch(`/api/matching/live/matched?strategy_id=${this._selectedStrategyId}`);
      const games = data?.games || [];
      const signals = data?.signals || [];

      let html = `
        <div style="background:var(--bg3);border-radius:8px;padding:12px;margin-bottom:8px">
          <div style="font-size:12px;color:var(--text3);margin-bottom:8px">🎯 Стратегия: <strong style="color:var(--text1)">${strategy.name}</strong></div>
          <div style="display:flex;gap:16px;font-size:12px">
            <span>Матчей: <strong style="color:var(--accent)">${games.length}</strong></span>
            <span>Активных сигналов: <strong style="color:var(--green)">${signals.length}</strong></span>
          </div>
        </div>`;

      // Активные сигналы
      if (signals.length > 0) {
        html += `<div style="font-size:11px;color:var(--text3);margin-bottom:6px;text-transform:uppercase;letter-spacing:.5px">⚡ Активные сигналы</div>`;
        html += signals.map(sig => `
          <div class="signal-card" style="background:rgba(16,185,129,.08);border:1px solid rgba(16,185,129,.3);border-radius:8px;padding:10px;margin-bottom:8px">
            <div style="font-weight:600;font-size:13px">${sig.home_team_name || '?'} vs ${sig.away_team_name || '?'}</div>
            <div style="font-size:12px;color:var(--text2);margin:4px 0">${sig.stat_event_code} • ${sig.direction?.toUpperCase()} ${sig.line}</div>
            <div style="display:flex;justify-content:space-between;font-size:12px">
              <span style="color:var(--green)">Уверенность: ${Math.round((sig.confidence||0)*100)}%</span>
              <span style="color:var(--accent)">Edge: ${((sig.edge||0)*100).toFixed(1)}%</span>
              <span style="color:var(--text3)">${sig.current_minute || 0}'</span>
            </div>
          </div>`).join('');
      }

      // Подходящие матчи
      if (games.length > 0) {
        html += `<div style="font-size:11px;color:var(--text3);margin:8px 0 6px;text-transform:uppercase;letter-spacing:.5px">📋 Подходящие матчи (${games.length})</div>`;
        html += games.slice(0, 10).map(g => {
          const statusBadge = g.status === 'live'
            ? `<span style="color:var(--red);font-weight:700;font-size:10px">🔴 LIVE ${g.minute || 0}'</span>`
            : `<span style="color:var(--text3);font-size:10px">${this._fmtTime(g.scheduled_at)}</span>`;

          return `
          <div class="match-row" onclick="library.applyToGame('${g.game_uuid}')" style="background:var(--bg3);border-radius:8px;padding:10px;margin-bottom:6px;cursor:pointer;border:1px solid transparent;transition:.2s" onmouseover="this.style.borderColor='var(--accent)'" onmouseout="this.style.borderColor='transparent'">
            <div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:4px">
              <span style="font-size:11px;color:var(--text3)">${g.tournament_short || g.tournament_name || ''}</span>
              ${statusBadge}
            </div>
            <div style="font-weight:600;font-size:13px">${g.home_team || g.home_short || '?'} — ${g.away_team || g.away_short || '?'}</div>
            ${g.status === 'live' ? `<div style="font-size:12px;color:var(--text2);margin-top:2px">${g.score_home ?? '?'} : ${g.score_away ?? '?'}</div>` : ''}
          </div>`;
        }).join('');

        if (games.length > 10) {
          html += `<div style="color:var(--text3);text-align:center;font-size:12px;padding:8px">+ ещё ${games.length - 10} матчей</div>`;
        }
      } else {
        html += `
          <div style="color:var(--text3);text-align:center;padding:30px 0">
            <div style="font-size:24px;margin-bottom:8px">🔍</div>
            <div>Нет подходящих матчей</div>
            <div style="font-size:12px;margin-top:6px">Стратегия будет активирована при появлении подходящих событий</div>
          </div>`;
      }

      content.innerHTML = html;
    } catch (e) {
      content.innerHTML = `<div style="color:var(--red);padding:16px">${e.message}</div>`;
    }
  },

  // ── Применить стратегию к конкретному матчу ───────────────────────────────
  async applyToGame(gameUuid) {
    if (!this._selectedStrategyId) return;
    const content = document.getElementById('lib-signal-content');
    if (content) {
      const analysisDiv = document.createElement('div');
      analysisDiv.style.cssText = 'background:var(--bg3);border-radius:8px;padding:14px;margin-bottom:10px';
      analysisDiv.innerHTML = `<div style="color:var(--text3);font-size:12px">⏳ Анализирую матч...</div>`;
      content.insertBefore(analysisDiv, content.firstChild);

      try {
        const r = await this._fetch('/api/matching/match/apply', 'POST', {
          strategy_id: this._selectedStrategyId,
          game_uuid: gameUuid,
        });

        if (r?.signal?.triggered) {
          const sig = r.signal;
          analysisDiv.innerHTML = `
            <div style="color:var(--green);font-weight:700;margin-bottom:8px">✅ СИГНАЛ ОБНАРУЖЕН!</div>
            <div style="font-size:13px"><strong>${sig.direction?.toUpperCase()} ${sig.line}</strong> (${this._getStatName(sig.stat_event_code)})</div>
            <div style="font-size:12px;color:var(--text2);margin:4px 0">
              Темп: ${r.signal.context?.projected_90min || '?'} | Текущий: ${r.signal.context?.current_total || 0} (${r.signal.context?.minute || 0}')
            </div>
            <div style="display:flex;gap:12px;font-size:12px;margin-top:6px">
              <span style="color:var(--green)">Уверенность: ${Math.round((sig.confidence||0)*100)}%</span>
              <span style="color:var(--accent)">Edge: ${((sig.edge||0)*100).toFixed(1)}%</span>
              <span>Коэф: ${sig.current_odds?.toFixed(2) || '—'}</span>
            </div>`;
        } else {
          analysisDiv.innerHTML = `
            <div style="color:var(--text3);font-size:12px">🔍 Условия стратегии не выполнены на текущий момент</div>
            ${r?.signal?.context ? `<div style="font-size:11px;color:var(--text3);margin-top:4px">Темп: ${r.signal.context.projected_90min || '—'} vs линия ${r.signal.context.line || '—'}</div>` : ''}`;
        }
        setTimeout(() => analysisDiv.remove(), 10000);
      } catch (e) {
        analysisDiv.innerHTML = `<div style="color:var(--red);font-size:12px">${e.message}</div>`;
        setTimeout(() => analysisDiv.remove(), 5000);
      }
    }
  },

  // ── Live polling ──────────────────────────────────────────────────────────
  _startLivePolling() {
    this._stopLivePolling();
    this._liveInterval = setInterval(() => this._renderSignalPanel(), 30000);
  },

  _stopLivePolling() {
    if (this._liveInterval) clearInterval(this._liveInterval);
    this._liveInterval = null;
  },

  // ── Стандартные действия ──────────────────────────────────────────────────
  loadStrategy(id) {
    const s = this._strategies.find(x => x.id == id);
    if (!s) return;
    if (s.code) {
      document.getElementById('strategyCode') && (document.getElementById('strategyCode').value = s.code);
      document.getElementById('strategyName') && (document.getElementById('strategyName').value = s.name);
    }
    app.showPanel('strategy');
    strategyBuilder?.showTab?.('code');
  },

  backtestStrategy(id) {
    app.showPanel('backtest');
    // Передаём ID стратегии в backtest если есть API
    setTimeout(() => {
      if (typeof backtest !== 'undefined' && backtest.loadStrategy) {
        backtest.loadStrategy(id);
      }
    }, 300);
  },

  search(q) { this._renderCards(); },

  // ── Встроенные стратегии ──────────────────────────────────────────────────
  _getBuiltins() {
    return [
      { id:'b1', name:'Value Betting (Poisson)', desc:'Poisson модель рассчитывает ожидаемые голы, сравнивает с рынком, ищет edge > 5%', roi:'+12.3%', bets:1847, sport_name:'Football', sport_icon:'⚽', sport_id:1, tags:['value','poisson','mathematical'], source:'builtin', stat_event_codes:['total_goals'], created_at:'2024-01-01' },
      { id:'b2', name:'Over 2.5 xG Model', desc:'Ставка Over 2.5 когда средний xG команд превышает 2.8 по данным FBref', roi:'+7.8%', bets:923, sport_name:'Football', sport_icon:'⚽', sport_id:1, tags:['xg','totals','advanced'], source:'builtin', stat_event_codes:['total_goals'], created_at:'2024-01-01' },
      { id:'b3', name:'Home Form Momentum', desc:'Ставим на хозяев с 4 победами в последних 5 дома vs гости с 2 победами в 5 на выезде', roi:'+5.4%', bets:2134, sport_name:'Football', sport_icon:'⚽', sport_id:1, tags:['form','home','momentum'], source:'builtin', stat_event_codes:[], created_at:'2024-01-01' },
      { id:'b4', name:'ELO Rating Value', desc:'Кастомные ELO рейтинги, ставим когда вероятность ELO бьёт рынок на 7%+', roi:'+9.1%', bets:1456, sport_name:'Football', sport_icon:'⚽', sport_id:1, tags:['elo','value','rating'], source:'builtin', stat_event_codes:[], created_at:'2024-01-01' },
      { id:'b5', name:'ATP Serve Dominance', desc:'Ставим на игроков с 65%+ побед на первой подаче против соперников с <55% на приёме', roi:'+11.7%', bets:892, sport_name:'Tennis', sport_icon:'🎾', sport_id:3, tags:['serve','atp','stats'], source:'builtin', stat_event_codes:['tennis_aces'], created_at:'2024-01-01' },
      { id:'b6', name:'NBA Over Pace Model', desc:'Ставим Тотал больше когда темп обеих команд выше 100 посессий за 48 мин', roi:'+8.4%', bets:743, sport_name:'Basketball', sport_icon:'🏀', sport_id:2, tags:['pace','nba','totals'], source:'builtin', stat_event_codes:['basketball_points'], created_at:'2024-01-01' },
      { id:'b7', name:'Corners Live Model', desc:'Мониторим угловые в live: при темпе >11/90мин ставим Over 9.5 если коэф > 1.85', roi:'+6.2%', bets:1203, sport_name:'Football', sport_icon:'⚽', sport_id:1, tags:['corners','live','pace'], source:'builtin', stat_event_codes:['total_corners'], created_at:'2024-01-01' },
    ];
  },

  // ── Utils ─────────────────────────────────────────────────────────────────
  _getStatName(code) {
    if (!code) return code;
    const found = this._meta?.statEventTypes?.find(s => s.code === code);
    if (found) return found.name_ru || found.name;
    const map = {
      'total_goals': 'Голы', 'total_corners': 'Угловые', 'total_yellow_cards': 'Жёлтые карточки',
      'shots_on_target': 'Удары в створ', 'basketball_points': 'Очки', 'hockey_goals': 'Голы хоккей',
      'tennis_aces': 'Эйсы', 'total_fouls': 'Фолы',
    };
    return map[code] || code;
  },

  _relTime(iso) {
    if (!iso) return '';
    const diff = Date.now() - new Date(iso).getTime();
    if (diff < 60000) return 'только что';
    if (diff < 3600000) return `${Math.floor(diff / 60000)} мин назад`;
    if (diff < 86400000) return `${Math.floor(diff / 3600000)} ч назад`;
    return new Date(iso).toLocaleDateString('ru');
  },

  _fmtTime(iso) {
    if (!iso) return '—';
    return new Date(iso).toLocaleString('ru', { month:'short', day:'numeric', hour:'2-digit', minute:'2-digit' });
  },

  _bindEvents() {
    // Дополнительные события если нужно
  },

  async _fetch(url, method = 'GET', body = null) {
    const opts = {
      method,
      headers: { 'Content-Type': 'application/json', 'x-auth-token': localStorage.getItem('bq_token') || 'demo' },
    };
    if (body) opts.body = JSON.stringify(body);
    const r = await fetch(url, opts);
    if (!r.ok) throw new Error(`HTTP ${r.status}: ${await r.text().catch(() => '')}`);
    return r.json();
  },
};