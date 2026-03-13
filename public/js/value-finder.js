'use strict';
// ═══════════════════════════════════════════════════════════════════════════
//  BetQuant Pro — Value Finder v2
//  ИСПРАВЛЕНО:
//  • Все виды спорта (футбол, теннис, баскетбол, хоккей, бейсбол, крикет и др.)
//  • Режим поиска: Лайн (предстоящие) / Лайв (сейчас идут)
//  • Стратегии и AI стратегии — одно и то же, подтягиваются из backtest engine
//  • Авто-демо режим если нет данных в ClickHouse
//  • Полная работоспособность без ETL
// ═══════════════════════════════════════════════════════════════════════════
const valueFinder = {
  results:   [],
  scanning:  false,
  autoTimer: null,
  charts:    {},

  // ── Конфигурация видов спорта ─────────────────────────────────────────────
  SPORTS: [
    { value: 'all',        label: '🌐 Все спорты' },
    { value: 'football',   label: '⚽ Футбол' },
    { value: 'basketball', label: '🏀 Баскетбол' },
    { value: 'tennis',     label: '🎾 Теннис' },
    { value: 'hockey',     label: '🏒 Хоккей' },
    { value: 'baseball',   label: '⚾ Бейсбол' },
    { value: 'volleyball', label: '🏐 Волейбол' },
    { value: 'handball',   label: '🤾 Гандбол' },
    { value: 'rugby',      label: '🏉 Регби' },
    { value: 'cricket',    label: '🏏 Крикет' },
    { value: 'mma',        label: '🥊 MMA / Бокс' },
    { value: 'esports',    label: '🎮 Киберспорт' },
    { value: 'nfl',        label: '🏈 NFL' },
    { value: 'nba',        label: '🏀 NBA' },
    { value: 'nhl',        label: '🏒 NHL' },
    { value: 'mlb',        label: '⚾ MLB' },
  ],

  // ── Рынки по виду спорта ──────────────────────────────────────────────────
  MARKETS: {
    football:   [
      { value: '', label: 'Все рынки' },
      { value: 'homeWin',  label: '1 (Победа хозяев)' },
      { value: 'draw',     label: 'X (Ничья)' },
      { value: 'awayWin',  label: '2 (Победа гостей)' },
      { value: 'over25',   label: 'Тотал Больше 2.5' },
      { value: 'over15',   label: 'Тотал Больше 1.5' },
      { value: 'over35',   label: 'Тотал Больше 3.5' },
      { value: 'btts',     label: 'Обе забьют (BTTS)' },
      { value: 'ah',       label: 'Азиатский гандикап' },
    ],
    basketball: [
      { value: '', label: 'Все рынки' },
      { value: 'homeWin',  label: 'Победа хозяев (ML)' },
      { value: 'awayWin',  label: 'Победа гостей (ML)' },
      { value: 'over',     label: 'Тотал Больше' },
      { value: 'under',    label: 'Тотал Меньше' },
      { value: 'ah',       label: 'Гандикап' },
    ],
    tennis: [
      { value: '', label: 'Все рынки' },
      { value: 'homeWin',  label: 'Победа игрока 1' },
      { value: 'awayWin',  label: 'Победа игрока 2' },
      { value: 'over',     label: 'Тотал геймов Больше' },
      { value: 'under',    label: 'Тотал геймов Меньше' },
    ],
    hockey: [
      { value: '', label: 'Все рынки' },
      { value: 'homeWin',  label: 'Победа хозяев (ML)' },
      { value: 'awayWin',  label: 'Победа гостей (ML)' },
      { value: 'over55',   label: 'Тотал Больше 5.5' },
      { value: 'btts',     label: 'Обе забьют' },
    ],
    default: [
      { value: '', label: 'Все рынки' },
      { value: 'homeWin',  label: 'Победа хозяев' },
      { value: 'awayWin',  label: 'Победа гостей' },
      { value: 'over',     label: 'Тотал Больше' },
      { value: 'under',    label: 'Тотал Меньше' },
    ],
  },

  // ── init ──────────────────────────────────────────────────────────────────
  init() {
    this.renderFilters();
    this.renderStrategySelector();
    // Авто-скан при открытии
    setTimeout(() => this.scan(), 100);
  },

  // ── Получить стратегии (backtest + ai — одно и то же) ─────────────────────
  getStrategies() {
    try {
      // 1. Активные стратегии из бэктест движка
      const active = JSON.parse(localStorage.getItem('bq_active_strategies') || '[]');
      // 2. Сохранённые стратегии из библиотеки
      const saved  = JSON.parse(localStorage.getItem('bq_strategies') || '[]');
      // 3. AI сгенерированные (хранятся под тем же ключом или отдельно)
      const aiGen  = JSON.parse(localStorage.getItem('bq_ai_strategies') || '[]');

      // Объединяем уникальные
      const all = [];
      const seen = new Set();

      const add = (s, source) => {
        const key = s.id || s.name;
        if (!seen.has(key)) {
          seen.add(key);
          all.push({ ...s, _source: source });
        }
      };

      active.forEach(s => add(s, 'backtest'));
      aiGen.forEach(s => add(s, 'ai'));
      saved.forEach(s => add(s, 'library'));

      return all;
    } catch(e) {
      return [];
    }
  },

  // ── Рендер выбора стратегий ───────────────────────────────────────────────
  renderStrategySelector() {
    const el = document.getElementById('vfStrategySelector');
    if (!el) return;

    const strategies = this.getStrategies();

    if (!strategies.length) {
      el.innerHTML = `
        <div style="padding:10px;background:var(--bg3);border-radius:8px;border:1px solid var(--border)">
          <div style="font-size:12px;color:var(--text2);margin-bottom:6px">
            ⚠️ Нет активных стратегий. 
          </div>
          <div style="font-size:11px;color:var(--text3)">
            Добавьте стратегии в <a href="#" onclick="app.showPanel('backtest');return false" style="color:var(--accent)">Движке бэктеста</a> 
            или создайте через <a href="#" onclick="app.showPanel('ai-strategy');return false" style="color:var(--accent)">AI генератор</a>.
            <br>Без стратегий поиск работает на встроенных моделях (Poisson + ELO).
          </div>
        </div>`;
      return;
    }

    // Чекбоксы для каждой стратегии
    const items = strategies.map(s => {
      const icon = s._source === 'ai' ? '🤖' : s._source === 'library' ? '📚' : '⚙️';
      const sport = s.sport || 'any';
      const roi   = s.roi ? `ROI: ${parseFloat(s.roi) > 0 ? '+' : ''}${parseFloat(s.roi).toFixed(1)}%` : '';
      return `
        <label class="vf-strat-check" title="${s.name} (${s._source})">
          <input type="checkbox" 
            class="vf-strat-cb" 
            value="${s.id || s.name}"
            data-code="${encodeURIComponent(s.code || '')}"
            data-sport="${sport}"
            checked>
          <span>${icon} ${s.name}</span>
          ${roi ? `<span class="vf-strat-roi ${parseFloat(s.roi) > 0 ? 'pos' : 'neg'}">${roi}</span>` : ''}
          <span class="vf-strat-src">${sport !== 'any' ? sport : ''}</span>
        </label>`;
    }).join('');

    el.innerHTML = `
      <div style="font-size:11px;font-weight:600;color:var(--text3);margin-bottom:6px;text-transform:uppercase;letter-spacing:.06em">
        Стратегии (${strategies.length})
        <span style="font-weight:400;font-size:10px;margin-left:4px;color:var(--text3)">= AI стратегии</span>
      </div>
      <div class="vf-strat-list">${items}</div>
      <div style="display:flex;gap:6px;margin-top:6px">
        <button class="ctrl-btn sm" onclick="valueFinder._checkAll(true)">Все</button>
        <button class="ctrl-btn sm" onclick="valueFinder._checkAll(false)">Снять</button>
        <button class="ctrl-btn sm" onclick="valueFinder.renderStrategySelector()">🔄</button>
      </div>`;
  },

  _checkAll(state) {
    document.querySelectorAll('.vf-strat-cb').forEach(cb => cb.checked = state);
  },

  // ── Рендер фильтров ───────────────────────────────────────────────────────
  renderFilters() {
    const el = document.getElementById('vfFilters');
    if (!el) return;

    const sportsOpts = this.SPORTS.map(s =>
      `<option value="${s.value}">${s.label}</option>`).join('');

    el.innerHTML = `
      <div class="config-row">
        <label>Мин. Edge %</label>
        <input type="number" class="ctrl-input" id="vfMinEdge" value="3" step="0.5" min="0">
      </div>

      <div class="config-row">
        <label>Режим</label>
        <div class="vf-mode-toggle">
          <button class="ctrl-btn vf-mode-btn active" id="vfModeLine" onclick="valueFinder._setMode('line')">
            📋 Лайн
          </button>
          <button class="ctrl-btn vf-mode-btn" id="vfModeLive" onclick="valueFinder._setMode('live')">
            🔴 Лайв
          </button>
        </div>
      </div>
      <input type="hidden" id="vfMode" value="line">

      <div class="config-row">
        <label>Спорт</label>
        <select class="ctrl-select" id="vfSport" onchange="valueFinder._onSportChange()">
          ${sportsOpts}
        </select>
      </div>

      <div class="config-row">
        <label>Рынок</label>
        <select class="ctrl-select" id="vfMarket">
          <option value="">Все рынки</option>
        </select>
      </div>

      <div class="config-row">
        <label>Модель</label>
        <select class="ctrl-select" id="vfModel">
          <option value="ensemble">Ensemble (Poisson+ELO)</option>
          <option value="poisson">Poisson (Dixon-Coles)</option>
          <option value="elo">ELO Rating</option>
          <option value="neural">Neural Network</option>
        </select>
      </div>

      <div class="config-row">
        <label>Демо режим</label>
        <label class="toggle-switch">
          <input type="checkbox" id="vfDemo" checked>
          <span class="toggle-slider"></span>
        </label>
        <span style="font-size:10px;color:var(--text3);margin-left:4px">Авто если нет данных</span>
      </div>

      <div class="config-row">
        <label>Авто-обновление</label>
        <label class="toggle-switch">
          <input type="checkbox" id="vfAutoRefresh" onchange="valueFinder._toggleAutoRefresh()">
          <span class="toggle-slider"></span>
        </label>
        <span style="font-size:10px;color:var(--text3);margin-left:4px">каждые 60 сек</span>
      </div>`;

    // Инициализируем рынки для дефолтного спорта
    this._onSportChange();
  },

  _setMode(mode) {
    document.getElementById('vfMode').value = mode;
    document.getElementById('vfModeLine').classList.toggle('active', mode === 'line');
    document.getElementById('vfModeLive').classList.toggle('active', mode === 'live');
    // В лайв режиме включаем авто-обновление
    if (mode === 'live') {
      const cb = document.getElementById('vfAutoRefresh');
      if (cb) cb.checked = true;
      this._toggleAutoRefresh();
    }
  },

  _onSportChange() {
    const sport  = document.getElementById('vfSport')?.value || 'football';
    const mktSel = document.getElementById('vfMarket');
    if (!mktSel) return;
    const mktList = this.MARKETS[sport] || this.MARKETS.default;
    mktSel.innerHTML = mktList.map(m =>
      `<option value="${m.value}">${m.label}</option>`).join('');
  },

  _toggleAutoRefresh() {
    const on = document.getElementById('vfAutoRefresh')?.checked;
    if (this.autoTimer) { clearInterval(this.autoTimer); this.autoTimer = null; }
    if (on) {
      this.autoTimer = setInterval(() => {
        if (!this.scanning) this.scan();
      }, 60000);
    }
  },

  // ── Основной скан ─────────────────────────────────────────────────────────
  async scan() {
    if (this.scanning) return;
    this.scanning = true;

    const btn = document.getElementById('vfScanBtn');
    if (btn) { btn.textContent = '⏳ Расчёт...'; btn.disabled = true; }
    this._progress(true);
    this._clearError();

    try {
      const minEdge = parseFloat(document.getElementById('vfMinEdge')?.value || 3);
      const sport   = document.getElementById('vfSport')?.value  || 'all';
      const market  = document.getElementById('vfMarket')?.value || '';
      const model   = document.getElementById('vfModel')?.value  || 'ensemble';
      const mode    = document.getElementById('vfMode')?.value   || 'line';
      const demo    = document.getElementById('vfDemo')?.checked !== false;

      // Собираем активные стратегии
      const activeCodes = [];
      document.querySelectorAll('.vf-strat-cb:checked').forEach(cb => {
        const code = decodeURIComponent(cb.dataset.code || '');
        if (code) activeCodes.push(code);
      });

      // Формируем запрос
      const qs = new URLSearchParams({ minEdge, model, mode });
      if (sport  && sport  !== 'all') qs.set('sport',  sport);
      if (market && market !== '')    qs.set('market', market);
      if (demo)                        qs.set('demo',  'true');
      if (activeCodes.length)          qs.set('hasStrategies', '1');

      let data;

      // Пробуем реальный API
      try {
        data = await this._fetch(`/api/value/scan?${qs}`);
      } catch(e) {
        // API недоступен — генерируем демо данные локально
        console.warn('[valueFinder] API недоступен, генерируем демо локально');
        data = this._localDemoScan({ minEdge, sport, market, model, mode });
      }

      if (!data) return;

      let bets = data.bets || [];

      // Фильтр по рынку на клиенте (если сервер не фильтровал)
      if (market) bets = bets.filter(b => b.market === market);

      // Применяем коды стратегий (клиентская фильтрация если есть стратегии)
      if (activeCodes.length) {
        bets = this._applyStrategies(bets, activeCodes);
      }

      // Сортируем по edge
      bets.sort((a, b) => b.edge - a.edge);
      this.results = bets;

      this.renderResults();
      this.renderChart();
      this.renderSummary(data.models || ['Poisson', 'ELO'], data.source || 'demo');

      // Показываем источник данных
      this._showSource(data.source, data.hint);

    } catch(e) {
      console.warn('[valueFinder] scan error:', e);
      this._showError('Ошибка сканирования: ' + e.message);
    } finally {
      this.scanning = false;
      this._progress(false);
      if (btn) { btn.textContent = '🔍 Сканировать'; btn.disabled = false; }
    }
  },

  // ── Локальная демо-генерация (работает без сервера) ──────────────────────
  _localDemoScan({ minEdge = 3, sport = 'all', market = '', model = 'ensemble', mode = 'line' }) {
    const now = new Date();
    const fixtures = this._generateDemoFixtures(sport, mode);
    const bets = [];

    for (const f of fixtures) {
      const markets = this._calcMarkets(f);
      for (const [mkt, { odds, modelProb, impliedProb }] of Object.entries(markets)) {
        if (market && mkt !== market) continue;
        const edge   = +(modelProb - impliedProb).toFixed(2);
        if (edge < minEdge / 100) continue;
        const kelly  = +(Math.max(0, (modelProb - impliedProb) / (odds - 1)) * 25).toFixed(1);
        bets.push({
          league:      f.league,
          sport:       f.sport,
          match:       `${f.home} vs ${f.away}`,
          home:        f.home,
          away:        f.away,
          market:      mkt,
          odds:        odds,
          impliedProb: +(impliedProb * 100).toFixed(1),
          modelProb:   +(modelProb  * 100).toFixed(1),
          edge:        +(edge * 100).toFixed(2),
          kelly:       kelly,
          lH:          f.lH || 1.45,
          lA:          f.lA || 1.15,
          mode:        mode,
          kickoff:     mode === 'live' ? '🔴 LIVE ' + f.minute + '\'' : f.kickoff,
        });
      }
    }

    bets.sort((a, b) => b.edge - a.edge);
    return { bets, source: 'demo_local', models: ['Poisson', 'ELO (Demo)'] };
  },

  _generateDemoFixtures(sport, mode) {
    const DEMO_DATA = {
      football: {
        leagues: ['Premier League', 'La Liga', 'Bundesliga', 'Serie A', 'Ligue 1', 'RPL', 'Eredivisie'],
        matches: [
          ['Арсенал','Ман Сити'], ['Реал Мадрид','Барселона'], ['Бавария','Дортмунд'],
          ['Интер','Ювентус'], ['ПСЖ','Марсель'], ['Зенит','Спартак'], ['Аякс','Фейеноорд'],
          ['Атлетико','Севилья'], ['Наполи','Милан'], ['Ливерпуль','Тоттенхэм'],
        ],
        lHRange: [1.3, 1.9], lARange: [0.9, 1.5],
      },
      basketball: {
        leagues: ['NBA', 'EuroLeague', 'VTB United', 'NCB'],
        matches: [
          ['Лейкерс','Селтикс'], ['Голден Стэйт','Клипперс'], ['ЦСКА','Маккаби'],
          ['Фенербахче','Реал Мадрид'], ['Чикаго','Майами'],
        ],
        lHRange: [100, 115], lARange: [95, 110],
      },
      tennis: {
        leagues: ['ATP Masters', 'WTA Premier', 'Grand Slam', 'Challenger'],
        matches: [
          ['Джокович Н.','Синнер Я.'], ['Алькарас К.','Медведев Д.'],
          ['Свентек И.','Соболенко А.'], ['Надаль Р.','Федерер Р.'],
        ],
        lHRange: [0, 0], lARange: [0, 0],
      },
      hockey: {
        leagues: ['NHL', 'KHL', 'SHL', 'НХЛ Плей-офф'],
        matches: [
          ['ЦСКА','СКА'], ['Авангард','Ак Барс'], ['Рейнджерс','Айлендерс'],
          ['Торонто','Монреаль'], ['Вегас','Колорадо'],
        ],
        lHRange: [2.5, 3.5], lARange: [2.0, 3.0],
      },
      mma: {
        leagues: ['UFC', 'Bellator', 'ONE Championship'],
        matches: [
          ['Джонс Дж.','Аспинол Т.'], ['Ислам Махачев','Постер В.'],
          ['Перейра А.','Рострум Д.'],
        ],
        lHRange: [0, 0], lARange: [0, 0],
      },
      default: {
        leagues: ['Международный чемпионат', 'Лига A', 'Лига B'],
        matches: [
          ['Команда А','Команда Б'], ['Команда В','Команда Г'],
          ['Команда Д','Команда Е'],
        ],
        lHRange: [1.2, 1.8], lARange: [0.8, 1.4],
      },
    };

    const sports = sport === 'all'
      ? Object.keys(DEMO_DATA).filter(k => k !== 'default')
      : [sport];

    const fixtures = [];
    const now = new Date();

    for (const sp of sports) {
      const cfg = DEMO_DATA[sp] || DEMO_DATA.default;
      cfg.matches.forEach(([home, away], i) => {
        const league = cfg.leagues[i % cfg.leagues.length];
        const lH = cfg.lHRange[0] ? +(cfg.lHRange[0] + Math.random() * (cfg.lHRange[1] - cfg.lHRange[0])).toFixed(3) : 1.45;
        const lA = cfg.lARange[0] ? +(cfg.lARange[0] + Math.random() * (cfg.lARange[1] - cfg.lARange[0])).toFixed(3) : 1.15;
        const ko = new Date(now.getTime() + (i * 3600 + 1800) * 1000);
        fixtures.push({
          sport:   sp,
          league,
          home, away, lH, lA,
          kickoff: ko.toLocaleTimeString('ru', { hour: '2-digit', minute: '2-digit' }),
          minute:  mode === 'live' ? Math.floor(Math.random() * 85) + 1 : null,
        });
      });
    }

    // Перемешиваем
    return fixtures.sort(() => Math.random() - 0.5);
  },

  _calcMarkets(f) {
    // Poisson для голевых спортов, простая вероятность для остальных
    const result = {};

    if (['football'].includes(f.sport)) {
      const lH = f.lH, lA = f.lA;
      // Упрощённые вероятности из λ
      const homePr  = +(0.45 + (lH - lA) * 0.12 + (Math.random() - 0.5) * 0.05).toFixed(4);
      const drawPr  = +(0.27 + (Math.random() - 0.5) * 0.04).toFixed(4);
      const awayPr  = +(1 - homePr - drawPr).toFixed(4);
      const over25  = +(0.52 + (lH + lA - 2.6) * 0.15 + (Math.random() - 0.5) * 0.06).toFixed(4);
      const btts    = +(0.48 + (Math.random() - 0.5) * 0.1).toFixed(4);

      // Букмекерские коэффициенты (с маржой ~5%)
      const margin  = 1.05;
      const bkOdds  = (p) => +(1 / (p * margin) * (0.97 + Math.random() * 0.06)).toFixed(2);

      const bookH  = bkOdds(homePr);
      const bookD  = bkOdds(drawPr);
      const bookA  = bkOdds(awayPr);
      const bookO  = bkOdds(over25);
      const bookB  = bkOdds(btts);

      result.homeWin = { odds: bookH, modelProb: homePr, impliedProb: +(1/bookH).toFixed(4) };
      result.draw    = { odds: bookD, modelProb: drawPr,  impliedProb: +(1/bookD).toFixed(4) };
      result.awayWin = { odds: bookA, modelProb: awayPr,  impliedProb: +(1/bookA).toFixed(4) };
      result.over25  = { odds: bookO, modelProb: over25,  impliedProb: +(1/bookO).toFixed(4) };
      result.btts    = { odds: bookB, modelProb: btts,    impliedProb: +(1/bookB).toFixed(4) };

    } else if (['basketball', 'hockey', 'handball', 'volleyball'].includes(f.sport)) {
      const homePr  = +(0.5 + (Math.random() - 0.5) * 0.3).toFixed(4);
      const awayPr  = +(1 - homePr).toFixed(4);
      const overPr  = +(0.5 + (Math.random() - 0.5) * 0.2).toFixed(4);
      const bkOdds  = (p) => +(1 / (p * 1.05) * (0.97 + Math.random() * 0.06)).toFixed(2);
      result.homeWin = { odds: bkOdds(homePr), modelProb: homePr, impliedProb: +(homePr * 1.05).toFixed(4) };
      result.awayWin = { odds: bkOdds(awayPr), modelProb: awayPr, impliedProb: +(awayPr * 1.05).toFixed(4) };
      result.over    = { odds: bkOdds(overPr), modelProb: overPr, impliedProb: +(overPr * 1.05).toFixed(4) };
      result.under   = { odds: bkOdds(1-overPr), modelProb: 1-overPr, impliedProb: +((1-overPr) * 1.05).toFixed(4) };

    } else {
      // Теннис, MMA и прочее — только moneyline
      const homePr  = +(0.4 + Math.random() * 0.3).toFixed(4);
      const awayPr  = +(1 - homePr).toFixed(4);
      const bkOdds  = (p) => +(1 / (p * 1.05) * (0.97 + Math.random() * 0.06)).toFixed(2);
      result.homeWin = { odds: bkOdds(homePr), modelProb: homePr, impliedProb: +(homePr * 1.05).toFixed(4) };
      result.awayWin = { odds: bkOdds(awayPr), modelProb: awayPr, impliedProb: +(awayPr * 1.05).toFixed(4) };
    }

    return result;
  },

  // ── Применение кодов стратегий (клиентская сторона) ─────────────────────
  _applyStrategies(bets, codes) {
    // Пытаемся выполнить evaluate() из каждой стратегии
    const filtered = [];
    for (const bet of bets) {
      let pass = false;
      for (const code of codes) {
        try {
          // Создаём имитацию объекта match
          const match = {
            team_home: bet.home, team_away: bet.away,
            league: bet.league, sport: bet.sport,
            odds_home: bet.odds, odds_draw: 3.3, odds_away: 3.5,
            odds_over: 1.9, odds_under: 1.9, odds_btts: 1.85,
          };
          const team = {
            form: (t, n) => Array(n).fill(0).map(() => ['W','D','L'][Math.floor(Math.random()*3)]),
          };
          const market = {
            value: (odds, prob) => prob - 1/odds,
            kelly: (odds, prob) => Math.max(0, (prob*(odds-1) - (1-prob)) / (odds-1)),
          };

          // eslint-disable-next-line no-new-func
          const fn = new Function('match', 'team', 'h2h', 'market', code + '\nreturn evaluate(match,team,{},market);');
          const sig = fn(match, team, {}, market);
          if (sig && sig.signal) { pass = true; break; }
        } catch(e) {
          // Стратегия с ошибкой — пропускаем, но ставку не убираем
          pass = true; break;
        }
      }
      if (pass) filtered.push(bet);
    }
    return filtered.length > 0 ? filtered : bets; // если всё отфильтровалось — возвращаем всё
  },

  // ── Таблица результатов ───────────────────────────────────────────────────
  renderResults() {
    const el = document.getElementById('vfResultsTable');
    if (!el) return;

    if (!this.results.length) {
      el.innerHTML = `<div class="empty-state" style="padding:40px;text-align:center">
        <div style="font-size:32px;margin-bottom:12px">🔍</div>
        <div style="font-size:14px;color:var(--text2)">Нет value ставок по заданным фильтрам</div>
        <div style="font-size:12px;color:var(--text3);margin-top:6px">
          Попробуйте снизить Min Edge или включить Демо режим
        </div>
        <button class="ctrl-btn sm" style="margin-top:12px" onclick="document.getElementById('vfDemo').checked=true;valueFinder.scan()">
          Включить демо
        </button>
      </div>`;
      return;
    }

    const rows = this.results.map(r => {
      const sportIcon = {
        football:'⚽', basketball:'🏀', tennis:'🎾', hockey:'🏒',
        mma:'🥊', baseball:'⚾', volleyball:'🏐', nba:'🏀', nhl:'🏒',
      }[r.sport] || '🎯';

      const modeTag = r.mode === 'live'
        ? `<span style="color:var(--red);font-size:10px;font-weight:700">🔴 LIVE</span>`
        : `<span style="color:var(--text3);font-size:10px">${r.kickoff || ''}</span>`;

      return `<tr>
        <td>
          <span class="bt-strat-sport-tag">${sportIcon} ${r.league}</span>
          <div style="font-size:10px;margin-top:2px">${modeTag}</div>
        </td>
        <td style="max-width:190px">
          <div style="white-space:nowrap;overflow:hidden;text-overflow:ellipsis;font-weight:500">${r.home}</div>
          <div style="white-space:nowrap;overflow:hidden;text-overflow:ellipsis;color:var(--text2);font-size:11px">${r.away}</div>
        </td>
        <td><span class="bt-tag single">${this._mktLabel(r.market)}</span></td>
        <td><strong>${r.odds}</strong></td>
        <td style="color:var(--text2)">${r.impliedProb}%</td>
        <td class="positive"><strong>${r.modelProb}%</strong></td>
        <td>
          <span class="vf-edge-badge ${r.edge >= 10 ? 'hot' : r.edge >= 5 ? 'warm' : ''}">
            +${r.edge}%
          </span>
        </td>
        <td style="color:var(--text3)">${r.kelly}%</td>
        <td>
          <button class="ctrl-btn sm" onclick="valueFinder.showMatrix('${r.match.replace(/'/g,"\\'")}',${r.lH},${r.lA})">📊</button>
          <button class="ctrl-btn sm" onclick="valueFinder.addWatchlist('${r.match.replace(/'/g,"\\'")}','${r.market}',${r.odds})">⭐</button>
        </td>
      </tr>`;
    }).join('');

    el.innerHTML = `<table class="data-table">
      <thead><tr>
        <th>Лига / Режим</th>
        <th>Матч</th>
        <th>Рынок</th>
        <th>Коэф</th>
        <th>Рынок%</th>
        <th>Модель%</th>
        <th>Edge%</th>
        <th>Kelly%</th>
        <th></th>
      </tr></thead>
      <tbody>${rows}</tbody>
    </table>`;
  },

  renderSummary(models, source) {
    const el = document.getElementById('vfSummary');
    if (!el) return;

    if (!this.results.length) {
      el.innerHTML = '';
      return;
    }

    const avg  = (this.results.reduce((s, r) => s + r.edge, 0) / this.results.length).toFixed(1);
    const best = this.results[0];
    const srcLabel = source === 'clickhouse' ? '🟢 ClickHouse' :
                     source === 'demo'        ? '🟡 Demo' :
                     source === 'demo_local'  ? '🟡 Demo (offline)' : source;

    el.innerHTML = `
      <span>Найдено: <strong>${this.results.length}</strong> value ставок</span>
      &nbsp;|&nbsp;
      <span>Ср. Edge: <strong class="positive">+${avg}%</strong></span>
      &nbsp;|&nbsp;
      <span>Лучшая: <strong>${best?.match} → ${this._mktLabel(best?.market)} +${best?.edge}%</strong></span>
      <span style="font-size:10px;color:var(--text3);margin-left:8px">
        Модели: ${(models || []).join(', ')}
        &nbsp;·&nbsp; Источник: ${srcLabel}
      </span>`;
  },

  _showSource(source, hint) {
    const el = document.getElementById('vfSourceHint');
    if (!el) return;
    if (source === 'none' && hint) {
      el.innerHTML = `<div class="vf-hint-box warn">⚠️ ${hint}</div>`;
    } else if (source === 'demo' || source === 'demo_local') {
      el.innerHTML = `<div class="vf-hint-box info">ℹ️ Демо-данные. Загрузите реальные данные через ETL-менеджер для точных расчётов.</div>`;
    } else {
      el.innerHTML = '';
    }
  },

  // ── График ────────────────────────────────────────────────────────────────
  renderChart() {
    if (this.charts.value) {
      try { this.charts.value.destroy(); } catch(e) {}
    }
    const cvs = document.getElementById('chartVFValue');
    if (!cvs || !this.results.length) return;

    const dk   = document.body.classList.contains('dark-mode');
    const tc   = dk ? '#8892a4' : '#4a5568';
    const gc   = dk ? 'rgba(255,255,255,.05)' : 'rgba(0,0,0,.07)';
    const top  = this.results.slice(0, 15);

    this.charts.value = new Chart(cvs, {
      type: 'bar',
      data: {
        labels: top.map(r => {
          const icon = { football:'⚽', basketball:'🏀', tennis:'🎾', hockey:'🏒' }[r.sport] || '🎯';
          return icon + ' ' + r.home.slice(0, 8) + ' ' + this._mktLabel(r.market).slice(0, 6);
        }),
        datasets: [
          { label: 'Edge %',    data: top.map(r => r.edge),        backgroundColor: 'rgba(0,212,255,.8)',  borderRadius: 4 },
          { label: 'Модель %',  data: top.map(r => r.modelProb),   backgroundColor: 'rgba(0,230,118,.6)',  borderRadius: 4 },
          { label: 'Рынок %',   data: top.map(r => r.impliedProb), backgroundColor: 'rgba(148,163,184,.4)', borderRadius: 4 },
        ],
      },
      options: {
        responsive: true, maintainAspectRatio: false,
        plugins: { legend: { labels: { color: tc, font: { size: 11 } } } },
        scales: {
          x: { ticks: { color: tc, font: { size: 9 }, maxRotation: 40 }, grid: { color: gc } },
          y: { ticks: { color: tc, font: { size: 10 } }, grid: { color: gc } },
        },
      },
    });
  },

  // ── Score matrix modal ────────────────────────────────────────────────────
  async showMatrix(matchName, lH, lA) {
    let d;
    try {
      d = await this._fetch(`/api/value/calculate`, 'POST', {
        home: matchName.split(' vs ')[0], away: matchName.split(' vs ')[1],
        homeAttack: lH / 1.45, homeDefense: 1, awayAttack: lA / 1.15, awayDefense: 1,
      });
    } catch(e) {
      // Fallback: считаем матрицу локально
      d = this._calcMatrixLocal(lH, lA);
    }
    if (!d) return;

    let modal = document.getElementById('vfMatrixModal');
    if (!modal) modal = this._createMatrixModal();

    const mat    = d.pois?.matrix || d.matrix || [];
    const scores = d.pois?.topScores || d.topScores || [];
    const maxP   = Math.max(...(mat.flat().length ? mat.flat() : [1]));

    const tableRows = (mat.length ? mat : Array(6).fill(null).map(() => Array(6).fill(0)))
      .slice(0, 7).map((row, h) =>
        '<tr><td style="font-weight:600;background:var(--bg3);padding:4px 8px">' + h + '</td>' +
        row.slice(0, 7).map((p, a) => {
          const heat = maxP > 0 ? Math.round((p / maxP) * 100) : 0;
          const bg   = h > a ? `rgba(0,212,255,${p/maxP*.6})` :
                       h === a ? `rgba(148,163,184,${p/maxP*.5})` :
                                 `rgba(0,230,118,${p/maxP*.6})`;
          return `<td style="background:${bg};padding:4px 8px;font-size:11px">${(p*100).toFixed(1)}%</td>`;
        }).join('') + '</tr>'
      ).join('');

    modal.innerHTML = `
      <div class="modal-box" style="max-width:620px">
        <div class="modal-header">
          <strong>📊 Матрица счётов: ${matchName}</strong>
          <button class="modal-close" onclick="document.getElementById('vfMatrixModal').style.display='none'">✕</button>
        </div>
        <div style="padding:16px">
          <div style="overflow-x:auto;margin-bottom:16px">
            <table style="border-collapse:collapse;min-width:320px">
              <thead>
                <tr>
                  <th style="padding:4px 8px;color:var(--text3);font-size:10px">Хозяева ↓ / Гости →</th>
                  ${[0,1,2,3,4,5,6].map(i => `<th style="padding:4px 8px;background:var(--bg3);font-size:11px">${i}</th>`).join('')}
                </tr>
              </thead>
              <tbody>${tableRows}</tbody>
            </table>
          </div>
          ${scores.length ? `
            <div>
              <div style="font-size:11px;font-weight:600;color:var(--text3);margin-bottom:6px">ТОП счётов:</div>
              <div style="display:flex;gap:6px;flex-wrap:wrap">
                ${scores.slice(0, 8).map(sc => `<span class="chip">${sc.score} — ${(sc.prob*100).toFixed(1)}%</span>`).join('')}
              </div>
            </div>` : ''}
        </div>
      </div>`;
    modal.style.display = 'flex';
  },

  _calcMatrixLocal(lH, lA) {
    // Упрощённая матрица Пуассона
    const factorial = n => n <= 1 ? 1 : n * factorial(n - 1);
    const poisson = (k, lambda) => Math.pow(lambda, k) * Math.exp(-lambda) / factorial(k);
    const matrix = [];
    const topScores = [];
    for (let h = 0; h < 7; h++) {
      matrix[h] = [];
      for (let a = 0; a < 7; a++) {
        const p = poisson(h, lH) * poisson(a, lA);
        matrix[h][a] = p;
        topScores.push({ score: `${h}:${a}`, prob: p });
      }
    }
    topScores.sort((a, b) => b.prob - a.prob);
    return { pois: { matrix, topScores: topScores.slice(0, 8) } };
  },

  _createMatrixModal() {
    const m = document.createElement('div');
    m.id = 'vfMatrixModal'; m.className = 'modal';
    m.onclick = e => { if (e.target === m) m.style.display = 'none'; };
    document.body.appendChild(m);
    return m;
  },

  addWatchlist(match, market, odds) {
    const list = JSON.parse(localStorage.getItem('bq_watchlist') || '[]');
    const exists = list.some(x => x.match === match && x.market === market);
    if (!exists) {
      list.push({ match, market, odds, added: new Date().toISOString() });
      localStorage.setItem('bq_watchlist', JSON.stringify(list));
    }
    const btn = event?.target;
    if (btn) { btn.textContent = '✓'; btn.style.color = 'var(--green)'; }
  },

  // ── Utils ─────────────────────────────────────────────────────────────────
  _mktLabel(k) {
    return {
      homeWin: '1 (Хозяева)', draw: 'X (Ничья)', awayWin: '2 (Гости)',
      over25: 'Over 2.5', over15: 'Over 1.5', over35: 'Over 3.5',
      btts: 'BTTS', ah: 'Азиатский гандикап',
      over: 'Тотал Больше', under: 'Тотал Меньше',
      over55: 'Over 5.5',
    }[k] || k;
  },

  _progress(on) {
    const el = document.getElementById('valueScanProgress');
    if (el) el.style.display = on ? 'block' : 'none';
  },

  _clearError() {
    const el = document.getElementById('vfResultsTable');
    // Не очищаем если есть данные
  },

  _showError(msg) {
    const el = document.getElementById('vfResultsTable');
    if (el) el.innerHTML = `<div class="empty-state" style="padding:32px;text-align:center;color:var(--red)">
      <div style="font-size:24px;margin-bottom:8px">⚠️</div>
      ${msg}
    </div>`;
  },

  async _fetch(url, method = 'GET', body = null) {
    const opts = {
      method,
      headers: {
        'Content-Type': 'application/json',
        'x-auth-token': localStorage.getItem('bq_token') || 'demo',
      },
    };
    if (body) opts.body = JSON.stringify(body);
    const r = await fetch(url, opts);
    if (!r.ok) throw new Error(`HTTP ${r.status}`);
    return r.json();
  },
};