'use strict';
// ═══════════════════════════════════════════════════════════════════════════
//  BetQuant Pro — Value Finder v3 
//  • Нет демо-данных — только реальные предстоящие матчи из Odds API
//  • POST /api/value/scan — передаём стратегии на сервер
//  • Стратегии = bq_bt_strategies (тот же ключ что в движке бэктеста)
//  • Лайн: матчи на ближайшие дни, Лайв: матчи с startTime < +2ч
//  • Авто-обновление каждые 5 мин в лайв-режиме
// ═══════════════════════════════════════════════════════════════════════════
const valueFinder = {
  results:   [],
  scanning:  false,
  autoTimer: null,
  charts:    {},

  SPORTS: [
    { value:'all',        label:'🌐 Все виды спорта' },
    { value:'football',   label:'⚽ Футбол' },
    { value:'basketball', label:'🏀 Баскетбол' },
    { value:'hockey',     label:'🏒 Хоккей' },
    { value:'tennis',     label:'🎾 Теннис' },
    { value:'baseball',   label:'⚾ Бейсбол' },
    { value:'mma',        label:'🥊 MMA / Бокс' },
    { value:'cricket',    label:'🏏 Крикет' },
    { value:'rugby',      label:'🏉 Регби' },
    { value:'esports',    label:'🎮 Киберспорт' },
  ],

  MARKETS: {
    football:   [
      {value:'',         label:'Все рынки'},
      {value:'homeWin',  label:'1 (Хозяева)'},
      {value:'draw',     label:'X (Ничья)'},
      {value:'awayWin',  label:'2 (Гости)'},
      {value:'over25',   label:'Тотал Больше 2.5'},
      {value:'under25',  label:'Тотал Меньше 2.5'},
      {value:'btts',     label:'Обе забьют (BTTS)'},
    ],
    basketball: [
      {value:'',label:'Все рынки'},{value:'homeWin',label:'Победа хозяев'},
      {value:'awayWin',label:'Победа гостей'},{value:'over25',label:'Тотал Больше'},
    ],
    hockey:     [
      {value:'',label:'Все рынки'},{value:'homeWin',label:'Победа хозяев (ML)'},
      {value:'awayWin',label:'Победа гостей (ML)'},{value:'draw',label:'Ничья / ОТ'},
    ],
    default:    [
      {value:'',label:'Все рынки'},{value:'homeWin',label:'Победа хозяев'},
      {value:'awayWin',label:'Победа гостей'},
    ],
  },

  // ── init ──────────────────────────────────────────────────────────────────
  init() {
    this.renderFilters();
    const doInit = () => {
      this.renderStrategySelector();
      setTimeout(() => this.scan(), 150);
    };
    if (typeof library !== 'undefined' && library._strategies && library._strategies.length) {
      doInit();
    } else if (typeof library !== 'undefined') {
      library.load().then(doInit).catch(doInit);
    } else {
      doInit();
    }
  },

  // ── Получить стратегии из бэктест движка ──────────────────────────────────
  getStrategies() {
    try {
      const all = [];
      const seen = new Set();
      const add = (s, source) => {
        const key = s.id || s.name;
        if (key && !seen.has(key)) {
          seen.add(key);
          all.push({ ...s, _source: source });
        }
      };
 
      // 1. Главный источник — library._strategies (DB + local + builtin)
      if (typeof library !== 'undefined' && Array.isArray(library._strategies)) {
        library._strategies.forEach(s => {
          const src = s.source === 'db' ? 'library' :
                      s.source === 'local' ? 'library' : 'builtin';
          add(s, src);
        });
      }
 
      // 2. Активные из бэктест движка
      JSON.parse(localStorage.getItem('bq_active_strategies') || '[]')
        .forEach(s => add(s, 'backtest'));
 
      // 3. AI-сгенерированные
      JSON.parse(localStorage.getItem('bq_ai_strategies') || '[]')
        .forEach(s => add(s, 'ai'));
 
      // 4. Legacy localStorage
      JSON.parse(localStorage.getItem('bq_strategies') || '[]')
        .forEach(s => add(s, 'library'));
 
      return all;
    } catch (e) {
      console.warn('[valueFinder] getStrategies:', e);
      return [];
    }
  },

  // ── UI стратегий ──────────────────────────────────────────────────────────
  async _refreshFromLibrary() {
    const btn = document.activeElement;
    const origText = btn?.textContent;
    if (btn && btn.classList.contains('ctrl-btn')) {
      btn.textContent = '⏳';
      btn.disabled = true;
    }
    try {
      if (typeof library !== 'undefined') await library.load();
      this.renderStrategySelector();
    } finally {
      if (btn && btn.classList.contains('ctrl-btn')) {
        btn.textContent = origText;
        btn.disabled = false;
      }
    }
  },
 
// ── 5. ЗАМЕНИ функцию _applyStrategies() ─────────────────────────────────
// (исправлено: работает с library-стратегиями без JS-кода — фильтр по спорту/условиям)
 
  _applyStrategies(bets, activeCodes) {
    // Получаем выбранные ID из чекбоксов
    const checkedIds = [];
    document.querySelectorAll('.vf-strat-cb:checked').forEach(cb => {
      checkedIds.push(cb.value);
    });
    if (!checkedIds.length) return bets;
 
    // Находим объекты стратегий
    const allStrats = this.getStrategies();
    const selected  = allStrats.filter(s => checkedIds.includes(s.id || s.name));
    if (!selected.length) return bets;
 
    const filtered = bets.filter(bet => {
      return selected.some(strat => this._matchesBet(bet, strat));
    });
 
    // Если всё отфильтровалось — возвращаем все (не показываем пустой экран)
    return filtered.length > 0 ? filtered : bets;
  },
 
// ── 6. ДОБАВЬ вспомогательную функцию _matchesBet() ─────────────────────
// (вызывается из _applyStrategies)
 
  _matchesBet(bet, strat) {
    // 1. Если есть JS-код — выполняем в sandbox
    if (strat.code && strat.code.trim()) {
      try {
        // Оборачиваем код в функцию; код должен вернуть true/false
        // или просто не бросить исключение = принять ставку
        const fn = new Function('bet', `
          try {
            ${strat.code}
            return true;
          } catch(e) { return true; }
        `);
        return fn({ ...bet });
      } catch (e) {
        // Ошибка компиляции — пропускаем через следующие фильтры
      }
    }
 
    // 2. Фильтр по спорту стратегии
    const stratSport = (strat.sport || strat.sport_name || '').toLowerCase();
    if (stratSport && stratSport !== 'any' && stratSport !== 'all') {
      const betSport = (bet.sport || '').toLowerCase();
      if (betSport && !betSport.includes(stratSport) && !stratSport.includes(betSport)) {
        return false;
      }
    }
 
    // 3. Фильтр по рынку стратегии (если задан)
    if (strat.market && strat.market !== '') {
      if (bet.market && bet.market !== strat.market) return false;
    }
 
    // 4. Фильтр по odds_filters / conditions из library-стратегии
    const conditions = strat.conditions || strat.odds_filters || strat.filters || [];
    if (Array.isArray(conditions) && conditions.length) {
      return conditions.every(cond => {
        const val = parseFloat(bet[cond.field] || bet[cond.stat] || 0);
        const cv  = parseFloat(cond.value || cond.val || 0);
        switch (String(cond.op || cond.operator || '>=')) {
          case '>':  return val >  cv;
          case '<':  return val <  cv;
          case '>=': return val >= cv;
          case '<=': return val <= cv;
          case '=':
          case '==': return val === cv;
          default:   return true;
        }
      });
    }
 
    // 5. Нет ни кода, ни условий — стратегия принимает все ставки
    return true;
  },

  _checkAll(on) {
    document.querySelectorAll('.vf-strat-cb').forEach(cb => cb.checked = on);
  },

  // ── Фильтры ───────────────────────────────────────────────────────────────
  renderFilters() {
    const el = document.getElementById('vfFilters');
    if (!el) return;
    el.innerHTML = `
      <div class="config-row">
        <label>Мин. Edge %</label>
        <input type="number" class="ctrl-input" id="vfMinEdge" value="3" step="0.5" min="0" max="50">
      </div>
      <div class="config-row">
        <label>Режим</label>
        <div class="vf-mode-toggle">
          <button class="ctrl-btn vf-mode-btn active" id="vfModeLine" onclick="valueFinder._setMode('line')">📋 Лайн</button>
          <button class="ctrl-btn vf-mode-btn"        id="vfModeLive" onclick="valueFinder._setMode('live')">🔴 Лайв</button>
        </div>
        <input type="hidden" id="vfMode" value="line">
      </div>
      <div class="config-row">
        <label>Спорт</label>
        <select class="ctrl-select" id="vfSport" onchange="valueFinder._onSportChange()">
          ${this.SPORTS.map(s=>`<option value="${s.value}">${s.label}</option>`).join('')}
        </select>
      </div>
      <div class="config-row">
        <label>Рынок</label>
        <select class="ctrl-select" id="vfMarket"></select>
      </div>
      <div class="config-row">
        <label>Авто-обновление</label>
        <label class="toggle-switch">
          <input type="checkbox" id="vfAutoRefresh" onchange="valueFinder._toggleAuto()">
          <span class="toggle-slider"></span>
        </label>
        <span style="font-size:10px;color:var(--text3);margin-left:4px" id="vfAutoLabel">выкл</span>
      </div>`;
    this._onSportChange();
  },

  _setMode(mode) {
    document.getElementById('vfMode').value = mode;
    document.getElementById('vfModeLine').classList.toggle('active', mode==='line');
    document.getElementById('vfModeLive').classList.toggle('active', mode==='live');
    if (mode === 'live') {
      const cb = document.getElementById('vfAutoRefresh');
      if (cb && !cb.checked) { cb.checked = true; this._toggleAuto(); }
    }
  },

  _onSportChange() {
    const sport  = document.getElementById('vfSport')?.value || 'all';
    const mktSel = document.getElementById('vfMarket');
    if (!mktSel) return;
    const list = this.MARKETS[sport] || this.MARKETS.default;
    mktSel.innerHTML = list.map(m=>`<option value="${m.value}">${m.label}</option>`).join('');
  },

  _toggleAuto() {
    const on    = document.getElementById('vfAutoRefresh')?.checked;
    const label = document.getElementById('vfAutoLabel');
    if (this.autoTimer) { clearInterval(this.autoTimer); this.autoTimer = null; }
    if (on) {
      const interval = 5 * 60 * 1000; // 5 мин
      this.autoTimer = setInterval(() => { if (!this.scanning) this.scan(); }, interval);
      if (label) label.textContent = 'каждые 5 мин';
    } else {
      if (label) label.textContent = 'выкл';
    }
  },

  // ── Основной скан ─────────────────────────────────────────────────────────
  async scan() {
    if (this.scanning) return;
    this.scanning = true;
    const btn = document.getElementById('vfScanBtn');
    if (btn) { btn.textContent = '⏳ Загрузка...'; btn.disabled = true; }
    this._progress(true);
    document.getElementById('vfSourceHint') && (document.getElementById('vfSourceHint').innerHTML = '');

    try {
      const minEdge = parseFloat(document.getElementById('vfMinEdge')?.value || 3);
      const sport   = document.getElementById('vfSport')?.value  || 'all';
      const market  = document.getElementById('vfMarket')?.value || '';
      const mode    = document.getElementById('vfMode')?.value   || 'line';

      // Собираем активные стратегии с кодом
      const strategies = [];
      document.querySelectorAll('.vf-strat-cb:checked').forEach(cb => {
        const code = decodeURIComponent(cb.dataset.code || '');
        if (code.trim()) {
          strategies.push({
            id:    cb.value,
            name:  cb.dataset.name || cb.value,
            sport: cb.dataset.sport || 'all',
            code,
          });
        }
      });

      const body = { minEdge, sport, market, strategies };

      let data;
      try {
        data = await this._post('/api/value/scan', body);
      } catch(e) {
        this._showError(e.message || 'Ошибка связи с сервером');
        return;
      }

      if (data.error) {
        this._showHint('warn', data.message || 'Ошибка сервера');
        this.results = [];
        this.renderResults();
        return;
      }

      if (!data) return;

      // Показываем ошибку если нет ключа или другая проблема
      if (data.error || data.source === 'no_key') {
        this._showHint('warn', data.message || 'Ошибка сервера');
        this.results = [];
        this.renderResults();
        return;
      }
      if (data.source === 'empty') {
        this._showHint('info', data.message || 'Нет матчей');
        this.results = [];
        this.renderResults();
        return;
      }

      let bets = data.bets || [];

      // Лайв: только матчи в ближайшие 2 часа или уже начавшиеся
      if (mode === 'live') {
        const now2h = Date.now() + 2 * 3600 * 1000;
        bets = bets.filter(b => {
          if (!b.startTime) return false;
          return new Date(b.startTime).getTime() <= now2h;
        });
      }
      // Лайн: только будущие матчи
      else {
        bets = bets.filter(b => {
          if (!b.startTime) return true;
          return new Date(b.startTime).getTime() > Date.now();
        });
      }

      this.results = bets;
      this.renderResults();
      this.renderChart();
      this.renderSummary(data);

      // Подсказки об источнике данных
      if (data.source === 'no_key') {
        this._showHint('warn', data.message);
      } else if (data.source === 'empty') {
        this._showHint('info', data.message);
      } else if (data.stratApplied && strategies.length) {
        this._showHint('info', `✅ Применено ${strategies.length} стратег. — показаны ${bets.length} из ${data.bets?.length || 0} value ставок`);
      }

    } catch(e) {
      console.error('[valueFinder]', e);
      this._showError('Неожиданная ошибка: ' + e.message);
    } finally {
      this.scanning = false;
      this._progress(false);
      if (btn) { btn.textContent = '🔍 Сканировать'; btn.disabled = false; }
    }
  },

  // ── Таблица результатов ───────────────────────────────────────────────────
  renderResults() {
    const el = document.getElementById('vfResultsTable');
    if (!el) return;

    if (!this.results.length) {
      el.innerHTML = `<div class="empty-state" style="padding:48px;text-align:center">
        <div style="font-size:36px;margin-bottom:12px">🔍</div>
        <div style="color:var(--text2);font-size:14px;margin-bottom:6px">Нет value ставок по текущим фильтрам</div>
        <div style="color:var(--text3);font-size:12px">
          Попробуйте снизить Мин. Edge % или выбрать другой вид спорта
        </div>
      </div>`;
      return;
    }

    const SPORT_ICON = {football:'⚽',basketball:'🏀',hockey:'🏒',tennis:'🎾',mma:'🥊',baseball:'⚾',cricket:'🏏',rugby:'🏉',esports:'🎮'};
    const MKT_LABEL  = {homeWin:'1 Хозяева',draw:'X Ничья',awayWin:'2 Гости',over25:'Over 2.5',under25:'Under 2.5',btts:'BTTS'};

    const rows = this.results.map(r => {
      const icon    = SPORT_ICON[r.sport] || '🎯';
      const edgeCls = r.edge >= 10 ? 'hot' : r.edge >= 5 ? 'warm' : '';
      const ko      = r.kickoff || '';
      const days    = r.daysToKickoff;
      const daysStr = days != null ? (days < 1 ? `<${Math.round(days*24)}ч` : `+${days.toFixed(0)}д`) : '';
      const bms     = r.bmCount ? `<span style="font-size:9px;color:var(--text3)">${r.bmCount} БК</span>` : '';

      // Сериализуем ставку для кнопки создания стратегии
      const betJson = encodeURIComponent(JSON.stringify({
        home: r.home, away: r.away, sport: r.sport, league: r.league,
        market: r.market, odds: r.odds, edge: r.edge,
        modelProb: r.modelProb, impliedProb: r.impliedProb,
      }));

      return `<tr>
        <td>
          <div style="font-size:11px;font-weight:600">${icon} ${r.league}</div>
          <div style="font-size:10px;color:var(--text3);margin-top:1px">${ko} ${daysStr} ${bms}</div>
        </td>
        <td>
          <div style="font-weight:500;font-size:12px">${r.home}</div>
          <div style="color:var(--text3);font-size:11px">${r.away}</div>
        </td>
        <td><span class="bt-tag single" style="font-size:11px">${MKT_LABEL[r.market]||r.market}</span></td>
        <td style="font-weight:700">${r.odds}</td>
        <td style="color:var(--text3)">${r.impliedProb}%</td>
        <td style="color:var(--green)">${r.modelProb}%</td>
        <td><span class="vf-edge-badge ${edgeCls}">+${r.edge}%</span></td>
        <td style="color:var(--text3);font-size:11px">${r.kelly}%</td>
        <td style="white-space:nowrap">
          <button class="ctrl-btn sm" onclick="valueFinder.showMatrix('${r.match.replace(/'/g,"\\'").replace(/"/g,'&quot;')}',${r.lH||1.45},${r.lA||1.15})" title="Матрица счётов">📊</button>
          <button class="ctrl-btn sm" onclick="valueFinder.addWatch('${r.match.replace(/'/g,"\\'").replace(/"/g,'&quot;')}','${r.market}',${r.odds})" title="В watchlist">⭐</button>
          <button class="ctrl-btn sm" onclick="valueFinder.createFromBet('${betJson}')" title="Создать стратегию по этой ставке">✏️</button>
        </td>
      </tr>`;
    }).join('');

    el.innerHTML = `<table class="data-table">
      <thead><tr>
        <th>Лига / Время</th><th>Матч</th><th>Рынок</th>
        <th>Коэф</th><th>Рынок%</th><th>Модель%</th>
        <th>Edge%</th><th>Kelly%</th><th></th>
      </tr></thead>
      <tbody>${rows}</tbody>
    </table>`;
  },

  // ── Summary bar ───────────────────────────────────────────────────────────
  renderSummary(data) {
    const el = document.getElementById('vfSummary');
    if (!el) return;
    if (!this.results.length) { el.innerHTML = ''; return; }

    const avg  = (this.results.reduce((s,r)=>s+r.edge,0)/this.results.length).toFixed(1);
    const best = this.results[0];
    const mkt  = {homeWin:'1',draw:'X',awayWin:'2',over25:'O2.5',under25:'U2.5',btts:'BTTS'};
    const stratInfo = data.stratApplied
      ? ` · <span style="color:var(--green)">✅ ${data.strategiesCount} стратег.</span>`
      : data.strategiesCount ? ` · <span style="color:var(--text3)">${data.strategiesCount} стратег. (нет сигналов)</span>` : '';

    el.innerHTML = `
      <strong>${this.results.length}</strong> value ставок &nbsp;·&nbsp;
      Ср. Edge: <strong class="positive">+${avg}%</strong> &nbsp;·&nbsp;
      Лучшая: <strong>${best.home} (${mkt[best.market]||best.market}) +${best.edge}%</strong>
      <span style="font-size:10px;color:var(--text3)">&nbsp;·&nbsp; ${data.totalFixtures||0} матчей проверено ${stratInfo}</span>`;
  },

  // ── Hint ──────────────────────────────────────────────────────────────────
  _showHint(type, msg) {
    const el = document.getElementById('vfSourceHint');
    if (!el) return;
    el.innerHTML = `<div class="vf-hint-box ${type}">${msg}</div>`;
  },

  // ── Chart ─────────────────────────────────────────────────────────────────
  renderChart() {
    if (this.charts.v) { try{this.charts.v.destroy();}catch(e){} }
    const cvs = document.getElementById('chartVFValue');
    if (!cvs || !this.results.length) return;
    const dk = document.body.classList.contains('dark-mode');
    const tc = dk?'#8892a4':'#4a5568', gc=dk?'rgba(255,255,255,.05)':'rgba(0,0,0,.07)';
    const top = this.results.slice(0,15);
    const ICON = {football:'⚽',basketball:'🏀',hockey:'🏒',tennis:'🎾',mma:'🥊',baseball:'⚾'};
    this.charts.v = new Chart(cvs,{
      type:'bar',
      data:{
        labels: top.map(r=>`${ICON[r.sport]||'🎯'} ${r.home.slice(0,9)} ${({homeWin:'1',draw:'X',awayWin:'2',over25:'O2.5'})[r.market]||r.market}`),
        datasets:[
          {label:'Edge %',   data:top.map(r=>r.edge),        backgroundColor:'rgba(0,212,255,.8)',  borderRadius:4},
          {label:'Модель %', data:top.map(r=>r.modelProb),   backgroundColor:'rgba(0,230,118,.55)', borderRadius:4},
          {label:'Рынок %',  data:top.map(r=>r.impliedProb), backgroundColor:'rgba(148,163,184,.35)',borderRadius:4},
        ],
      },
      options:{
        responsive:true, maintainAspectRatio:false,
        plugins:{legend:{labels:{color:tc,font:{size:11}}}},
        scales:{
          x:{ticks:{color:tc,font:{size:9},maxRotation:40},grid:{color:gc}},
          y:{ticks:{color:tc,font:{size:10}},grid:{color:gc}},
        },
      },
    });
  },

  // ── Score matrix ──────────────────────────────────────────────────────────
  async showMatrix(matchName, lH, lA) {
    let d;
    try {
      d = await this._post('/api/value/calculate', {
        home: matchName.split(' vs ')[0], away: matchName.split(' vs ')[1],
        homeAttack: lH/1.45, homeDefense:1, awayAttack: lA/1.15, awayDefense:1,
      });
    } catch(e) { d = this._localMatrix(lH, lA); }

    let modal = document.getElementById('vfMatrixModal');
    if (!modal) {
      modal = document.createElement('div');
      modal.id = 'vfMatrixModal'; modal.className = 'modal';
      modal.onclick = e => { if(e.target===modal) modal.style.display='none'; };
      document.body.appendChild(modal);
    }

    const mat   = d.pois?.matrix || [];
    const top   = d.pois?.topScores || [];
    const maxP  = Math.max(...(mat.flat().length ? mat.flat() : [1]));
    const thead = '<tr><th style="padding:4px 8px;font-size:10px;color:var(--text3)">Хозяева↓/Гости→</th>'
      + [0,1,2,3,4,5,6].map(i=>`<th style="padding:4px 8px;background:var(--bg3);font-size:11px">${i}</th>`).join('')+'</tr>';
    const tbody = (mat.length ? mat : Array(7).fill(Array(7).fill(0))).slice(0,7).map((row,h)=>
      `<tr><td style="font-weight:600;background:var(--bg3);padding:4px 8px">${h}</td>`+
      row.slice(0,7).map((p,a)=>{
        const bg = h>a?`rgba(0,212,255,${p/maxP*.55})`:h===a?`rgba(148,163,184,${p/maxP*.45})`:`rgba(0,230,118,${p/maxP*.55})`;
        return `<td style="background:${bg};padding:4px 8px;font-size:11px">${(p*100).toFixed(1)}%</td>`;
      }).join('')+'</tr>'
    ).join('');

    modal.innerHTML = `<div class="modal-box" style="max-width:580px">
      <div class="modal-header"><strong>📊 ${matchName}</strong>
        <button class="modal-close" onclick="document.getElementById('vfMatrixModal').style.display='none'">✕</button>
      </div>
      <div style="padding:16px;overflow-x:auto">
        <table style="border-collapse:collapse"><thead>${thead}</thead><tbody>${tbody}</tbody></table>
        ${top.length?`<div style="margin-top:12px;font-size:11px;color:var(--text3);margin-bottom:6px">ТОП счётов:</div>
          <div style="display:flex;gap:6px;flex-wrap:wrap">${top.slice(0,8).map(s=>`<span class="chip">${s.score} — ${(s.prob*100).toFixed(1)}%</span>`).join('')}</div>`:''}
      </div></div>`;
    modal.style.display='flex';
  },

  _localMatrix(lH, lA) {
    const FACT=[1,1,2,6,24,120,720,5040,40320,362880];
    const p=(k,l)=>k>9?0:Math.pow(l,k)*Math.exp(-l)/FACT[k];
    const mat=[];
    const top=[];
    for(let h=0;h<7;h++){mat[h]=[];for(let a=0;a<7;a++){const v=p(h,lH)*p(a,lA);mat[h][a]=v;top.push({score:`${h}:${a}`,prob:v});}}
    top.sort((a,b)=>b.prob-a.prob);
    return {pois:{matrix:mat,topScores:top.slice(0,8)}};
  },

  addWatch(match, market, odds) {
    const list=JSON.parse(localStorage.getItem('bq_watchlist')||'[]');
    if(!list.some(x=>x.match===match&&x.market===market)){
      list.push({match,market,odds,added:new Date().toISOString()});
      localStorage.setItem('bq_watchlist',JSON.stringify(list));
    }
    if(event?.target){event.target.textContent='✓';event.target.style.color='var(--green)';}
  },

  // ── Создание стратегии ────────────────────────────────────────────────────

  // Открывает AI генератор с готовым промптом
  openCreateStrategy(bet) {
    if (typeof app !== 'undefined') app.showPanel('ai-strategy');
    const mktNames = {
      homeWin:'победа хозяев', draw:'ничья', awayWin:'победа гостей',
      over25:'тотал больше 2.5', under25:'тотал меньше 2.5', btts:'обе забьют'
    };
    let prompt;
    if (bet) {
      prompt = `Создай стратегию value betting для ${bet.sport || 'футбола'}.
Пример сигнала который я хочу ловить:
  Матч: ${bet.home} vs ${bet.away} (${bet.league || ''})
  Рынок: ${mktNames[bet.market] || bet.market}
  Коэффициент букмекера: ${bet.odds}
  Вероятность модели: ${bet.modelProb}%
  Рыночная вероятность: ${bet.impliedProb}%
  Edge: +${bet.edge}%

Стратегия должна:
1. Искать похожие ситуации (edge > ${Math.max(3, bet.edge - 2)}%)
2. Применять Kelly criterion для размера ставки
3. Работать для спорта: ${bet.sport || 'football'}
4. Возвращать { signal: true, market: '${bet.market}', stake: ..., prob: ... }

Напиши полную функцию evaluate(match, team, h2h, market) готовую к бэктесту.`;
    } else {
      prompt = `Создай стратегию поиска ценных ставок (value betting) для футбола.
Стратегия должна:
1. Использовать форму команды (последние 5 матчей) для оценки вероятности
2. Сравнивать с рыночными коэффициентами — искать edge > 5%
3. Применять полу-Kelly для размера ставки
4. Работать для матчей с коэффициентом 1.5–4.0
5. Возвращать { signal: true, market: 'home'/'away'/'over', stake: ..., prob: ... }

Напиши полную функцию evaluate(match, team, h2h, market) готовую к бэктесту.`;
    }
    setTimeout(() => {
      const input = document.getElementById('aiInput');
      if (input) {
        input.value = prompt;
        input.focus();
        input.dispatchEvent(new Event('input'));
      }
    }, 350);
  },

  // Кнопка "✏️" в строке таблицы — создать стратегию по конкретной ставке
  createFromBet(encodedBet) {
    try {
      const bet = JSON.parse(decodeURIComponent(encodedBet));
      this.openCreateStrategy(bet);
    } catch(e) {
      this.openCreateStrategy(null);
    }
  },

  // ── Utils ─────────────────────────────────────────────────────────────────
  _progress(on) {
    const el=document.getElementById('valueScanProgress');
    if(el) el.style.display=on?'block':'none';
  },
  _showError(msg) {
    const el=document.getElementById('vfResultsTable');
    if(el) el.innerHTML=`<div class="empty-state" style="padding:40px;text-align:center">
      <div style="font-size:28px;margin-bottom:10px">⚠️</div>
      <div style="color:var(--red);font-size:13px">${msg}</div>
    </div>`;
    const s=document.getElementById('vfSummary');
    if(s) s.innerHTML='';
  },
  async _post(url, body) {
    const r = await fetch(url, {
      method:'POST',
      headers:{
        'Content-Type':'application/json',
        'x-auth-token': localStorage.getItem('bq_token')||'demo',
      },
      body: JSON.stringify(body),
    });
    if(!r.ok) throw new Error(`HTTP ${r.status}: ${await r.text().catch(()=>'')}`);
    return r.json();
  },
};