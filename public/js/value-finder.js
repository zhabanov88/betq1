'use strict';
// ═══════════════════════════════════════════════════════════════════════════
//  BetQuant Pro — Value Finder v6
//  • ESPN API как главный источник (все виды спорта без ключа)
//  • Тултипы на все столбцы таблицы
//  • Вкладки Сигналы / Все матчи
// ═══════════════════════════════════════════════════════════════════════════
const valueFinder = {
  results:      [],
  _allBets:     [],
  _allFixtures: [],
  _tab:         'signals',
  scanning:     false,
  autoTimer:    null,
  charts:       {},

  SPORTS: [
    {value:'all',       label:'🌐 Все виды спорта'},
    {value:'football',  label:'⚽ Футбол'},
    {value:'basketball',label:'🏀 Баскетбол'},
    {value:'hockey',    label:'🏒 Хоккей'},
    {value:'tennis',    label:'🎾 Теннис'},
    {value:'baseball',  label:'⚾ Бейсбол'},
    {value:'rugby',     label:'🏉 Регби'},
    {value:'volleyball',label:'🏐 Волейбол'},
    {value:'waterpolo', label:'🤽 Водное поло'},
    {value:'cricket',   label:'🏏 Крикет'},
    {value:'nfl',       label:'🏈 NFL'},
    {value:'mma',       label:'🥊 MMA / Бокс'},
    {value:'esports',   label:'🎮 Киберспорт'},
  ],

  MARKETS: {
    football:   [{value:'',label:'Все рынки'},{value:'homeWin',label:'1 (Хозяева)'},{value:'draw',label:'X (Ничья)'},{value:'awayWin',label:'2 (Гости)'},{value:'over25',label:'Тотал > 2.5'},{value:'under25',label:'Тотал < 2.5'},{value:'btts',label:'Обе забьют'}],
    basketball: [{value:'',label:'Все рынки'},{value:'homeWin',label:'Победа хозяев'},{value:'awayWin',label:'Победа гостей'},{value:'over25',label:'Тотал Больше'},{value:'under25',label:'Тотал Меньше'}],
    hockey:     [{value:'',label:'Все рынки'},{value:'homeWin',label:'Победа хозяев'},{value:'draw',label:'Ничья/ОТ'},{value:'awayWin',label:'Победа гостей'},{value:'over25',label:'Тотал > 5.5'},{value:'btts',label:'Обе забьют'}],
    tennis:     [{value:'',label:'Все рынки'},{value:'homeWin',label:'Победа P1'},{value:'awayWin',label:'Победа P2'}],
    default:    [{value:'',label:'Все рынки'},{value:'homeWin',label:'Победа хозяев'},{value:'awayWin',label:'Победа гостей'}],
  },

  ICON: {football:'⚽',basketball:'🏀',hockey:'🏒',tennis:'🎾',baseball:'⚾',rugby:'🏉',volleyball:'🏐',waterpolo:'🤽',cricket:'🏏',nfl:'🏈',mma:'🥊',esports:'🎮'},

  // ── Тултипы столбцов ──────────────────────────────────────────────────────
  COL_TIPS: {
    league:  'Лига и вид спорта. Источник: ESPN (без ключа), Odds API, football-data.org',
    match:   'Команды матча. Источник данных указан в иконке рядом с лигой',
    market:  'Рынок ставки:\n1 Хоз — победа хозяев\nX Нич — ничья\n2 Гост — победа гостей\nО>2.5 — тотал больше 2.5 гола\nBTTS — обе команды забьют',
    odds:    'Лучший коэффициент от букмекеров (из OddsAPI).\n~ означает расчётный коэф (нет данных букмекеров)',
    impliedProb: 'Вероятность по версии букмекера = 1 / коэффициент × 100%.\nПример: коэф 2.0 → вероятность 50%.\nВключает маржу букмекера (~5-8%)',
    modelProb: 'Вероятность по нашей математической модели.\n\nДля футбола: Poisson (Dixon-Coles) 65% + ELO рейтинг 35%\nДля хоккея: Poisson с λ голов + ELO\nДля баскетбола: ELO + средние очки команд\nДля тенниса: ELO рейтингов\n\nλ берётся из исторических данных ClickHouse.\nЕсли данных нет — дефолт (1.45/1.15 для футбола)',
    edge:    'Edge % = Модель% − Рынок%\n\nПоказывает насколько наша вероятность выше вероятности букмекера.\nПоложительный edge означает что букмекер недооценивает событие.\n\nПример: Модель 55%, Рынок 47% → Edge +8%\n\n> 3%  — слабый сигнал\n> 5%  — умеренный (жёлтый)\n> 10% — сильный (зелёный)\n\nEdge не гарантирует выигрыш, но статистически прибыльная ставка',
    kelly:   'Kelly % — оптимальный размер ставки по критерию Келли.\n= (p × (k−1) − (1−p)) / (k−1)\nгде p — вероятность, k — коэффициент\n\nМы используем Half Kelly (×0.5) для снижения риска.\nПример: Kelly 4% означает ставить 4% от банкролла',
    strategy:'Стратегии которые дали сигнал на эту ставку.\nСигнал = стратегия вернула {signal: true, market: ...}\nЕсли пусто — стратегия не выбрана или не сработала',
  },

  // ── init ──────────────────────────────────────────────────────────────────
  init() {
    this.renderFilters();
    const doInit = () => {
      this.renderStrategySelector();
      this._renderTabs();
      if (!document.getElementById('vfFixturesTable')) {
        const rt = document.getElementById('vfResultsTable');
        if (rt) { const ft=document.createElement('div');ft.id='vfFixturesTable';ft.style.display='none';rt.parentNode.insertBefore(ft,rt.nextSibling); }
      }
      setTimeout(() => this.scan(), 200);
    };
    if (typeof library !== 'undefined' && Array.isArray(library._strategies) && library._strategies.length) doInit();
    else if (typeof library !== 'undefined') library.load().then(doInit).catch(doInit);
    else doInit();
  },

  // ── Вкладки ───────────────────────────────────────────────────────────────
  _renderTabs() {
    let c = document.getElementById('vfTabsContainer');
    if (!c) {
      const rt = document.getElementById('vfResultsTable');
      if (!rt) return;
      c = document.createElement('div'); c.id='vfTabsContainer';
      rt.parentNode.insertBefore(c, rt);
    }
    c.innerHTML = `
      <div style="display:flex;gap:4px;margin-bottom:12px;border-bottom:1px solid var(--border);padding-bottom:8px;flex-wrap:wrap;align-items:center">
        <button class="ctrl-btn ${this._tab==='signals'?'primary':''}" onclick="valueFinder._setTab('signals')" style="font-size:12px">
          🎯 Сигналы <span id="vfSignalCount" style="margin-left:4px;background:rgba(0,212,255,.2);border-radius:10px;padding:1px 6px;font-size:10px">${this.results.length||''}</span>
        </button>
        <button class="ctrl-btn ${this._tab==='fixtures'?'primary':''}" onclick="valueFinder._setTab('fixtures')" style="font-size:12px">
          📋 Все матчи <span id="vfFixtureCount" style="margin-left:4px;background:rgba(0,230,118,.15);border-radius:10px;padding:1px 6px;font-size:10px">${this._allFixtures.length||''}</span>
        </button>
        <span style="font-size:10px;color:var(--text3);margin-left:8px">📡 ESPN + OddsAPI + FDORG</span>
      </div>`;
  },

  _setTab(tab) {
    this._tab = tab;
    this._renderTabs();
    const rt=document.getElementById('vfResultsTable'),ft=document.getElementById('vfFixturesTable'),ch=document.getElementById('chartVFValue')?.closest?.('.chart-card');
    if(tab==='signals'){if(rt)rt.style.display='';if(ft)ft.style.display='none';if(ch)ch.style.display='';this.renderResults();}
    else{if(rt)rt.style.display='none';if(ft)ft.style.display='';if(ch)ch.style.display='none';this._renderFixtures();}
  },

  _updateCounts() {
    const sc=document.getElementById('vfSignalCount'),fc=document.getElementById('vfFixtureCount');
    if(sc)sc.textContent=this.results.length||'';if(fc)fc.textContent=this._allFixtures.length||'';
  },

  // ── Стратегии ─────────────────────────────────────────────────────────────
  getStrategies() {
    try {
      const all=[],seen=new Set();
      const add=(s,src)=>{const k=String(s?.id||s?.name||'');if(k&&!seen.has(k)){seen.add(k);all.push({...s,_source:src});}};
      if(typeof backtestEngine!=='undefined'&&Array.isArray(backtestEngine.activeStrategies))backtestEngine.activeStrategies.filter(s=>s.enabled!==false).forEach(s=>add(s,'backtest'));
      try{JSON.parse(localStorage.getItem('bq_bt_strategies')||'[]').filter(s=>s.enabled!==false).forEach(s=>add(s,'backtest'));}catch(e){}
      if(typeof library!=='undefined'&&Array.isArray(library._strategies))library._strategies.forEach(s=>add(s,s.source==='db'?'library':'builtin'));
      try{JSON.parse(localStorage.getItem('bq_ai_strategies')||'[]').forEach(s=>add(s,'ai'));}catch(e){}
      ['bq_active_strategies','bq_strategies'].forEach(k=>{try{JSON.parse(localStorage.getItem(k)||'[]').forEach(s=>add(s,'library'));}catch(e){}});
      return all;
    }catch(e){return[];}
  },

  renderStrategySelector() {
    const el=document.getElementById('vfStrategySelector');if(!el)return;
    const strategies=this.getStrategies();
    if(!strategies.length){el.innerHTML=`<div style="padding:10px;background:var(--bg3);border-radius:8px;border:1px solid var(--border);font-size:12px;color:var(--text2)">⚠️ Нет стратегий.<div style="font-size:11px;color:var(--text3);margin-top:4px"><a href="#" onclick="app.showPanel('library');return false" style="color:var(--accent)">Библиотека</a> · <a href="#" onclick="app.showPanel('strategy');return false" style="color:var(--accent)">Конструктор</a></div><button class="ctrl-btn sm" style="margin-top:8px" onclick="valueFinder._refreshStrategies()">🔄 Обновить</button></div>`;return;}
    const items=strategies.map(s=>{const icon=s._source==='ai'?'🤖':s._source==='backtest'?'⚙️':s._source==='builtin'?'⭐':'📚';const roiV=parseFloat(s.roi);const roi=s.roi&&!isNaN(roiV)?`<span class="vf-strat-roi ${roiV>=0?'pos':'neg'}">${roiV>0?'+':''}${roiV.toFixed(1)}%</span>`:'';return`<label class="vf-strat-check"><input type="checkbox" class="vf-strat-cb" value="${s.id||s.name}" data-name="${(s.name||'').replace(/"/g,'&quot;')}" data-code="${encodeURIComponent(s.code||'')}" data-sport="${s.sport||'any'}" onchange="valueFinder._onStrategyChange()" checked><span>${icon} ${s.name||s.id}</span>${roi}<span class="vf-strat-src">${s.sport||'any'}</span></label>`;}).join('');
    el.innerHTML=`<div style="font-size:11px;font-weight:600;color:var(--text3);margin-bottom:6px;text-transform:uppercase;letter-spacing:.06em">Стратегии (${strategies.length})</div><div class="vf-strat-list">${items}</div><div style="display:flex;gap:6px;margin-top:8px;flex-wrap:wrap"><button class="ctrl-btn sm" onclick="valueFinder._checkAll(true)">Все</button><button class="ctrl-btn sm" onclick="valueFinder._checkAll(false)">Снять</button><button class="ctrl-btn sm" onclick="valueFinder._refreshStrategies()">🔄</button></div>`;
  },

  _checkAll(on){document.querySelectorAll('.vf-strat-cb').forEach(cb=>{cb.checked=on;});this._onStrategyChange();},
  async _refreshStrategies(){try{if(typeof library!=='undefined')await library.load();this.renderStrategySelector();}catch(e){}},
  _onStrategyChange(){
    if(!this._allBets.length){this.scan();return;}
    const ids=new Set();document.querySelectorAll('.vf-strat-cb:checked').forEach(cb=>ids.add(cb.value));
    this.results=!ids.size?[...this._allBets]:this._allBets.filter(bet=>(bet.matchedStrategies||[]).some(s=>ids.has(String(s.id))));
    if(this._tab==='signals'){this.renderResults();this.renderChart();}this._updateCounts();
  },

  // ── Фильтры ───────────────────────────────────────────────────────────────
  renderFilters() {
    const el=document.getElementById('vfFilters');if(!el)return;
    el.innerHTML=`
      <div class="config-row"><label>Мин. Edge %</label><input type="number" class="ctrl-input" id="vfMinEdge" value="3" step="0.5" min="0" max="50"></div>
      <div class="config-row"><label>Режим</label><div class="vf-mode-toggle"><button class="ctrl-btn vf-mode-btn active" id="vfModeLine" onclick="valueFinder._setMode('line')">📋 Лайн</button><button class="ctrl-btn vf-mode-btn" id="vfModeLive" onclick="valueFinder._setMode('live')">🔴 Лайв</button></div><input type="hidden" id="vfMode" value="line"></div>
      <div class="config-row"><label>Спорт</label><select class="ctrl-select" id="vfSport" onchange="valueFinder._onSportChange()">${this.SPORTS.map(s=>`<option value="${s.value}">${s.label}</option>`).join('')}</select></div>
      <div class="config-row"><label>Рынок</label><select class="ctrl-select" id="vfMarket"></select></div>
      <div class="config-row"><label>Авто</label><label class="toggle-switch"><input type="checkbox" id="vfAutoRefresh" onchange="valueFinder._toggleAuto()"><span class="toggle-slider"></span></label><span style="font-size:10px;color:var(--text3);margin-left:4px" id="vfAutoLabel">выкл</span></div>`;
    this._onSportChange();
  },

  _setMode(mode){document.getElementById('vfMode').value=mode;document.getElementById('vfModeLine').classList.toggle('active',mode==='line');document.getElementById('vfModeLive').classList.toggle('active',mode==='live');},
  _onSportChange(){const s=document.getElementById('vfSport')?.value||'all',m=document.getElementById('vfMarket');if(!m)return;const l=this.MARKETS[s]||this.MARKETS.default;m.innerHTML=l.map(x=>`<option value="${x.value}">${x.label}</option>`).join('');},
  _toggleAuto(){const on=document.getElementById('vfAutoRefresh')?.checked,lb=document.getElementById('vfAutoLabel');if(this.autoTimer){clearInterval(this.autoTimer);this.autoTimer=null;}if(on){this.autoTimer=setInterval(()=>{if(!this.scanning)this.scan();},5*60*1000);if(lb)lb.textContent='каждые 5 мин';}else{if(lb)lb.textContent='выкл';}},

  // ── Скан ─────────────────────────────────────────────────────────────────
  async scan() {
    if(this.scanning)return;
    this.scanning=true;
    const btn=document.getElementById('vfScanBtn');
    if(btn){btn.textContent='⏳ Загрузка...';btn.disabled=true;}
    this._progress(true);
    const hintEl=document.getElementById('vfSourceHint');if(hintEl)hintEl.innerHTML='';

    try{
      const minEdge=parseFloat(document.getElementById('vfMinEdge')?.value||3);
      const sport=document.getElementById('vfSport')?.value||'all';
      const market=document.getElementById('vfMarket')?.value||'';
      const mode=document.getElementById('vfMode')?.value||'line';
      const strategies=[];
      document.querySelectorAll('.vf-strat-cb:checked').forEach(cb=>{strategies.push({id:cb.value,name:cb.dataset.name||cb.value,sport:cb.dataset.sport||'all',code:decodeURIComponent(cb.dataset.code||'').trim()});});

      console.log(`[valueFinder] scan sport=${sport} minEdge=${minEdge} strats=${strategies.length}`);

      let data;
      try{data=await this._post('/api/value/scan',{minEdge,sport,market,strategies,mode});}
      catch(e){this._showError('Ошибка: '+e.message);return;}
      if(!data)return;

      console.log(`[valueFinder] response: total=${data.total} sources=${JSON.stringify(data.sources)} sports=${JSON.stringify(data.sportBreakdown)}`);

      if(!data.bets?.length&&!data.totalFixtures){this._showHint('warn',`ℹ️ Нет матчей. ${data.message||''} Источники: ${data.sources?.join(', ')||'нет'}`);this.results=this._allBets=[];this.renderResults();return;}

      let bets=data.bets||[];
      if(mode==='live'){const n2h=Date.now()+2*3600000;bets=bets.filter(b=>b.startTime&&new Date(b.startTime).getTime()<=n2h);}
      else bets=bets.filter(b=>!b.startTime||new Date(b.startTime).getTime()>Date.now());

      this._allBets=bets;this.results=bets;
      this._loadAllFixtures(sport,mode,data);
      if(this._tab==='signals'){this.renderResults();this.renderChart();this.renderSummary(data);}
      else this._renderFixtures();

      // Подсказка с разбивкой по спортам
      if(data.sources?.length){
        const sb=data.sportBreakdown||{};
        const sportStr=Object.entries(sb).map(([s,n])=>`${this.ICON[s]||'🎯'}${n}`).join(' ');
        this._showHint('info',`✅ ${data.sources.join(', ')} · ${sportStr||'нет данных'} · λ: ${data.lambdaFromHistory||0} команд`);
      }
      this._updateCounts();
    }catch(e){console.error('[valueFinder]',e);this._showError('Ошибка: '+e.message);}
    finally{this.scanning=false;this._progress(false);if(btn){btn.textContent='🔍 Сканировать';btn.disabled=false;}}
  },

  async _loadAllFixtures(sport,mode,scanData){
    const seen=new Set();
    this._allFixtures=(this._allBets||[]).filter(b=>{const k=`${b.sport}|${b.home}|${b.away}`;if(seen.has(k))return false;seen.add(k);return true;}).map(b=>({home:b.home,away:b.away,sport:b.sport,league:b.league,startTime:b.startTime,kickoff:b.kickoff,bmCount:b.bmCount,dataSource:b.dataSource,lH:b.lH,lA:b.lA,lambdaSrc:b.lambdaSrc,homeResolved:b.homeResolved,awayResolved:b.awayResolved,bets:this._allBets.filter(x=>x.home===b.home&&x.away===b.away&&x.sport===b.sport)}));
    try{const data=await this._post('/api/value/fixtures',{sport,mode});if(data?.fixtures?.length){this._allFixtures=data.fixtures.map(f=>({...f,bets:this._allBets.filter(b=>b.home===f.home&&b.away===f.away&&b.sport===f.sport)}));if(this._tab==='fixtures')this._renderFixtures();this._updateCounts();}}catch(e){console.warn('[valueFinder] fixtures:',e.message);if(this._tab==='fixtures')this._renderFixtures();this._updateCounts();}
  },

  // ── Все матчи ────────────────────────────────────────────────────────────
  _renderFixtures(){
    const el=document.getElementById('vfFixturesTable');if(!el)return;
    if(!this._allFixtures.length){el.innerHTML=`<div style="padding:40px;text-align:center"><div style="font-size:32px;margin-bottom:10px">📋</div><div style="color:var(--text2)">Нет матчей. Нажмите "Сканировать".</div></div>`;return;}
    const byLeague={};
    for(const f of this._allFixtures){const icon=this.ICON[f.sport]||'🎯';const k=`${icon} ${f.league}`;if(!byLeague[k])byLeague[k]=[];byLeague[k].push(f);}
    const ids=new Set();document.querySelectorAll('.vf-strat-cb:checked').forEach(cb=>ids.add(cb.value));
    let html='';
    for(const[league,fixtures]of Object.entries(byLeague)){
      html+=`<div style="margin-bottom:16px"><div style="font-size:11px;font-weight:700;color:var(--text3);text-transform:uppercase;letter-spacing:.5px;padding:5px 0;border-bottom:1px solid var(--border);margin-bottom:8px">${league} (${fixtures.length})</div><div style="display:grid;grid-template-columns:repeat(auto-fill,minmax(300px,1fr));gap:8px">`;
      for(const f of fixtures){
        const bets=f.bets||[];const sc=bets.filter(b=>(b.matchedStrategies||[]).some(ms=>ids.has(String(ms.id)))).length;const hs=sc>0;
        const ko=f.kickoff||(f.startTime?new Date(f.startTime).toLocaleString('ru',{month:'short',day:'numeric',hour:'2-digit',minute:'2-digit'}):'—');
        const encF=encodeURIComponent(JSON.stringify({home:f.home,away:f.away,league:f.league,sport:f.sport,startTime:f.startTime,kickoff:ko,lH:f.lH,lA:f.lA,dataSource:f.dataSource,homeResolved:f.homeResolved,awayResolved:f.awayResolved,bets}));
        const hb=bets.find(b=>b.market==='homeWin'),db_=bets.find(b=>b.market==='draw'),ab=bets.find(b=>b.market==='awayWin');
        html+=`<div class="vf-fixture-card ${hs?'has-signal':''}" onclick="valueFinder.showMatchDetail('${encF.replace(/'/g,"\\'")}')"><div class="vf-fc-header"><span style="font-size:10px;color:var(--text3)">${ko}</span>${hs?`<span class="vf-fc-signal">🎯 ${sc}</span>`:'<span style="font-size:10px;color:var(--text3)">нет</span>'}</div><div class="vf-fc-teams"><span>${f.home}</span><span style="color:var(--text3);font-size:11px">vs</span><span>${f.away}</span></div><div class="vf-fc-odds">${hb?`<span>1: <b>${hb.odds}</b></span>`:''}${db_?`<span>X: <b>${db_.odds}</b></span>`:''}${ab?`<span>2: <b>${ab.odds}</b></span>`:''}</div><div style="font-size:10px;color:var(--text3);margin-top:3px">λ:${f.lH||'?'}/${f.lA||'?'} · ${f.lambdaSrc||'?'} · ${f.dataSource||''}</div></div>`;
      }
      html+='</div></div>';
    }
    el.innerHTML=`<div style="overflow-y:auto;max-height:calc(100vh - 280px)">${html}</div>`;
  },

  // ── Таблица сигналов с тултипами ─────────────────────────────────────────
  renderResults() {
    const el=document.getElementById('vfResultsTable');if(!el)return;
    if(!this.results.length){
      const n=this._allFixtures.length;
      el.innerHTML=`<div style="padding:40px;text-align:center"><div style="font-size:36px;margin-bottom:12px">🔍</div><div style="color:var(--text2);font-size:14px;margin-bottom:6px">Нет сигналов</div><div style="color:var(--text3);font-size:12px">${n?`В линии ${n} матч(ей) — <a href="#" onclick="valueFinder._setTab('fixtures');return false" style="color:var(--accent)">посмотреть все →</a>`:'Нажмите Сканировать'}</div></div>`;
      return;
    }
    const MLBL={homeWin:'1 Хоз',draw:'X Нич',awayWin:'2 Гост',over25:'О>2.5',under25:'У<2.5',btts:'BTTS'};
    const T=this.COL_TIPS;

    // Хедер с тултипами
    const makeHeader=(label,tip,align='')=>`<th title="${tip.replace(/\n/g,'&#10;').replace(/"/g,'&quot;')}" style="cursor:help;user-select:none;${align?'text-align:'+align:''}" onclick="event.stopPropagation()">${label} <span style="opacity:.5;font-size:9px">ℹ</span></th>`;

    const rows=this.results.map(r=>{
      const icon=this.ICON[r.sport]||'🎯';
      const ec=r.edge>=10?'hot':r.edge>=5?'warm':'';
      const ms=r.matchedStrategies||[];
      const sb=ms.map(s=>`<span style="font-size:10px;padding:1px 5px;border-radius:3px;background:rgba(99,102,241,.15);color:#818cf8;margin:1px;display:inline-block;max-width:100px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap" title="${(s.name||s.id||'').replace(/"/g,'&quot;')}">${(s.name||s.id||'—').slice(0,13)}</span>`).join('');
      const encF=encodeURIComponent(JSON.stringify({home:r.home,away:r.away,league:r.league,sport:r.sport,startTime:r.startTime,kickoff:r.kickoff,lH:r.lH,lA:r.lA,dataSource:r.dataSource,homeResolved:r.homeResolved,awayResolved:r.awayResolved,bets:this._allBets.filter(b=>b.home===r.home&&b.away===r.away&&b.sport===r.sport)}));
      const estTag=r.oddsSource==='estimated'?'<sup style="color:var(--yellow,#f59e0b);font-size:9px" title="Расчётный коэффициент — нет данных букмекеров">~</sup>':'';
      return`<tr>
        <td style="min-width:130px"><div style="font-size:11px;font-weight:600;white-space:nowrap">${icon} ${r.league}</div><div style="font-size:10px;color:var(--text3)">${r.kickoff||''}</div></td>
        <td style="min-width:160px"><div style="font-weight:500;font-size:12px">${r.home}</div><div style="color:var(--text3);font-size:11px">${r.away}</div></td>
        <td><span class="bt-tag single" style="font-size:11px">${MLBL[r.market]||r.market}</span></td>
        <td style="font-weight:700">${r.odds}${estTag}</td>
        <td style="color:var(--text3)">${r.impliedProb}%</td>
        <td style="color:var(--green);font-weight:600">${r.modelProb}%</td>
        <td><span class="vf-edge-badge ${ec}" title="Edge = ${r.modelProb}% − ${r.impliedProb}% = +${r.edge}%">+${r.edge}%</span></td>
        <td style="color:var(--text3);font-size:11px" title="Half Kelly = ${r.kelly}% от банкролла">${r.kelly}%</td>
        <td style="min-width:130px">${sb}</td>
        <td style="white-space:nowrap">
          <button class="ctrl-btn sm" title="Детали матча: форма, H2H, стратегии" onclick="valueFinder.showMatchDetail('${encF.replace(/'/g,"\\'")}')">🔍</button>
          <button class="ctrl-btn sm" title="Матрица счётов Poisson" onclick="valueFinder.showMatrix('${r.match.replace(/'/g,"\\'")}',${r.lH||1.45},${r.lA||1.15})">📊</button>
          <button class="ctrl-btn sm" title="В watchlist" onclick="valueFinder.addWatch('${r.match.replace(/'/g,"\\'")}','${r.market}',${r.odds})">⭐</button>
        </td>
      </tr>`;
    }).join('');

    el.innerHTML=`<div style="overflow-x:auto;overflow-y:auto;max-height:440px">
      <table class="data-table" style="min-width:960px">
        <thead><tr>
          ${makeHeader('Лига / Время', T.league)}
          ${makeHeader('Матч', T.match)}
          ${makeHeader('Рынок', T.market)}
          ${makeHeader('Коэф', T.odds)}
          ${makeHeader('Рынок%', T.impliedProb)}
          ${makeHeader('Модель%', T.modelProb)}
          ${makeHeader('Edge%', T.edge)}
          ${makeHeader('Kelly%', T.kelly)}
          ${makeHeader('Стратегия', T.strategy)}
          <th></th>
        </tr></thead>
        <tbody>${rows}</tbody>
      </table>
    </div>`;
  },

  renderSummary(data){
    const el=document.getElementById('vfSummary');if(!el||!this.results.length){if(el)el.innerHTML='';return;}
    const avg=(this.results.reduce((s,r)=>s+r.edge,0)/this.results.length).toFixed(1);
    const best=this.results[0];
    const mkt={homeWin:'1',draw:'X',awayWin:'2',over25:'O2.5',under25:'U2.5',btts:'BTTS'};
    const sb=data?.sportBreakdown||{};
    const sportStr=Object.entries(sb).map(([s,n])=>`${this.ICON[s]||'🎯'}${n}`).join(' ');
    el.innerHTML=`<strong>${this.results.length}</strong> сигналов · Ср. Edge: <strong class="positive">+${avg}%</strong> · Лучшая: <strong>${best.home} (${mkt[best.market]||best.market}) +${best.edge}%</strong> · <span>${sportStr}</span> <span style="font-size:10px;color:var(--text3)">· ${data?.totalFixtures||0} матчей</span>`;
  },

  renderChart(){
    if(this.charts.v){try{this.charts.v.destroy();}catch(e){}}
    const cvs=document.getElementById('chartVFValue');if(!cvs||!this.results.length)return;
    const dk=document.body.classList.contains('dark-mode'),tc=dk?'#8892a4':'#4a5568',gc=dk?'rgba(255,255,255,.05)':'rgba(0,0,0,.07)';
    const top=this.results.slice(0,15);
    this.charts.v=new Chart(cvs,{type:'bar',data:{labels:top.map(r=>`${this.ICON[r.sport]||'🎯'} ${r.home.slice(0,8)} ${({homeWin:'1',draw:'X',awayWin:'2',over25:'O2.5',btts:'BB'})[r.market]||r.market}`),datasets:[{label:'Edge %',data:top.map(r=>r.edge),backgroundColor:'rgba(0,212,255,.8)',borderRadius:4},{label:'Модель %',data:top.map(r=>r.modelProb),backgroundColor:'rgba(0,230,118,.55)',borderRadius:4},{label:'Рынок %',data:top.map(r=>r.impliedProb),backgroundColor:'rgba(148,163,184,.35)',borderRadius:4}]},options:{responsive:true,maintainAspectRatio:false,plugins:{legend:{labels:{color:tc,font:{size:11}}},tooltip:{callbacks:{afterTitle:items=>{const r=top[items[0]?.dataIndex];return r?`${r.home} vs ${r.away}\nEdge = ${r.modelProb}% − ${r.impliedProb}% = +${r.edge}%`:'';}}}},scales:{x:{ticks:{color:tc,font:{size:9},maxRotation:40},grid:{color:gc}},y:{ticks:{color:tc},grid:{color:gc}}}}});
  },

  // ── Детали матча ─────────────────────────────────────────────────────────
  showMatchDetail(encodedFixture){
    let f;try{f=JSON.parse(decodeURIComponent(encodedFixture));}catch(e){return;}
    const bets=f.bets||[],strategies=this.getStrategies(),ids=new Set();
    document.querySelectorAll('.vf-strat-cb:checked').forEach(cb=>ids.add(cb.value));
    const MLBL={homeWin:'1 Хоз',draw:'X Нич',awayWin:'2 Гост',over25:'О>2.5',under25:'У<2.5',btts:'BTTS'};
    const mkts=['homeWin','draw','awayWin','over25','under25','btts'];
    const sb=bets[0]||{},homeRes=f.homeResolved||sb.homeResolved||f.home,awayRes=f.awayResolved||sb.awayResolved||f.away;
    const lH=+(f.lH||sb.lH||1.45),lA=+(f.lA||sb.lA||1.15),lSrc=sb.lambdaSrc||'default';
    const oddsRows=mkts.map(m=>{const b=bets.find(x=>x.market===m);if(!b)return'';const ec=b.edge>=10?'color:var(--green)':b.edge>=5?'color:#ffd740':'color:var(--text2)';return`<tr><td style="padding:5px 8px">${MLBL[m]}</td><td style="padding:5px 8px;font-weight:700">${b.odds}${b.oddsSource==='estimated'?'<sup style="color:var(--yellow,#f59e0b);font-size:9px" title="Расчётный — нет данных букмекеров">~</sup>':''}</td><td style="padding:5px 8px;color:var(--text3)" title="${this.COL_TIPS.impliedProb}">${b.impliedProb}%</td><td style="padding:5px 8px;color:var(--green)" title="${this.COL_TIPS.modelProb}">${b.modelProb}%</td><td style="padding:5px 8px;font-weight:700;${ec}" title="${this.COL_TIPS.edge}">${b.edge>0?'+':''}${b.edge}%</td><td style="padding:5px 8px;color:var(--text3)" title="${this.COL_TIPS.kelly}">${b.kelly}%</td></tr>`;}).join('');
    const activeStrats=strategies.filter(s=>ids.has(String(s.id||s.name)));
    let stratRows=!activeStrats.length?`<tr><td colspan="4" style="padding:12px;text-align:center;color:var(--text3)">Нет выбранных стратегий</td></tr>`:activeStrats.map(s=>{const icon=s._source==='ai'?'🤖':s._source==='backtest'?'⚙️':s._source==='builtin'?'⭐':'📚';const mr=mkts.map(m=>{const b=bets.find(x=>x.market===m);if(!b)return null;const ms=b.matchedStrategies||[];return{market:m,matched:ms.some(x=>String(x.id)===String(s.id||s.name)),bet:b};}).filter(Boolean);const any=mr.some(r=>r.matched);return`<tr style="${any?'background:rgba(0,230,118,.05)':''}"><td style="padding:6px 8px;font-weight:600">${icon} ${s.name||s.id}</td><td style="padding:6px 8px;font-size:10px;color:var(--text3)">${s.sport||'any'}</td><td style="padding:6px 8px">${mr.map(r=>`<span style="display:inline-block;margin:1px;padding:2px 5px;border-radius:3px;font-size:10px;font-weight:600;background:${r.matched?'rgba(0,230,118,.2)':'rgba(255,255,255,.05)'};color:${r.matched?'var(--green)':'var(--text3)'};border:1px solid ${r.matched?'rgba(0,230,118,.4)':'var(--border)'}">${MLBL[r.market]} ${r.matched?'✓':'✗'}</span>`).join('')}</td><td style="padding:6px 8px">${any?`<span style="color:var(--green);font-weight:700">✅</span>`:`<span style="color:var(--text3)">—</span>`}</td></tr>`;}).join('');
    let modal=document.getElementById('vfMatchModal');
    if(!modal){modal=document.createElement('div');modal.id='vfMatchModal';modal.className='modal';modal.onclick=e=>{if(e.target===modal)modal.style.display='none';};document.body.appendChild(modal);}
    modal.innerHTML=`<div class="modal-box" style="max-width:740px;max-height:88vh;overflow-y:auto">
      <div class="modal-header" style="position:sticky;top:0;background:var(--bg1);z-index:10;padding-bottom:10px;border-bottom:1px solid var(--border)">
        <div><div style="font-size:15px;font-weight:700">${this.ICON[f.sport]||'🎯'} ${f.home} vs ${f.away}</div><div style="font-size:11px;color:var(--text3);margin-top:2px">🏆 ${f.league} · 📅 ${f.kickoff||'—'} · 📡 ${f.dataSource||'—'}</div></div>
        <button class="modal-close" onclick="document.getElementById('vfMatchModal').style.display='none'">✕</button>
      </div>
      <div style="padding:14px;display:flex;flex-direction:column;gap:14px">
        <div>
          <div style="font-size:11px;font-weight:700;color:var(--text3);text-transform:uppercase;margin-bottom:6px">Данные команд</div>
          <div style="display:grid;grid-template-columns:1fr 1fr;gap:8px">
            ${[{name:f.home,res:homeRes,lambda:lH},{name:f.away,res:awayRes,lambda:lA}].map(t=>`<div style="background:var(--bg3);padding:10px;border-radius:8px;border:1px solid var(--border)"><div style="font-weight:600;font-size:13px;margin-bottom:5px">${t.name}</div>${t.res!==t.name?`<div style="font-size:10px;color:var(--accent);margin-bottom:4px">🔗 в БД: <b>${t.res}</b></div>`:''}<div style="font-size:11px;color:var(--text2);display:grid;grid-template-columns:1fr 1fr;gap:3px"><div title="${this.COL_TIPS.modelProb}">λ: <b>${t.lambda.toFixed(3)}</b></div><div>λ-src: <b style="color:${lSrc==='history'?'var(--green)':'var(--yellow,#f59e0b)'}">${lSrc}</b></div></div></div>`).join('')}
          </div>
          <div style="margin-top:6px;padding:6px 10px;background:var(--bg3);border-radius:6px;font-size:10px;color:var(--text3);font-family:monospace">
            🔍 Поиск в ClickHouse: "${homeRes}" vs "${awayRes}" · sport: ${f.sport}${lSrc!=='history'?' · ⚠️ Нет исторических данных (λ дефолтный)':''}
          </div>
        </div>
        <div>
          <div style="font-size:11px;font-weight:700;color:var(--text3);text-transform:uppercase;margin-bottom:6px" title="${this.COL_TIPS.edge}">Рынки · Вероятности · Edge <span style="opacity:.5;font-size:9px">ℹ</span></div>
          ${bets.length?`<div style="overflow-x:auto"><table class="data-table" style="min-width:400px"><thead><tr><th>Рынок</th><th title="${this.COL_TIPS.odds}">Коэф ℹ</th><th title="${this.COL_TIPS.impliedProb}">Рынок% ℹ</th><th title="${this.COL_TIPS.modelProb}">Модель% ℹ</th><th title="${this.COL_TIPS.edge}">Edge% ℹ</th><th title="${this.COL_TIPS.kelly}">Kelly% ℹ</th></tr></thead><tbody>${oddsRows}</tbody></table></div>`:`<div style="color:var(--text3);font-size:12px;padding:8px">Нет рынков с edge > минимального</div>`}
        </div>
        <div>
          <div style="font-size:11px;font-weight:700;color:var(--text3);text-transform:uppercase;margin-bottom:6px" title="${this.COL_TIPS.strategy}">Проверка стратегий ℹ</div>
          <div style="overflow-x:auto"><table class="data-table" style="min-width:380px"><thead><tr><th>Стратегия</th><th>Спорт</th><th>По рынкам</th><th>Итог</th></tr></thead><tbody>${stratRows}</tbody></table></div>
          ${lSrc!=='history'?`<div style="margin-top:8px;padding:8px 10px;background:rgba(255,214,0,.07);border:1px solid rgba(255,214,0,.2);border-radius:6px;font-size:11px;color:#ffd740">⚠️ Нет исторических данных в ClickHouse для этих команд. Стратегии использующие form/H2H вернут null.</div>`:''}
        </div>
      </div>
    </div>`;
    modal.style.display='flex';
  },

  // ── Matrix ────────────────────────────────────────────────────────────────
  async showMatrix(matchName,lH,lA){
    let d;try{d=await this._post('/api/value/calculate',{home:matchName.split(' vs ')[0],away:matchName.split(' vs ')[1],homeAttack:lH/1.45,homeDefense:1,awayAttack:lA/1.15,awayDefense:1});}catch(e){d=this._localMatrix(lH,lA);}
    let modal=document.getElementById('vfMatrixModal');if(!modal){modal=document.createElement('div');modal.id='vfMatrixModal';modal.className='modal';modal.onclick=e=>{if(e.target===modal)modal.style.display='none';};document.body.appendChild(modal);}
    const mat=d.pois?.matrix||[],top=d.pois?.topScores||[],maxP=Math.max(...(mat.flat().length?mat.flat():[1]));
    const thead='<tr><th style="padding:4px 8px;font-size:10px;color:var(--text3)">Хоз↓/Гост→</th>'+[0,1,2,3,4,5,6].map(i=>`<th style="padding:4px 8px;background:var(--bg3);font-size:11px">${i}</th>`).join('')+'</tr>';
    const tbody=(mat.length?mat:Array(7).fill(Array(7).fill(0))).slice(0,7).map((row,h)=>`<tr><td style="font-weight:600;background:var(--bg3);padding:4px 8px">${h}</td>`+row.slice(0,7).map((p,a)=>{const bg=h>a?`rgba(0,212,255,${p/maxP*.55})`:h===a?`rgba(148,163,184,${p/maxP*.45})`:`rgba(0,230,118,${p/maxP*.55})`;return`<td style="background:${bg};padding:4px 8px;font-size:11px;cursor:default" title="${h}:${a} — ${(p*100).toFixed(2)}%">${(p*100).toFixed(1)}%</td>`;}).join('')+'</tr>').join('');
    modal.innerHTML=`<div class="modal-box" style="max-width:560px"><div class="modal-header"><strong>📊 ${matchName}</strong><button class="modal-close" onclick="document.getElementById('vfMatrixModal').style.display='none'">✕</button></div><div style="padding:16px;overflow-x:auto"><div style="font-size:11px;color:var(--text3);margin-bottom:8px">Poisson матрица счётов. Синий = победа хозяев, серый = ничья, зелёный = победа гостей</div><table style="border-collapse:collapse"><thead>${thead}</thead><tbody>${tbody}</tbody></table>${top.length?`<div style="margin-top:10px;display:flex;gap:5px;flex-wrap:wrap">${top.slice(0,8).map(s=>`<span class="chip" title="${(s.prob*100).toFixed(2)}%">${s.score} — ${(s.prob*100).toFixed(1)}%</span>`).join('')}</div>`:''}</div></div>`;
    modal.style.display='flex';
  },
  _localMatrix(lH,lA){const F=[1,1,2,6,24,120,720,5040,40320,362880];const p=(k,l)=>k>9?0:Math.pow(l,k)*Math.exp(-l)/F[k];const mat=[],top=[];for(let h=0;h<7;h++){mat[h]=[];for(let a=0;a<7;a++){const v=p(h,lH)*p(a,lA);mat[h][a]=v;top.push({score:`${h}:${a}`,prob:v});}}top.sort((a,b)=>b.prob-a.prob);return{pois:{matrix:mat,topScores:top.slice(0,8)}};},

  addWatch(match,market,odds){const list=JSON.parse(localStorage.getItem('bq_watchlist')||'[]');if(!list.some(x=>x.match===match&&x.market===market)){list.push({match,market,odds,added:new Date().toISOString()});localStorage.setItem('bq_watchlist',JSON.stringify(list));}if(event?.target){event.target.textContent='✓';event.target.style.color='var(--green)';}},

  _progress(on){const el=document.getElementById('valueScanProgress');if(el)el.style.display=on?'block':'none';},
  _showError(msg){const el=document.getElementById('vfResultsTable');if(el)el.innerHTML=`<div style="padding:40px;text-align:center"><div style="font-size:28px;margin-bottom:10px">⚠️</div><div style="color:var(--red);font-size:13px">${msg}</div></div>`;},
  _showHint(type,msg){const el=document.getElementById('vfSourceHint');if(el)el.innerHTML=`<div class="vf-hint-box ${type}">${msg}</div>`;},
  async _post(url,body){const r=await fetch(url,{method:'POST',headers:{'Content-Type':'application/json','x-auth-token':localStorage.getItem('bq_token')||'demo'},body:JSON.stringify(body)});if(!r.ok)throw new Error(`HTTP ${r.status}: ${await r.text().catch(()=>'')}`);return r.json();},
};