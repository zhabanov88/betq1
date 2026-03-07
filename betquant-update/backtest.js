'use strict';
// ═══════════════════════════════════════════════════════════════════════════
//  BetQuant Pro — Backtest Engine v3
//  • Мульти-стратегия: несколько стратегий одновременно, общий результат
//  • Экспрессы: перекрещивание сигналов из разных стратегий/спортов
//  • Per-strategy breakdown в таблице ставок
// ═══════════════════════════════════════════════════════════════════════════
const backtestEngine = {
  charts:  {},
  running: false,

  BUILT_IN: {
    value_home: {
      name: 'Value Home (Football)',
      sport: 'football',
      code: `function evaluate(match, team, h2h, market) {
  const form = team.form(match.team_home, 5);
  const wins = form.filter(r => r === 'W').length;
  const prob = 0.3 + wins * 0.07;
  const edge = market.value(match.odds_home, prob);
  if (edge > 0.04 && match.odds_home >= 1.6 && match.odds_home <= 3.5)
    return { signal: true, market: 'home', prob, stake: market.kelly(match.odds_home, prob) * 0.5 };
  return null;
}`,
    },
    over25_xg: {
      name: 'Over 2.5 xG Model (Football)',
      sport: 'football',
      code: `function evaluate(match, team, h2h, market) {
  const hxg = team.xG(match.team_home, 5);
  const axg = team.xG(match.team_away, 5);
  const prob = Math.min(0.85, (hxg + axg) / 4.5);
  if (prob > 0.55 && match.odds_over >= 1.5 && match.odds_over <= 2.2)
    return { signal: true, market: 'over', prob, stake: market.kelly(match.odds_over, prob) * 0.5 };
  return null;
}`,
    },
    away_form: {
      name: 'Away Form (Football)',
      sport: 'football',
      code: `function evaluate(match, team, h2h, market) {
  const form = team.form(match.team_away, 5);
  const wins = form.filter(r => r === 'W').length;
  if (wins >= 4 && match.odds_away >= 2.0 && match.odds_away <= 4.5) {
    const prob = 0.2 + wins * 0.06;
    return { signal: true, market: 'away', prob, stake: market.kelly(match.odds_away, prob) * 0.4 };
  }
  return null;
}`,
    },
    h2h_dominant: {
      name: 'H2H Dominant (Football)',
      sport: 'football',
      code: `function evaluate(match, team, h2h, market) {
  const hist = h2h.results;
  if (hist.length < 3) return null;
  const homeWins = hist.filter(r => r.result === 'home').length;
  if (homeWins / hist.length >= 0.7 && match.odds_home <= 2.5) {
    const prob = homeWins / hist.length * 0.85;
    return { signal: true, market: 'home', prob, stake: market.kelly(match.odds_home, prob) * 0.4 };
  }
  return null;
}`,
    },
    tennis_serve: {
      name: 'Serve Dominance (Tennis)',
      sport: 'tennis',
      code: `function evaluate(match, team, h2h, market) {
  const rankDiff = (match.rank_away || 50) - (match.rank_home || 50);
  const prob = 0.5 + Math.min(0.25, rankDiff * 0.005);
  if (prob > 0.58 && match.odds_home >= 1.3 && match.odds_home <= 2.0)
    return { signal: true, market: 'home', prob, stake: market.kelly(match.odds_home, prob) * 0.5 };
  return null;
}`,
    },
    nba_home: {
      name: 'NBA Home Advantage',
      sport: 'basketball',
      code: `function evaluate(match, team, h2h, market) {
  const form = team.form(match.team_home, 5);
  const wins = form.filter(r => r === 'W').length;
  const prob = 0.55 + wins * 0.03;
  if (prob > 0.6 && match.odds_home >= 1.3 && match.odds_home <= 2.0)
    return { signal: true, market: 'home', prob, stake: market.kelly(match.odds_home, prob) * 0.5 };
  return null;
}`,
    },
  },

  activeStrategies: [],
  parlayRules: [],

  init() {
    this.activeStrategies = this.loadActiveStrategies();
    this.renderStrategySlots();
    this.renderParlayRules();
  },

  loadActiveStrategies() {
    const saved = JSON.parse(localStorage.getItem('bq_bt_strategies') || 'null');
    if (saved && saved.length) return saved;
    return [
      { id: 'value_home', ...this.BUILT_IN.value_home,  color: '#00d4ff', enabled: true },
      { id: 'over25_xg',  ...this.BUILT_IN.over25_xg,   color: '#00e676', enabled: true },
    ];
  },

  saveActiveStrategies() {
    localStorage.setItem('bq_bt_strategies', JSON.stringify(this.activeStrategies));
  },

  addStrategySlot() {
    const id = 'custom_' + Date.now();
    this.activeStrategies.push({
      id, name: 'New Strategy', sport: 'football',
      code: `function evaluate(match, team, h2h, market) {\n  // Твой код здесь\n  return null;\n}`,
      color: this.randomColor(), enabled: true,
    });
    this.saveActiveStrategies();
    this.renderStrategySlots();
  },

  removeStrategySlot(id) {
    this.activeStrategies = this.activeStrategies.filter(s => s.id !== id);
    this.saveActiveStrategies();
    this.renderStrategySlots();
    this.renderParlayRules();
  },

  randomColor() {
    const c = ['#f59e0b','#8b5cf6','#ef4444','#06b6d4','#84cc16','#f97316','#ec4899'];
    return c[Math.floor(Math.random() * c.length)];
  },

  renderStrategySlots() {
    const container = document.getElementById('btStrategySlots');
    if (!container) return;
    container.innerHTML = this.activeStrategies.map(s => `
      <div class="bt-strategy-slot">
        <div class="bt-slot-header">
          <span class="bt-slot-color" style="background:${s.color}"></span>
          <input class="ctrl-input bt-slot-name" value="${s.name.replace(/"/g,'&quot;')}"
            onchange="backtestEngine.updateSlot('${s.id}','name',this.value)">
          <select class="ctrl-select bt-slot-sport"
            onchange="backtestEngine.updateSlot('${s.id}','sport',this.value)">
            ${['football','tennis','basketball','hockey','baseball'].map(sp =>
              `<option value="${sp}" ${s.sport===sp?'selected':''}>${sp}</option>`
            ).join('')}
          </select>
          <label class="toggle-switch" style="margin-left:auto">
            <input type="checkbox" ${s.enabled?'checked':''}
              onchange="backtestEngine.toggleSlot('${s.id}',this.checked)">
            <span class="toggle-slider"></span>
          </label>
          <button class="ctrl-btn sm danger" onclick="backtestEngine.removeStrategySlot('${s.id}')">✕</button>
        </div>
        <textarea class="code-editor bt-slot-code" rows="7" spellcheck="false"
          onblur="backtestEngine.updateSlot('${s.id}','code',this.value)">${s.code}</textarea>
        <div class="bt-slot-footer">
          <select class="ctrl-select" style="flex:1" onchange="backtestEngine.loadBuiltIn('${s.id}',this.value)">
            <option value="">— Загрузить шаблон —</option>
            ${Object.entries(this.BUILT_IN).map(([k,v]) => `<option value="${k}">${v.name}</option>`).join('')}
          </select>
          <button class="ctrl-btn sm" onclick="backtestEngine.loadFromLibrary('${s.id}')">📚 Из библиотеки</button>
        </div>
      </div>
    `).join('');
  },

  updateSlot(id, field, value) {
    const s = this.activeStrategies.find(x => x.id === id);
    if (s) { s[field] = value; this.saveActiveStrategies(); }
  },

  toggleSlot(id, enabled) {
    this.updateSlot(id, 'enabled', enabled);
  },

  loadBuiltIn(slotId, key) {
    if (!key) return;
    const s = this.activeStrategies.find(x => x.id === slotId);
    const bi = this.BUILT_IN[key];
    if (s && bi) { Object.assign(s, bi); this.saveActiveStrategies(); this.renderStrategySlots(); }
  },

  loadFromLibrary(slotId) {
    const libs = JSON.parse(localStorage.getItem('bq_library') || '[]');
    if (!libs.length) { alert('Библиотека пуста. Сохрани стратегию в Strategy Builder.'); return; }
    const n = prompt('Стратегии:\n' + libs.map((l,i)=>`${i+1}. ${l.name}`).join('\n') + '\n\nВведи номер:');
    const idx = parseInt(n) - 1;
    if (isNaN(idx) || !libs[idx]) return;
    const l = libs[idx];
    const s = this.activeStrategies.find(x => x.id === slotId);
    if (s) { s.name = l.name; s.code = l.code; s.sport = l.sport||s.sport; this.saveActiveStrategies(); this.renderStrategySlots(); }
  },

  // ── PARLAY RULES ──────────────────────────────────────────────────────────
  renderParlayRules() {
    const container = document.getElementById('btParlayRules');
    if (!container) return;
    if (!this.parlayRules.length) {
      container.innerHTML = '<div class="bt-parlay-empty">Правил нет — бэктест считает одиночные ставки.<br>Добавь правило для формирования экспрессов.</div>';
      return;
    }
    container.innerHTML = this.parlayRules.map((rule, i) => `
      <div class="bt-parlay-rule">
        <div class="bt-parlay-rule-row">
          <span class="bt-parlay-label">🎯 Экспресс</span>
          <label>Ног от</label>
          <input type="number" class="ctrl-input xs" value="${rule.minLegs}" min="2" max="8"
            onchange="backtestEngine.updateParlay(${i},'minLegs',+this.value)">
          <label>до</label>
          <input type="number" class="ctrl-input xs" value="${rule.maxLegs}" min="2" max="8"
            onchange="backtestEngine.updateParlay(${i},'maxLegs',+this.value)">
          <label class="bt-parlay-check-label">
            <input type="checkbox" ${rule.requireDifferentSports?'checked':''}
              onchange="backtestEngine.updateParlay(${i},'requireDifferentSports',this.checked)">
            Разные спорты
          </label>
          <button class="ctrl-btn sm danger" style="margin-left:auto" onclick="backtestEngine.removeParlay(${i})">✕ Удалить</button>
        </div>
        <div class="bt-parlay-rule-row" style="margin-top:8px;flex-wrap:wrap;gap:6px">
          <span class="bt-parlay-label">Стратегии:</span>
          ${this.activeStrategies.filter(s=>s.enabled).map(s => `
            <label class="bt-parlay-strat-check">
              <input type="checkbox"
                ${(rule.strategyIds||[]).includes(s.id)?'checked':''}
                onchange="backtestEngine.toggleParlayStrat(${i},'${s.id}',this.checked)">
              <span style="color:${s.color}">●</span> ${s.name}
            </label>`).join('')}
        </div>
      </div>
    `).join('');
  },

  addParlayRule() {
    this.parlayRules.push({
      minLegs: 2, maxLegs: 3,
      strategyIds: this.activeStrategies.filter(s=>s.enabled).map(s=>s.id),
      requireDifferentSports: false,
    });
    this.renderParlayRules();
  },

  removeParlay(i) { this.parlayRules.splice(i,1); this.renderParlayRules(); },
  updateParlay(i, f, v) { if (this.parlayRules[i]) this.parlayRules[i][f] = v; },
  toggleParlayStrat(ri, sid, checked) {
    const r = this.parlayRules[ri]; if (!r) return;
    r.strategyIds = r.strategyIds || [];
    if (checked) { if (!r.strategyIds.includes(sid)) r.strategyIds.push(sid); }
    else r.strategyIds = r.strategyIds.filter(x => x !== sid);
  },

  // ══════════════════════════════════════════════════════════════════════════
  //  MAIN RUN
  // ══════════════════════════════════════════════════════════════════════════
  async run() {
    if (this.running) return;
    this.running = true;
    document.getElementById('btnRunBacktest').style.display = 'none';
    document.getElementById('btnStopBacktest').style.display = '';

    const cfg = this.readConfig();
    const enabled = this.activeStrategies.filter(s => s.enabled);
    if (!enabled.length) { alert('Включи хотя бы одну стратегию'); this.stopUI(); return; }

    this.showProgress(10, 'Компиляция стратегий...');
    const evalFns = enabled.map(s => ({ ...s, fn: this.compileStrategy(s.code) }));

    // ── Пробуем получить реальные данные с сервера ───────────────
    this.showProgress(25, 'Запрос реальных данных из БД...');
    let serverResult = null;
    try {
      for (const s of enabled) {
        const payload = {
          ...cfg,
          code: s.code,
          sport: s.sport || 'football',
          league: cfg.league || 'all',
          season: cfg.season || 'all',
        };
        const resp = await fetch('/api/backtest/run', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify(payload),
        });
        const data = await resp.json();
        if (data && data.trades && data.trades.length > 0) {
          // Успех — используем серверный результат
          serverResult = data;
          s._serverResult = data;
          this.showProgress(70, `✓ Сервер: ${data.trades.length} ставок из реальных данных`);
          break;
        }
      }
    } catch (e) {
      console.warn('Server backtest unavailable, using demo data:', e.message);
    }

    let result;
    if (serverResult) {
      // Используем серверный результат
      result = { byStrategy: {} };
      enabled.forEach(s => {
        result.byStrategy[s.id] = s._serverResult || serverResult;
      });
      result.combined = serverResult;
    } else {
      // Fallback — демо данные
      this.showProgress(30, 'Данные не найдены в БД — используем демо...');
      const matchesBySport = this.generateAllMatches(cfg, enabled);
      this.showProgress(40, 'Прогон бэктеста (демо)...');
      result = this.parlayRules.length
        ? this.runParlayEngine(evalFns, matchesBySport, cfg)
        : this.runSinglesEngine(evalFns, matchesBySport, cfg);
    }

    this.showProgress(85, 'Отрисовка графиков...');
    this.displayResults(result, enabled);
    this.renderCharts(result);

    this.showProgress(100, 'Готово ✓');
    setTimeout(() => { const w = document.getElementById('btProgressWrap'); if(w) w.style.display='none'; }, 700);
    this.stopUI();
  },

  readConfig() {
    return {
      dateFrom:    document.getElementById('btDateFrom')?.value   || '2020-01-01',
      dateTo:      document.getElementById('btDateTo')?.value     || '2024-12-31',
      staking:     document.getElementById('btStaking')?.value    || 'half_kelly',
      bankroll:    parseFloat(document.getElementById('btBankroll')?.value)   || 1000,
      maxStakePct: parseFloat(document.getElementById('btMaxStake')?.value)   || 5,
      commission:  parseFloat(document.getElementById('btCommission')?.value) || 0,
      minOdds:     parseFloat(document.getElementById('btMinOdds')?.value)    || 1.3,
      maxOdds:     parseFloat(document.getElementById('btMaxOdds')?.value)    || 15,
      league:      document.getElementById('btLeagueFilter')?.value  || 'all',
      season:      document.getElementById('btSeasonFilter')?.value  || 'all',
      sport:       document.getElementById('btSportFilter')?.value   || 'football',
    };
  },

  compileStrategy(code) {
    try {
      const m = code.match(/function evaluate\s*\([^)]*\)\s*\{([\s\S]*)\}/);
      if (!m) return null;
      return new Function('match','team','h2h','market', m[1] + '\nreturn null;');
    } catch(e) { return null; }
  },

  generateAllMatches(cfg, strategies) {
    const out = {};
    for (const sport of [...new Set(strategies.map(s=>s.sport))]) {
      out[sport] = this.generateMatchData(cfg, sport);
    }
    return out;
  },

  generateMatchData(cfg, sport='football') {
    const from = new Date(cfg.dateFrom).getTime();
    const to   = new Date(cfg.dateTo).getTime();
    const interval = sport === 'tennis' ? 86400000 : 86400000 * 2;
    const teamMap = {
      football:   ['Arsenal','Chelsea','Liverpool','Man City','Bayern','Dortmund','PSG','Real Madrid','Barcelona','Juventus','Inter','Napoli','Ajax','Porto','Sevilla','Lyon'],
      tennis:     ['Djokovic','Alcaraz','Sinner','Medvedev','Zverev','Rublev','Tsitsipas','Rune','Fritz','Hurkacz','Musetti','Shelton'],
      basketball: ['Lakers','Warriors','Celtics','Heat','Bucks','Nuggets','76ers','Nets','Bulls','Mavericks','Clippers','Suns'],
      hockey:     ['Capitals','Penguins','Rangers','Bruins','Canadiens','Maple Leafs','Oilers','Flames','Blues','Blackhawks'],
      baseball:   ['Yankees','Red Sox','Dodgers','Giants','Cubs','Cardinals','Astros','Braves','Mets','Phillies'],
    };
    const leagueMap = {
      football:   ['EPL','La Liga','Bundesliga','Serie A','Ligue 1','Champions League'],
      tennis:     ['ATP 500','Grand Slam','Masters 1000','ATP 250'],
      basketball: ['NBA','Euroleague'],
      hockey:     ['NHL','KHL'],
      baseball:   ['MLB'],
    };
    const teams   = teamMap[sport]   || teamMap.football;
    const leagues = leagueMap[sport] || ['League'];
    const matches = [];

    for (let t = from; t < to; t += interval + Math.random() * interval * 0.5) {
      const hi = Math.floor(Math.random() * teams.length);
      let ai = Math.floor(Math.random() * teams.length);
      if (ai === hi) ai = (ai+1) % teams.length;

      const ho = +(1.4 + Math.random()*3).toFixed(2);
      const do_ = +(2.5 + Math.random()*2).toFixed(2);
      const ao = +(1.8 + Math.random()*4).toFixed(2);
      const oo = +(1.55 + Math.random()*0.7).toFixed(2);
      const r  = Math.random();
      const result = sport === 'football'
        ? (r<0.46?'home':r<0.74?'away':'draw')
        : (r<0.55?'home':'away');
      const hg = Math.floor(Math.random()*4), ag = Math.floor(Math.random()*4);

      matches.push({
        date: new Date(t).toISOString().split('T')[0],
        sport, league: leagues[Math.floor(Math.random()*leagues.length)],
        team_home: teams[hi], team_away: teams[ai],
        odds_home: ho, odds_draw: do_, odds_away: ao,
        odds_over: oo, odds_under: +(oo*0.9).toFixed(2), odds_btts: +(1.65+Math.random()*0.6).toFixed(2),
        result, home_goals: hg, away_goals: ag,
        over25: hg+ag > 2, btts: hg>0 && ag>0,
        rank_home: Math.ceil(Math.random()*100), rank_away: Math.ceil(Math.random()*100),
        prob_home: +(0.93/ho).toFixed(3), prob_draw: +(0.93/do_).toFixed(3), prob_away: +(0.93/ao).toFixed(3),
      });
    }
    return matches.sort((a,b) => a.date.localeCompare(b.date));
  },

  makeTeamAPI(m, all) {
    return {
      form: (name, n) => all.filter(x=>x.team_home===name||x.team_away===name).slice(-n)
        .map(x => x.result==='draw'?'D':((x.team_home===name&&x.result==='home')||(x.team_away===name&&x.result==='away'))?'W':'L'),
      goalsScored:   () => +(1.0+Math.random()*1.2).toFixed(2),
      goalsConceded: () => +(0.8+Math.random()*1.0).toFixed(2),
      xG:            () => +(1.0+Math.random()*0.9).toFixed(2),
    };
  },
  makeH2H: (m, all) => ({
    results: all.filter(x =>
      (x.team_home===m.team_home&&x.team_away===m.team_away)||
      (x.team_home===m.team_away&&x.team_away===m.team_home)
    ).slice(-8)
  }),
  makeMarketAPI: () => ({
    implied: o => 1/o,
    value:   (o,p) => p - 1/o,
    kelly:   (o,p) => Math.max(0, ((o-1)*p-(1-p))/(o-1)),
  }),

  // ══════════════════════════════════════════════════════════════════════════
  //  SINGLES ENGINE
  // ══════════════════════════════════════════════════════════════════════════
  runSinglesEngine(evalFns, matchesBySport, cfg) {
    let bank = cfg.bankroll;
    const equity = [bank], trades = [];
    const ss = {}; evalFns.forEach(s => { ss[s.id] = {bets:0,wins:0,pnl:0,stakes:0}; });

    const signalsByDate = {};
    for (const ev of evalFns) {
      if (!ev.fn) continue;
      const matches = matchesBySport[ev.sport] || [];
      for (const m of matches) {
        let sig = null;
        try { sig = ev.fn(m, this.makeTeamAPI(m,matches), this.makeH2H(m,matches), this.makeMarketAPI()); } catch(e) {}
        if (!sig?.signal) continue;
        const odds = m['odds_'+(sig.market||'home')];
        if (!odds || odds < cfg.minOdds || odds > cfg.maxOdds) continue;
        if (!signalsByDate[m.date]) signalsByDate[m.date] = [];
        signalsByDate[m.date].push({ m, sig, odds, ev });
      }
    }

    for (const date of Object.keys(signalsByDate).sort()) {
      for (const { m, sig, odds, ev } of signalsByDate[date]) {
        const stake = this.calcStake(cfg, bank, odds, sig.prob||0.5);
        if (stake < 0.01) continue;
        const won = this.checkWin(m, sig.market);
        const pnl = won ? stake*(odds-1)*(1-cfg.commission/100) : -stake;
        bank = Math.max(0, bank + pnl);
        equity.push(bank);
        const s = ss[ev.id]; s.bets++; s.stakes+=stake; s.pnl+=pnl; if(won) s.wins++;
        trades.push({
          date, type:'single',
          match: `${m.team_home} vs ${m.team_away}`,
          sport: m.sport, league: m.league,
          strategyId: ev.id, strategyName: ev.name, strategyColor: ev.color,
          market: sig.market, odds, legs: 1,
          stake: stake.toFixed(2), won: won?'W':'L',
          pnl: pnl.toFixed(2), bankroll: bank.toFixed(2),
        });
      }
    }
    return { trades, equity, stratStats: ss, stats: this.calcStats(trades, cfg.bankroll, equity) };
  },

  // ══════════════════════════════════════════════════════════════════════════
  //  PARLAY ENGINE
  // ══════════════════════════════════════════════════════════════════════════
  runParlayEngine(evalFns, matchesBySport, cfg) {
    let bank = cfg.bankroll;
    const equity = [bank], trades = [];
    const ss = {}; evalFns.forEach(s => { ss[s.id]={bets:0,wins:0,pnl:0,stakes:0}; });

    // Все сигналы по дате
    const signalsByDate = {};
    for (const ev of evalFns) {
      if (!ev.fn) continue;
      const matches = matchesBySport[ev.sport] || [];
      for (const m of matches) {
        let sig = null;
        try { sig = ev.fn(m, this.makeTeamAPI(m,matches), this.makeH2H(m,matches), this.makeMarketAPI()); } catch(e){}
        if (!sig?.signal) continue;
        const odds = m['odds_'+(sig.market||'home')];
        if (!odds||odds<cfg.minOdds||odds>cfg.maxOdds) continue;
        if (!signalsByDate[m.date]) signalsByDate[m.date]=[];
        signalsByDate[m.date].push({ m, sig, odds, ev });
      }
    }

    const allParlayStratIds = new Set(this.parlayRules.flatMap(r=>r.strategyIds||[]));

    for (const date of Object.keys(signalsByDate).sort()) {
      const daySignals = signalsByDate[date];

      // Обрабатываем каждое правило парлея
      for (const rule of this.parlayRules) {
        const cands = daySignals.filter(s =>
          (!rule.strategyIds?.length || rule.strategyIds.includes(s.ev.id))
        );
        if (cands.length < rule.minLegs) continue;

        // Уникальные матчи
        const byMatch = {};
        for (const c of cands) {
          const k = c.m.team_home+'_'+c.m.team_away+'_'+c.ev.sport;
          if (!byMatch[k]) byMatch[k] = c;
        }
        let legs = Object.values(byMatch);

        if (rule.requireDifferentSports) {
          const bySport = {};
          legs.forEach(l => { if (!bySport[l.ev.sport]) bySport[l.ev.sport]=l; });
          legs = Object.values(bySport);
        }

        legs = legs.slice(0, rule.maxLegs);
        if (legs.length < rule.minLegs) continue;

        const totalOdds = +legs.reduce((a,l)=>a*l.odds, 1).toFixed(3);
        const combProb  = legs.reduce((a,l)=>a*(l.sig.prob||1/l.odds), 1);
        const stake = this.calcStake(cfg, bank, totalOdds, combProb);
        if (stake < 0.01) continue;

        const allWon = legs.every(l => this.checkWin(l.m, l.sig.market));
        const pnl = allWon ? stake*(totalOdds-1)*(1-cfg.commission/100) : -stake;
        bank = Math.max(0, bank + pnl);
        equity.push(bank);

        legs.forEach(l => {
          const s = ss[l.ev.id]; if(s){ s.bets++; s.stakes+=stake/legs.length; s.pnl+=pnl/legs.length; if(allWon) s.wins++; }
        });

        trades.push({
          date, type: `parlay_${legs.length}`,
          match: legs.map(l=>`${l.m.team_home} vs ${l.m.team_away}`).join(' + '),
          sport: [...new Set(legs.map(l=>l.ev.sport))].join('+'),
          league: [...new Set(legs.map(l=>l.m.league))].join('+'),
          strategyId: legs.map(l=>l.ev.id).join(','),
          strategyName: legs.map(l=>l.ev.name).join(' × '),
          strategyColor: '#f59e0b',
          legs: legs.length,
          legsDetail: legs.map(l=>({
            match: `${l.m.team_home} vs ${l.m.team_away}`,
            market: l.sig.market, odds: l.odds,
            strategy: l.ev.name, sport: l.ev.sport, won: this.checkWin(l.m, l.sig.market),
          })),
          market: legs.map(l=>l.sig.market).join('+'),
          odds: totalOdds,
          stake: stake.toFixed(2), won: allWon?'W':'L',
          pnl: pnl.toFixed(2), bankroll: bank.toFixed(2),
        });
      }

      // Одиночные для стратегий вне парлей
      for (const { m, sig, odds, ev } of daySignals) {
        if (allParlayStratIds.has(ev.id)) continue;
        if (!ev.fn) continue;
        const stake = this.calcStake(cfg, bank, odds, sig.prob||0.5);
        if (stake < 0.01) continue;
        const won = this.checkWin(m, sig.market);
        const pnl = won ? stake*(odds-1)*(1-cfg.commission/100) : -stake;
        bank = Math.max(0, bank + pnl);
        equity.push(bank);
        const s = ss[ev.id]; if(s){ s.bets++; s.stakes+=stake; s.pnl+=pnl; if(won) s.wins++; }
        trades.push({
          date, type:'single',
          match:`${m.team_home} vs ${m.team_away}`,
          sport:m.sport, league:m.league,
          strategyId:ev.id, strategyName:ev.name, strategyColor:ev.color,
          market:sig.market, odds, legs:1,
          stake:stake.toFixed(2), won:won?'W':'L',
          pnl:pnl.toFixed(2), bankroll:bank.toFixed(2),
        });
      }
    }

    return { trades, equity, stratStats: ss, stats: this.calcStats(trades, cfg.bankroll, equity) };
  },

  calcStake(cfg, bank, odds, prob) {
    let s = bank * 0.02;
    const kelly = Math.max(0, ((odds-1)*prob-(1-prob))/(odds-1));
    if (cfg.staking==='kelly')       s = bank * kelly;
    else if (cfg.staking==='half_kelly') s = bank * kelly * 0.5;
    else if (cfg.staking==='fixed_pct') s = bank * cfg.maxStakePct/100;
    return Math.min(Math.max(s, 0.01), bank * cfg.maxStakePct/100, bank);
  },

  checkWin(m, market) {
    if (market==='home')  return m.result==='home';
    if (market==='away')  return m.result==='away';
    if (market==='draw')  return m.result==='draw';
    if (market==='over')  return m.over25;
    if (market==='under') return !m.over25;
    if (market==='btts')  return m.btts;
    return false;
  },

  // ══════════════════════════════════════════════════════════════════════════
  //  STATS
  // ══════════════════════════════════════════════════════════════════════════
  calcStats(trades, startBank, equity) {
    if (!trades.length) return { bets:0 };
    const wins      = trades.filter(t=>t.won==='W').length;
    const totalPnL  = trades.reduce((s,t)=>s+parseFloat(t.pnl),0);
    const totalStk  = trades.reduce((s,t)=>s+parseFloat(t.stake),0);
    const roi       = totalStk ? (totalPnL/totalStk)*100 : 0;
    const winRate   = wins/trades.length*100;
    let peak=startBank, maxDD=0;
    equity.forEach(v=>{ if(v>peak) peak=v; const dd=(peak-v)/Math.max(1,peak)*100; if(dd>maxDD) maxDD=dd; });
    const rets = trades.map(t=>parseFloat(t.pnl)/Math.max(0.01,parseFloat(t.stake)));
    const avgR = rets.reduce((s,r)=>s+r,0)/rets.length;
    const stdR = Math.sqrt(rets.reduce((s,r)=>s+(r-avgR)**2,0)/rets.length);
    const sharpe = stdR>0 ? (avgR/stdR)*Math.sqrt(252) : 0;
    const n=trades.length, p=winRate/100;
    const exp = trades.reduce((s,t)=>s+1/Math.max(1.01,t.odds),0)/n;
    const z = Math.sqrt(n)*(p-exp)/Math.max(0.001,Math.sqrt(exp*(1-exp)));
    const pval = Math.max(0, 1-0.5*(1+Math.sign(z)*this.erf(Math.abs(z)/Math.sqrt(2))));
    return {
      bets: n,
      singles: trades.filter(t=>t.type==='single').length,
      parlays: trades.filter(t=>t.type?.startsWith('parlay')).length,
      winRate: winRate.toFixed(1), roi: roi.toFixed(2), profit: totalPnL.toFixed(2),
      yield: roi.toFixed(2), sharpe: sharpe.toFixed(2), maxDD: maxDD.toFixed(1),
      clv: (roi*0.3).toFixed(2), pval: pval.toFixed(3),
      avgOdds: (trades.reduce((s,t)=>s+(t.odds||1),0)/n).toFixed(2),
      strike: winRate.toFixed(1), zscore: z.toFixed(2),
    };
  },

  erf(x) {
    const t=1/(1+0.3275911*Math.abs(x));
    const p=t*(0.254829592+t*(-0.284496736+t*(1.421413741+t*(-1.453152027+t*1.061405429))));
    return Math.sign(x)*(1-p*Math.exp(-x*x));
  },

  // ══════════════════════════════════════════════════════════════════════════
  //  DISPLAY
  // ══════════════════════════════════════════════════════════════════════════
  displayResults(result, strategies) {
    if (!result?.stats) return;
    const s = result.stats;
    const map = { 'bts-bets':s.bets,'bts-winrate':s.winRate,'bts-roi':s.roi,'bts-profit':s.profit,
      'bts-yield':s.yield,'bts-sharpe':s.sharpe,'bts-maxdd':s.maxDD,'bts-clv':s.clv,
      'bts-pval':s.pval,'bts-avgodds':s.avgOdds,'bts-strike':s.strike,'bts-expected':s.zscore };
    for (const [id,val] of Object.entries(map)) {
      const el = document.getElementById(id); if(!el) continue;
      el.textContent = val??'—';
      const v = parseFloat(val);
      if (['bts-roi','bts-profit','bts-yield','bts-sharpe','bts-clv'].includes(id))
        { el.classList.toggle('positive',v>0); el.classList.toggle('negative',v<=0); }
    }
    const te = document.getElementById('bts-types');
    if (te) te.textContent = `${s.singles||0} ординаров / ${s.parlays||0} экспрессов`;

    this.renderStrategyBreakdown(result.stratStats, strategies);
    this.renderTradesTable(result.trades);
  },

  renderStrategyBreakdown(stratStats, strategies) {
    const el = document.getElementById('btStratBreakdown'); if(!el) return;
    el.innerHTML = strategies.filter(s=>s.enabled).map(s => {
      const ss = stratStats[s.id]||{bets:0,wins:0,pnl:0,stakes:0};
      const roi = ss.stakes ? (ss.pnl/ss.stakes*100).toFixed(1) : '0.0';
      const wr  = ss.bets ? (ss.wins/ss.bets*100).toFixed(0) : '0';
      const cls = ss.pnl>=0?'positive':'negative';
      return `<div class="bt-strat-breakdown-row">
        <span class="bt-slot-color sm" style="background:${s.color}"></span>
        <span class="bt-strat-bname">${s.name}</span>
        <span class="bt-strat-sport-tag">${s.sport}</span>
        <span class="bt-strat-stat">${ss.bets} ставок</span>
        <span class="bt-strat-stat">${wr}% WR</span>
        <span class="bt-strat-stat">ROI ${roi}%</span>
        <span class="bt-strat-stat ${cls}">${ss.pnl>=0?'+':''}${ss.pnl.toFixed(0)}</span>
      </div>`;
    }).join('');
  },

  renderTradesTable(trades) {
    const container = document.getElementById('btTradesTable'); if(!container) return;
    if (!trades.length) { container.innerHTML='<div class="empty-state" style="padding:32px">Нет ставок</div>'; return; }
    const rows = trades.slice(-300).reverse().map(t => {
      const isParlay = t.type?.startsWith('parlay');
      const typeTag = isParlay
        ? `<span class="bt-tag parlay">${t.legs}× экспресс</span>`
        : `<span class="bt-tag single">1× ординар</span>`;
      const wonCls = t.won==='W'?'positive':'negative';
      const pnlCls = parseFloat(t.pnl)>0?'positive':'negative';
      const stratHtml = isParlay
        ? `<span style="font-size:10px">${t.strategyName}</span>`
        : `<span style="color:${t.strategyColor};font-size:11px">● ${t.strategyName}</span>`;
      return `<tr class="${isParlay?'bt-row-parlay':''}">
        <td>${t.date}</td>
        <td>${typeTag}</td>
        <td style="max-width:200px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap" title="${t.match}">${t.match}</td>
        <td><span class="bt-strat-sport-tag">${t.sport}</span></td>
        <td>${stratHtml}</td>
        <td style="font-size:11px">${t.market}</td>
        <td><strong>${(+t.odds).toFixed(2)}</strong></td>
        <td>${t.stake}</td>
        <td class="${wonCls}"><strong>${t.won}</strong></td>
        <td class="${pnlCls}">${parseFloat(t.pnl)>0?'+':''}${t.pnl}</td>
        <td>${t.bankroll}</td>
      </tr>`;
    }).join('');
    container.innerHTML = `<table class="data-table">
      <thead><tr>
        <th>Дата</th><th>Тип</th><th>Матч</th><th>Спорт</th>
        <th>Стратегия</th><th>Рынок</th><th>Коэф</th>
        <th>Ставка</th><th>Рез</th><th>PnL</th><th>Банкролл</th>
      </tr></thead>
      <tbody>${rows}</tbody>
    </table>`;
  },

  // ══════════════════════════════════════════════════════════════════════════
  //  CHARTS
  // ══════════════════════════════════════════════════════════════════════════
  renderCharts(result) {
    this.destroyCharts(); if(!result?.equity?.length) return;
    const isDark = document.body.classList.contains('dark-mode');
    const tc = isDark?'#8892a4':'#4a5568', gc = isDark?'rgba(255,255,255,0.05)':'rgba(0,0,0,0.07)';
    const base = { responsive:true, maintainAspectRatio:false,
      plugins:{legend:{display:false}},
      scales:{ x:{ticks:{color:tc,font:{size:8},maxTicksLimit:10},grid:{color:gc}},
               y:{ticks:{color:tc,font:{size:9}},grid:{color:gc}} } };

    const eq = result.equity;
    const labels = eq.map((_,i)=>i);
    const hasParlays = result.trades?.some(t=>t.type?.startsWith('parlay'));

    // Отдельные equity для ординаров и экспрессов
    let sb=eq[0], pb=eq[0];
    const seq=[eq[0]], peq=[eq[0]];
    result.trades?.forEach(t => {
      const p=parseFloat(t.pnl);
      if (t.type==='single') { sb=Math.max(0,sb+p); seq.push(sb); peq.push(pb); }
      else { pb=Math.max(0,pb+p); peq.push(pb); seq.push(sb); }
    });

    const datasets = [
      {data:eq,label:'Общий',borderColor:'#00d4ff',backgroundColor:'rgba(0,212,255,0.06)',borderWidth:2,pointRadius:0,fill:true,tension:0.3},
      ...(hasParlays?[{data:peq,label:'Экспрессы',borderColor:'#f59e0b',backgroundColor:'transparent',borderWidth:1.5,pointRadius:0,tension:0.3,borderDash:[5,4]}]:[]),
    ];
    this.charts.equity = new Chart(document.getElementById('chartBtEquity'), {
      type:'line', data:{labels,datasets},
      options:{...base, plugins:{legend:{display:hasParlays,labels:{color:tc,boxWidth:12,font:{size:11}}}}},
    });

    // Drawdown
    let peak=eq[0]; const dd=eq.map(v=>{if(v>peak) peak=v; return peak>0?-((peak-v)/peak*100):0;});
    this.charts.dd = new Chart(document.getElementById('chartBtDrawdown'),{
      type:'line',data:{labels,datasets:[{data:dd,borderColor:'#ff4560',backgroundColor:'rgba(255,69,96,0.1)',borderWidth:1.5,pointRadius:0,fill:true}]},
      options:{...base,scales:{...base.scales,y:{...base.scales.y,max:0}}},
    });

    // Monthly PnL
    const monthly={};
    result.trades?.forEach(t=>{const k=(t.date||'').substring(0,7);if(k) monthly[k]=(monthly[k]||0)+parseFloat(t.pnl);});
    const mk=Object.keys(monthly).sort();
    this.charts.monthly = new Chart(document.getElementById('chartBtMonthly'),{
      type:'bar',data:{labels:mk,datasets:[{data:mk.map(k=>monthly[k]),backgroundColor:mk.map(k=>monthly[k]>0?'rgba(0,230,118,0.7)':'rgba(255,69,96,0.7)'),borderRadius:3}]},
      options:base,
    });

    // Odds distribution
    const singles=(result.trades||[]).filter(t=>t.type==='single').map(t=>t.odds||1);
    const parlays=(result.trades||[]).filter(t=>t.type?.startsWith('parlay')).map(t=>t.odds||1);
    const bins=Array(12).fill(0); const pb2=Array(12).fill(0);
    singles.forEach(o=>{bins[Math.min(Math.floor((o-1)/0.5),11)]++;});
    parlays.forEach(o=>{pb2[Math.min(Math.floor(Math.log2(Math.max(1,o))/0.5),11)]++;});
    this.charts.distrib = new Chart(document.getElementById('chartBtDistrib'),{
      type:'bar',
      data:{labels:bins.map((_,i)=>(1+i*0.5).toFixed(1)),datasets:[
        {data:bins,label:'Ординары',backgroundColor:'rgba(0,212,255,0.6)',borderRadius:3},
        ...(hasParlays?[{data:pb2,label:'Экспрессы',backgroundColor:'rgba(245,158,11,0.6)',borderRadius:3}]:[]),
      ]},
      options:{...base,plugins:{legend:{display:hasParlays,labels:{color:tc,boxWidth:10}}}},
    });
  },

  destroyCharts() { Object.values(this.charts).forEach(c=>{try{c.destroy();}catch(e){}}); this.charts={}; },
  stop() { this.running=false; this.stopUI(); },
  stopUI() {
    this.running=false;
    const r=document.getElementById('btnRunBacktest'); if(r) r.style.display='';
    const s=document.getElementById('btnStopBacktest'); if(s) s.style.display='none';
  },
  showProgress(pct, text) {
    const w=document.getElementById('btProgressWrap'); if(w) w.style.display='';
    const b=document.getElementById('btProgressBar'); if(b) b.style.setProperty('--progress',pct+'%');
    const t=document.getElementById('btProgressText'); if(t) t.textContent=text;
  },
};
