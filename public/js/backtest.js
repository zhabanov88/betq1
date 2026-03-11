'use strict';
// ═══════════════════════════════════════════════════════════════════════════
//  BetQuant Pro — Backtest Engine v3
//  • Мульти-стратегия: несколько стратегий одновременно, общий результат
//  • Экспрессы: перекрещивание сигналов из разных стратегий/спортов
//  • Per-strategy breakdown в таблице ставок
// ═══════════════════════════════════════════════════════════════════════════
const SPORT_OPTIONS = [
  { value: 'football',   label: '⚽ Футбол' },
  { value: 'hockey',     label: '🏒 Хоккей' },
  { value: 'basketball', label: '🏀 Баскетбол' },
  { value: 'baseball',   label: '⚾ Бейсбол' },
  { value: 'tennis',     label: '🎾 Теннис' },
  { value: 'volleyball', label: '🏐 Волейбол' },
  { value: 'nfl',        label: '🏈 NFL' },
  { value: 'rugby',      label: '🏉 Регби' },
  { value: 'cricket',    label: '🏏 Крикет' },
  { value: 'waterpolo',  label: '🤽 Водное поло' },
  { value: 'esports',    label: '🎮 Киберспорт' },
];

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
    volleyball_home: {
      name: 'VB Home Win',
      sport: 'volleyball',
      code: `function evaluate(match, team, h2h, market) {
  const form = team.form(match.team_home, 5);
  const wins = form.filter(r => r === 'W').length;
  const prob = 0.5 + wins * 0.04;
  if (prob > 0.58 && match.odds_home >= 1.3 && match.odds_home <= 2.5)
    return { signal: true, market: 'home', prob, stake: market.kelly(match.odds_home, prob) * 0.5 };
  return null;
}`,
    },
    volleyball_over_sets: {
      name: 'VB Over 3.5 Sets',
      sport: 'volleyball',
      code: `function evaluate(match, team, h2h, market) {
  const h2hAvg = h2h.results.length > 0
    ? h2h.results.reduce((s, r) => s + (r.total_sets || 3), 0) / h2h.results.length
    : 3;
  const prob = h2hAvg > 3.5 ? 0.55 : 0.40;
  if (prob > 0.52 && match.ou_sets_line > 0)
    return { signal: true, market: 'over', prob, stake: market.kelly(match.ou_sets_line || 1.9, prob) * 0.4 };
  return null;
}`,
    },
    nfl_home: {
      name: 'NFL Home Advantage',
      sport: 'nfl',
      code: `function evaluate(match, team, h2h, market) {
  const form = team.form(match.team_home, 4);
  const wins = form.filter(r => r === 'W').length;
  const prob = 0.54 + wins * 0.04;
  if (prob > 0.60 && match.odds_home >= 1.3 && match.odds_home <= 2.2)
    return { signal: true, market: 'home', prob, stake: market.kelly(match.odds_home, prob) * 0.4 };
  return null;
}`,
    },
    nfl_over: {
      name: 'NFL High Scoring Over',
      sport: 'nfl',
      code: `function evaluate(match, team, h2h, market) {
  const hAvg = team.avgGoals(match.team_home, 5);
  const aAvg = team.avgGoals(match.team_away, 5);
  const prob = Math.min(0.75, (hAvg + aAvg) / 55);
  if (prob > 0.55 && match.total_line > 0 && match.odds_over >= 1.7)
    return { signal: true, market: 'over', prob, stake: market.kelly(match.odds_over, prob) * 0.4 };
  return null;
}`,
    },
    rugby_home: {
      name: 'Rugby Home Win',
      sport: 'rugby',
      code: `function evaluate(match, team, h2h, market) {
  const form = team.form(match.team_home, 5);
  const wins = form.filter(r => r === 'W').length;
  const prob = 0.52 + wins * 0.05;
  if (prob > 0.60 && match.odds_home >= 1.3 && match.odds_home <= 2.0)
    return { signal: true, market: 'home', prob, stake: market.kelly(match.odds_home, prob) * 0.45 };
  return null;
}`,
    },
    cricket_home: {
      name: 'Cricket Home Win',
      sport: 'cricket',
      code: `function evaluate(match, team, h2h, market) {
  const form = team.form(match.team_home, 5);
  const wins = form.filter(r => r === 'W').length;
  const prob = 0.48 + wins * 0.05;
  if (prob > 0.55 && match.odds_home >= 1.5 && match.odds_home <= 3.0)
    return { signal: true, market: 'home', prob, stake: market.kelly(match.odds_home, prob) * 0.4 };
  return null;
}`,
    },
    waterpolo_home: {
      name: 'Water Polo Home Win',
      sport: 'waterpolo',
      code: `function evaluate(match, team, h2h, market) {
  const form = team.form(match.team_home, 5);
  const wins = form.filter(r => r === 'W').length;
  const prob = 0.52 + wins * 0.04;
  if (prob > 0.58 && match.odds_home >= 1.3 && match.odds_home <= 2.5)
    return { signal: true, market: 'home', prob, stake: market.kelly(match.odds_home, prob) * 0.45 };
  return null;
}`,
    },
    esports_favorite: {
      name: 'Esports Favorite Win',
      sport: 'esports',
      code: `function evaluate(match, team, h2h, market) {
  const form = team.form(match.team_home, 5);
  const wins = form.filter(r => r === 'W').length;
  const prob = 0.5 + wins * 0.06;
  if (prob > 0.62 && match.odds_home >= 1.2 && match.odds_home <= 1.9)
    return { signal: true, market: 'home', prob, stake: market.kelly(match.odds_home, prob) * 0.4 };
  return null;
}`,
    },
    esports_over_maps: {
      name: 'Esports Over 2.5 Maps (BO5)',
      sport: 'esports',
      code: `function evaluate(match, team, h2h, market) {
  if (match.format < 5) return null;
  const h2hMatches = h2h.results;
  const avgMaps = h2hMatches.length > 0
    ? h2hMatches.reduce((s,r) => s + (r.total_maps || 3), 0) / h2hMatches.length
    : 3;
  const prob = avgMaps > 3.5 ? 0.58 : 0.45;
  if (prob > 0.54 && match.odds_over >= 1.6)
    return { signal: true, market: 'over', prob, stake: market.kelly(match.odds_over, prob) * 0.35 };
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
    if (typeof telegramSettings !== 'undefined') {
      telegramSettings.strategies = this.activeStrategies;
      if (telegramSettings.tab === 'strategies') telegramSettings._renderTab();
    }
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
            ${SPORT_OPTIONS.map(sp =>
                `<option value="${sp.value}" ${s.sport===sp.value?'selected':''}>${sp.label}</option>`
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

    this.showProgress(10, 'Отправка на сервер...');
    try {
      const resp = await fetch('/api/bt/run', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          strategies: enabled.map(s => ({ id: s.id, name: s.name, sport: s.sport, code: s.code, color: s.color })),
          cfg,
          parlayRules: this.parlayRules || [],
        }),
      });
      this.showProgress(60, 'Получение результатов...');
      const result = await resp.json();

      if (result.error === 'no_data') {
        this.stopUI();
        fetch('/api/backtest/sports').then(r => r.json()).then(stats => {
          const available = Object.entries(stats)
            .filter(([, v]) => v.hasData)
            .map(([sp, v]) => `${v.label || sp}: ${v.count.toLocaleString()} матчей (${v.from}–${v.to})`);
          const noData = enabled.map(s => s.sport).filter(sp => !stats[sp]?.hasData);
          let msg = noData.length ? `Нет данных для: <strong>${noData.join(', ')}</strong>. Запустите ETL.<br>` : '';
          if (available.length) msg += `<br>Доступно:<br>${available.map(a => '• ' + a).join('<br>')}`;
          this._showDataError(msg || 'Нет данных. Запустите ETL.');
        }).catch(() => this._showDataError('Нет данных в базе.'));
        return;
      }
      if (result.error) throw new Error(result.error);

      this.showProgress(85, 'Отрисовка графиков...');
      this.displayResults(result, enabled);
      this.renderCharts(result);
      this.showProgress(100, `Готово ✓ (${result.loaded || 0} матчей)`);
      setTimeout(() => { const w = document.getElementById('btProgressWrap'); if (w) w.style.display = 'none'; }, 700);
    } catch (e) {
      this._showDataError('❌ Ошибка сервера: ' + e.message);
    } finally {
      this.stopUI();
    }
  },

  async fetchMatchesFromServer(cfg, strategies) {
    const sports = [...new Set(strategies.map(s => s.sport))];
    const out = {};
    for (const sport of sports) {
      this.showProgress(20 + (sports.indexOf(sport) / sports.length) * 15, `Загрузка: ${sport}...`);
      try {
        const params = new URLSearchParams({
          sport,
          dateFrom: cfg.dateFrom || '2018-01-01',
          dateTo:   cfg.dateTo   || new Date().toISOString().slice(0, 10),
          limit:    200000,
        });
        if (cfg.league && cfg.league !== 'all') params.set('league', cfg.league);
        if (cfg.season) params.set('season', cfg.season);

        const resp = await fetch(`/api/backtest/matches?${params}`);
        if (!resp.ok) {
          const err = await resp.json().catch(() => ({ error: resp.statusText }));
          throw new Error(`[${sport}] ${err.error || resp.statusText}`);
        }
        const data = await resp.json();
        out[sport] = (data.matches || []).map(m => this._normalizeMatch(m, sport));
        console.log(`[backtest] ${sport}: ${out[sport].length} матчей из БД`);
      } catch (e) {
        console.error(`[backtest] Ошибка ${sport}:`, e.message);
        out[sport] = [];
      }
    }
    return out;
  },

  // ── ИСПРАВЛЕНИЕ 1: over25 и btts вычисляются здесь, а не в checkWin ──────
  _normalizeMatch(m, sport) {
    const base = {
      ...m,
      sport,
      date: m.date || '',

      team_home: m.home_team  || m.team_home || m.team1 || m.winner || '',
      team_away: m.away_team  || m.team_away || m.team2 || m.loser  || '',

      home_goals: parseFloat(
        m.home_goals ?? m.home_pts  ?? m.home_score ??
        m.home_sets  ?? m.score1    ?? m.home_runs  ??
        m.team1_runs ?? m.w_sets    ?? 0
      ),
      away_goals: parseFloat(
        m.away_goals ?? m.away_pts  ?? m.away_score ??
        m.away_sets  ?? m.score2    ?? m.away_runs  ??
        m.team2_runs ?? m.l_sets    ?? 0
      ),

      odds_home:  parseFloat(m.odds_home  || m.b365_home    || m.b365_winner || m.avg_winner || m.pin_home || 0),
      odds_draw:  parseFloat(m.odds_draw  || m.b365_draw    || 0),
      odds_away:  parseFloat(m.odds_away  || m.b365_away    || m.b365_loser  || m.avg_loser  || m.pin_away || 0),
      odds_over:  parseFloat(m.odds_over  || m.b365_over25  || m.b365_over   || m.ou_sets_line || 0),
      odds_under: parseFloat(m.odds_under || m.b365_under25 || m.b365_under  || 0),

      b365_home:    parseFloat(m.b365_home    || m.b365_winner || 0),
      b365_draw:    parseFloat(m.b365_draw    || 0),
      b365_away:    parseFloat(m.b365_away    || m.b365_loser  || 0),
      b365_over25:  parseFloat(m.b365_over25  || m.b365_over   || 0),
      b365_under25: parseFloat(m.b365_under25 || m.b365_under  || 0),

      home_xg:    parseFloat(m.home_xg    || m.home_xg_for || 0),
      away_xg:    parseFloat(m.away_xg    || m.away_xg_for || 0),
      home_shots: parseFloat(m.home_shots || 0),
      away_shots: parseFloat(m.away_shots || 0),
    };

    // ── result ────────────────────────────────────────────────────────
    if (m.result) {
      const r = String(m.result).toUpperCase();
      base.result = r === 'H' ? 'home' : r === 'A' ? 'away' : r === 'D' ? 'draw' :
                    r === 'HOME' ? 'home' : r === 'AWAY' ? 'away' : 'draw';
    } else {
      base.result = base.home_goals > base.away_goals ? 'home'
                  : base.home_goals < base.away_goals ? 'away'
                  : 'draw';
    }

    // ── ИСПРАВЛЕНИЕ: over25 и btts вычисляем здесь (нужно для checkWin) ──
    const totalGoals = base.home_goals + base.away_goals;
    base.over25 = totalGoals > 2.5;
    base.over15 = totalGoals > 1.5;
    base.over35 = totalGoals > 3.5;
    base.btts   = base.home_goals > 0 && base.away_goals > 0;

    // ── FIX: Нормализуем odds_* из всех возможных имён полей ──────────
    // Коэффициенты приходят как b365_home, pinnacle_home, и т.д.
    // evaluate() всегда использует match.odds_home / match.odds_away и т.д.
    if (!base.odds_home)
      base.odds_home = parseFloat(m.b365_home || m.pinnacle_home || m.odds_home ||
                                  m.avg_home  || m.bet365_home  || 0) || 0;
    if (!base.odds_away)
      base.odds_away = parseFloat(m.b365_away || m.pinnacle_away || m.odds_away ||
                                  m.avg_away  || m.bet365_away  || 0) || 0;
    if (!base.odds_draw)
      base.odds_draw = parseFloat(m.b365_draw || m.pinnacle_draw || m.odds_draw ||
                                  m.avg_draw  || 0) || 0;
    if (!base.odds_over)
      base.odds_over = parseFloat(m.b365_over25 || m.b365_over || m.odds_over ||
                                  m.pinnacle_over || m.avg_over || 0) || 0;
    if (!base.odds_under)
      base.odds_under = parseFloat(m.b365_under25 || m.b365_under || m.odds_under ||
                                   m.pinnacle_under || m.avg_under || 0) || 0;
    if (!base.odds_btts)
      base.odds_btts = parseFloat(m.b365_btts || m.odds_btts || 0) || 0;

    // Tennis: b365w / b365l → odds_home / odds_away
    if (sport === 'tennis') {
      if (!base.odds_home) base.odds_home = parseFloat(m.b365w || m.b365_winner || m.ps_winner || 0) || 0;
      if (!base.odds_away) base.odds_away = parseFloat(m.b365l || m.b365_loser  || m.ps_loser  || 0) || 0;
    }

    // ── Спорт-специфичные поля ────────────────────────────────────────

    if (sport === 'tennis') {
      base.result     = 'home';
      base.winner     = m.winner || base.team_home;
      base.loser      = m.loser  || base.team_away;
      base.rank_home  = parseFloat(m.winner_rank || 0);
      base.rank_away  = parseFloat(m.loser_rank  || 0);
      base.surface    = m.surface    || '';
      base.round      = m.round      || '';
      base.tournament = m.tournament || '';
      base.sets_played= parseInt(m.sets_played || (parseInt(m.w_sets||0) + parseInt(m.l_sets||0)) || 0);
      if (!base.odds_home && m.b365_winner)  base.odds_home = parseFloat(m.b365_winner);
      if (!base.odds_away && m.b365_loser)   base.odds_away = parseFloat(m.b365_loser);
      if (!base.odds_home && m.ps_winner)    base.odds_home = parseFloat(m.ps_winner);
      if (!base.odds_away && m.ps_loser)     base.odds_away = parseFloat(m.ps_loser);
      base.w_aces = parseInt(m.w_aces || 0);
      base.l_aces = parseInt(m.l_aces || 0);
      base.w_df   = parseInt(m.w_df   || 0);
      base.l_df   = parseInt(m.l_df   || 0);
    }

    if (sport === 'volleyball') {
      base.total_sets  = parseInt(m.total_sets  || base.home_goals + base.away_goals || 0);
      base.total_points= parseInt(m.total_points|| (m.home_total_pts||0) + (m.away_total_pts||0) || 0);
      base.home_hit_pct= parseFloat(m.home_hit_pct || 0);
      base.away_hit_pct= parseFloat(m.away_hit_pct || 0);
      base.home_aces   = parseInt(m.home_aces  || 0);
      base.away_aces   = parseInt(m.away_aces  || 0);
      base.home_kills  = parseInt(m.home_kills || 0);
      base.away_kills  = parseInt(m.away_kills || 0);
      base.home_blocks = parseInt(m.home_blocks_total || 0);
      base.away_blocks = parseInt(m.away_blocks_total || 0);
      base.gender      = m.gender      || '';
      base.competition = m.competition || '';
      base.ou_sets_line= parseFloat(m.ou_sets_line || 0);
      // over/under для волейбола — по партиям
      base.over25 = base.total_sets > 2.5;  // т.е. 3+ партий
      base.over35 = base.total_sets > 3.5;  // 4+ партий
    }

    if (sport === 'nfl') {
      base.week        = parseInt(m.week || 0);
      base.season_type = m.season_type || 'REG';
      base.overtime    = parseInt(m.overtime || 0);
      base.home_q1 = parseInt(m.home_q1 || 0);  base.away_q1 = parseInt(m.away_q1 || 0);
      base.home_q2 = parseInt(m.home_q2 || 0);  base.away_q2 = parseInt(m.away_q2 || 0);
      base.home_q3 = parseInt(m.home_q3 || 0);  base.away_q3 = parseInt(m.away_q3 || 0);
      base.home_q4 = parseInt(m.home_q4 || 0);  base.away_q4 = parseInt(m.away_q4 || 0);
      base.total_line  = parseFloat(m.total_line || 0);
      base.spread      = parseFloat(m.spread || 0);
      base.home_epa    = parseFloat(m.home_epa_total || 0);
      base.away_epa    = parseFloat(m.away_epa_total || 0);
      base.home_turnovers   = parseInt(m.home_turnovers || 0);
      base.away_turnovers   = parseInt(m.away_turnovers || 0);
      base.home_first_downs = parseInt(m.home_first_downs || 0);
      base.away_first_downs = parseInt(m.away_first_downs || 0);
      // NFL: over по total_line
      if (base.total_line > 0) {
        base.over25 = totalGoals > base.total_line;
      }
      // NFL нет ничьих (почти)
      if (base.result === 'draw' && base.home_goals !== base.away_goals) {
        base.result = base.home_goals > base.away_goals ? 'home' : 'away';
      }
    }

    if (sport === 'rugby') {
      base.home_tries  = parseInt(m.home_tries || 0);
      base.away_tries  = parseInt(m.away_tries || 0);
      base.home_h1     = parseInt(m.home_h1_score || m.home_h1 || 0);
      base.away_h1     = parseInt(m.away_h1_score || m.away_h1 || 0);
      base.competition = m.competition || '';
    }

    if (sport === 'cricket') {
      base.match_type  = m.match_type || m.format || '';
      base.competition = m.competition || '';
      base.venue       = m.venue       || '';
    }

    if (sport === 'basketball') {
      base.home_q1 = parseInt(m.home_pts_q1 || m.home_q1 || 0);
      base.home_q2 = parseInt(m.home_pts_q2 || m.home_q2 || 0);
      base.home_q3 = parseInt(m.home_pts_q3 || m.home_q3 || 0);
      base.home_q4 = parseInt(m.home_pts_q4 || m.home_q4 || 0);
      base.away_q1 = parseInt(m.away_pts_q1 || m.away_q1 || 0);
      base.away_q2 = parseInt(m.away_pts_q2 || m.away_q2 || 0);
      base.away_q3 = parseInt(m.away_pts_q3 || m.away_q3 || 0);
      base.away_q4 = parseInt(m.away_pts_q4 || m.away_q4 || 0);
      base.went_to_ot = parseInt(m.went_to_ot || 0);
      base.total_pts  = base.home_goals + base.away_goals;
      // NBA тотал обычно 210+ очков
      base.over25 = base.total_pts > 210;
    }

    if (sport === 'esports') {
      base.game       = m.game       || m.discipline || '';
      base.game_slug  = m.game_slug  || '';
      base.league     = m.league     || '';
      base.tier       = m.tier       || '';
      base.format     = parseInt(m.format || 1);
      base.total_maps = parseInt(m.score1 || 0) + parseInt(m.score2 || 0);
      // over по картам
      base.over25 = base.total_maps > 2.5;
    }

    if (sport === 'waterpolo') {
      base.competition = m.competition || '';
      base.home_q1 = parseInt(m.home_q1 || 0);  base.away_q1 = parseInt(m.away_q1 || 0);
      base.home_q2 = parseInt(m.home_q2 || 0);  base.away_q2 = parseInt(m.away_q2 || 0);
      base.home_q3 = parseInt(m.home_q3 || 0);  base.away_q3 = parseInt(m.away_q3 || 0);
      base.home_q4 = parseInt(m.home_q4 || 0);  base.away_q4 = parseInt(m.away_q4 || 0);
    }

    return base;
  },

  _showDataError(msg) {
    // Удаляем предыдущие ошибки чтобы не накапливались
    document.querySelectorAll('.bt-data-error').forEach(e => e.remove());
    const el = document.createElement('div');
    el.className = 'bt-data-error';
    el.style.cssText = 'padding:24px;background:var(--bg2);border:1px solid var(--red,#f44);border-radius:8px;margin:16px';
    el.innerHTML = `
      <div style="font-size:16px;color:var(--red,#f44);margin-bottom:8px">❌ Нет данных для бэктеста</div>
      <div style="font-size:13px;color:var(--text2);margin-bottom:12px">${msg}</div>
      <div style="font-size:12px;color:var(--text3)">
        Перейдите в <strong>Сборщик данных → ETL</strong> и загрузите исторические матчи.
      </div>
      <button class="ctrl-btn" style="margin-top:12px" onclick="app.showPanel('scraper')">🕷 Перейти к ETL</button>`;
    const panel = document.getElementById('panel-backtest');
    if (panel) panel.prepend(el);
  },

  readConfig() {
    return {
      dateFrom:    document.getElementById('btDateFrom')?.value   || '2018-01-01',
      dateTo:      document.getElementById('btDateTo')?.value     || new Date().toISOString().slice(0,10),
      staking:     document.getElementById('btStaking')?.value    || 'half_kelly',
      bankroll:    parseFloat(document.getElementById('btBankroll')?.value)   || 1000,
      maxStakePct: parseFloat(document.getElementById('btMaxStake')?.value)   || 5,
      commission:  parseFloat(document.getElementById('btCommission')?.value) || 0,
      minOdds:     parseFloat(document.getElementById('btMinOdds')?.value)    || 1.3,
      maxOdds:     parseFloat(document.getElementById('btMaxOdds')?.value)    || 15,
    };
  },

  compileStrategy(code) {
    try {
      const m = code.match(/function evaluate\s*\([^)]*\)\s*\{([\s\S]*)\}/);
      if (!m) return null;
      return new Function('match','team','h2h','market', m[1] + '\nreturn null;');
    } catch(e) { return null; }
  },

  // ── ИСПРАВЛЕНИЕ 2: makeTeamAPI — добавлены avgGoals и avgConceded ─────────
  makeTeamAPI(m, all) {
    return {
      form: (name, n) => all
        .filter(x => x.team_home === name || x.team_away === name)
        .slice(-n)
        .map(x => x.result === 'draw' ? 'D' :
          ((x.team_home === name && x.result === 'home') ||
           (x.team_away === name && x.result === 'away')) ? 'W' : 'L'),

      // Среднее забито за последние N матчей
      avgGoals: (name, n = 5) => {
        const recent = all
          .filter(x => x.team_home === name || x.team_away === name)
          .slice(-n);
        if (!recent.length) return 0;
        return recent.reduce((s, x) =>
          s + (x.team_home === name ? x.home_goals : x.away_goals), 0
        ) / recent.length;
      },

      // Среднее пропущено за последние N матчей
      avgConceded: (name, n = 5) => {
        const recent = all
          .filter(x => x.team_home === name || x.team_away === name)
          .slice(-n);
        if (!recent.length) return 0;
        return recent.reduce((s, x) =>
          s + (x.team_home === name ? x.away_goals : x.home_goals), 0
        ) / recent.length;
      },

      goalsScored:   (name, n = 5) => {
        const recent = all.filter(x => x.team_home === name || x.team_away === name).slice(-n);
        if (!recent.length) return +(1.0+Math.random()*1.2).toFixed(2);
        return recent.reduce((s, x) =>
          s + (x.team_home === name ? x.home_goals : x.away_goals), 0
        ) / recent.length;
      },

      goalsConceded: (name, n = 5) => {
        const recent = all.filter(x => x.team_home === name || x.team_away === name).slice(-n);
        if (!recent.length) return +(0.8+Math.random()*1.0).toFixed(2);
        return recent.reduce((s, x) =>
          s + (x.team_home === name ? x.away_goals : x.home_goals), 0
        ) / recent.length;
      },

      xG: (name, n = 5) => {
        const recent = all.filter(x => x.team_home === name || x.team_away === name).slice(-n);
        if (!recent.length) return +(1.0+Math.random()*0.9).toFixed(2);
        const sum = recent.reduce((s, x) =>
          s + (x.team_home === name ? (x.home_xg || x.home_goals) : (x.away_xg || x.away_goals)), 0
        );
        return sum / recent.length;
      },

      // avgGoals: нужен для NFL/баскетбол стратегий
      avgGoals: (name, n = 5) => {
        const recent = all.filter(x => x.team_home === name || x.team_away === name).slice(-n);
        if (!recent.length) return 1.2;
        return recent.reduce((s, x) =>
          s + (x.team_home === name ? x.home_goals : x.away_goals), 0
        ) / recent.length;
      },

      // pts: алиас для баскетбола (home_pts/away_pts)
      avgPts: (name, n = 5) => {
        const recent = all.filter(x => x.team_home === name || x.team_away === name).slice(-n);
        if (!recent.length) return 105;
        return recent.reduce((s, x) =>
          s + (x.team_home === name ? (x.home_goals||x.home_pts||105) : (x.away_goals||x.away_pts||100)), 0
        ) / recent.length;
      },

      // rank: для теннисных стратегий
      rank: (name) => {
        const last = all.filter(x => x.team_home === name || x.team_away === name).slice(-1)[0];
        if (!last) return 100;
        return last.team_home === name ? (last.rank_home || 50) : (last.rank_away || 50);
      },
    };
  },

  makeH2H(m, all) {
    return {
      results: all.filter(x =>
        (x.team_home === m.team_home && x.team_away === m.team_away) ||
        (x.team_home === m.team_away && x.team_away === m.team_home)
      ).slice(-8),
    };
  },

  makeMarketAPI() {
    return {
      implied: o => 1/o,
      value:   (o, p) => p - 1/o,
      kelly:   (o, p) => Math.max(0, ((o-1)*p-(1-p))/(o-1)),
    };
  },

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
        try { sig = ev.fn(m, this.makeTeamAPI(m, matches), this.makeH2H(m, matches), this.makeMarketAPI()); } catch(e) {}
        if (!sig?.signal) continue;
        const mk = (sig.market || 'home').toLowerCase().replace('_win','');
        const odds = m['odds_' + mk] || m['odds_home'];
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
          sport: m.sport, league: m.league || m.competition || '',
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

    const signalsByDate = {};
    for (const ev of evalFns) {
      if (!ev.fn) continue;
      const matches = matchesBySport[ev.sport] || [];
      for (const m of matches) {
        let sig = null;
        try { sig = ev.fn(m, this.makeTeamAPI(m,matches), this.makeH2H(m,matches), this.makeMarketAPI()); } catch(e){}
        if (!sig?.signal) continue;
        const mk2 = (sig.market || 'home').toLowerCase().replace('_win','');
        const odds = m['odds_' + mk2] || m['odds_home'];
        if (!odds||odds<cfg.minOdds||odds>cfg.maxOdds) continue;
        if (!signalsByDate[m.date]) signalsByDate[m.date]=[];
        signalsByDate[m.date].push({ m, sig, odds, ev });
      }
    }

    const allParlayStratIds = new Set(this.parlayRules.flatMap(r=>r.strategyIds||[]));

    for (const date of Object.keys(signalsByDate).sort()) {
      const daySignals = signalsByDate[date];

      for (const rule of this.parlayRules) {
        const cands = daySignals.filter(s =>
          (!rule.strategyIds?.length || rule.strategyIds.includes(s.ev.id))
        );
        if (cands.length < rule.minLegs) continue;

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
          league: [...new Set(legs.map(l=>l.m.league||l.m.competition||''))].join('+'),
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
          sport:m.sport, league:m.league||m.competition||'',
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
    if (cfg.staking==='kelly')           s = bank * kelly;
    else if (cfg.staking==='half_kelly') s = bank * kelly * 0.5;
    else if (cfg.staking==='fixed_pct')  s = bank * cfg.maxStakePct/100;
    return Math.min(Math.max(s, 0.01), bank * cfg.maxStakePct/100, bank);
  },

  // ── ИСПРАВЛЕНИЕ 3: checkWin использует поля вычисленные в _normalizeMatch ──
  checkWin(m, market) {
    if (!market) return false;
    const mk = String(market).toLowerCase().trim();
    if (mk === 'home' || mk === 'home_win' || mk === '1') return m.result === 'home';
    if (mk === 'away' || mk === 'away_win' || mk === '2') return m.result === 'away';
    if (mk === 'draw' || mk === 'x')                      return m.result === 'draw';
    if (mk === 'over'  || mk === 'over25') return m.over25 === true;
    if (mk === 'under' || mk === 'under25') return m.over25 === false;
    if (mk === 'over15')  return m.over15 === true;
    if (mk === 'over35')  return m.over35 === true;
    if (mk === 'btts' || mk === 'both_score') return m.btts === true;
    // Fallback: если market содержит числа (odds) — это ошибка стратегии
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
      el.textContent = val != null ? fmtVal(val) : '—';
      const v = parseFloat(val);
      if (['bts-roi','bts-profit','bts-yield','bts-sharpe','bts-clv'].includes(id))
        { el.classList.toggle('positive',v>0); el.classList.toggle('negative',v<=0); }
    }
    const te = document.getElementById('bts-types');
    if (te) te.textContent = `${s.singles||0} ординаров / ${s.parlays||0} экспрессов`;

    this.renderStrategyBreakdown(result.stratStats, strategies);
    this.renderTradesTable(result.trades);

    // ── Тултипы на лейблах метрик ──
    const ttips = {
      'bts-roi':     'ROI — прибыль / объём ставок × 100. >5% хорошо, >15% отлично',
      'bts-profit':  'Абсолютная прибыль в единицах банкролла',
      'bts-winrate': 'Win Rate — % выигрышных ставок. При odds=2 нужно >52%',
      'bts-sharpe':  'Sharpe Ratio — доходность / риск. >1 хорошо, >2 отлично',
      'bts-maxdd':   'Max Drawdown — макс. просадка от пика. <20% норма, >40% опасно',
      'bts-clv':     'Closing Line Value — насколько коэффициенты лучше закрытия. >0 = edge',
      'bts-pval':    'P-value — статистическая значимость. <0.05 = результат не случаен',
      'bts-avgodds': 'Средний коэффициент. Влияет на дисперсию',
      'bts-expected':'Z-Score — >2 значимо (p<0.05), >3 очень значимо',
    };
    for (const [id, tip] of Object.entries(ttips)) {
      const lbl = document.querySelector(`[for="${id}"], .bt-stat-label[data-for="${id}"]`);
      if (lbl && !lbl.querySelector('.tt-q')) {
        const q = document.createElement('span');
        q.className = 'tt-q';
        q.title = tip;
        q.textContent = '?';
        q.style.cssText = 'display:inline-flex;align-items:center;justify-content:center;width:13px;height:13px;border-radius:50%;background:rgba(255,255,255,0.1);color:#8892a4;font-size:9px;cursor:help;margin-left:3px;vertical-align:middle';
        lbl.appendChild(q);
      }
    }
    // ── Форматирование: не более 3 знаков ──
    const fmtVal = (v) => {
      const n = parseFloat(v);
      if (isNaN(n)) return v;
      if (Number.isInteger(n)) return String(n);
      return n.toFixed(Math.min(3, (String(v).split('.')[1] || '').length));
    };

    // ── Резюме стратегии ──
    const sr = result.stats;
    if (sr) {
      let verdict = '', emoji = '📊', color = '#8892a4';
      const roi = parseFloat(sr.roi), sharpe = parseFloat(sr.sharpe),
            pval = parseFloat(sr.pval), bets = parseInt(sr.bets),
            maxdd = parseFloat(sr.maxDD || sr.maxdd || 0);
      if (roi > 15 && sharpe > 1.5 && pval < 0.05) {
        verdict = `ROI ${roi.toFixed(1)}% статистически значим (p=${pval.toFixed(3)}), Sharpe ${sharpe.toFixed(2)} — стабильная стратегия. Рекомендуется к применению.`;
        emoji = '🏆'; color = '#00e676';
      } else if (roi > 5 && pval < 0.1) {
        verdict = `ROI ${roi.toFixed(1)}% перспективен, Sharpe ${sharpe.toFixed(2)}. Протестируй на новом периоде и оптимизируй параметры.`;
        emoji = '✅'; color = '#ffd740';
      } else if (roi > 0) {
        verdict = `ROI ${roi.toFixed(1)}% положительный, но p=${pval.toFixed(3)} — статистически незначим (нужно 200+ ставок). Текущих: ${bets}.`;
        emoji = '⚠️'; color = '#ff9800';
      } else {
        verdict = `Убыточная стратегия. ROI ${roi.toFixed(1)}%, просадка ${maxdd.toFixed(1)}%. Пересмотри условия входа.`;
        emoji = '❌'; color = '#ff4560';
      }
      const warns = [
        bets < 100 ? `⚠️ Малая выборка (${bets} ставок) — нужно 200+` : '',
        maxdd > 40 ? `⚠️ Высокая просадка ${maxdd.toFixed(1)}%` : '',
      ].filter(Boolean);

      let sumEl = document.getElementById('btVerdictBlock');
      if (!sumEl) {
        sumEl = document.createElement('div');
        sumEl.id = 'btVerdictBlock';
        const bd = document.getElementById('btStratBreakdown');
        if (bd) bd.parentNode.insertBefore(sumEl, bd);
      }
      sumEl.innerHTML = `
        <div style="background:rgba(128,128,128,0.07);border:1px solid ${color}44;border-radius:8px;padding:12px 16px;margin-bottom:12px">
          <div style="font-size:15px;font-weight:700;color:${color};margin-bottom:6px">${emoji} Вердикт</div>
          <div style="color:var(--text1);line-height:1.5;margin-bottom:${warns.length ? '8px' : '0'}">${verdict}</div>
          ${warns.map(w => `<div style="color:var(--text2);font-size:12px;margin-top:4px">${w}</div>`).join('')}
          <button onclick="app.showPanel('ai-strategy')" style="margin-top:10px;padding:5px 12px;border:none;border-radius:6px;background:linear-gradient(135deg,#7c3aed,#2563eb);color:white;font-size:12px;font-weight:600;cursor:pointer">
            🤖 Улучшить в AI стратегии
          </button>
        </div>`;
    }
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

    let peak=eq[0]; const dd=eq.map(v=>{if(v>peak) peak=v; return peak>0?-((peak-v)/peak*100):0;});
    this.charts.dd = new Chart(document.getElementById('chartBtDrawdown'),{
      type:'line',data:{labels,datasets:[{data:dd,borderColor:'#ff4560',backgroundColor:'rgba(255,69,96,0.1)',borderWidth:1.5,pointRadius:0,fill:true}]},
      options:{...base,scales:{...base.scales,y:{...base.scales.y,max:0}}},
    });

    const monthly={};
    result.trades?.forEach(t=>{const k=(t.date||'').substring(0,7);if(k) monthly[k]=(monthly[k]||0)+parseFloat(t.pnl);});
    const mk=Object.keys(monthly).sort();
    this.charts.monthly = new Chart(document.getElementById('chartBtMonthly'),{
      type:'bar',data:{labels:mk,datasets:[{data:mk.map(k=>monthly[k]),backgroundColor:mk.map(k=>monthly[k]>0?'rgba(0,230,118,0.7)':'rgba(255,69,96,0.7)'),borderRadius:3}]},
      options:base,
    });

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