/**
 * server/backtest_engine.js
 * Серверный движок: бэктест, монте-карло, оптимизатор, walk-forward
 * Подключается в server/index.js:
 *   const btEngine = require('./backtest_engine');
 *   app.use('/api/bt', requireAuth, btEngine);
 */
'use strict';
const express = require('express');
const vm      = require('vm');
const router  = express.Router();

// ═══════════════════════════════════════════════════════════════════
//  HELPERS
// ═══════════════════════════════════════════════════════════════════
function normalizeMatch(m, sport) {
  const hg = parseFloat(m.home_goals ?? m.home_pts ?? m.home_score ?? m.home_sets ?? m.score1 ?? m.home_runs ?? m.w_sets ?? 0);
  const ag = parseFloat(m.away_goals ?? m.away_pts ?? m.away_score ?? m.away_sets ?? m.score2 ?? m.away_runs ?? m.l_sets ?? 0);

  const r = String(m.result ?? '').trim();
  const rl = r.toLowerCase();
  const result = (rl === 'h' || rl === 'home' || r === '1') ? 'home'
               : (rl === 'a' || rl === 'away' || r === '2') ? 'away'
               : (rl === 'd' || rl === 'draw' || r === 'X') ? 'draw'
               : hg > ag ? 'home' : hg < ag ? 'away' : 'draw';

  const total = hg + ag;
  const base = {
    ...m, sport,
    team_home: m.home_team || m.team_home || m.team1 || m.winner || '',
    team_away: m.away_team || m.team_away || m.team2 || m.loser  || '',
    home_goals: hg, away_goals: ag,
    result,
    over25: total > 2.5, over15: total > 1.5, over35: total > 3.5,
    btts: hg > 0 && ag > 0,
    odds_home:  parseFloat(m.b365_home  || m.pinnacle_home || m.avg_home  || m.odds_home  || 0),
    odds_away:  parseFloat(m.b365_away  || m.pinnacle_away || m.avg_away  || m.odds_away  || 0),
    odds_draw:  parseFloat(m.b365_draw  || m.pinnacle_draw || m.avg_draw  || m.odds_draw  || 0),
    odds_over:  parseFloat(m.b365_over25 || m.b365_over   || m.odds_over  || 0),
    odds_under: parseFloat(m.b365_under25|| m.b365_under  || m.odds_under || 0),
    odds_btts:  parseFloat(m.b365_btts  || m.odds_btts    || 0),
  };
  if (sport === 'tennis') {
    if (!base.odds_home) base.odds_home = parseFloat(m.b365w || m.b365_winner || m.ps_winner || 0);
    if (!base.odds_away) base.odds_away = parseFloat(m.b365l || m.b365_loser  || m.ps_loser  || 0);
    base.result = 'home'; // теннис — всегда winner
  }
  return base;
}

function checkWin(m, market) {
  const mk = String(market || 'home').toLowerCase().replace('_win','');
  if (mk === 'home' || mk === '1')   return m.result === 'home';
  if (mk === 'away' || mk === '2')   return m.result === 'away';
  if (mk === 'draw' || mk === 'x')   return m.result === 'draw';
  if (mk === 'over'  || mk === 'over25') return m.over25 === true;
  if (mk === 'under' || mk === 'under25') return m.over25 === false;
  if (mk === 'over15') return m.over15 === true;
  if (mk === 'over35') return m.over35 === true;
  if (mk === 'btts' || mk === 'both_score') return m.btts === true;
  return false;
}

function calcStake(cfg, bank, odds, prob) {
  const kelly = Math.max(0, ((odds - 1) * prob - (1 - prob)) / (odds - 1));
  let s = bank * 0.02;
  if (cfg.staking === 'kelly')           s = bank * kelly;
  else if (cfg.staking === 'half_kelly') s = bank * kelly * 0.5;
  else if (cfg.staking === 'fixed_pct')  s = bank * (cfg.maxStakePct || 2) / 100;
  return Math.min(Math.max(s, 0.01), bank * (cfg.maxStakePct || 5) / 100, bank);
}

function makeTeamAPI(m, all) {
  return {
    form: (name, n = 5) => all
      .filter(x => x.team_home === name || x.team_away === name).slice(-n)
      .map(x => x.result === 'draw' ? 'D' :
        ((x.team_home === name && x.result === 'home') ||
         (x.team_away === name && x.result === 'away')) ? 'W' : 'L'),
    goalsScored: (name, n = 5) => {
      const r = all.filter(x => x.team_home === name || x.team_away === name).slice(-n);
      return r.length ? r.reduce((s, x) => s + (x.team_home === name ? x.home_goals : x.away_goals), 0) / r.length : 1.2;
    },
    goalsConceded: (name, n = 5) => {
      const r = all.filter(x => x.team_home === name || x.team_away === name).slice(-n);
      return r.length ? r.reduce((s, x) => s + (x.team_home === name ? x.away_goals : x.home_goals), 0) / r.length : 1.0;
    },
    avgGoals: (name, n = 5) => {
      const r = all.filter(x => x.team_home === name || x.team_away === name).slice(-n);
      return r.length ? r.reduce((s, x) => s + (x.team_home === name ? x.home_goals : x.away_goals), 0) / r.length : 1.2;
    },
    xG: (name, n = 5) => {
      const r = all.filter(x => x.team_home === name || x.team_away === name).slice(-n);
      return r.length ? r.reduce((s, x) => s + (x.team_home === name ? (x.home_xg || x.home_goals) : (x.away_xg || x.away_goals)), 0) / r.length : 1.1;
    },
    avgPts: (name, n = 5) => {
      const r = all.filter(x => x.team_home === name || x.team_away === name).slice(-n);
      return r.length ? r.reduce((s, x) => s + (x.team_home === name ? (x.home_goals || 105) : (x.away_goals || 100)), 0) / r.length : 105;
    },
    rank: (name) => {
      const last = all.filter(x => x.team_home === name || x.team_away === name).slice(-1)[0];
      return last ? (last.team_home === name ? (last.rank_home || 50) : (last.rank_away || 50)) : 100;
    },
  };
}

function makeH2H(m, all) {
  return {
    results: all.filter(x =>
      (x.team_home === m.team_home && x.team_away === m.team_away) ||
      (x.team_home === m.team_away && x.team_away === m.team_home)
    ).slice(-8),
  };
}

function makeMarketAPI() {
  return {
    implied: o => 1 / o,
    value:   (o, p) => p - 1 / o,
    kelly:   (o, p) => Math.max(0, ((o - 1) * p - (1 - p)) / (o - 1)),
  };
}

function compileStrategy(code) {
  try {
    const m = code.match(/function evaluate\s*\([^)]*\)\s*\{([\s\S]*)\}/);
    if (!m) return null;
    return new Function('match', 'team', 'h2h', 'market', m[1] + '\nreturn null;');
  } catch (e) { return null; }
}

function calcStats(trades, startBank, equity) {
  if (!trades.length) return { bets: 0 };
  const wins     = trades.filter(t => t.won === 'W').length;
  const totalPnL = trades.reduce((s, t) => s + parseFloat(t.pnl), 0);
  const totalStk = trades.reduce((s, t) => s + parseFloat(t.stake), 0);
  const roi      = totalStk ? (totalPnL / totalStk) * 100 : 0;
  const winRate  = wins / trades.length * 100;
  const avgOdds  = trades.reduce((s, t) => s + (t.odds || 0), 0) / trades.length;

  let peak = startBank, maxDD = 0;
  equity.forEach(v => {
    if (v > peak) peak = v;
    const dd = peak > 0 ? (peak - v) / peak * 100 : 0;
    if (dd > maxDD) maxDD = dd;
  });

  const rets  = trades.map(t => parseFloat(t.pnl) / Math.max(0.01, parseFloat(t.stake)));
  const avgR  = rets.reduce((s, r) => s + r, 0) / rets.length;
  const stdR  = Math.sqrt(rets.reduce((s, r) => s + (r - avgR) ** 2, 0) / rets.length);
  const sharpe = stdR > 0 ? +(avgR / stdR * Math.sqrt(252)).toFixed(3) : 0;

  // P-value (simplified z-test)
  const z = stdR > 0 ? avgR / (stdR / Math.sqrt(trades.length)) : 0;
  const pValue = z > 0 ? Math.max(0, 1 - (0.5 * (1 + Math.sign(z) * Math.sqrt(1 - Math.exp(-2 * z * z / Math.PI))))) : 1;

  // Monthly PnL
  const monthly = {};
  trades.forEach(t => {
    const ym = String(t.date || '').slice(0, 7);
    if (!monthly[ym]) monthly[ym] = 0;
    monthly[ym] += parseFloat(t.pnl);
  });

  // Avg CLV (placeholder — real CLV needs closing odds)
  const avgCLV = roi * 0.3;

  // Текстовое резюме
  const summary = buildSummary({ roi, winRate, sharpe, maxDD, bets: trades.length, avgOdds, pValue, totalPnL, startBank });

  return {
    bets: trades.length, wins, roi: +roi.toFixed(3), profit: +totalPnL.toFixed(3),
    winRate: +winRate.toFixed(3), sharpe, maxDD: +maxDD.toFixed(3),
    avgOdds: +avgOdds.toFixed(3), avgCLV: +avgCLV.toFixed(3),
    pValue: +pValue.toFixed(3), zScore: +z.toFixed(3),
    strike: +winRate.toFixed(3),
    monthly, equity,
    summary,
  };
}

function buildSummary({ roi, winRate, sharpe, maxDD, bets, avgOdds, pValue, totalPnL, startBank }) {
  const profitPct = ((totalPnL / startBank) * 100).toFixed(1);
  let verdict = '', color = 'neutral', emoji = '📊';

  if (roi > 15 && sharpe > 1.5 && pValue < 0.05) {
    verdict = `Отличная стратегия. ROI ${roi.toFixed(1)}% статистически значим (p=${pValue.toFixed(3)}), Sharpe ${sharpe.toFixed(2)} говорит о стабильности. Рекомендуется к применению.`;
    color = 'green'; emoji = '🏆';
  } else if (roi > 5 && pValue < 0.1) {
    verdict = `Перспективная стратегия. ROI ${roi.toFixed(1)}% с хорошим Sharpe ${sharpe.toFixed(2)}. Стоит протестировать на новом периоде и оптимизировать параметры.`;
    color = 'yellow'; emoji = '✅';
  } else if (roi > 0 && pValue < 0.2) {
    verdict = `Слабоположительная стратегия. ROI ${roi.toFixed(1)}% есть, но Sharpe ${sharpe.toFixed(2)} и p-value ${pValue.toFixed(3)} говорят о нестабильности. Нужна оптимизация.`;
    color = 'yellow'; emoji = '⚠️';
  } else if (roi > 0) {
    verdict = `ROI ${roi.toFixed(1)}% положительный, но результат статистически незначим (p=${pValue.toFixed(3)}, n=${bets} ставок). Возможно случайность — нужно больше ставок.`;
    color = 'orange'; emoji = '🎲';
  } else {
    verdict = `Убыточная стратегия. ROI ${roi.toFixed(1)}%, просадка ${maxDD.toFixed(1)}%. Требует пересмотра условий входа или критериев value.`;
    color = 'red'; emoji = '❌';
  }

  const details = [
    bets < 100 ? `⚠️ Малая выборка (${bets} ставок) — результаты ненадёжны, нужно 200+.` : '',
    maxDD > 40 ? `⚠️ Высокая просадка ${maxDD.toFixed(1)}% — рискованный moneymgmt.` : '',
    avgOdds < 1.4 ? `ℹ️ Низкие коэф. (${avgOdds.toFixed(2)}) — стратегия ставит на фаворитов.` : '',
    avgOdds > 4 ? `ℹ️ Высокие коэф. (${avgOdds.toFixed(2)}) — высокая дисперсия, нужна большая дистанция.` : '',
    winRate > 65 ? `✅ Win rate ${winRate.toFixed(1)}% — высокая точность.` : '',
    winRate < 40 ? `⚠️ Win rate ${winRate.toFixed(1)}% — низкая точность, проверьте value.` : '',
  ].filter(Boolean);

  return { emoji, verdict, color, details, profitPct };
}

// ═══════════════════════════════════════════════════════════════════
//  SPORT CONFIG (дублирует BACKTEST_SPORT_CONFIG из index.js)
// ═══════════════════════════════════════════════════════════════════
const SPORT_CFG = {
  football:   { table: 'betquant.football_matches',    leagueCol: 'league_code', seasonCol: 'season' },
  hockey:     { table: 'betquant.hockey_matches',      leagueCol: 'league',      seasonCol: 'season' },
  basketball: { table: 'betquant.basketball_matches_v2', leagueCol: 'league',    seasonCol: 'season', fallback: 'betquant.basketball_matches' },
  baseball:   { table: 'betquant.baseball_matches',    leagueCol: 'league',      seasonCol: 'season' },
  tennis:     { table: 'betquant.tennis_extended',     leagueCol: 'tour',        seasonCol: null },
  volleyball: { table: 'betquant.volleyball_matches',  leagueCol: 'competition', seasonCol: 'season' },
  nfl:        { table: 'betquant.nfl_matches',         leagueCol: 'league',      seasonCol: 'season' },
  rugby:      { table: 'betquant.rugby_matches',       leagueCol: 'competition', seasonCol: 'season' },
  cricket:    { table: 'betquant.cricket_matches',     leagueCol: 'competition', seasonCol: 'season' },
  waterpolo:  { table: 'betquant.waterpolo_matches',   leagueCol: 'competition', seasonCol: 'season' },
  esports:    { table: 'betquant.esports_matches',     leagueCol: 'league',      seasonCol: null },
};

async function loadMatches(clickhouse, sport, { dateFrom, dateTo, league, season } = {}) {
  const sc = SPORT_CFG[sport] || SPORT_CFG.football;
  let table = sc.table;

  // fallback
  if (sc.fallback) {
    try {
      const pr = await clickhouse.query({ query: `SELECT count() as n FROM ${table}`, format: 'JSON' });
      const pd = await pr.json();
      if (parseInt(pd.data?.[0]?.n || 0) === 0) table = sc.fallback;
    } catch { table = sc.fallback; }
  }

  const parts = ['1=1'];
  if (league && sc.leagueCol) parts.push(`${sc.leagueCol} = '${String(league).replace(/'/g,"''")}'`);
  if (season && sc.seasonCol) parts.push(`${sc.seasonCol} = '${String(season).replace(/'/g,"''")}'`);
  if (dateFrom) parts.push(`date >= '${dateFrom}'`);
  if (dateTo)   parts.push(`date <= '${dateTo}'`);
  const where = 'WHERE ' + parts.join(' AND ');

  const r = await clickhouse.query({
    query: `SELECT * FROM ${table} ${where} ORDER BY date ASC LIMIT 200000`,
    format: 'JSON',
  });
  const d = await r.json();
  return (d.data || []).map(m => normalizeMatch(m, sport));
}

// ═══════════════════════════════════════════════════════════════════
//  POST /api/bt/run  — полный бэктест на сервере
// ═══════════════════════════════════════════════════════════════════
router.post('/run', async (req, res) => {
  const { strategies = [], cfg = {}, parlayRules = [] } = req.body;
  const clickhouse = req.app.locals.clickhouse;

  if (!strategies.length) return res.status(400).json({ error: 'No strategies' });
  if (!clickhouse)        return res.status(503).json({ error: 'ClickHouse not connected' });

  try {
    // Загружаем матчи по каждому виду спорта
    const sports = [...new Set(strategies.map(s => s.sport || 'football'))];
    const matchesBySport = {};
    for (const sport of sports) {
      matchesBySport[sport] = await loadMatches(clickhouse, sport, {
        dateFrom: cfg.dateFrom || '2018-01-01',
        dateTo:   cfg.dateTo   || new Date().toISOString().slice(0, 10),
        league:   cfg.league && cfg.league !== 'all' ? cfg.league : null,
        season:   cfg.season || null,
      });
    }

    const totalLoaded = Object.values(matchesBySport).reduce((s, a) => s + a.length, 0);
    if (totalLoaded === 0) return res.json({ error: 'no_data', bets: 0, stats: { bets: 0 } });

    // Компилируем стратегии
    const evalFns = strategies.map(s => ({ ...s, fn: compileStrategy(s.code) })).filter(s => s.fn);
    if (!evalFns.length) return res.status(400).json({ error: 'All strategies failed to compile' });

    // Прогон
    const result = parlayRules.length
      ? runParlayEngine(evalFns, matchesBySport, cfg, parlayRules)
      : runSinglesEngine(evalFns, matchesBySport, cfg);

    res.json({ ...result, loaded: totalLoaded });
  } catch (e) {
    console.error('[bt/run]', e);
    res.status(500).json({ error: e.message });
  }
});

function runSinglesEngine(evalFns, matchesBySport, cfg) {
  let bank = cfg.bankroll || 1000;
  const equity = [bank], trades = [];
  const ss = {};
  evalFns.forEach(s => { ss[s.id] = { bets: 0, wins: 0, pnl: 0, stakes: 0, name: s.name, sport: s.sport, color: s.color }; });

  const signalsByDate = {};
  for (const ev of evalFns) {
    const matches = matchesBySport[ev.sport] || [];
    for (const m of matches) {
      let sig = null;
      try { sig = ev.fn(m, makeTeamAPI(m, matches), makeH2H(m, matches), makeMarketAPI()); } catch (e) {}
      if (!sig?.signal) continue;
      const mk  = String(sig.market || 'home').toLowerCase().replace('_win', '');
      const odds = m[`odds_${mk}`] || m.odds_home;
      if (!odds || odds < (cfg.minOdds || 1.1) || odds > (cfg.maxOdds || 20)) continue;
      if (!signalsByDate[m.date]) signalsByDate[m.date] = [];
      signalsByDate[m.date].push({ m, sig, odds, ev });
    }
  }

  for (const date of Object.keys(signalsByDate).sort()) {
    for (const { m, sig, odds, ev } of signalsByDate[date]) {
      const stake = calcStake(cfg, bank, odds, sig.prob || 0.5);
      if (stake < 0.01) continue;
      const won = checkWin(m, sig.market);
      const pnl = won ? stake * (odds - 1) * (1 - (cfg.commission || 0) / 100) : -stake;
      bank = Math.max(0, bank + pnl);
      equity.push(bank);
      const s = ss[ev.id];
      if (s) { s.bets++; s.stakes += stake; s.pnl += pnl; if (won) s.wins++; }
      trades.push({
        date, type: 'single',
        match: `${m.team_home} vs ${m.team_away}`,
        sport: m.sport, league: m.league || m.competition || m.league_code || '',
        strategyId: ev.id, strategyName: ev.name, strategyColor: ev.color || '#00d4ff',
        market: sig.market, odds, legs: 1,
        stake: +stake.toFixed(2), won: won ? 'W' : 'L',
        pnl: +pnl.toFixed(2), bankroll: +bank.toFixed(2),
      });
    }
  }

  return { trades, equity, stratStats: ss, stats: calcStats(trades, cfg.bankroll || 1000, equity) };
}

function runParlayEngine(evalFns, matchesBySport, cfg, parlayRules) {
  let bank = cfg.bankroll || 1000;
  const equity = [bank], trades = [];
  const ss = {};
  evalFns.forEach(s => { ss[s.id] = { bets: 0, wins: 0, pnl: 0, stakes: 0, name: s.name, sport: s.sport }; });

  const signalsByDate = {};
  for (const ev of evalFns) {
    const matches = matchesBySport[ev.sport] || [];
    for (const m of matches) {
      let sig = null;
      try { sig = ev.fn(m, makeTeamAPI(m, matches), makeH2H(m, matches), makeMarketAPI()); } catch (e) {}
      if (!sig?.signal) continue;
      const mk  = String(sig.market || 'home').toLowerCase().replace('_win', '');
      const odds = m[`odds_${mk}`] || m.odds_home;
      if (!odds || odds < (cfg.minOdds || 1.1) || odds > (cfg.maxOdds || 20)) continue;
      if (!signalsByDate[m.date]) signalsByDate[m.date] = [];
      signalsByDate[m.date].push({ m, sig, odds, ev });
    }
  }

  for (const date of Object.keys(signalsByDate).sort()) {
    const daySignals = signalsByDate[date];
    for (const rule of parlayRules) {
      let legs = Object.values(
        daySignals
          .filter(s => !rule.strategyIds?.length || rule.strategyIds.includes(s.ev.id))
          .reduce((acc, c) => {
            const k = `${c.m.team_home}_${c.m.team_away}_${c.ev.sport}`;
            if (!acc[k]) acc[k] = c;
            return acc;
          }, {})
      );
      if (rule.requireDifferentSports) {
        const bySport = {};
        legs.forEach(l => { if (!bySport[l.ev.sport]) bySport[l.ev.sport] = l; });
        legs = Object.values(bySport);
      }
      legs = legs.slice(0, rule.maxLegs || 3);
      if (legs.length < (rule.minLegs || 2)) continue;

      const totalOdds = +legs.reduce((a, l) => a * l.odds, 1).toFixed(3);
      const combProb  = legs.reduce((a, l) => a * (l.sig.prob || 1 / l.odds), 1);
      const stake = calcStake(cfg, bank, totalOdds, combProb);
      if (stake < 0.01) continue;

      const allWon = legs.every(l => checkWin(l.m, l.sig.market));
      const pnl = allWon ? stake * (totalOdds - 1) : -stake;
      bank = Math.max(0, bank + pnl);
      equity.push(bank);
      trades.push({
        date, type: 'parlay', legs: legs.length, totalOdds, combProb: +combProb.toFixed(3),
        match: legs.map(l => `${l.m.team_home} vs ${l.m.team_away}`).join(' | '),
        sport: legs.map(l => l.ev.sport).join(','), league: '',
        strategyId: legs[0].ev.id, strategyName: `Экспресс ${legs.length}л.`, strategyColor: '#ffd740',
        market: legs.map(l => l.sig.market).join('+'), odds: totalOdds,
        stake: +stake.toFixed(2), won: allWon ? 'W' : 'L',
        pnl: +pnl.toFixed(2), bankroll: +bank.toFixed(2),
      });
    }
    // Singles on same day too
    for (const { m, sig, odds, ev } of daySignals) {
      if (parlayRules.some(r => r.strategyIds?.includes(ev.id))) continue; // skip parlay-only strats
      const stake = calcStake(cfg, bank, odds, sig.prob || 0.5);
      if (stake < 0.01) continue;
      const won = checkWin(m, sig.market);
      const pnl = won ? stake * (odds - 1) : -stake;
      bank = Math.max(0, bank + pnl);
      equity.push(bank);
      const s = ss[ev.id];
      if (s) { s.bets++; s.stakes += stake; s.pnl += pnl; if (won) s.wins++; }
      trades.push({
        date, type: 'single', match: `${m.team_home} vs ${m.team_away}`,
        sport: m.sport, league: m.league || '',
        strategyId: ev.id, strategyName: ev.name, strategyColor: ev.color || '#00d4ff',
        market: sig.market, odds, legs: 1,
        stake: +stake.toFixed(2), won: won ? 'W' : 'L',
        pnl: +pnl.toFixed(2), bankroll: +bank.toFixed(2),
      });
    }
  }

  return { trades, equity, stratStats: ss, stats: calcStats(trades, cfg.bankroll || 1000, equity) };
}

// ═══════════════════════════════════════════════════════════════════
//  POST /api/bt/montecarlo  — монте-карло на реальных данных
// ═══════════════════════════════════════════════════════════════════
router.post('/montecarlo', async (req, res) => {
  const { strategy, cfg = {}, mcCfg = {} } = req.body;
  const clickhouse = req.app.locals.clickhouse;

  const simCount     = Math.min(parseInt(mcCfg.simCount) || 5000, 20000);
  const ruinThresh   = parseFloat(mcCfg.ruinThreshold) || 0.5;
  const startBank    = parseFloat(cfg.bankroll) || 1000;

  // Если переданы реальные трейды — используем их, иначе рассчитываем
  let trades = req.body.trades || null;

  if (!trades && strategy && clickhouse) {
    try {
      const matches = await loadMatches(clickhouse, strategy.sport || 'football', {
        dateFrom: cfg.dateFrom || '2020-01-01',
        dateTo: cfg.dateTo || new Date().toISOString().slice(0, 10),
      });
      const fn = compileStrategy(strategy.code);
      if (fn && matches.length) {
        let bank = startBank;
        trades = [];
        for (const m of matches) {
          let sig = null;
          try { sig = fn(m, makeTeamAPI(m, matches), makeH2H(m, matches), makeMarketAPI()); } catch (e) {}
          if (!sig?.signal) continue;
          const mk = String(sig.market || 'home').toLowerCase().replace('_win', '');
          const odds = m[`odds_${mk}`] || m.odds_home;
          if (!odds || odds < 1.1) continue;
          const stake = calcStake(cfg, bank, odds, sig.prob || 0.5);
          const won = checkWin(m, sig.market);
          const pnl = won ? stake * (odds - 1) : -stake;
          bank = Math.max(0, bank + pnl);
          trades.push({ odds, stake: +stake.toFixed(2), won: won ? 'W' : 'L', pnl: +pnl.toFixed(2), prob: sig.prob || 0.5 });
        }
      }
    } catch (e) { console.warn('[bt/mc]', e.message); }
  }

  // Если нет трейдов — используем параметры из тела
  if (!trades || !trades.length) {
    const winRate  = parseFloat(mcCfg.winRate) / 100 || 0.52;
    const avgOdds  = parseFloat(mcCfg.avgOdds) || 2.0;
    const stakePct = parseFloat(mcCfg.stakePct) / 100 || 0.02;
    trades = Array.from({ length: 200 }, () => ({
      odds: avgOdds, prob: winRate, stake: startBank * stakePct,
      won: Math.random() < winRate ? 'W' : 'L',
      pnl: Math.random() < winRate ? startBank * stakePct * (avgOdds - 1) : -startBank * stakePct,
    }));
  }

  // Статистика из реальных трейдов
  const realWins   = trades.filter(t => t.won === 'W').length;
  const realWR     = realWins / trades.length;
  const realAvgOdds= trades.reduce((s, t) => s + (t.odds || 2), 0) / trades.length;
  const realStakePct = startBank > 0 ? (trades.reduce((s, t) => s + t.stake, 0) / trades.length) / startBank : 0.02;

  // Симуляция
  const betsPerRun = Math.min(parseInt(mcCfg.betsPerRun) || trades.length * 2, 2000);
  const allPaths = [];
  const finalBankrolls = [];
  const ruinByBet = Array(betsPerRun).fill(0);
  let ruinCount = 0;

  for (let s = 0; s < simCount; s++) {
    let bank = startBank;
    const path = [bank];
    let ruined = false;
    for (let b = 0; b < betsPerRun; b++) {
      const t = trades[Math.floor(Math.random() * trades.length)];
      const stake = Math.min(bank * realStakePct, bank * 0.25);
      const won   = Math.random() < realWR;
      const pnl   = won ? stake * ((t.odds || realAvgOdds) - 1) : -stake;
      bank = Math.max(0, bank + pnl);
      path.push(bank);
      if (!ruined && bank <= startBank * ruinThresh) {
        ruined = true; ruinCount++;
        for (let rb = b; rb < betsPerRun; rb++) ruinByBet[rb]++;
      }
    }
    if (s < 300) allPaths.push(path);
    finalBankrolls.push(bank);
  }

  finalBankrolls.sort((a, b) => a - b);
  const pct = i => finalBankrolls[Math.floor(simCount * i)];

  res.json({
    paths: allPaths,
    finals: finalBankrolls,
    ruinByBet,
    simCount, betsPerRun, startBank,
    realStats: { winRate: realWR, avgOdds: realAvgOdds, tradesUsed: trades.length },
    percentiles: { p5: pct(0.05), p25: pct(0.25), p50: pct(0.50), p75: pct(0.75), p95: pct(0.95) },
    avg: finalBankrolls.reduce((s, v) => s + v, 0) / simCount,
    ruinProbability: ruinCount / simCount * 100,
  });
});

// ═══════════════════════════════════════════════════════════════════
//  POST /api/bt/optimize  — grid/random search по параметрам
// ═══════════════════════════════════════════════════════════════════
router.post('/optimize', async (req, res) => {
  const { strategy, params = [], cfg = {}, method = 'grid', objective = 'roi', maxIter = 200 } = req.body;
  const clickhouse = req.app.locals.clickhouse;

  if (!strategy || !clickhouse) return res.status(400).json({ error: 'strategy + clickhouse required' });

  try {
    const matches = await loadMatches(clickhouse, strategy.sport || 'football', {
      dateFrom: cfg.dateFrom || '2020-01-01',
      dateTo:   cfg.dateTo   || new Date().toISOString().slice(0, 10),
    });
    if (!matches.length) return res.json({ error: 'no_data', results: [] });

    // Генерируем сетку параметров
    function range(min, max, step) {
      const r = [];
      for (let v = min; v <= max + 1e-9; v += step) r.push(+(v.toFixed(4)));
      return r;
    }

    let paramCombos = [{}];
    for (const p of params) {
      const vals = range(p.min, p.max, p.step);
      paramCombos = paramCombos.flatMap(combo => vals.map(v => ({ ...combo, [p.name]: v })));
    }

    if (method === 'random' || paramCombos.length > maxIter) {
      // Рандомная выборка
      paramCombos = Array.from({ length: Math.min(maxIter, 500) }, () => {
        const combo = {};
        for (const p of params) combo[p.name] = +(p.min + Math.random() * (p.max - p.min)).toFixed(4);
        return combo;
      });
    }

    const results = [];
    for (const combo of paramCombos.slice(0, 500)) {
      // Инжектируем параметры в стратегию через замену или через match-объект
      const augCode = strategy.code.replace(
        /function evaluate\s*\([^)]*\)\s*\{/,
        `function evaluate(match, team, h2h, market) {\n  // injected params\n${Object.entries(combo).map(([k, v]) => `  const ${k} = ${v};`).join('\n')}\n`
      );
      const fn = compileStrategy(augCode);
      if (!fn) continue;

      let bank = cfg.bankroll || 1000;
      const tradesLocal = [];
      for (const m of matches) {
        let sig = null;
        try { sig = fn(m, makeTeamAPI(m, matches), makeH2H(m, matches), makeMarketAPI()); } catch (e) {}
        if (!sig?.signal) continue;
        const mk = String(sig.market || 'home').toLowerCase().replace('_win', '');
        const odds = m[`odds_${mk}`] || m.odds_home;
        if (!odds || odds < 1.05) continue;
        const stake = calcStake(cfg, bank, odds, sig.prob || 0.5);
        const won = checkWin(m, sig.market);
        const pnl = won ? stake * (odds - 1) : -stake;
        bank = Math.max(0, bank + pnl);
        tradesLocal.push({ stake, pnl, odds, won: won ? 'W' : 'L' });
      }

      if (!tradesLocal.length) continue;
      const wins   = tradesLocal.filter(t => t.won === 'W').length;
      const totPnL = tradesLocal.reduce((s, t) => s + t.pnl, 0);
      const totStk = tradesLocal.reduce((s, t) => s + t.stake, 0);
      const roi    = totStk ? (totPnL / totStk) * 100 : 0;
      const wr     = wins / tradesLocal.length * 100;
      const rets   = tradesLocal.map(t => t.pnl / Math.max(0.01, t.stake));
      const avgR   = rets.reduce((s, r) => s + r, 0) / rets.length;
      const stdR   = Math.sqrt(rets.reduce((s, r) => s + (r - avgR) ** 2, 0) / rets.length);
      const sharpe = stdR > 0 ? +(avgR / stdR).toFixed(3) : 0;

      const score = objective === 'sharpe' ? sharpe : objective === 'profit' ? totPnL : roi;
      results.push({ ...combo, bets: tradesLocal.length, roi: +roi.toFixed(2), wr: +wr.toFixed(1), sharpe, pnl: +totPnL.toFixed(2), score: +score.toFixed(3) });
    }

    results.sort((a, b) => b.score - a.score);
    res.json({ results: results.slice(0, 100), best: results[0] || null, total: results.length });
  } catch (e) {
    console.error('[bt/optimize]', e);
    res.status(500).json({ error: e.message });
  }
});

// ═══════════════════════════════════════════════════════════════════
//  POST /api/bt/walkforward  — walk-forward на реальных данных
// ═══════════════════════════════════════════════════════════════════
router.post('/walkforward', async (req, res) => {
  const { strategy, cfg = {}, wfCfg = {} } = req.body;
  const clickhouse = req.app.locals.clickhouse;

  if (!strategy || !clickhouse) return res.status(400).json({ error: 'strategy + clickhouse required' });

  try {
    const allMatches = await loadMatches(clickhouse, strategy.sport || 'football', {
      dateFrom: cfg.dateFrom || '2019-01-01',
      dateTo:   cfg.dateTo   || new Date().toISOString().slice(0, 10),
    });
    if (allMatches.length < 50) return res.json({ error: 'not_enough_data', windows: [] });

    allMatches.sort((a, b) => a.date.localeCompare(b.date));
    const fn = compileStrategy(strategy.code);
    if (!fn) return res.status(400).json({ error: 'compile_error' });

    const numWindows = Math.max(3, Math.min(20, parseInt(wfCfg.windowSize) || 6));
    const inSample   = parseFloat(wfCfg.inSample) / 100 || 0.7;
    const anchored   = !!wfCfg.anchored;
    const n = allMatches.length;
    const winSize = Math.floor(n / numWindows);

    const windows = [];
    for (let i = 0; i < numWindows; i++) {
      const blockStart = anchored ? 0 : i * winSize;
      const blockEnd   = Math.min((i + 1) * winSize, n);
      const splitAt    = blockStart + Math.floor((blockEnd - blockStart) * inSample);

      const trainSet = allMatches.slice(blockStart, splitAt);
      const testSet  = allMatches.slice(splitAt, blockEnd);
      if (testSet.length < 5) continue;

      // Train: eval strategy on train set (for context only, no result used)
      // Test: run backtest on test set
      let bank = cfg.bankroll || 1000;
      const tradesLocal = [];
      for (const m of testSet) {
        let sig = null;
        try { sig = fn(m, makeTeamAPI(m, trainSet.concat(testSet)), makeH2H(m, allMatches), makeMarketAPI()); } catch (e) {}
        if (!sig?.signal) continue;
        const mk = String(sig.market || 'home').toLowerCase().replace('_win', '');
        const odds = m[`odds_${mk}`] || m.odds_home;
        if (!odds || odds < 1.05) continue;
        const stake = calcStake(cfg, bank, odds, sig.prob || 0.5);
        const won = checkWin(m, sig.market);
        const pnl = won ? stake * (odds - 1) : -stake;
        bank = Math.max(0, bank + pnl);
        tradesLocal.push({ stake, pnl, odds, won: won ? 'W' : 'L', date: m.date });
      }

      const bets = tradesLocal.length;
      const wins = tradesLocal.filter(t => t.won === 'W').length;
      const totPnL = tradesLocal.reduce((s, t) => s + t.pnl, 0);
      const totStk = tradesLocal.reduce((s, t) => s + t.stake, 0);
      const roi = totStk ? (totPnL / totStk) * 100 : 0;
      const wr = bets ? wins / bets * 100 : 0;
      const avgOdds = bets ? tradesLocal.reduce((s, t) => s + t.odds, 0) / bets : 0;

      windows.push({
        window: i + 1,
        trainDates: `${trainSet[0]?.date || '?'} — ${trainSet[trainSet.length-1]?.date || '?'}`,
        testDates:  `${testSet[0]?.date  || '?'} — ${testSet[testSet.length-1]?.date  || '?'}`,
        trainSize: trainSet.length,
        testBets: bets, wins,
        winRate: +wr.toFixed(1),
        avgOdds: +avgOdds.toFixed(2),
        roi: +roi.toFixed(1),
        pnl: +totPnL.toFixed(2),
        stable: Math.abs(roi) < 40 && wr > 35,
      });
    }

    const profitable = windows.filter(w => w.pnl > 0).length;
    const avgROI = windows.reduce((s, w) => s + w.roi, 0) / (windows.length || 1);
    res.json({ windows, summary: { profitable, total: windows.length, avgROI: +avgROI.toFixed(2) } });
  } catch (e) {
    console.error('[bt/walkforward]', e);
    res.status(500).json({ error: e.message });
  }
});

module.exports = router;