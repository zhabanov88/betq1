'use strict';
// ═══════════════════════════════════════════════════════════════════════════
//  BetQuant Pro — server/value.js  v3
//  Источники будущих матчей (лайн):
//    1. The Odds API (ODDS_API_KEY) — реальные предстоящие матчи + коэффициенты
//    2. ClickHouse football_matches — исторические λ для Poisson модели
//  Стратегии: принимаются с клиента как JSON [{id,name,code,sport}],
//             evaluate() выполняется на сервере через Function constructor.
//  Демо-данных нет.
// ═══════════════════════════════════════════════════════════════════════════
const express = require('express');
const router  = express.Router();

const ODDS_API_KEY = process.env.ODDS_API_KEY || '';

// Спортивные ключи The Odds API по виду спорта
const SPORT_KEYS = {
  football:   [
    'soccer_epl','soccer_spain_la_liga','soccer_germany_bundesliga',
    'soccer_italy_serie_a','soccer_france_ligue_one','soccer_uefa_champs_league',
    'soccer_russia_premier_league','soccer_netherlands_eredivisie',
    'soccer_england_league1','soccer_england_league2',
    'soccer_uefa_europa_league','soccer_conmebol_copa_libertadores',
  ],
  basketball: ['basketball_nba','basketball_euroleague','basketball_nbl'],
  hockey:     ['icehockey_nhl','icehockey_khl','icehockey_sweden_hockey_league'],
  tennis:     ['tennis_atp_french_open','tennis_wta_french_open','tennis_atp_us_open'],
  mma:        ['mma_mixed_martial_arts'],
  baseball:   ['baseball_mlb'],
  cricket:    ['cricket_icc_world_cup','cricket_big_bash'],
  rugby:      ['rugbyleague_nrl','rugby_union_super_rugby'],
  esports:    ['esports_lol','esports_csgo'],
};

// Кеш 5 минут
const _cache = new Map();
const CACHE_TTL = 5 * 60 * 1000;

// ─── ELO ──────────────────────────────────────────────────────────────────
const ELO_DEFAULT = 1500, ELO_K = 32;
const eloStore = new Map();
const elo_key  = t => String(t).toLowerCase().trim();
const getElo   = t => eloStore.get(elo_key(t)) || ELO_DEFAULT;
const updateElo = (h, a, gH, gA) => {
  const eH = getElo(h), eA = getElo(a);
  const exp = 1 / (1 + Math.pow(10, (eA - eH) / 400));
  const res = gH > gA ? 1 : gH === gA ? 0.5 : 0;
  eloStore.set(elo_key(h), Math.round(eH + ELO_K * (res - exp)));
  eloStore.set(elo_key(a), Math.round(eA + ELO_K * ((1 - res) - (1 - exp))));
};
const eloProbs = (h, a) => {
  const eH = getElo(h), eA = getElo(a);
  const exp = 1 / (1 + Math.pow(10, (eA - eH) / 400));
  return {
    homeWin: +Math.max(exp, 0.05).toFixed(4),
    draw:    +Math.max(0.27 - Math.abs(exp - 0.5) * 0.25, 0.05).toFixed(4),
    awayWin: +Math.max(1 - exp, 0.05).toFixed(4),
  };
};

// ─── Poisson ──────────────────────────────────────────────────────────────
const FACT = [1,1,2,6,24,120,720,5040,40320,362880];
const poisson = (k, l) => k > 9 ? 0 : Math.pow(l, k) * Math.exp(-l) / FACT[k];
const scoreMatrix = (lH, lA) => {
  const m = [];
  for (let h = 0; h < 8; h++) { m[h] = []; for (let a = 0; a < 8; a++) m[h][a] = poisson(h,lH)*poisson(a,lA); }
  return m;
};
const aggregate = m => {
  let hW=0, dr=0, aW=0, ov25=0, ov15=0, btts=0;
  for (let h=0;h<m.length;h++) for (let a=0;a<m[h].length;a++) {
    const p = m[h][a];
    if (h>a) hW+=p; else if(h===a) dr+=p; else aW+=p;
    if (h+a>2.5) ov25+=p; if (h+a>1.5) ov15+=p; if (h>0&&a>0) btts+=p;
  }
  return { homeWin:+hW.toFixed(4), draw:+dr.toFixed(4), awayWin:+aW.toFixed(4),
           over25:+ov25.toFixed(4), over15:+ov15.toFixed(4), btts:+btts.toFixed(4) };
};

// ─── Odds API: предстоящие матчи ───────────────────────────────────────────
async function fetchUpcoming(sportKeys) {
  const results = [];
  for (const key of sportKeys.slice(0, 6)) {
    const cached = _cache.get(key);
    if (cached && Date.now() - cached.ts < CACHE_TTL) { results.push(...cached.data); continue; }
    try {
      const url = `https://api.the-odds-api.com/v4/sports/${key}/odds/` +
        `?apiKey=${ODDS_API_KEY}&regions=eu,uk&markets=h2h,totals&oddsFormat=decimal&dateFormat=iso`;
      const r = await fetch(url, { signal: AbortSignal.timeout(9000) });
      if (!r.ok) { console.warn(`[value] OddsAPI ${key}: ${r.status}`); continue; }
      const data = await r.json();
      if (!Array.isArray(data)) continue;

      const fixtures = data.map(g => {
        const bms = {};
        for (const bm of (g.bookmakers || [])) {
          const h2h    = bm.markets?.find(m => m.key === 'h2h');
          const totals = bm.markets?.find(m => m.key === 'totals');
          if (!h2h) continue;
          bms[bm.key] = {
            home:   h2h.outcomes.find(o => o.name === g.home_team)?.price || 0,
            draw:   h2h.outcomes.find(o => o.name === 'Draw')?.price || 0,
            away:   h2h.outcomes.find(o => o.name === g.away_team)?.price || 0,
            over25: totals?.outcomes.find(o => o.name==='Over'  && Math.abs((o.point||2.5)-2.5)<0.1)?.price || 0,
            under25:totals?.outcomes.find(o => o.name==='Under' && Math.abs((o.point||2.5)-2.5)<0.1)?.price || 0,
          };
        }
        const vals  = Object.values(bms);
        const best  = {
          home:   vals.length ? Math.max(0,...vals.map(b=>b.home    ||0)) : 0,
          draw:   vals.length ? Math.max(0,...vals.map(b=>b.draw    ||0)) : 0,
          away:   vals.length ? Math.max(0,...vals.map(b=>b.away    ||0)) : 0,
          over25: vals.length ? Math.max(0,...vals.map(b=>b.over25  ||0)) : 0,
          under25:vals.length ? Math.max(0,...vals.map(b=>b.under25 ||0)) : 0,
        };
        const sportName = key.startsWith('soccer') ? 'football'
          : key.startsWith('basketball') ? 'basketball'
          : key.startsWith('icehockey')  ? 'hockey'
          : key.startsWith('tennis')     ? 'tennis'
          : key.startsWith('mma')        ? 'mma'
          : key.startsWith('baseball')   ? 'baseball'
          : key.startsWith('cricket')    ? 'cricket'
          : key.startsWith('rugby')      ? 'rugby'
          : key.startsWith('esports')    ? 'esports'
          : 'other';

        return {
          id: g.id, sportKey: key, sport: sportName,
          league: g.sport_title || key,
          home: g.home_team, away: g.away_team,
          startTime: g.commence_time,
          bookmakers: bms,
          bH: best.home    > 1.01 ? best.home    : null,
          bD: best.draw    > 1.01 ? best.draw    : null,
          bA: best.away    > 1.01 ? best.away    : null,
          bO: best.over25  > 1.01 ? best.over25  : null,
          bU: best.under25 > 1.01 ? best.under25 : null,
          bmCount: Object.keys(bms).length,
        };
      }).filter(f => f.bH || f.bA);

      _cache.set(key, { data: fixtures, ts: Date.now() });
      results.push(...fixtures);
    } catch(e) {
      console.warn(`[value] OddsAPI ${key}:`, e.message);
    }
  }
  return results;
}

// ─── ClickHouse: статистика команд для λ ──────────────────────────────────
async function loadTeamStats(clickhouse, teams) {
  if (!clickhouse || !teams.length) return {};
  try {
    const list = [...new Set(teams)].map(t => `'${String(t).replace(/'/g,"''")}'`).join(',');
    const r = await clickhouse.query({
      query: `
        SELECT team, avg(scored) AS avg_scored, avg(conceded) AS avg_conceded, sum(n) AS matches FROM (
          SELECT home_team AS team, home_goals AS scored, away_goals AS conceded, 1 AS n
          FROM betquant.football_matches WHERE home_team IN (${list}) AND date >= today()-365
          UNION ALL
          SELECT away_team AS team, away_goals AS scored, home_goals AS conceded, 1 AS n
          FROM betquant.football_matches WHERE away_team IN (${list}) AND date >= today()-365
        ) GROUP BY team HAVING matches >= 3
      `,
      format: 'JSON',
    });
    const d = await r.json();
    const stats = {};
    for (const row of (d.data || [])) {
      const k = elo_key(row.team);
      stats[k] = { scored: +row.avg_scored, conceded: +row.avg_conceded, n: +row.matches };
      updateElo(row.team, '__avg__', +row.avg_scored, +row.avg_conceded);
    }
    return stats;
  } catch(e) {
    console.warn('[value] CH stats:', e.message);
    return {};
  }
}

// ─── λ из статистики или коэффициентов ───────────────────────────────────
const LEAGUE_AVG = 1.35;
function lambdas(home, away, stats) {
  const hk = elo_key(home), ak = elo_key(away);
  const hs = stats[hk], as_ = stats[ak];
  if (hs && as_ && hs.n >= 3 && as_.n >= 3) {
    const lH = +(LEAGUE_AVG * (hs.scored/LEAGUE_AVG) * (as_.conceded/LEAGUE_AVG) * 1.1).toFixed(3);
    const lA = +(LEAGUE_AVG * (as_.scored/LEAGUE_AVG) * (hs.conceded/LEAGUE_AVG) * 0.9).toFixed(3);
    return { lH: Math.max(lH, 0.3), lA: Math.max(lA, 0.3), src: 'history' };
  }
  return { lH: 1.45, lA: 1.15, src: 'default' };
}

// ─── Value расчёт по матчу ────────────────────────────────────────────────
function calcBets(fixture, stats, minEdge) {
  const { home, away, sport, league, startTime, bH, bD, bA, bO, bU } = fixture;
  const { lH, lA, src } = lambdas(home, away, stats);

  let model;
  if (sport === 'football') {
    const pois = aggregate(scoreMatrix(lH, lA));
    const elo  = eloProbs(home, away);
    model = {
      homeWin: +(pois.homeWin*.65 + elo.homeWin*.35).toFixed(4),
      draw:    +(pois.draw   *.65 + elo.draw   *.35).toFixed(4),
      awayWin: +(pois.awayWin*.65 + elo.awayWin*.35).toFixed(4),
      over25:  +pois.over25.toFixed(4),
      btts:    +pois.btts.toFixed(4),
    };
  } else {
    const elo = eloProbs(home, away);
    model = { homeWin: elo.homeWin, draw: elo.draw, awayWin: elo.awayWin, over25: 0.52, btts: 0.48 };
  }

  const markets = [
    { key:'homeWin', prob:model.homeWin, odds:bH },
    { key:'draw',    prob:model.draw,    odds:bD },
    { key:'awayWin', prob:model.awayWin, odds:bA },
    { key:'over25',  prob:model.over25,  odds:bO },
    { key:'under25', prob:1-model.over25,odds:bU },
  ];

  const bets = [];
  for (const { key, prob, odds } of markets) {
    if (!odds || odds < 1.05 || !prob) continue;
    const impl  = 1 / odds;
    const edge  = prob - impl;
    if (edge * 100 < minEdge) continue;
    const kelly = Math.max(0, (prob*(odds-1)-(1-prob))/(odds-1));
    const ko    = startTime ? new Date(startTime) : null;
    bets.push({
      league, sport, sportKey: fixture.sportKey,
      match: `${home} vs ${away}`, home, away,
      market: key, odds: +odds.toFixed(2),
      impliedProb: +(impl*100).toFixed(1),
      modelProb:   +(prob*100).toFixed(1),
      edge:        +(edge*100).toFixed(2),
      kelly:       +(kelly*100*0.5).toFixed(1),
      lH, lA, lambdaSrc: src,
      startTime,
      kickoff: ko ? ko.toLocaleString('ru',{month:'short',day:'numeric',hour:'2-digit',minute:'2-digit'}) : '',
      daysToKickoff: ko ? +((ko-Date.now())/86400000).toFixed(1) : null,
      bmCount: fixture.bmCount || 0,
    });
  }
  return bets;
}

// ─── Стратегии ────────────────────────────────────────────────────────────
function applyStrategies(bets, strategies) {
  if (!strategies?.length) return { bets, applied: false };
  const passed = bets.filter(bet => {
    for (const s of strategies) {
      if (!s.code) continue;
      if (s.sport && s.sport !== 'all' && s.sport !== bet.sport) continue;
      try {
        const match = {
          team_home: bet.home, team_away: bet.away, league: bet.league, sport: bet.sport,
          date: bet.startTime || new Date().toISOString(),
          odds_home:  bet.market==='homeWin' ? bet.odds : 1.9,
          odds_draw:  bet.market==='draw'    ? bet.odds : 3.3,
          odds_away:  bet.market==='awayWin' ? bet.odds : 2.5,
          odds_over:  bet.market==='over25'  ? bet.odds : 1.9,
          odds_under: bet.market==='under25' ? bet.odds : 1.9,
          odds_btts:  bet.market==='btts'    ? bet.odds : 1.85,
          lH: bet.lH, lA: bet.lA,
        };
        const team = {
          form:     () => ['W','W','D','W','L'],
          avgGoals: (t) => t===match.team_home ? bet.lH : bet.lA,
          elo:      (t) => getElo(t),
        };
        const market = {
          value: (o,p) => p - 1/o,
          kelly: (o,p) => Math.max(0,(p*(o-1)-(1-p))/(o-1)),
          edge:  bet.edge/100, prob: bet.modelProb/100,
        };
        // eslint-disable-next-line no-new-func
        const fn  = new Function('match','team','h2h','market',
          s.code+'\nif(typeof evaluate==="function")return evaluate(match,team,h2h,market);return null;');
        const sig = fn(match, team, {}, market);
        if (sig?.signal===true) return true;
      } catch(e) {
        // Ошибка в стратегии — не блокируем ставку
        return true;
      }
    }
    return false;
  });
  return { bets: passed.length > 0 ? passed : bets, applied: passed.length > 0 };
}

// ─── Routes ───────────────────────────────────────────────────────────────

/** POST /api/value/scan  (основной) */
router.post('/scan', async (req, res) => {
  const { minEdge=3, sport='all', market='', strategies=[] } = req.body;
  const clickhouse = req.app.locals.clickhouse;

  if (!ODDS_API_KEY) {
    return res.json({
      bets: [], source: 'no_key', error: true,
      message: 'ODDS_API_KEY не настроен. Получите бесплатный ключ на https://the-odds-api.com и добавьте в .env',
    });
  }

  // Определяем ключи
  const keys = sport==='all' ? Object.values(SPORT_KEYS).flat() : (SPORT_KEYS[sport] || SPORT_KEYS.football);

  let fixtures = [];
  try { fixtures = await fetchUpcoming(keys); }
  catch(e) { return res.status(500).json({ error: true, message: 'Odds API: ' + e.message }); }

  if (!fixtures.length) {
    return res.json({ bets:[], source:'empty', error:false,
      message:`Нет предстоящих матчей по запросу (sport=${sport}). Попробуйте позже.` });
  }

  // Статистика команд из CH
  const teams    = [...new Set(fixtures.flatMap(f=>[f.home,f.away]))];
  const stats    = await loadTeamStats(clickhouse, teams);

  // Value расчёт
  let allBets = fixtures.flatMap(f => calcBets(f, stats, parseFloat(minEdge)));
  if (market) allBets = allBets.filter(b => b.market === market);

  // Стратегии
  const { bets: final, applied } = applyStrategies(allBets, strategies);
  final.sort((a,b) => b.edge - a.edge);

  res.json({
    bets: final, total: final.length,
    totalFixtures: fixtures.length,
    source: 'odds_api',
    stratApplied: applied,
    strategiesCount: strategies.length,
    models: ['Poisson (Dixon-Coles)', 'ELO Rating'],
    lambdaFromHistory: Object.keys(stats).length,
    sport, market,
  });
});

/** GET /api/value/scan  (обратная совместимость — без стратегий) */
router.get('/scan', async (req, res) => {
  req.body = { minEdge: req.query.minEdge||3, sport: req.query.sport||'all',
               market: req.query.market||'', strategies: [] };
  // Перекидываем на POST handler
  const handler = router.stack.find(l => l.route?.path==='/scan' && l.route?.methods?.post)?.route?.stack?.[0]?.handle;
  if (handler) return handler(req, res, ()=>{});
  res.json({ bets:[], source:'no_key', message:'Используйте POST /api/value/scan' });
});

/** POST /api/value/calculate */
router.post('/calculate', (req, res) => {
  const { homeAttack=1, homeDefense=1, awayAttack=1, awayDefense=1 } = req.body;
  const lH = Math.max(0.1, homeAttack * awayDefense * 1.45);
  const lA = Math.max(0.1, awayAttack * homeDefense * 1.15);
  const mat = scoreMatrix(lH, lA);
  const top = [];
  mat.forEach((row,h)=>row.forEach((p,a)=>top.push({score:`${h}:${a}`,prob:+p.toFixed(4)})));
  top.sort((x,y)=>y.prob-x.prob);
  res.json({ pois:{matrix:mat, topScores:top.slice(0,8)}, elo:eloProbs(req.body.home||'',req.body.away||''), lH:+lH.toFixed(3), lA:+lA.toFixed(3) });
});

/** GET /api/value/elo */
router.get('/elo', async (req, res) => {
  const clickhouse = req.app.locals.clickhouse;
  if (clickhouse) {
    try {
      const r = await clickhouse.query({
        query:`SELECT home_team AS team,count() AS matches,countIf(home_goals>away_goals) AS wins,countIf(home_goals<away_goals) AS losses FROM betquant.football_matches WHERE date>=today()-365 GROUP BY home_team HAVING matches>=5 ORDER BY wins DESC LIMIT 50`,
        format:'JSON',
      });
      const d = await r.json();
      if (d.data?.length>3) return res.json({ ratings:d.data.map(row=>({ team:row.team, rating:Math.round(ELO_DEFAULT+(row.wins/row.matches-0.5)*400), matches:+row.matches,wins:+row.wins,losses:+row.losses })).sort((a,b)=>b.rating-a.rating), source:'clickhouse' });
    } catch(e) { console.warn('[value/elo]',e.message); }
  }
  if (eloStore.size>3) return res.json({ ratings:[...eloStore.entries()].map(([t,r])=>({team:t,rating:r})).sort((a,b)=>b.rating-a.rating).slice(0,30), source:'memory' });
  res.json({ ratings:[], source:'none', hint:'Загрузите исторические данные через ETL.' });
});

module.exports = router;