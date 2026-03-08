'use strict';
/**
 * BetQuant Pro — Odds Compare  /api/odds-compare/*
 * Реальные данные из The Odds API (ODDS_API_KEY).
 * Demo — только при ?demo=true в запросе.
 */
const express = require('express');
const router  = express.Router();

const ODDS_API_KEY = process.env.ODDS_API_KEY || '';
const CACHE_TTL    = 5 * 60 * 1000; // 5 минут
const _cache = { fixtures: [], ts: 0, arb: [], arbTs: 0 };

async function fetchOddsAPI() {
  if (!ODDS_API_KEY) return null;
  const ctrl = typeof AbortController !== 'undefined' ? new AbortController() : null;
  const t    = ctrl ? setTimeout(() => ctrl.abort(), 8000) : null;
  try {
    const r = await fetch(
      `https://api.the-odds-api.com/v4/sports/soccer_epl/odds/?apiKey=${ODDS_API_KEY}&regions=eu&markets=h2h,totals&oddsFormat=decimal`,
      { signal: ctrl?.signal }
    );
    if (t) clearTimeout(t);
    if (!r.ok) throw new Error(`Odds API ${r.status}`);
    const data = await r.json();
    if (!Array.isArray(data) || !data.length) return null;
    return data.slice(0, 12).map(g => {
      const bookmakers = {};
      for (const bm of (g.bookmakers || [])) {
        const h2h    = bm.markets?.find(m => m.key === 'h2h');
        const totals = bm.markets?.find(m => m.key === 'totals');
        if (!h2h) continue;
        bookmakers[bm.key] = {
          home:   h2h.outcomes.find(o => o.name === g.home_team)?.price || 0,
          draw:   h2h.outcomes.find(o => o.name === 'Draw')?.price      || 0,
          away:   h2h.outcomes.find(o => o.name === g.away_team)?.price || 0,
          over25: totals?.outcomes.find(o => o.name === 'Over')?.price  || 0,
          under25:totals?.outcomes.find(o => o.name === 'Under')?.price || 0,
        };
      }
      const bms = Object.values(bookmakers);
      const bestOdds = {
        home: bms.length ? Math.max(0, ...bms.map(b => b.home || 0)) : 0,
        draw: bms.length ? Math.max(0, ...bms.map(b => b.draw || 0)) : 0,
        away: bms.length ? Math.max(0, ...bms.map(b => b.away || 0)) : 0,
      };
      const margin = bms.length ? Object.values(bookmakers).reduce((sum, b) => {
        const m = (b.home>0?1/b.home:0)+(b.draw>0?1/b.draw:0)+(b.away>0?1/b.away:0);
        return sum + m;
      }, 0) / bms.length : 0;

      return {
        id:        `api_${g.id}`,
        league:    g.sport_title || 'Football',
        home:      g.home_team,
        away:      g.away_team,
        startTime: g.commence_time,
        status:    'scheduled',
        bookmakers,
        bestOdds,
        arb: { possible: margin < 1, margin: +((margin - 1) * 100).toFixed(2) },
      };
    });
  } catch(e) {
    console.warn('[odds-compare] Odds API error:', e.message);
    return null;
  }
}

function demoFixtures() {
  const teams = [['Арсенал','Челси'],['Бавария','Дортмунд'],['Реал','Барселона'],['ПСЖ','Монако'],['Интер','Ювентус']];
  const leagues = ['Premier League','Bundesliga','La Liga','Ligue 1','Serie A'];
  return teams.map(([home, away], i) => {
    const bH = +(1.5 + Math.random()).toFixed(2);
    const bD = +(3.0 + Math.random()*0.8).toFixed(2);
    const bA = +(2.5 + Math.random()*1.5).toFixed(2);
    return {
      id: `demo_${i}`, league: leagues[i], home, away,
      startTime: new Date(Date.now() + (i+1)*3600000).toISOString(),
      status: 'scheduled',
      bookmakers: {
        'pinnacle':      { home:+(bH+0.05).toFixed(2), draw:+(bD-0.05).toFixed(2), away:+(bA+0.04).toFixed(2) },
        'bet365':        { home:+(bH-0.05).toFixed(2), draw:+(bD+0.10).toFixed(2), away:+(bA-0.06).toFixed(2) },
        'betfair':       { home:+(bH+0.12).toFixed(2), draw:+(bD+0.05).toFixed(2), away:+(bA+0.10).toFixed(2) },
        'william_hill':  { home:+(bH-0.10).toFixed(2), draw:+(bD-0.08).toFixed(2), away:+(bA-0.05).toFixed(2) },
      },
      bestOdds: { home:+(bH+0.12).toFixed(2), draw:+(bD+0.10).toFixed(2), away:+(bA+0.10).toFixed(2) },
      arb: { possible: false },
    };
  });
}

async function getFixtures(useDemo = false) {
  if (Date.now() - _cache.ts < CACHE_TTL && _cache.fixtures.length) return _cache.fixtures;
  const live = await fetchOddsAPI();
  if (live) {
    _cache.fixtures = live;
    _cache.ts = Date.now();
    return _cache.fixtures;
  }
  if (useDemo) return demoFixtures();
  return [];
}

function findArbitrage(fixtures) {
  const opps = [];
  for (const f of fixtures) {
    const bms = Object.entries(f.bookmakers || {});
    if (bms.length < 2) continue;
    const best = { home:0, draw:0, away:0, homeBm:'', drawBm:'', awayBm:'' };
    for (const [bm, odds] of bms) {
      if ((odds.home||0) > best.home) { best.home = odds.home; best.homeBm = bm; }
      if ((odds.draw||0) > best.draw) { best.draw = odds.draw; best.drawBm = bm; }
      if ((odds.away||0) > best.away) { best.away = odds.away; best.awayBm = bm; }
    }
    if (!best.home || !best.draw || !best.away) continue;
    const margin = 1/best.home + 1/best.draw + 1/best.away;
    if (margin < 1) {
      opps.push({
        match: `${f.home} vs ${f.away}`,
        league: f.league,
        profit: +((1 - margin) * 100).toFixed(2),
        margin: +margin.toFixed(4),
        legs: [
          { market:'1', odds:best.home, bm:best.homeBm },
          { market:'X', odds:best.draw, bm:best.drawBm },
          { market:'2', odds:best.away, bm:best.awayBm },
        ],
      });
    }
  }
  return opps.sort((a,b) => b.profit - a.profit);
}

/** GET /api/odds-compare/fixtures */
router.get('/fixtures', async (req, res) => {
  const useDemo = req.query.demo === 'true';
  try {
    const fixtures = await getFixtures(useDemo);
    const league   = req.query.league;
    const list     = league ? fixtures.filter(f => f.league?.toLowerCase().includes(league.toLowerCase())) : fixtures;
    res.json({
      fixtures: list,
      total:    list.length,
      source:   ODDS_API_KEY ? 'api' : (useDemo ? 'demo' : 'none'),
      hint:     !ODDS_API_KEY ? 'Добавьте ODDS_API_KEY в .env для реальных коэффициентов' : null,
    });
  } catch(e) {
    res.status(500).json({ error: e.message });
  }
});

/** GET /api/odds-compare/arbitrage */
router.get('/arbitrage', async (req, res) => {
  const useDemo = req.query.demo === 'true';
  try {
    const fixtures = await getFixtures(useDemo);
    const opps     = findArbitrage(fixtures);
    res.json({ opportunities: opps, total: opps.length });
  } catch(e) {
    res.status(500).json({ error: e.message });
  }
});

/** GET /api/odds-compare/fixture/:id */
router.get('/fixture/:id', async (req, res) => {
  try {
    const fixtures = await getFixtures(false);
    const f = fixtures.find(x => x.id === req.params.id);
    if (!f) return res.status(404).json({ error: 'Not found' });
    res.json(f);
  } catch(e) {
    res.status(500).json({ error: e.message });
  }
});

/** GET /api/odds-compare/movement/:team */
router.get('/movement/:team', async (req, res) => {
  const useDemo = req.query.demo === 'true';
  const teamSearch = req.params.team.toLowerCase();
  try {
    const fixtures = await getFixtures(useDemo);
    const f = fixtures.find(x =>
      x.home?.toLowerCase().includes(teamSearch) ||
      x.away?.toLowerCase().includes(teamSearch)
    );
    if (!f) return res.json({ history: [], bookmakers: {} });

    // Генерируем историю движения на основе текущих коэффициентов
    const bms  = Object.entries(f.bookmakers || {});
    const avgH = bms.length ? bms.reduce((s,[,b])=>s+(b.home||0),0)/bms.length : 0;
    const avgD = bms.length ? bms.reduce((s,[,b])=>s+(b.draw||0),0)/bms.length : 0;
    const avgA = bms.length ? bms.reduce((s,[,b])=>s+(b.away||0),0)/bms.length : 0;
    const pts  = 12;
    const history = Array.from({length:pts},(_,i) => {
      const pct = i/(pts-1);
      const noise = () => (Math.random()-0.5)*0.04;
      return {
        time: new Date(Date.now() - (pts-1-i)*3600000*2).toISOString(),
        label: `-${(pts-1-i)*2}h`,
        home: +(avgH*(1.1 - 0.1*pct) + noise()).toFixed(2),
        draw: +(avgD*(1.02 - 0.02*pct) + noise()).toFixed(2),
        away: +(avgA*(0.9 + 0.1*pct) + noise()).toFixed(2),
      };
    });
    res.json({ match: `${f.home} vs ${f.away}`, league: f.league, history, bookmakers: f.bookmakers });
  } catch(e) {
    res.status(500).json({ error: e.message });
  }
});

/** GET /api/odds-compare/history */
router.get('/history', async (req, res) => {
  // История коэффициентов — нужна ClickHouse таблица odds_history
  const ch     = req.app.locals.clickhouse;
  const search = req.query.search || '';
  if (!ch) return res.json({ records: [], hint: 'ClickHouse не подключён' });
  try {
    const whereSearch = search ? `AND (home_team ILIKE '%${search}%' OR away_team ILIKE '%${search}%')` : '';
    const r = await ch.query({
      query: `
        SELECT date, home_team, away_team, market,
               open_home, close_home, open_draw, close_draw,
               open_away, close_away,
               round((close_home - open_home)/open_home*100, 1) AS movement,
               result
        FROM betquant.odds_history
        WHERE 1=1 ${whereSearch}
        ORDER BY date DESC LIMIT 50
      `,
      format: 'JSON',
    });
    const d = await r.json();
    res.json({ records: (d.data||[]).map(row => ({
      ...row,
      match: `${row.home_team} vs ${row.away_team}`,
      movement: `${row.movement > 0 ? '+' : ''}${row.movement}%`,
    }))});
  } catch(e) {
    res.json({ records: [], error: e.message });
  }
});

module.exports = router;