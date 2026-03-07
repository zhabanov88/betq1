'use strict';
/**
 * BetQuant Pro — Odds Compare API  /api/odds-compare/*
 *
 * GET /api/odds-compare/fixtures    — список матчей с коэффициентами 8 букмекеров
 * GET /api/odds-compare/fixture/:id — детали одного матча
 * GET /api/odds-compare/arbitrage   — арбитражные ситуации
 * GET /api/odds-compare/movement/:id — история движения линий
 *
 * Источники:
 *  • Если задан ODDS_API_KEY → The Odds API (https://the-odds-api.com)
 *  • Иначе → встроенные demo-данные (всё работает без ключа)
 */

const express = require('express');
const router  = express.Router();

const ODDS_API_KEY = process.env.ODDS_API_KEY || '';

// ─── Cache ────────────────────────────────────────────────────────────────
let _cache = { fixtures: [], ts: 0 };
const CACHE_TTL = 5 * 60 * 1000; // 5 минут

// ─── Demo data ────────────────────────────────────────────────────────────
function demoFixtures() {
  const now = Date.now();
  const fwd = h => new Date(now + h * 3600000).toISOString();

  const BOOKMAKERS = ['pinnacle','bet365','betfair','unibet','williamhill','bwin','1xbet','betway'];

  function generateOdds(baseH, baseD, baseA) {
    const result = {};
    for (const bm of BOOKMAKERS) {
      const n = () => 1 + (Math.random() - 0.5) * 0.18;
      const h = +(baseH * n()).toFixed(2);
      const d = +(baseD * n()).toFixed(2);
      const a = +(baseA * n()).toFixed(2);
      // Pinnacle — самые острые линии (минимальная маржа)
      const margin = bm === 'pinnacle' ? 1.035 : bm === 'betfair' ? 1.04 : 1.06 + Math.random() * 0.04;
      const o25 = +(1.80 * n() * (margin / 1.05)).toFixed(2);
      const u25 = +(2.05 * n() * (margin / 1.05)).toFixed(2);
      result[bm] = { home: h, draw: d, away: a, over25: o25, under25: u25 };
    }
    return result;
  }

  function detectArb(bookmakers) {
    const bms = Object.entries(bookmakers);
    let minSum = Infinity;
    let bestLegs = [];

    // 3-way arb: 1X2
    const bestH = bms.reduce((b, [k,v]) => v.home > b.o ? { bm:k, o:v.home } : b, { bm:'', o:0 });
    const bestD = bms.reduce((b, [k,v]) => v.draw > b.o ? { bm:k, o:v.draw } : b, { bm:'', o:0 });
    const bestA = bms.reduce((b, [k,v]) => v.away > b.o ? { bm:k, o:v.away } : b, { bm:'', o:0 });

    const sumInv = 1/bestH.o + 1/bestD.o + 1/bestA.o;
    if (sumInv < 1) {
      bestLegs = [
        { outcome:'home', bm: bestH.bm, odds: bestH.o, stake: +(100 / bestH.o / sumInv).toFixed(2) },
        { outcome:'draw', bm: bestD.bm, odds: bestD.o, stake: +(100 / bestD.o / sumInv).toFixed(2) },
        { outcome:'away', bm: bestA.bm, odds: bestA.o, stake: +(100 / bestA.o / sumInv).toFixed(2) },
      ];
      return { possible: true, profit: +((1/sumInv - 1) * 100).toFixed(2), legs: bestLegs };
    }
    return { possible: false };
  }

  const fixtures = [
    { id:'oc1', league:'Premier League', home:'Arsenal',       away:'Chelsea',        baseH:1.85, baseD:3.50, baseA:4.20, hours:2 },
    { id:'oc2', league:'Premier League', home:'Liverpool',     away:'Man City',       baseH:2.20, baseD:3.30, baseA:3.20, hours:5 },
    { id:'oc3', league:'La Liga',        home:'Real Madrid',   away:'Barcelona',      baseH:2.10, baseD:3.40, baseA:3.50, hours:6 },
    { id:'oc4', league:'Bundesliga',     home:'Bayern Munich', away:'Dortmund',       baseH:1.55, baseD:4.20, baseA:6.50, hours:3 },
    { id:'oc5', league:'Serie A',        home:'Inter Milan',   away:'AC Milan',       baseH:2.30, baseD:3.10, baseA:3.20, hours:8 },
    { id:'oc6', league:'La Liga',        home:'Atletico',      away:'Sevilla',        baseH:1.90, baseD:3.40, baseA:4.10, hours:9 },
    { id:'oc7', league:'Champions League', home:'PSG',         away:'Manchester City',baseH:2.60, baseD:3.20, baseA:2.80, hours:26 },
    { id:'oc8', league:'Premier League', home:'Tottenham',     away:'Newcastle',      baseH:2.00, baseD:3.30, baseA:3.80, hours:4 },
  ];

  return fixtures.map(f => {
    const bookmakers = generateOdds(f.baseH, f.baseD, f.baseA);
    const arb        = detectArb(bookmakers);
    return {
      id:         f.id,
      league:     f.league,
      home:       f.home,
      away:       f.away,
      startTime:  fwd(f.hours),
      status:     'scheduled',
      bookmakers,
      arb,
      bestOdds: {
        home: Math.max(...Object.values(bookmakers).map(b => b.home)),
        draw: Math.max(...Object.values(bookmakers).map(b => b.draw)),
        away: Math.max(...Object.values(bookmakers).map(b => b.away)),
      },
    };
  });
}

// ─── The Odds API integration ─────────────────────────────────────────────
async function fetchFromOddsAPI() {
  if (!ODDS_API_KEY) return null;
  try {
    const ctrl = typeof AbortController !== 'undefined' ? new AbortController() : null;
    const t    = ctrl ? setTimeout(() => ctrl.abort(), 8000) : null;
    const r    = await fetch(
      `https://api.the-odds-api.com/v4/sports/soccer_epl/odds/?apiKey=${ODDS_API_KEY}&regions=eu&markets=h2h,totals&oddsFormat=decimal`,
      { signal: ctrl?.signal }
    );
    if (t) clearTimeout(t);
    if (!r.ok) throw new Error(`Odds API ${r.status}`);
    const data = await r.json();
    if (!Array.isArray(data) || !data.length) return null;

    return data.slice(0, 10).map((g, i) => {
      const bookmakers = {};
      for (const bm of (g.bookmakers || [])) {
        const h2h     = bm.markets?.find(m => m.key === 'h2h');
        const totals  = bm.markets?.find(m => m.key === 'totals');
        if (!h2h) continue;
        const home = h2h.outcomes.find(o => o.name === g.home_team)?.price || 0;
        const away = h2h.outcomes.find(o => o.name === g.away_team)?.price || 0;
        const draw = h2h.outcomes.find(o => o.name === 'Draw')?.price     || 0;
        const over = totals?.outcomes.find(o => o.name === 'Over')?.price  || 0;
        const under= totals?.outcomes.find(o => o.name === 'Under')?.price || 0;
        bookmakers[bm.key] = { home, draw, away, over25: over, under25: under };
      }
      return {
        id:        `api_${g.id}`,
        league:    g.sport_title || 'Football',
        home:      g.home_team,
        away:      g.away_team,
        startTime: g.commence_time,
        status:    'scheduled',
        bookmakers,
        arb:       { possible: false },
        bestOdds: {
          home: Math.max(0, ...Object.values(bookmakers).map(b => b.home || 0)),
          draw: Math.max(0, ...Object.values(bookmakers).map(b => b.draw || 0)),
          away: Math.max(0, ...Object.values(bookmakers).map(b => b.away || 0)),
        },
      };
    });
  } catch(e) {
    console.warn('[odds-compare] Odds API error:', e.message);
    return null;
  }
}

// ─── Refresh cache ────────────────────────────────────────────────────────
async function getFixtures() {
  if (Date.now() - _cache.ts < CACHE_TTL && _cache.fixtures.length) {
    return _cache.fixtures;
  }
  const live = await fetchFromOddsAPI();
  _cache.fixtures = live || demoFixtures();
  _cache.ts       = Date.now();
  return _cache.fixtures;
}

// ─── Routes ───────────────────────────────────────────────────────────────

/** GET /api/odds-compare/fixtures */
router.get('/fixtures', async (req, res) => {
  try {
    const fixtures = await getFixtures();
    const league   = req.query.league;
    const list     = league ? fixtures.filter(f => f.league.toLowerCase().includes(league.toLowerCase())) : fixtures;
    res.json({
      fixtures:   list,
      total:      list.length,
      source:     ODDS_API_KEY ? 'the-odds-api' : 'demo',
      lastUpdate: _cache.ts,
    });
  } catch(e) {
    res.status(500).json({ error: e.message });
  }
});

/** GET /api/odds-compare/fixture/:id */
router.get('/fixture/:id', async (req, res) => {
  try {
    const fixtures = await getFixtures();
    const f = fixtures.find(x => x.id === req.params.id);
    if (!f) return res.status(404).json({ error: 'Fixture not found' });
    res.json(f);
  } catch(e) {
    res.status(500).json({ error: e.message });
  }
});

/** GET /api/odds-compare/arbitrage */
router.get('/arbitrage', async (req, res) => {
  try {
    const fixtures = await getFixtures();
    const opportunities = fixtures
      .filter(f => f.arb?.possible)
      .map(f => ({
        id:        f.id,
        league:    f.league,
        match:     `${f.home} vs ${f.away}`,
        startTime: f.startTime,
        profit:    f.arb.profit,
        legs:      f.arb.legs || [],
      }));
    res.json({ opportunities, total: opportunities.length });
  } catch(e) {
    res.status(500).json({ error: e.message });
  }
});

/** GET /api/odds-compare/movement/:id */
router.get('/movement/:id', async (req, res) => {
  try {
    const fixtures = await getFixtures();
    const f = fixtures.find(x => x.id === req.params.id);
    if (!f) return res.status(404).json({ error: 'Fixture not found' });

    // Генерируем демо-историю движения линий (реальная — из Odds API с timestamps)
    const start     = new Date(f.startTime).getTime();
    const now       = Date.now();
    const hoursLeft = (start - now) / 3600000;
    const points    = [];

    for (let h = Math.min(48, Math.ceil(hoursLeft + 4)); h >= 0; h -= 3) {
      const drift = (Math.random() - 0.5) * 0.12;
      points.push({
        t:    new Date(start - h * 3600000).toISOString(),
        home: Math.max(1.01, +(f.bestOdds.home + drift).toFixed(2)),
        draw: Math.max(1.01, +(f.bestOdds.draw - drift * 0.5).toFixed(2)),
        away: Math.max(1.01, +(f.bestOdds.away - drift).toFixed(2)),
      });
    }

    res.json({ fixtureId: f.id, match: `${f.home} vs ${f.away}`, history: points });
  } catch(e) {
    res.status(500).json({ error: e.message });
  }
});

module.exports = router;