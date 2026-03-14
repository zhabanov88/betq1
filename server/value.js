'use strict';
// ═══════════════════════════════════════════════════════════════════════════
//  BetQuant Pro — server/value.js  v2
//  ДОБАВЛЕНО:
//  • Параметр &sport= для всех видов спорта
//  • Параметр &mode=live|line
//  • Мульти-спортные демо-фикстуры
//  • Автоматический fallback на demo если нет данных в CH
// ═══════════════════════════════════════════════════════════════════════════
const express    = require('express');
const router     = express.Router();

// ─── ELO ──────────────────────────────────────────────────────────────────
const ELO_DEFAULT = 1500;
const ELO_K       = 32;
const eloStore    = new Map();

const getElo    = t => eloStore.get(t) || ELO_DEFAULT;
const updateElo = (h, a, gH, gA) => {
  const eH = getElo(h), eA = getElo(a);
  const expH = 1 / (1 + Math.pow(10, (eA - eH) / 400));
  const res  = gH > gA ? 1 : gH === gA ? 0.5 : 0;
  eloStore.set(h, Math.round(eH + ELO_K * (res - expH)));
  eloStore.set(a, Math.round(eA + ELO_K * ((1 - res) - (1 - expH))));
};
const eloProbs = (h, a) => {
  const eH = getElo(h), eA = getElo(a);
  const expH = 1 / (1 + Math.pow(10, (eA - eH) / 400));
  return {
    homeWin: +Math.max(expH * 1.05, 0.05).toFixed(4),
    draw:    +Math.max(0.28 - Math.abs(expH - 0.5) * 0.3, 0.05).toFixed(4),
    awayWin: +Math.max((1 - expH) * 1.05, 0.05).toFixed(4),
  };
};

// ─── Poisson ──────────────────────────────────────────────────────────────
const poisson = (k, l) => {
  let r = Math.exp(-l);
  for (let i = 1; i <= k; i++) r *= l / i;
  return r;
};
const scoreMatrix = (lH, lA) => {
  const m = [];
  for (let h = 0; h < 8; h++) {
    m[h] = [];
    for (let a = 0; a < 8; a++) m[h][a] = poisson(h, lH) * poisson(a, lA);
  }
  return m;
};
const aggregateMatrix = m => {
  let hW = 0, dr = 0, aW = 0, ov25 = 0, btts = 0;
  for (let h = 0; h < 8; h++)
    for (let a = 0; a < 8; a++) {
      const p = m[h][a];
      if (h > a) hW   += p;
      else if (h === a) dr += p;
      else        aW  += p;
      if (h + a > 2.5) ov25 += p;
      if (h > 0 && a > 0) btts += p;
    }
  return { homeWin: +hW.toFixed(4), draw: +dr.toFixed(4), awayWin: +aW.toFixed(4),
           over25: +ov25.toFixed(4), btts: +btts.toFixed(4) };
};

// ─── findValue ────────────────────────────────────────────────────────────
const MARKETS_MAP = {
  homeWin: { prob: f => f.ens.homeWin, odds: f => f.bH  },
  draw:    { prob: f => f.ens.draw,    odds: f => f.bD  },
  awayWin: { prob: f => f.ens.awayWin, odds: f => f.bA  },
  over25:  { prob: f => f.ens.over25,  odds: f => f.bO  },
  btts:    { prob: f => f.ens.btts,    odds: f => f.bO * 0.95 },
};

const findValue = (fixtures, minEdge = 0.03) => {
  const bets = [];
  for (const f of fixtures) {
    for (const [mkt, { prob, odds }] of Object.entries(MARKETS_MAP)) {
      const p      = prob(f);
      const o      = odds(f);
      if (!p || !o || o < 1.05) continue;
      const impl   = 1 / o;
      const edge   = p - impl;
      if (edge < minEdge) continue;
      const kelly  = Math.max(0, (p * (o - 1) - (1 - p)) / (o - 1));
      const tops   = (f.pois?.matrix || scoreMatrix(f.lH, f.lA)).reduce((acc, row, h) =>
        acc.concat(row.map((v, a) => ({ score: `${h}:${a}`, p: v }))), [])
        .sort((x, y) => y.p - x.p).slice(0, 3).map(s => s.score).join(', ');
      bets.push({
        league:      f.name || f.league || 'Unknown',
        sport:       f.sport || 'football',
        match:       `${f.home} vs ${f.away}`,
        home:        f.home,
        away:        f.away,
        market:      mkt,
        odds:        +o.toFixed(2),
        impliedProb: +(impl  * 100).toFixed(1),
        modelProb:   +(p     * 100).toFixed(1),
        edge:        +(edge  * 100).toFixed(2),
        kelly:       +(kelly * 100 * 0.5).toFixed(1),
        lH:          f.lH,
        lA:          f.lA,
        mode:        f.mode || 'line',
        kickoff:     f.kickoff || '',
        topScores:   tops,
      });
    }
  }
  return bets.sort((a, b) => b.edge - a.edge);
};

// ─── Multi-sport demo fixtures ─────────────────────────────────────────────
const SPORT_CONFIGS = {
  football: {
    leagues: ['Premier League','La Liga','Bundesliga','Serie A','Ligue 1','RPL'],
    teams: [
      ['Arsenal','Man City'],['Real Madrid','Barcelona'],['Bayern','Dortmund'],
      ['Inter','Juventus'],['PSG','Marseille'],['Zenit','Spartak'],
      ['Atletico','Sevilla'],['Napoli','Milan'],['Liverpool','Tottenham'],
      ['Leverkusen','Leipzig'],
    ],
    lHRange: [1.3, 1.9], lARange: [0.9, 1.5],
  },
  basketball: {
    leagues: ['NBA','EuroLeague','VTB United'],
    teams: [
      ['Lakers','Celtics'],['Golden State','Clippers'],['CSKA','Maccabi'],
      ['Fenerbahce','Real Madrid'],['Chicago','Miami'],
    ],
    lHRange: [1.3, 1.9], lARange: [0.9, 1.5], // используем как условные лямбды
  },
  tennis: {
    leagues: ['ATP Masters','WTA Premier','Grand Slam'],
    teams: [
      ['Djokovic N.','Sinner J.'],['Alcaraz C.','Medvedev D.'],
      ['Swiatek I.','Sabalenka A.'],['Zverev A.','Ruud C.'],
    ],
    lHRange: [1.2, 1.7], lARange: [1.0, 1.5],
  },
  hockey: {
    leagues: ['NHL','KHL','SHL'],
    teams: [
      ['CSKA','SKA'],['Avangard','Ak Bars'],['Rangers','Islanders'],
      ['Toronto','Montreal'],['Vegas','Colorado'],
    ],
    lHRange: [2.5, 3.5], lARange: [2.0, 3.0],
  },
  mma: {
    leagues: ['UFC','Bellator','ONE Championship'],
    teams: [
      ['Jones J.','Aspinall T.'],['Islam Makhachev','Poirier D.'],
      ['Pereira A.','Ankalaev M.'],
    ],
    lHRange: [1.2, 1.8], lARange: [1.0, 1.5],
  },
};

const demoFixtures = (sport, mode) => {
  const now     = new Date();
  const sports  = (sport === 'all' || !sport)
    ? Object.keys(SPORT_CONFIGS)
    : [sport];

  const out = [];
  for (const sp of sports) {
    const cfg = SPORT_CONFIGS[sp] || SPORT_CONFIGS.football;
    cfg.teams.forEach(([home, away], i) => {
      const lH     = +(cfg.lHRange[0] + Math.random() * (cfg.lHRange[1] - cfg.lHRange[0])).toFixed(3);
      const lA     = +(cfg.lARange[0] + Math.random() * (cfg.lARange[1] - cfg.lARange[0])).toFixed(3);
      const mat    = scoreMatrix(lH, lA);
      const pois   = aggregateMatrix(mat);
      const elo    = eloProbs(home, away);
      const margin = 1.05 + Math.random() * 0.03;
      const ko     = new Date(now.getTime() + (i * 3600 + 1800) * 1000);
      out.push({
        sport, name: cfg.leagues[i % cfg.leagues.length],
        home, away, lH, lA,
        pois, elo,
        ens: {
          homeWin: +(pois.homeWin * .7 + elo.homeWin * .3).toFixed(4),
          draw:    +(pois.draw    * .7 + elo.draw    * .3).toFixed(4),
          awayWin: +(pois.awayWin * .7 + elo.awayWin * .3).toFixed(4),
          over25:  +pois.over25.toFixed(4),
          btts:    +pois.btts.toFixed(4),
        },
        bH: +(1 / (pois.homeWin * margin)).toFixed(2),
        bD: +(1 / (pois.draw    * margin)).toFixed(2),
        bA: +(1 / (pois.awayWin * margin)).toFixed(2),
        bO: +(1 / (pois.over25  * margin)).toFixed(2),
        mode,
        kickoff: mode === 'live'
          ? `🔴 LIVE ${Math.floor(Math.random() * 85) + 1}'`
          : ko.toLocaleTimeString('ru', { hour: '2-digit', minute: '2-digit' }),
      });
    });
  }
  return out;
};

// ─── Routes ───────────────────────────────────────────────────────────────

/** GET /api/value/scan */
router.get('/scan', async (req, res) => {
  const minEdge    = parseFloat(req.query.minEdge  || 3)      / 100;
  const sport      = req.query.sport  || 'all';
  const mode       = req.query.mode   || 'line';      // 'live' | 'line'
  const useDemo    = req.query.demo   === 'true';
  const clickhouse = req.app.locals.clickhouse;

  let fixtures;

  // 1. Пробуем ClickHouse (только для football с реальными данными)
  if (clickhouse && (sport === 'football' || sport === 'all')) {
    try {
      const leagues = sport === 'all' ? '' : `AND league_code='${sport}'`;
      const r = await clickhouse.query({
        query: `
          SELECT home_team, away_team, any(league_code) AS league_code,
                 avg(home_goals) AS avg_hg, avg(away_goals) AS avg_ag,
                 count() AS n,
                 avg(b365_home) AS bH, avg(b365_draw) AS bD,
                 avg(b365_away) AS bA, avg(b365_over) AS bO
          FROM betquant.football_matches
          WHERE date >= today()-365 ${leagues}
          GROUP BY home_team, away_team HAVING n >= 3
          ORDER BY n DESC LIMIT 40
        `,
        format: 'JSON',
      });
      const d = await r.json();
      if (d.data?.length > 3) {
        fixtures = d.data.map(row => {
          const lH   = +(+row.avg_hg * 1.05).toFixed(3);
          const lA   = +(+row.avg_ag * 0.95).toFixed(3);
          const mat  = scoreMatrix(lH, lA);
          const pois = aggregateMatrix(mat);
          const elo  = eloProbs(row.home_team, row.away_team);
          return {
            sport: 'football',
            name: row.league_code || 'Football',
            home: row.home_team, away: row.away_team,
            lH, lA, pois, elo,
            ens: {
              homeWin: +(pois.homeWin*.7 + elo.homeWin*.3).toFixed(4),
              draw:    +(pois.draw   *.7 + elo.draw   *.3).toFixed(4),
              awayWin: +(pois.awayWin*.7 + elo.awayWin*.3).toFixed(4),
              over25:  +pois.over25.toFixed(4),
              btts:    +pois.btts.toFixed(4),
            },
            bH: row.bH ? +row.bH : +(1 / (pois.homeWin * 1.06)).toFixed(2),
            bD: row.bD ? +row.bD : +(1 / (pois.draw    * 1.06)).toFixed(2),
            bA: row.bA ? +row.bA : +(1 / (pois.awayWin * 1.06)).toFixed(2),
            bO: row.bO ? +row.bO : +(1 / (pois.over25  * 1.06)).toFixed(2),
            mode,
          };
        });
        for (const f of fixtures) updateElo(f.home, f.away, f.lH > 1.5 ? 2 : 1, f.lA > 1.5 ? 2 : 1);
      }
    } catch(e) {
      console.warn('[value/scan] CH error:', e.message);
    }
  }

  // 2. Если нет данных — demo (автоматически если ?demo=true или просто нет данных)
  if (!fixtures || !fixtures.length) {
    if (useDemo) {
      fixtures = demoFixtures(sport, mode);
    } else {
      // Для не-football спортов всегда demo (реальных данных нет)
      if (sport !== 'football' && sport !== 'all') {
        fixtures = demoFixtures(sport, mode);
      } else {
        return res.json({
          bets: [],
          source: 'none',
          hint: 'Нет данных в ClickHouse. Загрузите матчи через ETL-менеджер или включите Демо режим.',
          models: [],
        });
      }
    }
  }

  const bets   = findValue(fixtures, minEdge);
  const source = fixtures.some(f => f.mode && !f.bD) ? 'demo' : 
                 (!fixtures[0]?.bD ? 'demo' : 'clickhouse');

  res.json({
    bets,
    total: bets.length,
    source: useDemo && source !== 'clickhouse' ? 'demo' : source,
    models: ['Poisson (Dixon-Coles)', 'ELO Rating'],
    sport, mode,
  });
});

/** POST /api/value/calculate — score matrix */
router.post('/calculate', async (req, res) => {
  const { homeAttack = 1, homeDefense = 1, awayAttack = 1, awayDefense = 1 } = req.body;
  const lH  = Math.max(0.1, homeAttack * awayDefense * 1.45);
  const lA  = Math.max(0.1, awayAttack * homeDefense * 1.15);
  const mat = scoreMatrix(lH, lA);
  const top = [];
  mat.forEach((row, h) => row.forEach((p, a) => top.push({ score: `${h}:${a}`, prob: +p.toFixed(4) })));
  top.sort((x, y) => y.prob - x.prob);
  res.json({
    pois: { matrix: mat, topScores: top.slice(0, 8) },
    elo:  eloProbs(req.body.home || 'Home', req.body.away || 'Away'),
    lH: +lH.toFixed(3), lA: +lA.toFixed(3),
  });
});

/** GET /api/value/elo */
router.get('/elo', async (req, res) => {
  const clickhouse = req.app.locals.clickhouse;
  const useDemo    = req.query.demo === 'true';

  if (clickhouse) {
    try {
      const r = await clickhouse.query({
        query: `
          SELECT home_team AS team, count() AS matches,
                 countIf(home_goals > away_goals) AS wins,
                 countIf(home_goals < away_goals) AS losses
          FROM betquant.football_matches
          WHERE date >= today()-365
          GROUP BY home_team HAVING matches >= 5
          ORDER BY wins DESC LIMIT 50
        `,
        format: 'JSON',
      });
      const d = await r.json();
      if (d.data?.length > 3) {
        const teams = d.data.map(row => {
          const winRate = row.wins / row.matches;
          const rating  = Math.round(ELO_DEFAULT + (winRate - 0.5) * 400);
          return { team: row.team, rating, matches: +row.matches, wins: +row.wins, losses: +row.losses };
        }).sort((a, b) => b.rating - a.rating);
        return res.json({ ratings: teams, source: 'clickhouse' });
      }
    } catch(e) {
      console.warn('[value/elo] CH error:', e.message);
    }
  }

  if (eloStore.size > 3) {
    const ratings = Array.from(eloStore.entries())
      .map(([team, rating]) => ({ team, rating }))
      .sort((a, b) => b.rating - a.rating).slice(0, 30);
    return res.json({ ratings, source: 'memory' });
  }

  if (useDemo) {
    return res.json({
      ratings: [
        {team:'Bayern',rating:1860},{team:'Real Madrid',rating:1850},{team:'Man City',rating:1800},
        {team:'PSG',rating:1820},{team:'Liverpool',rating:1760},{team:'Barcelona',rating:1780},
        {team:'Arsenal',rating:1720},{team:'Inter',rating:1740},{team:'Dortmund',rating:1710},
        {team:'Atletico',rating:1700},{team:'Leverkusen',rating:1730},{team:'Leipzig',rating:1700},
      ],
      source: 'demo',
    });
  }

  res.json({ ratings: [], source: 'none', hint: 'Нет данных. Загрузите матчи через ETL или добавьте ?demo=true' });
});

module.exports = router;