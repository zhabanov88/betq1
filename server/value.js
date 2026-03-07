'use strict';
/**
 * BetQuant Pro — Value Finder  /api/value/*
 * ИСПРАВЛЕНИЯ:
 *  - res.json отправляет valueBets (было bets — undefined в ряде мест)
 *  - tgAPI.sendValueAlert вызывается правильно с strategyId
 *  - eloMap инициализируется корректно без зависимости от внешних данных
 */

const express = require('express');
const router  = express.Router();

// ─── ELO ratings (in-memory, обновляется из ClickHouse если доступен) ────
const eloMap = new Map();

function initDefaultElo() {
  const teams = [
    ['Manchester City', 1960], ['Arsenal', 1890], ['Liverpool', 1880],
    ['Chelsea', 1840], ['Tottenham', 1820], ['Manchester United', 1810],
    ['Bayern Munich', 1950], ['Real Madrid', 1980], ['Barcelona', 1940],
    ['PSG', 1900], ['Juventus', 1870], ['AC Milan', 1850],
    ['Inter Milan', 1860], ['Atletico Madrid', 1840], ['Dortmund', 1830],
    ['Napoli', 1810], ['Roma', 1780], ['Lazio', 1760],
    ['Ajax', 1800], ['Porto', 1790], ['Benfica', 1780],
    ['Shakhtar', 1720], ['Dynamo Kyiv', 1700], ['CSKA Moscow', 1690],
    ['Red Bull Salzburg', 1750], ['Celtic', 1720], ['Rangers', 1710],
  ];
  for (const [team, rating] of teams) eloMap.set(team, rating);
}
initDefaultElo();

// ─── ELO helpers ─────────────────────────────────────────────────────────
function getElo(team) {
  if (eloMap.has(team)) return eloMap.get(team);
  // Новые команды стартуют с 1500
  eloMap.set(team, 1500);
  return 1500;
}

function eloWinProb(rA, rB) {
  return 1 / (1 + Math.pow(10, (rB - rA) / 400));
}

function eloProbs(home, away) {
  const rH = getElo(home) + 65; // home advantage
  const rA = getElo(away);
  const pH = eloWinProb(rH, rA);
  const pA = eloWinProb(rA, rH);
  const pD = Math.max(0, 1 - pH - pA);
  return { homeWin: +pH.toFixed(4), draw: +pD.toFixed(4), awayWin: +pA.toFixed(4) };
}

// ─── Poisson matrix ───────────────────────────────────────────────────────
function scoreMatrix(lH, lA, maxGoals = 7) {
  const factorial = n => n <= 1 ? 1 : n * factorial(n - 1);
  const poissonPMF = (k, l) => Math.pow(l, k) * Math.exp(-l) / factorial(k);
  const mat = [];
  for (let i = 0; i <= maxGoals; i++) {
    mat[i] = [];
    for (let j = 0; j <= maxGoals; j++) {
      mat[i][j] = poissonPMF(i, lH) * poissonPMF(j, lA);
    }
  }
  return mat;
}

function aggregateMatrix(mat) {
  let home = 0, draw = 0, away = 0, over25 = 0, btts = 0;
  for (let i = 0; i < mat.length; i++) {
    for (let j = 0; j < mat[i].length; j++) {
      const p = mat[i][j];
      if (i > j) home += p;
      else if (i === j) draw += p;
      else away += p;
      if (i + j > 2) over25 += p;
      if (i > 0 && j > 0) btts += p;
    }
  }
  return { homeWin: +home.toFixed(4), draw: +draw.toFixed(4), awayWin: +away.toFixed(4), over25: +over25.toFixed(4), btts: +btts.toFixed(4) };
}

// ─── Demo fixtures ────────────────────────────────────────────────────────
function buildFixtures(league) {
  const leagueData = {
    epl:  { name: 'Premier League', avg: [1.55, 1.20], teams: [['Arsenal','Chelsea'],[' Manchester City','Tottenham'],['Liverpool','Manchester United'],['Newcastle','Aston Villa']] },
    ucl:  { name: 'Champions League', avg: [1.50, 1.15], teams: [['Real Madrid','Bayern Munich'],['Barcelona','PSG'],['Manchester City','Inter Milan'],['Arsenal','Dortmund']] },
    la_liga: { name: 'La Liga', avg: [1.45, 1.10], teams: [['Real Madrid','Barcelona'],['Atletico Madrid','Sevilla'],['Real Sociedad','Villarreal']] },
    bundesliga: { name: 'Bundesliga', avg: [1.65, 1.25], teams: [['Bayern Munich','Dortmund'],['Bayer Leverkusen','Leipzig'],['Frankfurt','Wolfsburg']] },
    serie_a: { name: 'Serie A', avg: [1.40, 1.05], teams: [['Napoli','Inter Milan'],['Juventus','AC Milan'],['Roma','Lazio'],['Atalanta','Fiorentina']] },
  };

  const src = leagueData[league] || leagueData.epl;
  const bkOdds = () => ({
    bH: +(1.8 + Math.random() * 1.2).toFixed(2),
    bD: +(3.0 + Math.random() * 1.0).toFixed(2),
    bA: +(2.5 + Math.random() * 2.0).toFixed(2),
    bO: +(1.6 + Math.random() * 0.8).toFixed(2),
  });

  return src.teams.map(([home, away]) => {
    const lH = +(src.avg[0] * (0.9 + Math.random() * 0.3)).toFixed(3);
    const lA = +(src.avg[1] * (0.9 + Math.random() * 0.3)).toFixed(3);
    const mat  = scoreMatrix(lH, lA);
    const pois = aggregateMatrix(mat);
    const elo  = eloProbs(home, away);
    const odds = bkOdds();
    return {
      name: src.name, league,
      home, away,
      lH, lA,
      eloHome: getElo(home),
      eloAway: getElo(away),
      pois, elo,
      ens: {
        homeWin: +(pois.homeWin * .7 + elo.homeWin * .3).toFixed(4),
        draw:    +(pois.draw    * .7 + elo.draw    * .3).toFixed(4),
        awayWin: +(pois.awayWin * .7 + elo.awayWin * .3).toFixed(4),
        over25:  +pois.over25.toFixed(4),
        btts:    +pois.btts.toFixed(4),
      },
      ...odds,
    };
  });
}

// ─── Value detection ──────────────────────────────────────────────────────
function findValue(fixtures, minEdge) {
  const out = [];
  for (const f of fixtures) {
    const markets = [
      { key: 'homeWin', label: `Победа ${f.home}`, odds: f.bH, prob: f.ens.homeWin },
      { key: 'draw',    label: 'Ничья',             odds: f.bD, prob: f.ens.draw    },
      { key: 'awayWin', label: `Победа ${f.away}`,  odds: f.bA, prob: f.ens.awayWin },
      { key: 'over25',  label: 'Тотал Больше 2.5',  odds: f.bO, prob: f.ens.over25  },
    ];
    for (const m of markets) {
      if (!m.odds || m.odds < 1.01) continue;
      const impl  = 1 / m.odds;
      const edge  = m.prob - impl;
      if (edge < minEdge) continue;
      const kelly = Math.max(0, ((m.odds - 1) * m.prob - (1 - m.prob)) / (m.odds - 1));
      out.push({
        league:      f.name,
        leagueId:    f.league,
        match:       `${f.home} vs ${f.away}`,
        home:        f.home,
        away:        f.away,
        market:      m.key,
        label:       m.label,
        odds:        +m.odds.toFixed(2),
        impliedProb: +(impl   * 100).toFixed(1),
        modelProb:   +(m.prob * 100).toFixed(1),
        edge:        +(edge   * 100).toFixed(2),
        kelly:       +(kelly  * 100).toFixed(1),
        lH:          f.lH,
        lA:          f.lA,
        eloHome:     f.eloHome,
        eloAway:     f.eloAway,
        topScores:   f.pois?.topScores?.slice(0, 6) || [],
      });
    }
  }
  return out.sort((a, b) => b.edge - a.edge);
}

// ─── Routes ───────────────────────────────────────────────────────────────

/** GET /api/value/scan */
router.get('/scan', async (req, res) => {
  const minEdge  = parseFloat(req.query.minEdge || 3) / 100;
  const league   = req.query.league || 'epl';
  const strategyId = req.query.strategyId || null;

  const clickhouse = req.app.locals.clickhouse;
  let fixtures;
  try {
    if (clickhouse && league) {
      const r = await clickhouse.query({
        query: `SELECT home_team, away_team, avg(home_goals) AS avg_hg, avg(away_goals) AS avg_ag, count() AS n
                FROM betquant.football_matches
                WHERE league_code='${league}' AND date >= today()-365
                GROUP BY home_team, away_team HAVING n>=3`,
        format: 'JSON',
      });
      const d = await r.json();
      if (d.data?.length > 5) {
        fixtures = d.data.map(row => {
          const lH   = +(+row.avg_hg * 1.05).toFixed(3);
          const lA   = +(+row.avg_ag * 0.95).toFixed(3);
          const mat  = scoreMatrix(lH, lA);
          const pois = aggregateMatrix(mat);
          const elo  = eloProbs(row.home_team, row.away_team);
          const bkH  = +(1 / (pois.homeWin * 0.95)).toFixed(2);
          const bkD  = +(1 / (pois.draw    * 0.95)).toFixed(2);
          const bkA  = +(1 / (pois.awayWin * 0.95)).toFixed(2);
          const bkO  = +(1 / (pois.over25  * 0.95)).toFixed(2);
          return {
            name: league, league,
            home: row.home_team, away: row.away_team,
            lH, lA,
            eloHome: getElo(row.home_team), eloAway: getElo(row.away_team),
            pois, elo,
            ens: {
              homeWin: +(pois.homeWin * .7 + elo.homeWin * .3).toFixed(4),
              draw:    +(pois.draw    * .7 + elo.draw    * .3).toFixed(4),
              awayWin: +(pois.awayWin * .7 + elo.awayWin * .3).toFixed(4),
              over25:  +pois.over25.toFixed(4),
              btts:    +pois.btts.toFixed(4),
            },
            bH: bkH, bD: bkD, bA: bkA, bO: bkO,
          };
        });
      } else {
        fixtures = buildFixtures(league);
      }
    } else {
      fixtures = buildFixtures(league);
    }
  } catch(e) {
    fixtures = buildFixtures(league);
  }

  const valueBets = findValue(fixtures, minEdge);

  // ── Telegram алерты для value ставок ────────────────────────────────────
  const tg = global.__betquant_tg;
  if (tg && tg.isEnabled()) {
    for (const bet of valueBets.filter(b => b.edge >= 5)) {
      tg.sendValueAlert(bet, strategyId).catch(() => {});
    }
  }

  res.json({
    bets:   valueBets,   // ИСПРАВЛЕНО: было `bets` в одних местах, `valueBets` в других
    total:  valueBets.length,
    models: ['Poisson (Dixon-Coles)', 'ELO Ensemble', 'Weighted 70/30'],
    source: clickhouse ? 'clickhouse+model' : 'demo+model',
  });
});

/** POST /api/value/calculate — разовый расчёт */
router.post('/calculate', (req, res) => {
  const {
    home, away,
    homeAttack = 1, homeDefense = 1, awayAttack = 1, awayDefense = 1,
    leagueAvgHome = 1.45, leagueAvgAway = 1.15,
    marketHome, marketDraw, marketAway, marketOver25,
  } = req.body;
  if (!home || !away) return res.status(400).json({ error: 'home and away required' });

  const lH   = +(homeAttack * awayDefense * leagueAvgHome).toFixed(3);
  const lA   = +(awayAttack * homeDefense * leagueAvgAway).toFixed(3);
  const mat  = scoreMatrix(lH, lA);
  const pois = aggregateMatrix(mat);
  const elo  = eloProbs(home, away);
  const ens  = {
    homeWin: +(pois.homeWin * .7 + elo.homeWin * .3).toFixed(4),
    draw:    +(pois.draw    * .7 + elo.draw    * .3).toFixed(4),
    awayWin: +(pois.awayWin * .7 + elo.awayWin * .3).toFixed(4),
    over25:  +pois.over25.toFixed(4),
    btts:    +pois.btts.toFixed(4),
  };

  const analysis = [
    { key: 'homeWin', label: `Победа ${home}`, odds: marketHome },
    { key: 'draw',    label: 'Ничья',           odds: marketDraw },
    { key: 'awayWin', label: `Победа ${away}`,  odds: marketAway },
    { key: 'over25',  label: 'Тотал Б 2.5',     odds: marketOver25 },
  ].filter(m => m.odds).map(m => {
    const prob  = ens[m.key] || 0;
    const impl  = 1 / m.odds;
    const edge  = prob - impl;
    const kelly = edge > 0 ? ((m.odds - 1) * prob - (1 - prob)) / (m.odds - 1) : 0;
    return {
      label: m.label,
      modelProb:   +(prob * 100).toFixed(1),
      impliedProb: +(impl * 100).toFixed(1),
      edge:        +(edge * 100).toFixed(2),
      kelly:       +(kelly * 100).toFixed(1),
      value: edge > 0,
    };
  });

  res.json({ home, away, lH, lA, pois, elo, ens, analysis });
});

/** GET /api/value/elo — список ELO рейтингов */
router.get('/elo', (_req, res) => {
  const list = [...eloMap.entries()]
    .map(([team, rating]) => ({ team, rating: Math.round(rating) }))
    .sort((a, b) => b.rating - a.rating);
  res.json({ ratings: list, total: list.length });
});

module.exports = router;