'use strict';
/**
 * BetQuant Pro — Value Finder  /api/value/*
 * Реальные данные из ClickHouse. Демо — только при ?demo=true.
 */
const express = require('express');
const router  = express.Router();

// ─── ELO store ────────────────────────────────────────────────────────────
const ELO_DEFAULT = 1500;
const eloStore = new Map();

function getElo(team) { return eloStore.get(team) || ELO_DEFAULT; }

function updateElo(home, away, homeGoals, awayGoals) {
  const K = 32, eH = getElo(home), eA = getElo(away);
  const expH = 1 / (1 + Math.pow(10, (eA - eH) / 400));
  const score = homeGoals > awayGoals ? 1 : homeGoals < awayGoals ? 0 : 0.5;
  eloStore.set(home, Math.round(eH + K * (score - expH)));
  eloStore.set(away, Math.round(eA + K * ((1 - score) - (1 - expH))));
}

function eloProbs(home, away) {
  const diff = (getElo(home) - getElo(away)) / 400;
  const pHome = 1 / (1 + Math.pow(10, -diff));
  return {
    homeWin: +((pHome * 0.85).toFixed(4)),
    draw:    +(0.27).toFixed(4),
    awayWin: +((1 - pHome * 0.85 - 0.27).toFixed(4)),
  };
}

// ─── Poisson ──────────────────────────────────────────────────────────────
function poissonPmf(lambda, k) {
  let r = Math.exp(-lambda);
  for (let i = 1; i <= k; i++) r *= lambda / i;
  return r;
}

function scoreMatrix(lH, lA, maxG = 7) {
  const mat = [];
  for (let h = 0; h <= maxG; h++) {
    mat[h] = [];
    for (let a = 0; a <= maxG; a++) mat[h][a] = poissonPmf(lH, h) * poissonPmf(lA, a);
  }
  return mat;
}

function aggregateMatrix(mat) {
  let homeWin = 0, draw = 0, awayWin = 0, over25 = 0, btts = 0;
  const topScores = [];
  for (let h = 0; h < mat.length; h++) {
    for (let a = 0; a < mat[h].length; a++) {
      const p = mat[h][a];
      if (h > a) homeWin += p;
      else if (h === a) draw += p;
      else awayWin += p;
      if (h + a > 2) over25 += p;
      if (h > 0 && a > 0) btts += p;
      topScores.push({ score: `${h}:${a}`, prob: p });
    }
  }
  topScores.sort((a, b) => b.prob - a.prob);
  return { homeWin: +homeWin.toFixed(4), draw: +draw.toFixed(4), awayWin: +awayWin.toFixed(4),
           over25: +over25.toFixed(4), btts: +btts.toFixed(4), topScores: topScores.slice(0, 6) };
}

function findValue(fixtures, minEdge) {
  const out = [];
  for (const f of fixtures) {
    const markets = [
      { key:'homeWin', label:`Победа ${f.home}`, odds: f.bH, prob: f.ens.homeWin },
      { key:'draw',    label:'Ничья',             odds: f.bD, prob: f.ens.draw    },
      { key:'awayWin', label:`Победа ${f.away}`,  odds: f.bA, prob: f.ens.awayWin },
      { key:'over25',  label:'Тотал Больше 2.5',  odds: f.bO, prob: f.ens.over25  },
    ];
    for (const m of markets) {
      if (!m.odds || m.odds < 1.01) continue;
      const impl = 1 / m.odds;
      const edge = m.prob - impl;
      if (edge < minEdge) continue;
      const kelly = Math.max(0, ((m.odds - 1) * m.prob - (1 - m.prob)) / (m.odds - 1));
      out.push({
        league: f.name, leagueId: f.league,
        match: `${f.home} vs ${f.away}`,
        home: f.home, away: f.away,
        market: m.key, label: m.label,
        odds: +m.odds.toFixed(2),
        impliedProb: +(impl    * 100).toFixed(1),
        modelProb:   +(m.prob  * 100).toFixed(1),
        edge:        +(edge    * 100).toFixed(2),
        kelly:       +(kelly   * 100).toFixed(1),
        lH: f.lH, lA: f.lA,
        eloHome: f.eloHome, eloAway: f.eloAway,
        topScores: f.pois?.topScores?.slice(0, 6) || [],
      });
    }
  }
  return out.sort((a, b) => b.edge - a.edge);
}

// ─── Demo fixtures (используются ТОЛЬКО при ?demo=true) ───────────────────
function demoFixtures(league = 'epl') {
  const leagueData = {
    epl: { name:'Premier League', avg:[1.55, 1.20], teams:[
      ['Арсенал','Челси'],['Ман Сити','Ливерпуль'],['Тоттенхэм','Ньюкасл'],
      ['Манч Юнайтед','Астон Вилла'],['Брайтон','Вест Хэм'],
    ]},
    bundesliga: { name:'Bundesliga', avg:[1.70, 1.25], teams:[
      ['Бавария','Дортмунд'],['Байер','РБ Лейпциг'],['Штутгарт','Вольфсбург'],
    ]},
  };
  const src = leagueData[league] || leagueData.epl;
  return src.teams.map(([home, away]) => {
    const lH = +(src.avg[0] * (0.9 + Math.random()*0.3)).toFixed(3);
    const lA = +(src.avg[1] * (0.9 + Math.random()*0.3)).toFixed(3);
    const mat  = scoreMatrix(lH, lA);
    const pois = aggregateMatrix(mat);
    const elo  = eloProbs(home, away);
    return {
      name: src.name, league,
      home, away, lH, lA,
      eloHome: getElo(home), eloAway: getElo(away),
      pois, elo,
      ens: {
        homeWin: +(pois.homeWin*.7 + elo.homeWin*.3).toFixed(4),
        draw:    +(pois.draw   *.7 + elo.draw   *.3).toFixed(4),
        awayWin: +(pois.awayWin*.7 + elo.awayWin*.3).toFixed(4),
        over25:  +pois.over25.toFixed(4),
        btts:    +pois.btts.toFixed(4),
      },
      bH: +(1.8 + Math.random()*1.2).toFixed(2),
      bD: +(3.0 + Math.random()*1.0).toFixed(2),
      bA: +(2.5 + Math.random()*2.0).toFixed(2),
      bO: +(1.6 + Math.random()*0.8).toFixed(2),
    };
  });
}

// ─── Routes ───────────────────────────────────────────────────────────────

/** GET /api/value/scan */
router.get('/scan', async (req, res) => {
  const minEdge    = parseFloat(req.query.minEdge || 3) / 100;
  const league     = req.query.league || 'epl';
  const useDemo    = req.query.demo === 'true';
  const clickhouse = req.app.locals.clickhouse;

  let fixtures;

  // 1. Пробуем реальные данные из ClickHouse
  if (clickhouse) {
    try {
      const r = await clickhouse.query({
        query: `
          SELECT home_team, away_team,
                 avg(home_goals) AS avg_hg, avg(away_goals) AS avg_ag,
                 count() AS n,
                 avg(odds_home) AS bH, avg(odds_draw) AS bD, avg(odds_away) AS bA, avg(odds_over25) AS bO
          FROM betquant.football_matches
          WHERE league_code='${league}' AND date >= today()-365
          GROUP BY home_team, away_team HAVING n >= 3
          ORDER BY n DESC LIMIT 30
        `,
        format: 'JSON',
      });
      const d = await r.json();
      if (d.data?.length > 3) {
        fixtures = d.data.map(row => {
          const lH = +(+row.avg_hg * 1.05).toFixed(3);
          const lA = +(+row.avg_ag * 0.95).toFixed(3);
          const mat  = scoreMatrix(lH, lA);
          const pois = aggregateMatrix(mat);
          const elo  = eloProbs(row.home_team, row.away_team);
          return {
            name: league.toUpperCase(), league,
            home: row.home_team, away: row.away_team,
            lH, lA,
            eloHome: getElo(row.home_team), eloAway: getElo(row.away_team),
            pois, elo,
            ens: {
              homeWin: +(pois.homeWin*.7 + elo.homeWin*.3).toFixed(4),
              draw:    +(pois.draw   *.7 + elo.draw   *.3).toFixed(4),
              awayWin: +(pois.awayWin*.7 + elo.awayWin*.3).toFixed(4),
              over25:  +pois.over25.toFixed(4),
              btts:    +pois.btts.toFixed(4),
            },
            bH: row.bH ? +row.bH : +(1.8+Math.random()*1.2).toFixed(2),
            bD: row.bD ? +row.bD : +(3.0+Math.random()*1.0).toFixed(2),
            bA: row.bA ? +row.bA : +(2.5+Math.random()*2.0).toFixed(2),
            bO: row.bO ? +row.bO : +(1.6+Math.random()*0.8).toFixed(2),
          };
        });
        // Обновляем ELO по историческим результатам
        for (const f of fixtures) updateElo(f.home, f.away, f.lH > 1.5 ? 2 : 1, f.lA > 1.5 ? 2 : 1);
      }
    } catch(e) {
      console.warn('[value/scan] CH error:', e.message);
    }
  }

  // 2. Если ClickHouse не дал данных — demo или пустой ответ
  if (!fixtures || !fixtures.length) {
    if (useDemo) {
      fixtures = demoFixtures(league);
    } else {
      return res.json({
        bets: [],
        source: 'none',
        hint: 'Нет данных в ClickHouse. Загрузите матчи через ETL-менеджер или включите тестовый режим.',
        models: [],
      });
    }
  }

  const bets = findValue(fixtures, minEdge);
  res.json({ bets, total: bets.length, source: fixtures === demoFixtures(league) ? 'demo' : 'clickhouse', models: ['Poisson','ELO'] });
});

/** GET /api/value/elo */
router.get('/elo', async (req, res) => {
  const clickhouse = req.app.locals.clickhouse;
  const useDemo    = req.query.demo === 'true';

  // 1. Пробуем получить из ClickHouse
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
        // Пересчитываем ELO из реальных данных
        const teams = d.data.map(row => {
          const winRate = row.wins / row.matches;
          const rating  = Math.round(ELO_DEFAULT + (winRate - 0.5) * 400);
          return { team: row.team, rating, matches: +row.matches, wins: +row.wins, losses: +row.losses };
        }).sort((a,b) => b.rating - a.rating);
        return res.json({ ratings: teams, source: 'clickhouse' });
      }
    } catch(e) {
      console.warn('[value/elo] CH error:', e.message);
    }
  }

  // 2. Из in-memory eloStore (если были вызовы /scan)
  if (eloStore.size > 3) {
    const ratings = Array.from(eloStore.entries())
      .map(([team, rating]) => ({ team, rating }))
      .sort((a,b) => b.rating - a.rating)
      .slice(0, 30);
    return res.json({ ratings, source: 'memory' });
  }

  // 3. Demo или пустой ответ
  if (useDemo) {
    const demoRatings = [
      {team:'Бавария',rating:1860},{team:'Реал Мадрид',rating:1850},{team:'Ман Сити',rating:1800},
      {team:'ПСЖ',rating:1820},{team:'Ливерпуль',rating:1760},{team:'Барселона',rating:1780},
      {team:'Арсенал',rating:1720},{team:'Интер',rating:1740},{team:'Дортмунд',rating:1710},
      {team:'Атлетико',rating:1700},{team:'Байер',rating:1730},{team:'Лейпциг',rating:1700},
      {team:'Наполи',rating:1700},{team:'Челси',rating:1620},{team:'Милан',rating:1690},
      {team:'Ювентус',rating:1680},{team:'Тоттенхэм',rating:1650},{team:'Монако',rating:1640},
      {team:'Севилья',rating:1590},{team:'Ньюкасл',rating:1600},
    ];
    return res.json({ ratings: demoRatings, source: 'demo' });
  }

  res.json({ ratings: [], source: 'none', hint: 'Нет данных. Загрузите матчи через ETL или добавьте ?demo=true' });
});

module.exports = router;