'use strict';
/**
 * BetQuant Pro — Stats API  /api/stats/*
 * ═══════════════════════════════════════════════════════════════
 * НОЛЬ хардкода. Все данные из ClickHouse.
 * Поддержка: football, hockey, basketball, tennis, baseball, esports
 * ═══════════════════════════════════════════════════════════════
 * Деплой: server/stats_routes.js
 */

const express = require('express');
const router  = express.Router();

// ── Конфиг таблиц по спортам ──────────────────────────────────────────────
const SPORT = {
  football: {
    table:       'betquant.football_matches',
    leagueCol:   'league_code',
    leagueLabel: 'league_name',
    seasonCol:   'season',
    homeTeam:    'home_team',
    awayTeam:    'away_team',
    homeGoals:   'home_goals',
    awayGoals:   'away_goals',
    xgHome:      'home_xg',
    xgAway:      'away_xg',
    shotsHome:   'home_shots',
    shotsAway:   'away_shots',
    cornersHome: 'home_corners',
    cornersAway: 'away_corners',
    yellowHome:  'home_yellow',
    yellowAway:  'away_yellow',
    hasXG:       true,
    hasDraw:     true,
    eventsTable: 'betquant.football_events',
    eventsMinute:'minute',
    formTable:   'betquant.football_team_form',
  },
  hockey: {
    table:       'betquant.hockey_matches',
    leagueCol:   'league',
    leagueLabel: 'league',
    seasonCol:   'season',
    homeTeam:    'home_team',
    awayTeam:    'away_team',
    homeGoals:   'home_goals',
    awayGoals:   'away_goals',
    xgHome:      'home_xg_for',
    xgAway:      'away_xg_for',
    shotsHome:   'home_shots',
    shotsAway:   'away_shots',
    hasXG:       true,
    hasDraw:     false,
    eventsTable: 'betquant.hockey_events',
    formTable:   'betquant.hockey_team_form',
  },
  basketball: {
    table:       'betquant.basketball_matches',
    leagueCol:   'league',
    leagueLabel: 'league',
    seasonCol:   'season',
    homeTeam:    'home_team',
    awayTeam:    'away_team',
    homeGoals:   'home_pts',
    awayGoals:   'away_pts',
    xgHome:      null,
    xgAway:      null,
    hasXG:       false,
    hasDraw:     false,
  },
  baseball: {
    table:       'betquant.baseball_matches',
    leagueCol:   'league',
    leagueLabel: 'league',
    seasonCol:   'season',
    homeTeam:    'home_team',
    awayTeam:    'away_team',
    homeGoals:   'home_runs',
    awayGoals:   'away_runs',
    xgHome:      null,
    xgAway:      null,
    hasXG:       false,
    hasDraw:     false,
  },
  tennis: {
    table:       'betquant.tennis_matches',
    leagueCol:   'tour',
    leagueLabel: 'tour',
    seasonCol:   'toYear(date)',
    homeTeam:    'winner',
    awayTeam:    'loser',
    homeGoals:   'sets_played',
    awayGoals:   'toUInt8(0)',
    xgHome:      null,
    xgAway:      null,
    hasXG:       false,
    hasDraw:     false,
  },
};

function getDB(req) {
  // Пробуем разные способы получить clickhouse клиент
  return req.app.locals.clickhouse
      || req.app.locals.ch
      || global.__betquant_ch
      || null;
}

function esc(v) {
  return String(v || '').replace(/'/g, "''").replace(/[;\-\-]/g, '');
}

function whereClause(cfg, { league, season }) {
  const parts = ['1=1'];
  if (league) parts.push(`${cfg.leagueCol} = '${esc(league)}'`);
  if (season) parts.push(`${cfg.seasonCol} = '${esc(season)}'`);
  return 'WHERE ' + parts.join(' AND ');
}

async function chQuery(db, query) {
  const r = await db.query({ query, format: 'JSON' });
  const d = await r.json();
  return d.data || [];
}

// ══════════════════════════════════════════════════════════════════════
//  GET /api/stats/leagues?sport=football
//  Реальные лиги из БД — для заполнения селекта
// ══════════════════════════════════════════════════════════════════════
router.get('/leagues', async (req, res) => {
  const db    = getDB(req);
  const sport = req.query.sport || 'football';
  const cfg   = SPORT[sport];

  if (!db)  return res.json({ leagues: [], error: 'ClickHouse не подключён' });
  if (!cfg) return res.json({ leagues: [], error: `Неизвестный спорт: ${sport}` });

  try {
    const rows = await chQuery(db, `
      SELECT
        ${cfg.leagueCol}   AS code,
        ${cfg.leagueLabel} AS label,
        count()            AS matches
      FROM ${cfg.table}
      GROUP BY ${cfg.leagueCol}, ${cfg.leagueLabel}
      ORDER BY matches DESC
      LIMIT 40
    `);

    // Убираем пустые
    const leagues = rows
      .filter(r => r.code && r.code.trim())
      .map(r => ({ code: r.code.trim(), label: (r.label || r.code).trim(), matches: +r.matches }));

    res.json({ leagues, sport, total: leagues.length });
  } catch(e) {
    console.error('[stats/leagues]', e.message);
    res.json({ leagues: [], error: e.message });
  }
});

// ══════════════════════════════════════════════════════════════════════
//  GET /api/stats/seasons?sport=football&league=E0
// ══════════════════════════════════════════════════════════════════════
router.get('/seasons', async (req, res) => {
  const db     = getDB(req);
  const sport  = req.query.sport  || 'football';
  const league = req.query.league || '';
  const cfg    = SPORT[sport];

  if (!db || !cfg) return res.json({ seasons: [] });

  try {
    const lf  = league ? `WHERE ${cfg.leagueCol} = '${esc(league)}'` : '';
    const rows = await chQuery(db, `
      SELECT DISTINCT ${cfg.seasonCol} AS season, count() AS n
      FROM ${cfg.table} ${lf}
      GROUP BY ${cfg.seasonCol}
      ORDER BY ${cfg.seasonCol} DESC
      LIMIT 20
    `);

    const seasons = rows
      .map(r => ({ season: String(r.season || '').trim(), matches: +r.n }))
      .filter(r => r.season);

    res.json({ seasons, sport, league });
  } catch(e) {
    res.json({ seasons: [], error: e.message });
  }
});

// ══════════════════════════════════════════════════════════════════════
//  GET /api/stats/teams?sport=&league=&season=&limit=
// ══════════════════════════════════════════════════════════════════════
router.get('/teams', async (req, res) => {
  const db     = getDB(req);
  const sport  = req.query.sport  || 'football';
  const league = req.query.league || '';
  const season = req.query.season || '';
  const limit  = Math.min(50, parseInt(req.query.limit) || 30);
  const cfg    = SPORT[sport];

  if (!db)  return res.json({ teams: [], total: 0, error: 'ClickHouse не подключён' });
  if (!cfg) return res.json({ teams: [], total: 0, error: `Неизвестный спорт: ${sport}` });

  const where = whereClause(cfg, { league, season });

  try {
    // Считаем строк
    const cntRows = await chQuery(db, `SELECT count() AS n FROM ${cfg.table} ${where}`);
    const total   = parseInt(cntRows[0]?.n || 0);

    if (!total) {
      // Подсказываем реальные лиги
      const lgRows = await chQuery(db, `
        SELECT DISTINCT ${cfg.leagueCol} AS lg, count() AS n
        FROM ${cfg.table}
        GROUP BY ${cfg.leagueCol} ORDER BY n DESC LIMIT 15
      `);
      const available = lgRows.map(r => r.lg).filter(Boolean);
      return res.json({
        teams: [], total: 0, source: 'clickhouse', sport,
        hint: available.length
          ? `Нет данных для лиги "${league}". Доступные: ${available.join(', ')}`
          : `Таблица ${cfg.table} пустая — запустите ETL`,
        availableLeagues: available,
      });
    }

    // ── Запрос статистики команд — универсальный через UNION home+away ──
    const hg  = cfg.homeGoals,  ag  = cfg.awayGoals;
    const ht  = cfg.homeTeam,   at  = cfg.awayTeam;
    const xgh = cfg.xgHome,     xga = cfg.xgAway;
    const sh  = cfg.shotsHome,  sa  = cfg.shotsAway;
    const ch_ = cfg.cornersHome, ca = cfg.cornersAway;
    const yh  = cfg.yellowHome,  ya = cfg.yellowAway;

    let query;

    if (sport === 'football') {
      query = `
        SELECT
          team,
          sum(gf) AS goals_for,
          sum(ga) AS goals_against,
          sum(gf) - sum(ga) AS gd,
          count() AS matches,
          sum(w)  AS wins,
          sum(d)  AS draws,
          sum(l)  AS losses,
          sum(w)*3 + sum(d) AS points,
          round(avg(xgf), 2) AS xg,
          round(avg(xgc), 2) AS xga,
          round(avg(shots), 1) AS shots,
          round(avg(corners), 1) AS corners
        FROM (
          SELECT
            ${ht} AS team,
            toInt32(${hg}) AS gf, toInt32(${ag}) AS ga,
            ${hg} > ${ag} ? 1 : 0 AS w,
            ${hg} = ${ag} ? 1 : 0 AS d,
            ${hg} < ${ag} ? 1 : 0 AS l,
            ifNull(${xgh}, 0) AS xgf, ifNull(${xga}, 0) AS xgc,
            ifNull(${sh},  0) AS shots,
            ifNull(${ch_}, 0) AS corners
          FROM ${cfg.table} ${where}
          UNION ALL
          SELECT
            ${at} AS team,
            toInt32(${ag}) AS gf, toInt32(${hg}) AS ga,
            ${ag} > ${hg} ? 1 : 0 AS w,
            ${ag} = ${hg} ? 1 : 0 AS d,
            ${ag} < ${hg} ? 1 : 0 AS l,
            ifNull(${xga}, 0) AS xgf, ifNull(${xgh}, 0) AS xgc,
            ifNull(${sa},  0) AS shots,
            ifNull(${ca},  0) AS corners
          FROM ${cfg.table} ${where}
        )
        GROUP BY team
        HAVING matches >= 2
        ORDER BY points DESC, gd DESC
        LIMIT ${limit}
      `;
    } else if (sport === 'hockey') {
      query = `
        SELECT
          team,
          count() AS matches,
          sum(w)  AS wins,
          sum(otw) AS ot_wins,
          sum(l)  AS losses,
          sum(gf) AS goals_for,
          sum(ga) AS goals_against,
          sum(w)*2 + sum(otw) AS points,
          round(avg(shots), 1) AS shots_for,
          round(if(sum(pp_opp)>0, sum(pp_goals)/sum(pp_opp)*100, 0), 1) AS pp_pct,
          round(if(sum(sa)>0, sum(saves)/sum(sa)*100, 0), 1) AS sv_pct
        FROM (
          SELECT
            ${ht} AS team,
            toInt32(${hg}) AS gf, toInt32(${ag}) AS ga,
            ${hg} > ${ag} AND went_to_ot = 0 ? 1 : 0 AS w,
            ${hg} > ${ag} AND went_to_ot = 1 ? 1 : 0 AS otw,
            ${hg} < ${ag} ? 1 : 0 AS l,
            home_shots AS shots, away_shots AS sa,
            (away_shots - away_goals) AS saves,
            home_pp_goals AS pp_goals, home_pp_opp AS pp_opp
          FROM ${cfg.table} ${where}
          UNION ALL
          SELECT
            ${at} AS team,
            toInt32(${ag}) AS gf, toInt32(${hg}) AS ga,
            ${ag} > ${hg} AND went_to_ot = 0 ? 1 : 0 AS w,
            ${ag} > ${hg} AND went_to_ot = 1 ? 1 : 0 AS otw,
            ${ag} < ${hg} ? 1 : 0 AS l,
            away_shots AS shots, home_shots AS sa,
            (home_shots - home_goals) AS saves,
            away_pp_goals AS pp_goals, away_pp_opp AS pp_opp
          FROM ${cfg.table} ${where}
        )
        GROUP BY team
        HAVING matches >= 2
        ORDER BY points DESC
        LIMIT ${limit}
      `;
    } else {
      // Generic
      query = `
        SELECT
          team,
          count() AS matches,
          sum(w) AS wins,
          sum(l) AS losses,
          sum(gf) AS goals_for,
          sum(ga) AS goals_against
        FROM (
          SELECT ${ht} AS team, toInt32(${hg}) AS gf, toInt32(${ag}) AS ga,
                 ${hg} > ${ag} ? 1 : 0 AS w, ${hg} < ${ag} ? 1 : 0 AS l
          FROM ${cfg.table} ${where}
          UNION ALL
          SELECT ${at} AS team, toInt32(${ag}) AS gf, toInt32(${hg}) AS ga,
                 ${ag} > ${hg} ? 1 : 0 AS w, ${ag} < ${hg} ? 1 : 0 AS l
          FROM ${cfg.table} ${where}
        )
        GROUP BY team HAVING matches >= 2
        ORDER BY wins DESC LIMIT ${limit}
      `;
    }

    const teams = await chQuery(db, query);
    res.json({ teams, total, source: 'clickhouse', sport, league, season });

  } catch(e) {
    console.error('[stats/teams]', e.message, '\nQuery sport:', sport, 'league:', league);
    res.json({ teams: [], total: 0, error: e.message, sport });
  }
});

// ══════════════════════════════════════════════════════════════════════
//  GET /api/stats/home-away?sport=&league=&season=
// ══════════════════════════════════════════════════════════════════════
router.get('/home-away', async (req, res) => {
  const db     = getDB(req);
  const sport  = req.query.sport  || 'football';
  const league = req.query.league || '';
  const season = req.query.season || '';
  const cfg    = SPORT[sport];

  if (!db || !cfg) return res.json({ stats: {} });

  const where = whereClause(cfg, { league, season });
  const hg = cfg.homeGoals, ag = cfg.awayGoals;

  try {
    const rows = await chQuery(db, `
      SELECT
        countIf(${hg} > ${ag}) AS home_wins,
        countIf(${hg} = ${ag}) AS draws,
        countIf(${hg} < ${ag}) AS away_wins,
        count()                 AS total,
        round(avg(${hg}), 2)    AS avg_home_goals,
        round(avg(${ag}), 2)    AS avg_away_goals,
        round(avg(${hg} + ${ag}), 2) AS avg_total_goals
      FROM ${cfg.table} ${where}
    `);
    res.json({ stats: rows[0] || {}, source: 'clickhouse', sport });
  } catch(e) {
    res.json({ stats: {}, error: e.message });
  }
});

// ══════════════════════════════════════════════════════════════════════
//  GET /api/stats/xg-vs-actual?sport=&league=&season=&limit=
// ══════════════════════════════════════════════════════════════════════
router.get('/xg-vs-actual', async (req, res) => {
  const db     = getDB(req);
  const sport  = req.query.sport  || 'football';
  const league = req.query.league || '';
  const season = req.query.season || '';
  const limit  = Math.min(30, parseInt(req.query.limit) || 20);
  const cfg    = SPORT[sport];

  if (!db || !cfg || !cfg.hasXG)
    return res.json({ teams: [], hint: 'xG недоступен для этого спорта' });

  const where = whereClause(cfg, { league, season });
  const xgh = cfg.xgHome, xga = cfg.xgAway;
  const hg = cfg.homeGoals, ag = cfg.awayGoals, ht = cfg.homeTeam, at = cfg.awayTeam;

  try {
    const teams = await chQuery(db, `
      SELECT team,
        round(avg(xgf), 2) AS xg,
        round(avg(gf), 2)  AS goals,
        count()            AS matches
      FROM (
        SELECT ${ht} AS team, toFloat64(ifNull(${xgh}, 0)) AS xgf, toFloat64(${hg}) AS gf
        FROM ${cfg.table} ${where}
        UNION ALL
        SELECT ${at} AS team, toFloat64(ifNull(${xga}, 0)) AS xgf, toFloat64(${ag}) AS gf
        FROM ${cfg.table} ${where}
      )
      GROUP BY team HAVING matches >= 5
      ORDER BY xg DESC LIMIT ${limit}
    `);
    res.json({ teams, source: 'clickhouse', sport });
  } catch(e) {
    res.json({ teams: [], error: e.message });
  }
});

// ══════════════════════════════════════════════════════════════════════
//  GET /api/stats/goals-by-minute?sport=&league=&season=
// ══════════════════════════════════════════════════════════════════════
router.get('/goals-by-minute', async (req, res) => {
  const db     = getDB(req);
  const sport  = req.query.sport  || 'football';
  const league = req.query.league || '';
  const season = req.query.season || '';
  const cfg    = SPORT[sport];

  if (!db || !cfg) return res.json([]);

  try {
    if (sport === 'football' && cfg.eventsTable) {
      const lf = league ? `AND league_code = '${esc(league)}'` : '';
      const sf = season ? `AND season = '${esc(season)}'`      : '';
      const rows = await chQuery(db, `
        SELECT
          intDiv(minute, 5) * 5 AS minute,
          countIf(event_type = 'goal') AS goals,
          count()                      AS events
        FROM ${cfg.eventsTable}
        WHERE minute > 0 AND minute <= 95 ${lf} ${sf}
        GROUP BY minute ORDER BY minute
      `);
      // Если events таблица пустая — считаем из matches по avg голов
      if (!rows.length) {
        const mwhere = whereClause(cfg, { league, season });
        const mrows = await chQuery(db, `
          SELECT
            round(avg(home_goals + away_goals), 2) AS avg_goals,
            count() AS matches
          FROM ${cfg.table} ${mwhere}
        `);
        return res.json({ type: 'summary', data: mrows, hint: 'events таблица пустая' });
      }
      return res.json(rows);
    }

    if (sport === 'hockey' && cfg.eventsTable) {
      const lf = league ? `AND league = '${esc(league)}'` : '';
      const rows = await chQuery(db, `
        SELECT
          period AS minute,
          countIf(event_type = 'goal') AS goals,
          count() AS events
        FROM ${cfg.eventsTable}
        WHERE period >= 1 AND period <= 4 ${lf}
        GROUP BY period ORDER BY period
      `);
      const labels = { '1':'1й период', '2':'2й период', '3':'3й период', '4':'Овертайм' };
      return res.json(rows.map(r => ({ ...r, label: labels[String(r.minute)] || String(r.minute) })));
    }

    // Для остальных спортов — простое распределение по результатам
    const where = whereClause(cfg, { league, season });
    const hg = cfg.homeGoals, ag = cfg.awayGoals;
    const rows = await chQuery(db, `
      SELECT
        (${hg} + ${ag}) AS total_score,
        count() AS matches
      FROM ${cfg.table} ${where}
      GROUP BY total_score ORDER BY total_score LIMIT 30
    `);
    return res.json(rows.map(r => ({ minute: r.total_score, goals: r.matches, label: `Счёт ${r.total_score}` })));

  } catch(e) {
    console.error('[stats/goals-by-minute]', e.message);
    res.json([]);
  }
});

// ══════════════════════════════════════════════════════════════════════
//  GET /api/stats/summary  — обновлённый (мульти-спорт)
// ══════════════════════════════════════════════════════════════════════
router.get('/summary', async (req, res) => {
  const db = getDB(req);
  if (!db) return res.json({ total: 0, sports: {}, error: 'no db' });

  const result = { sports: {} };
  await Promise.all(Object.entries(SPORT).map(async ([sport, cfg]) => {
    try {
      const rows = await chQuery(db, `SELECT count() AS n FROM ${cfg.table}`);
      result.sports[sport] = parseInt(rows[0]?.n || 0);
    } catch { result.sports[sport] = 0; }
  }));
  result.total = Object.values(result.sports).reduce((a, b) => a + b, 0);
  res.json(result);
});

module.exports = router;