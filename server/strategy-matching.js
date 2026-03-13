'use strict';
// ═══════════════════════════════════════════════════════════════════════════
//  BetQuant Pro — Strategy Matching Engine
//  Движок сопоставления стратегий с live данными через единую систему маппингов
// ═══════════════════════════════════════════════════════════════════════════

const { Router } = require('express');
const router = Router();

let _pg = null;
let _ch = null;

function init(pgPool, clickhouse) {
  _pg = pgPool;
  _ch = clickhouse;
}

// ── Получить все стратегии пользователя ─────────────────────────────────────
router.get('/strategies/list', async (req, res) => {
  try {
    const userId = req.session?.userId;
    if (!_pg) return res.json({ strategies: [], total: 0 });

    const r = await _pg.query(`
      SELECT 
        s.id, s.name, s.description, s.code, s.language, s.tags,
        s.is_public, s.is_ai_generated, s.active, s.last_signal_at,
        s.stat_event_codes, s.sub_event_codes, s.tournament_ids, s.country_ids,
        s.conditions, s.parameters, s.created_at, s.updated_at,
        sp.id AS sport_id, sp.name AS sport_name, sp.icon AS sport_icon,
        COALESCE(sig_stats.total_signals, 0) AS total_signals,
        COALESCE(sig_stats.won_signals, 0) AS won_signals
      FROM strategies s
      LEFT JOIN sports sp ON sp.id = s.sport_id
      LEFT JOIN (
        SELECT strategy_id,
               COUNT(*) AS total_signals,
               COUNT(*) FILTER (WHERE status = 'won') AS won_signals
        FROM strategy_signals
        GROUP BY strategy_id
      ) sig_stats ON sig_stats.strategy_id = s.id
      WHERE (s.user_id = $1 OR s.is_public = true) AND s.active = true
      ORDER BY s.updated_at DESC
    `, [userId || '00000000-0000-0000-0000-000000000000']);

    res.json({ strategies: r.rows, total: r.rowCount });
  } catch (e) {
    console.error('[strategy-matching] list error:', e.message);
    res.json({ strategies: [], total: 0, error: e.message });
  }
});

// ── Получить все маппинги для UI ─────────────────────────────────────────────
router.get('/mappings/meta', async (req, res) => {
  try {
    if (!_pg) return res.json(getDefaultMeta());

    const [sports, tournaments, countries, statEvents, subEvents, sources] = await Promise.all([
      _pg.query('SELECT id, name, slug, icon FROM sports ORDER BY name'),
      _pg.query('SELECT t.id, t.name, t.short_name, t.tier, s.slug AS sport_slug, c.name AS country_name FROM tournaments t LEFT JOIN sports s ON s.id = t.sport_id LEFT JOIN countries c ON c.id = t.country_id ORDER BY t.name'),
      _pg.query('SELECT id, name, iso2, continent FROM countries ORDER BY name'),
      _pg.query('SELECT id, code, name, name_ru, unit, scope, typical_over_under, s.slug AS sport_slug FROM stat_event_types se LEFT JOIN sports s ON s.id = se.sport_id ORDER BY se.sport_id, se.category, se.code'),
      _pg.query('SELECT id, code, name, name_ru, category, is_countable, s.slug AS sport_slug FROM sub_event_types sue LEFT JOIN sports s ON s.id = sue.sport_id ORDER BY sue.sport_id, sue.category, sue.code'),
      _pg.query('SELECT id, code, name, sports, is_realtime FROM api_sources WHERE active = true ORDER BY priority'),
    ]);

    res.json({
      sports: sports.rows,
      tournaments: tournaments.rows,
      countries: countries.rows,
      statEventTypes: statEvents.rows,
      subEventTypes: subEvents.rows,
      apiSources: sources.rows,
    });
  } catch (e) {
    console.error('[strategy-matching] meta error:', e.message);
    res.json(getDefaultMeta());
  }
});

// ── Получить live матчи с применёнными стратегиями ──────────────────────────
router.get('/live/matched', async (req, res) => {
  try {
    const userId = req.session?.userId;
    const strategyId = req.query.strategy_id;

    // Получаем live матчи
    let liveGames = [];
    if (_pg) {
      const gameQuery = await _pg.query(`
        SELECT 
          g.id AS game_uuid, g.status, g.minute, g.score_home, g.score_away,
          g.scheduled_at, g.external_ids, g.metadata,
          sp.name AS sport_name, sp.slug AS sport_slug, sp.icon AS sport_icon,
          t.name AS tournament_name, t.short_name AS tournament_short,
          c.name AS country_name,
          ht.name AS home_team, ht.short_name AS home_short,
          at_.name AS away_team, at_.short_name AS away_short
        FROM games g
        LEFT JOIN sports sp ON sp.id = g.sport_id
        LEFT JOIN tournaments t ON t.id = g.tournament_id
        LEFT JOIN countries c ON c.id = g.country_id
        LEFT JOIN teams ht ON ht.id = g.home_team_id
        LEFT JOIN teams at_ ON at_.id = g.away_team_id
        WHERE g.status IN ('live', 'scheduled')
          AND g.scheduled_at BETWEEN NOW() - INTERVAL '3 hours' AND NOW() + INTERVAL '24 hours'
        ORDER BY g.status DESC, g.scheduled_at ASC
        LIMIT 100
      `);
      liveGames = gameQuery.rows;
    }

    // Если есть конкретная стратегия — применяем её фильтры
    let matchedGames = liveGames;
    if (strategyId && _pg) {
      const stratQ = await _pg.query(
        'SELECT * FROM strategies WHERE id = $1 AND (user_id = $2 OR is_public = true)',
        [strategyId, userId]
      );
      if (stratQ.rows.length > 0) {
        const strat = stratQ.rows[0];
        matchedGames = filterGamesByStrategy(liveGames, strat);
      }
    }

    // Получаем активные сигналы
    let signals = [];
    if (_pg && strategyId) {
      const sigQ = await _pg.query(`
        SELECT ss.*, g.score_home, g.score_away, g.minute AS game_minute
        FROM strategy_signals ss
        LEFT JOIN games g ON g.id = ss.game_id
        WHERE ss.strategy_id = $1 AND ss.status = 'pending'
          AND ss.created_at > NOW() - INTERVAL '24 hours'
        ORDER BY ss.created_at DESC
      `, [strategyId]);
      signals = sigQ.rows;
    }

    res.json({
      games: matchedGames,
      signals,
      total: matchedGames.length,
      filtered: strategyId ? true : false,
    });
  } catch (e) {
    console.error('[strategy-matching] live/matched error:', e.message);
    res.json({ games: [], signals: [], total: 0 });
  }
});

// ── Применить стратегию к конкретному матчу ──────────────────────────────────
router.post('/match/apply', async (req, res) => {
  try {
    const { strategy_id, game_uuid } = req.body;
    if (!strategy_id || !game_uuid) return res.status(400).json({ error: 'strategy_id и game_uuid обязательны' });

    if (!_pg) return res.json({ signal: null, message: 'PostgreSQL недоступен' });

    // Загружаем стратегию
    const stratQ = await _pg.query('SELECT * FROM strategies WHERE id = $1', [strategy_id]);
    if (!stratQ.rows.length) return res.status(404).json({ error: 'Стратегия не найдена' });
    const strategy = stratQ.rows[0];

    // Загружаем матч со всеми данными
    const gameQ = await _pg.query(`
      SELECT * FROM v_games_full WHERE id = $1
    `, [game_uuid]);
    if (!gameQ.rows.length) return res.status(404).json({ error: 'Матч не найден' });
    const game = gameQ.rows[0];

    // Получаем последний снапшот live данных
    let snapshot = null;
    if (_ch) {
      try {
        const snapR = await _ch.query({
          query: `
            SELECT stat_code, stat_total, stat_home, stat_away, odds_over25, odds_home, odds_away, minute
            FROM betquant.live_data_stream
            WHERE game_uuid = '${game_uuid.replace(/'/g, '')}'
            ORDER BY snapshot_at DESC
            LIMIT 50
          `,
          format: 'JSON'
        });
        const snapData = await snapR.json();
        snapshot = buildSnapshotFromRows(snapData.data || []);
      } catch (chErr) {
        console.warn('[strategy-matching] CH snapshot error:', chErr.message);
      }
    }

    // Вычисляем сигнал
    const signal = evaluateStrategy(strategy, game, snapshot);

    // Сохраняем сигнал если есть
    if (signal && signal.triggered && _pg) {
      const sigInsert = await _pg.query(`
        INSERT INTO strategy_signals
          (strategy_id, game_id, stat_event_code, signal_type, direction, line,
           confidence, edge, recommended_odds, current_odds, current_minute, current_score, context)
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)
        RETURNING id
      `, [
        strategy_id, game_uuid,
        signal.stat_event_code, signal.signal_type, signal.direction,
        signal.line, signal.confidence, signal.edge,
        signal.recommended_odds, signal.current_odds,
        game.minute || 0, `${game.score_home || 0}:${game.score_away || 0}`,
        JSON.stringify(signal.context || {})
      ]);

      // Обновляем last_signal_at у стратегии
      await _pg.query('UPDATE strategies SET last_signal_at = NOW() WHERE id = $1', [strategy_id]);
      signal.id = sigInsert.rows[0].id;
    }

    res.json({ signal, game, snapshot });
  } catch (e) {
    console.error('[strategy-matching] apply error:', e.message);
    res.status(500).json({ error: e.message });
  }
});

// ── Получить маппинги команд ──────────────────────────────────────────────────
router.get('/mappings/teams', async (req, res) => {
  try {
    const { source, sport_id } = req.query;
    if (!_pg) return res.json({ teams: [] });

    let query = `
      SELECT t.id, t.name, t.short_name, t.country,
             tm.api_source_id, tm.external_id, tm.external_name, tm.confidence, tm.is_verified,
             src.code AS source_code, src.name AS source_name
      FROM teams t
      LEFT JOIN team_mappings tm ON tm.team_id = t.id
      LEFT JOIN api_sources src ON src.id = tm.api_source_id
      WHERE 1=1
    `;
    const params = [];

    if (source) {
      params.push(source);
      query += ` AND src.code = $${params.length}`;
    }
    if (sport_id) {
      params.push(sport_id);
      query += ` AND t.sport_id = $${params.length}`;
    }

    query += ' ORDER BY t.name LIMIT 500';
    const r = await _pg.query(query, params);
    res.json({ teams: r.rows });
  } catch (e) {
    res.json({ teams: [], error: e.message });
  }
});

// ── Добавить/обновить маппинг команды ────────────────────────────────────────
router.post('/mappings/teams', async (req, res) => {
  try {
    const { team_id, source_code, external_id, external_name, confidence = 1.0 } = req.body;
    if (!_pg) return res.status(503).json({ error: 'PostgreSQL недоступен' });

    const srcQ = await _pg.query('SELECT id FROM api_sources WHERE code = $1', [source_code]);
    if (!srcQ.rows.length) return res.status(404).json({ error: 'API источник не найден' });

    await _pg.query(`
      INSERT INTO team_mappings (team_id, api_source_id, external_id, external_name, confidence)
      VALUES ($1, $2, $3, $4, $5)
      ON CONFLICT (api_source_id, external_id)
      DO UPDATE SET external_name = $4, confidence = $5, updated_at = NOW()
    `, [team_id, srcQ.rows[0].id, external_id, external_name, confidence]);

    res.json({ ok: true });
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

// ── Получить историю сигналов стратегии ──────────────────────────────────────
router.get('/signals/:strategy_id', async (req, res) => {
  try {
    if (!_pg) return res.json({ signals: [] });

    const r = await _pg.query(`
      SELECT ss.*, 
             g.score_home, g.score_away, g.scheduled_at,
             g.home_team_name, g.away_team_name,
             g.tournament_name, g.sport_name
      FROM strategy_signals ss
      LEFT JOIN v_games_full g ON g.id = ss.game_id
      WHERE ss.strategy_id = $1
      ORDER BY ss.created_at DESC
      LIMIT 200
    `, [req.params.strategy_id]);

    res.json({ signals: r.rows });
  } catch (e) {
    res.json({ signals: [], error: e.message });
  }
});

// ── Обновить условия стратегии (conditions) ───────────────────────────────────
router.put('/strategies/:id/conditions', async (req, res) => {
  try {
    const { conditions, stat_event_codes, sub_event_codes, tournament_ids, country_ids, active } = req.body;
    if (!_pg) return res.json({ ok: false, error: 'no db' });

    await _pg.query(`
      UPDATE strategies SET
        conditions = $1,
        stat_event_codes = $2,
        sub_event_codes = $3,
        tournament_ids = $4,
        country_ids = $5,
        active = $6,
        updated_at = NOW()
      WHERE id = $7 AND user_id = $8
    `, [
      JSON.stringify(conditions || {}),
      stat_event_codes || [],
      sub_event_codes || [],
      tournament_ids || [],
      country_ids || [],
      active !== false,
      req.params.id,
      req.session?.userId
    ]);

    res.json({ ok: true });
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

// ════════════════════════════════════════════════════════════════════════════
//  HELPER FUNCTIONS
// ════════════════════════════════════════════════════════════════════════════

function filterGamesByStrategy(games, strategy) {
  return games.filter(game => {
    // Фильтр по спорту
    if (strategy.sport_id && game.sport_id !== strategy.sport_id) return false;
    // Фильтр по турнирам
    if (strategy.tournament_ids && strategy.tournament_ids.length > 0) {
      if (!strategy.tournament_ids.includes(game.tournament_id)) return false;
    }
    // Фильтр по странам
    if (strategy.country_ids && strategy.country_ids.length > 0) {
      if (!strategy.country_ids.includes(game.country_id)) return false;
    }
    return true;
  });
}

function buildSnapshotFromRows(rows) {
  const snap = { stats: {}, odds: {}, minute: 0 };
  for (const row of rows) {
    snap.stats[row.stat_code] = {
      total: parseFloat(row.stat_total) || 0,
      home: parseFloat(row.stat_home) || 0,
      away: parseFloat(row.stat_away) || 0,
    };
    if (row.odds_over25) snap.odds.over25 = parseFloat(row.odds_over25);
    if (row.odds_home) snap.odds.home = parseFloat(row.odds_home);
    if (row.odds_away) snap.odds.away = parseFloat(row.odds_away);
    if (row.minute) snap.minute = Math.max(snap.minute, parseInt(row.minute) || 0);
  }
  return snap;
}

function evaluateStrategy(strategy, game, snapshot) {
  // Базовый движок оценки стратегии
  const conditions = strategy.conditions || {};
  const result = {
    triggered: false,
    signal_type: 'bet',
    stat_event_code: conditions.stat_event_code || 'total_goals',
    direction: conditions.direction || 'over',
    line: conditions.line || 2.5,
    confidence: 0,
    edge: 0,
    recommended_odds: 0,
    current_odds: 0,
    context: {},
  };

  if (!snapshot) return result;

  const statCode = result.stat_event_code;
  const statData = snapshot.stats[statCode];
  if (!statData) return result;

  // Простая логика: проверяем текущий темп vs линия
  const minute = snapshot.minute || game.minute || 45;
  const currentTotal = statData.total || 0;
  const projectedRate = minute > 0 ? (currentTotal / minute) * 90 : 0;

  result.context = {
    current_total: currentTotal,
    projected_90min: parseFloat(projectedRate.toFixed(2)),
    line: result.line,
    minute,
    pace_diff: parseFloat((projectedRate - result.line).toFixed(2)),
  };

  // Получаем коэффициент
  const oddsKey = result.direction === 'over' ? 'over25' : 'under25';
  result.current_odds = snapshot.odds[oddsKey] || snapshot.odds.home || 0;

  // Рассчитываем edge
  if (result.direction === 'over' && projectedRate > result.line + 0.3) {
    const prob = Math.min(0.85, 0.5 + (projectedRate - result.line) * 0.1);
    const fairOdds = 1 / prob;
    result.confidence = parseFloat(prob.toFixed(3));
    result.recommended_odds = parseFloat(fairOdds.toFixed(2));
    if (result.current_odds > 0 && result.current_odds > fairOdds * 1.05) {
      result.edge = parseFloat(((result.current_odds / fairOdds) - 1).toFixed(4));
      result.triggered = result.confidence > (conditions.min_confidence || 0.6);
    }
  } else if (result.direction === 'under' && projectedRate < result.line - 0.3) {
    const prob = Math.min(0.85, 0.5 + (result.line - projectedRate) * 0.1);
    const fairOdds = 1 / prob;
    result.confidence = parseFloat(prob.toFixed(3));
    result.recommended_odds = parseFloat(fairOdds.toFixed(2));
    if (result.current_odds > 0 && result.current_odds > fairOdds * 1.05) {
      result.edge = parseFloat(((result.current_odds / fairOdds) - 1).toFixed(4));
      result.triggered = result.confidence > (conditions.min_confidence || 0.6);
    }
  }

  return result;
}

function getDefaultMeta() {
  return {
    sports: [
      { id: 1, name: 'Football', slug: 'football', icon: '⚽' },
      { id: 2, name: 'Basketball', slug: 'basketball', icon: '🏀' },
      { id: 3, name: 'Tennis', slug: 'tennis', icon: '🎾' },
      { id: 4, name: 'Ice Hockey', slug: 'hockey', icon: '🏒' },
    ],
    tournaments: [],
    countries: [],
    statEventTypes: [
      { id: 1, code: 'total_goals', name: 'Total Goals', name_ru: 'Тотал голов', sport_slug: 'football', typical_over_under: 2.5 },
      { id: 2, code: 'total_corners', name: 'Total Corners', name_ru: 'Тотал угловых', sport_slug: 'football', typical_over_under: 9.5 },
      { id: 3, code: 'total_yellow_cards', name: 'Yellow Cards', name_ru: 'Жёлтые карточки', sport_slug: 'football', typical_over_under: 3.5 },
      { id: 4, code: 'basketball_points', name: 'Total Points', name_ru: 'Тотал очков', sport_slug: 'basketball', typical_over_under: 220.5 },
      { id: 5, code: 'hockey_goals', name: 'Total Goals', name_ru: 'Тотал голов', sport_slug: 'hockey', typical_over_under: 5.5 },
    ],
    subEventTypes: [],
    apiSources: [],
  };
}

module.exports = { router, init };