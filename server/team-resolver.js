'use strict';
// ═══════════════════════════════════════════════════════════════════════════
//  BetQuant Pro — server/team-resolver.js  v2
//  Загружает команды из ВСЕХ таблиц ClickHouse (не только football)
//  Fuzzy-матчинг: точное → нормализация → trigram similarity
// ═══════════════════════════════════════════════════════════════════════════

// ─── Нормализация ──────────────────────────────────────────────────────────
const PREFIXES = /^(fc|ac|afc|sc|ssc|cd|cf|bk|fk|sk|nk|vfb|vfl|rb|sv|tsv|rsb|rcd|rc|sd|ud|ca|real|atletico|sporting|deportivo)\s+/i;
const SUFFIXES = /\s+(fc|afc|sc|cf|united|city|town|utd|athletic|atletico|sport|sports|bc|basketball club|football club)$/i;

function normalize(name) {
  if (!name) return '';
  return name
    .toLowerCase()
    .replace(/[éèê]/g,'e').replace(/[àâ]/g,'a').replace(/[üúù]/g,'u')
    .replace(/[ïîí]/g,'i').replace(/[öó]/g,'o').replace(/ñ/g,'n')
    .replace(/ß/g,'ss').replace(/ø/g,'o').replace(/æ/g,'ae')
    .replace(PREFIXES, '').replace(SUFFIXES, '')
    .replace(/\b(de|del|la|el|le|der|van|den|the)\b/gi, '')
    .replace(/[^a-z0-9\s]/g, '').replace(/\s+/g, ' ').trim();
}

function trigrams(s) {
  const p = ` ${s} `; const set = new Set();
  for (let i = 0; i < p.length - 2; i++) set.add(p.slice(i, i+3));
  return set;
}
function trigramSim(a, b) {
  if (!a || !b) return 0;
  const ta = trigrams(a), tb = trigrams(b); let common = 0;
  for (const t of ta) if (tb.has(t)) common++;
  return (2 * common) / (ta.size + tb.size);
}

// ─── Конфиг таблиц по спортам ──────────────────────────────────────────────
const SPORT_TABLES = {
  football:   { table: 'betquant.football_matches',    home: 'home_team', away: 'away_team' },
  hockey:     { table: 'betquant.hockey_matches',      home: 'home_team', away: 'away_team' },
  basketball: { table: 'betquant.basketball_matches',  home: 'home_team', away: 'away_team', fallback: 'betquant.basketball_matches_v2' },
  tennis:     { table: 'betquant.tennis_extended',     home: 'winner',    away: 'loser',     alt: 'betquant.tennis_matches' },
  baseball:   { table: 'betquant.baseball_matches',    home: 'home_team', away: 'away_team' },
  rugby:      { table: 'betquant.rugby_matches',       home: 'home_team', away: 'away_team' },
  volleyball: { table: 'betquant.volleyball_matches',  home: 'home_team', away: 'away_team' },
  waterpolo:  { table: 'betquant.waterpolo_matches',   home: 'home_team', away: 'away_team' },
  cricket:    { table: 'betquant.cricket_matches',     home: 'team1',     away: 'team2' },
  nfl:        { table: 'betquant.nfl_games',           home: 'home_team', away: 'away_team' },
  mma:        { table: 'betquant.mma_matches',         home: 'fighter1',  away: 'fighter2' },
  esports:    { table: 'betquant.esports_matches',     home: 'team1',     away: 'team2' },
};

class TeamResolver {
  constructor() {
    // Map: sport → Map(normalizedName → originalName)
    this._normBySport = new Map();
    // Map: normalizedName → originalName (все виды спорта объединённо)
    this._normAll     = new Map();
    this._pgMappings  = new Map(); // norm(external) → internal
    this._cache       = new Map(); // inputName → resolvedName
    this._lastLoad    = 0;
    this._TTL         = 30 * 60 * 1000;
    this._loading     = false;
  }

  async init(clickhouse, pgPool) {
    if (this._loading) return;
    if (Date.now() - this._lastLoad < this._TTL && this._normAll.size > 0) return;
    this._loading = true;
    try {
      await Promise.all([
        this._loadAllSports(clickhouse),
        this._loadPgMappings(pgPool),
      ]);
      this._lastLoad = Date.now();
      console.log(`[Resolver] loaded ${this._normAll.size} teams across ${this._normBySport.size} sports`);
    } catch(e) {
      console.warn('[Resolver] init error:', e.message);
    } finally {
      this._loading = false;
    }
  }

  async _loadAllSports(ch) {
    if (!ch) return;
    this._normBySport.clear();
    this._normAll.clear();

    for (const [sport, cfg] of Object.entries(SPORT_TABLES)) {
      const sportMap = new Map();
      const tables = [cfg.table, cfg.fallback, cfg.alt].filter(Boolean);

      for (const table of tables) {
        try {
          const r = await ch.query({
            query: `
              SELECT DISTINCT ${cfg.home} AS team FROM ${table} WHERE date >= today()-730
              UNION ALL
              SELECT DISTINCT ${cfg.away} AS team FROM ${table} WHERE date >= today()-730
            `,
            format: 'JSON',
          });
          const d = await r.json();
          for (const row of (d.data || [])) {
            const t = row.team;
            if (!t) continue;
            const n = normalize(t);
            sportMap.set(n, t);
            this._normAll.set(n, t);
          }
          break; // если основная таблица OK — не нужен fallback
        } catch(e) {
          // Таблица не существует — пробуем следующую
        }
      }

      if (sportMap.size > 0) {
        this._normBySport.set(sport, sportMap);
        console.log(`[Resolver] ${sport}: ${sportMap.size} teams`);
      }
    }
  }

  async _loadPgMappings(pg) {
    if (!pg) return;
    try {
      const r = await pg.query(`
        SELECT tm.external_name, t.name AS internal_name
        FROM team_mappings tm JOIN teams t ON t.id = tm.team_id
        WHERE tm.external_name IS NOT NULL AND tm.confidence >= 0.5
      `);
      this._pgMappings.clear();
      for (const row of r.rows)
        this._pgMappings.set(normalize(row.external_name), row.internal_name);
    } catch(e) {} // team_mappings может не существовать
  }

  resolve(inputName, sport = null) {
    if (!inputName) return inputName;
    const cacheKey = `${sport||'any'}:${inputName}`;
    if (this._cache.has(cacheKey)) return this._cache.get(cacheKey);
    const result = this._doResolve(inputName, sport);
    this._cache.set(cacheKey, result);
    return result;
  }

  _doResolve(name, sport) {
    const normInput = normalize(name);

    // 1. PG маппинг (наивысший приоритет)
    if (this._pgMappings.has(normInput)) return this._pgMappings.get(normInput);

    // 2. Точное совпадение в таблице конкретного спорта
    if (sport && this._normBySport.has(sport)) {
      const sm = this._normBySport.get(sport);
      if (sm.has(normInput)) return sm.get(normInput);
    }

    // 3. Точное совпадение в общем индексе
    if (this._normAll.has(normInput)) return this._normAll.get(normInput);

    // 4. Trigram в таблице конкретного спорта
    const candidates = sport && this._normBySport.has(sport)
      ? this._normBySport.get(sport)
      : this._normAll;

    let best = 0, bestName = null;
    for (const [normCh, origCh] of candidates) {
      const score = trigramSim(normInput, normCh);
      if (score > best) { best = score; bestName = origCh; }
    }
    if (best >= 0.55 && bestName) return bestName;

    return name; // не нашли → оригинал
  }

  resolvePair(home, away, sport = null) {
    const rHome = this.resolve(home, sport);
    const rAway = this.resolve(away, sport);
    const changed = rHome !== home || rAway !== away;
    if (changed) console.log(`[Resolver] ${sport||'?'}: "${home}"→"${rHome}" | "${away}"→"${rAway}"`);
    return { home: rHome, away: rAway, resolved: changed };
  }

  stats() {
    const bySport = {};
    for (const [sport, m] of this._normBySport) bySport[sport] = m.size;
    return { total: this._normAll.size, bySport, pgMappings: this._pgMappings.size, cache: this._cache.size, lastLoad: new Date(this._lastLoad).toISOString() };
  }
}

module.exports = new TeamResolver();