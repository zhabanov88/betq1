'use strict';
// ═══════════════════════════════════════════════════════════════════════════
//  BetQuant Pro — server/value.js  v9  МУЛЬТИСПОРТ — БЕСПЛАТНЫЕ ИСТОЧНИКИ
//
//  ИСТОЧНИКИ (без ключей):
//  1. ESPN API  — NBA, NHL, MLB, NFL, Tennis ATP/WTA, MMA UFC
//                 MLS, Rugby, Cricket, Volleyball (NCAA)
//     base: https://site.api.espn.com/apis/site/v2/sports/
//
//  2. API-Sports free (rapidapi) — баскетбол, хоккей, волейбол
//     (если есть RAPIDAPI_KEY в .env)
//
//  3. The Odds API — коэффициенты для спортов где есть
//     (если есть ODDS_API_KEY)
//
//  4. Football-data.org / ApiFootball — футбол
//
//  5. OpenLigaDB — Бундеслига (без ключа)
//
//  АЛГОРИТМ:
//  1. ESPN даёт МАТЧИ (scheduled/live) для 12+ видов спорта
//  2. OddsAPI (если есть ключ) накладывает КОЭФФИЦИЕНТЫ поверх матчей
//  3. ClickHouse даёт λ для расчёта вероятностей
//  4. Для матчей без коэффициентов — расчётные через Poisson/ELO
// ═══════════════════════════════════════════════════════════════════════════
const express  = require('express');
const router   = express.Router();
const resolver = require('./team-resolver');

const ODDS_API_KEY = process.env.ODDS_API_KEY      || '';
const FDORG_KEY    = process.env.FOOTBALL_DATA_KEY  || '';
const APIFB_KEY    = process.env.APIFOOTBALL_KEY    || '';

const _cache    = new Map();
const CACHE_TTL = 5 * 60 * 1000;

// ─── ELO ──────────────────────────────────────────────────────────────────
const ELO_DEFAULT = 1500, ELO_K = 32;
const eloStore = new Map();
const elo_key  = t => String(t).toLowerCase().trim();
const getElo   = t => eloStore.get(elo_key(t)) || ELO_DEFAULT;
const updateElo = (h, a, gH, gA) => {
  const eH = getElo(h), eA = getElo(a);
  const expV = 1/(1+Math.pow(10,(eA-eH)/400));
  const res  = gH>gA?1:gH===gA?0.5:0;
  eloStore.set(elo_key(h),Math.round(eH+ELO_K*(res-expV)));
  eloStore.set(elo_key(a),Math.round(eA+ELO_K*((1-res)-(1-expV))));
};
const eloProbs = (h,a) => {
  const eH=getElo(h),eA=getElo(a);
  const expV=1/(1+Math.pow(10,(eA-eH)/400));
  return{homeWin:+Math.max(expV,0.05).toFixed(4),draw:+Math.max(0.27-Math.abs(expV-0.5)*0.25,0.05).toFixed(4),awayWin:+Math.max(1-expV,0.05).toFixed(4)};
};

// ─── Poisson ──────────────────────────────────────────────────────────────
const FACT=[1,1,2,6,24,120,720,5040,40320,362880];
const poisson=(k,l)=>k>9?0:Math.pow(l,k)*Math.exp(-l)/FACT[k];
const scoreMatrix=(lH,lA)=>{const m=[];for(let h=0;h<8;h++){m[h]=[];for(let a=0;a<8;a++)m[h][a]=poisson(h,lH)*poisson(a,lA);}return m;};
const aggregate=m=>{let hW=0,dr=0,aW=0,ov25=0,ov15=0,btts=0;for(let h=0;h<m.length;h++)for(let a=0;a<m[h].length;a++){const p=m[h][a];if(h>a)hW+=p;else if(h===a)dr+=p;else aW+=p;if(h+a>2.5)ov25+=p;if(h+a>1.5)ov15+=p;if(h>0&&a>0)btts+=p;}return{homeWin:+hW.toFixed(4),draw:+dr.toFixed(4),awayWin:+aW.toFixed(4),over25:+ov25.toFixed(4),btts:+btts.toFixed(4)};};

async function fetchJSON(url,headers={},ms=12000){
  const ctrl=new AbortController();const t=setTimeout(()=>ctrl.abort(),ms);
  try{const r=await fetch(url,{headers:{'User-Agent':'Mozilla/5.0 BetQuantBot/1.0',...headers},signal:ctrl.signal});if(!r.ok)throw new Error(`HTTP ${r.status}`);return await r.json();}
  catch(e){if(e.name==='AbortError')throw new Error('Timeout');throw e;}
  finally{clearTimeout(t);}
}

// ═══════════════════════════════════════════════════════════════════
//  ИСТОЧНИК 1: ESPN API (полностью бесплатно, без ключа)
//  Покрывает: NBA, NHL, MLB, NFL, MLS, UFC, Tennis, Rugby, Cricket
// ═══════════════════════════════════════════════════════════════════

// ESPN endpoint конфигурация по видам спорта
const ESPN_SPORTS = {
  basketball: [
    { path:'basketball/nba',        league:'NBA',        sport:'basketball' },
    { path:'basketball/wnba',       league:'WNBA',       sport:'basketball' },
    { path:'basketball/mens-college-basketball', league:'NCAA Basketball', sport:'basketball' },
  ],
  hockey: [
    { path:'hockey/nhl',            league:'NHL',        sport:'hockey'     },
  ],
  baseball: [
    { path:'baseball/mlb',          league:'MLB',        sport:'baseball'   },
  ],
  nfl: [
    { path:'football/nfl',          league:'NFL',        sport:'nfl'        },
    { path:'football/college-football', league:'NCAA Football', sport:'nfl' },
  ],
  football: [
    { path:'soccer/usa.1',          league:'MLS',        sport:'football'   },
    { path:'soccer/eng.1',          league:'Premier League', sport:'football'},
    { path:'soccer/esp.1',          league:'La Liga',    sport:'football'   },
    { path:'soccer/ger.1',          league:'Bundesliga', sport:'football'   },
    { path:'soccer/ita.1',          league:'Serie A',    sport:'football'   },
    { path:'soccer/fra.1',          league:'Ligue 1',    sport:'football'   },
    { path:'soccer/uefa.champions', league:'Champions League', sport:'football'},
  ],
  tennis: [
    { path:'tennis/atp',            league:'ATP Tennis', sport:'tennis'     },
    { path:'tennis/wta',            league:'WTA Tennis', sport:'tennis'     },
  ],
  mma: [
    { path:'mma/ufc',               league:'UFC',        sport:'mma'        },
  ],
  rugby: [
    { path:'rugby/nrl',             league:'NRL Rugby',  sport:'rugby'      },
    { path:'rugby/premiership',     league:'Premiership Rugby', sport:'rugby'},
    { path:'rugby/super.rugby',     league:'Super Rugby',sport:'rugby'      },
  ],
  cricket: [
    { path:'cricket/icc.cricket',   league:'Cricket International', sport:'cricket'},
    { path:'cricket/ipl',           league:'IPL Cricket',sport:'cricket'    },
  ],
  volleyball: [
    { path:'volleyball/mens-college-volleyball', league:'NCAA Volleyball', sport:'volleyball'},
  ],
};

async function fetchFromESPN(targetSport) {
  const ck = `espn_${targetSport}`;
  const ca = _cache.get(ck);
  if (ca && Date.now()-ca.ts < CACHE_TTL) return ca.data;

  const endpoints = targetSport === 'all'
    ? Object.values(ESPN_SPORTS).flat()
    : (ESPN_SPORTS[targetSport] || []);

  const fixtures = [];
  const now = Date.now();

  for (const ep of endpoints) {
    try {
      const url = `https://site.api.espn.com/apis/site/v2/sports/${ep.path}/scoreboard`;
      const data = await fetchJSON(url, {}, 10000);

      for (const event of (data.events || [])) {
        // Берём только scheduled и in-progress
        const status = event.status?.type?.name || '';
        if (status === 'STATUS_FINAL') continue;

        const comp       = event.competitions?.[0];
        if (!comp) continue;
        const competitors = comp.competitors || [];
        if (competitors.length < 2) continue;

        const home = competitors.find(c=>c.homeAway==='home') || competitors[0];
        const away = competitors.find(c=>c.homeAway==='away') || competitors[1];

        const homeName = home.team?.displayName || home.team?.name || 'Home';
        const awayName = away.team?.displayName || away.team?.name || 'Away';
        if (!homeName || !awayName) continue;

        // Коэффициенты из ESPN (если есть)
        const odds = comp.odds?.[0] || {};
        const homeOdds = odds.homeTeamOdds?.moneyLine
          ? moneylineToDecimal(odds.homeTeamOdds.moneyLine) : null;
        const awayOdds = odds.awayTeamOdds?.moneyLine
          ? moneylineToDecimal(odds.awayTeamOdds.moneyLine) : null;
        const overUnder = odds.overUnder || null;

        fixtures.push({
          id:         `espn_${event.id}`,
          sport:      ep.sport,
          sportKey:   ep.path,
          league:     ep.league,
          home:       homeName,
          away:       awayName,
          startTime:  event.date || new Date().toISOString(),
          bH:         homeOdds,
          bD:         null,
          bA:         awayOdds,
          bO:         overUnder ? 1.90 : null,  // ESPN редко даёт O/U
          bU:         overUnder ? 1.90 : null,
          bB:         null,
          bmCount:    homeOdds ? 1 : 0,
          _source:    'espn',
          _overUnder: overUnder,
          _status:    status,
        });
      }
      console.log(`[value] ESPN ${ep.path}: ${fixtures.length} total so far`);
    } catch(e) {
      console.warn(`[value] ESPN ${ep.path}: ${e.message}`);
    }
  }

  _cache.set(ck, { ts: Date.now(), data: fixtures });
  return fixtures;
}

// Перевод американских коэффициентов (moneyline) в десятичные
function moneylineToDecimal(ml) {
  if (!ml || ml === 0) return null;
  const n = +ml;
  if (isNaN(n)) return null;
  return n > 0 ? +(n/100 + 1).toFixed(2) : +(100/Math.abs(n) + 1).toFixed(2);
}

// ═══════════════════════════════════════════════════════════════════
//  ИСТОЧНИК 2: The Odds API (коэффициенты, если есть ключ)
// ═══════════════════════════════════════════════════════════════════
const ODDS_SPORT_KEYS = {
  football:   ['soccer_epl','soccer_spain_la_liga','soccer_germany_bundesliga','soccer_italy_serie_a','soccer_france_ligue_one','soccer_uefa_champs_league','soccer_russia_premier_league'],
  basketball: ['basketball_nba','basketball_euroleague','basketball_nbl'],
  hockey:     ['icehockey_nhl','icehockey_khl'],
  tennis:     ['tennis_atp_french_open','tennis_wta_french_open','tennis_atp_wimbledon'],
  mma:        ['mma_mixed_martial_arts'],
  baseball:   ['baseball_mlb'],
  cricket:    ['cricket_icc_world_cup'],
  rugby:      ['rugbyleague_nrl','rugby_union_super_rugby'],
  nfl:        ['americanfootball_nfl'],
  esports:    ['esports_lol'],
};

async function fetchOddsForFixtures(fixtures) {
  if (!ODDS_API_KEY || !fixtures.length) return fixtures;

  // Собираем все нужные спорты
  const sportsNeeded = [...new Set(fixtures.map(f=>f.sport))];
  const oddsMap = new Map(); // `${home}|${away}` → odds

  for (const sport of sportsNeeded) {
    const keys = ODDS_SPORT_KEYS[sport] || [];
    for (const key of keys.slice(0,2)) {
      const ck = `odds_${key}`;
      const ca = _cache.get(ck);
      let data;
      if (ca && Date.now()-ca.ts < CACHE_TTL) {
        data = ca.data;
      } else {
        try {
          const raw = await fetchJSON(
            `https://api.the-odds-api.com/v4/sports/${key}/odds/?apiKey=${ODDS_API_KEY}&regions=eu,uk&markets=h2h,totals,btts&oddsFormat=decimal&dateFormat=iso`
          );
          data = Array.isArray(raw) ? raw : [];
          _cache.set(ck, { ts:Date.now(), data });
          console.log(`[value] OddsAPI ${key}: ${data.length} fixtures`);
        } catch(e) {
          console.warn(`[value] OddsAPI ${key}: ${e.message}`);
          continue;
        }
      }
      for (const g of data) {
        const bms = {};
        for (const bm of (g.bookmakers||[])) {
          const h2h    = bm.markets?.find(m=>m.key==='h2h');
          const totals = bm.markets?.find(m=>m.key==='totals');
          const btts   = bm.markets?.find(m=>m.key==='btts');
          if (!h2h) continue;
          bms[bm.key] = {
            home:    h2h.outcomes.find(o=>o.name===g.home_team)?.price||0,
            draw:    h2h.outcomes.find(o=>o.name==='Draw')?.price||0,
            away:    h2h.outcomes.find(o=>o.name===g.away_team)?.price||0,
            over25:  totals?.outcomes.find(o=>o.name==='Over'&&Math.abs((o.point||2.5)-2.5)<0.1)?.price||0,
            under25: totals?.outcomes.find(o=>o.name==='Under'&&Math.abs((o.point||2.5)-2.5)<0.1)?.price||0,
            btts:    btts?.outcomes.find(o=>o.name==='Yes')?.price||0,
          };
        }
        const vals = Object.values(bms);
        const best = {
          home:    vals.length?Math.max(0,...vals.map(b=>b.home||0)):0,
          draw:    vals.length?Math.max(0,...vals.map(b=>b.draw||0)):0,
          away:    vals.length?Math.max(0,...vals.map(b=>b.away||0)):0,
          over25:  vals.length?Math.max(0,...vals.map(b=>b.over25||0)):0,
          under25: vals.length?Math.max(0,...vals.map(b=>b.under25||0)):0,
          btts:    vals.length?Math.max(0,...vals.map(b=>b.btts||0)):0,
        };
        const mk = `${g.home_team}|${g.away_team}`;
        oddsMap.set(mk, { bH:best.home>1.01?best.home:null, bD:best.draw>1.01?best.draw:null, bA:best.away>1.01?best.away:null, bO:best.over25>1.01?best.over25:null, bU:best.under25>1.01?best.under25:null, bB:best.btts>1.01?best.btts:null, bmCount:Object.keys(bms).length });
      }
    }
  }

  // Накладываем коэффициенты на ESPN-матчи через fuzzy matching
  return fixtures.map(f => {
    // Точный матч
    let key = `${f.home}|${f.away}`;
    let odds = oddsMap.get(key);

    // Fuzzy: ищем похожие имена в oddsMap
    if (!odds) {
      const normHome = f.home.toLowerCase().replace(/[^a-z]/g,'');
      const normAway = f.away.toLowerCase().replace(/[^a-z]/g,'');
      for (const [k,v] of oddsMap) {
        const [kH,kA] = k.split('|');
        const nH = kH.toLowerCase().replace(/[^a-z]/g,'');
        const nA = kA.toLowerCase().replace(/[^a-z]/g,'');
        if (nH.includes(normHome.slice(0,6)) || normHome.includes(nH.slice(0,6))) {
          if (nA.includes(normAway.slice(0,6)) || normAway.includes(nA.slice(0,6))) {
            odds = v; break;
          }
        }
      }
    }

    if (odds) {
      return { ...f, ...odds, _source: f._source === 'espn' ? 'espn+odds' : f._source };
    }
    return f;
  });
}

// ═══════════════════════════════════════════════════════════════════
//  ИСТОЧНИК 3: Дополнительные футбольные источники
// ═══════════════════════════════════════════════════════════════════
async function fetchFromFootballDataOrg(){
  if(!FDORG_KEY)return[];const ck='fdorg';const ca=_cache.get(ck);
  if(ca&&Date.now()-ca.ts<CACHE_TTL)return ca.data;
  try{
    const today=new Date();const from=today.toISOString().slice(0,10);const to=new Date(today.getTime()+7*86400000).toISOString().slice(0,10);
    const data=await fetchJSON(`https://api.football-data.org/v4/matches?dateFrom=${from}&dateTo=${to}&status=SCHEDULED`,{'X-Auth-Token':FDORG_KEY});
    if(!data?.matches?.length)return[];
    const f=data.matches.map(m=>({id:`fdorg_${m.id}`,home:m.homeTeam?.name||'Home',away:m.awayTeam?.name||'Away',league:m.competition?.name||'Football',sport:'football',sportKey:'soccer',startTime:m.utcDate,bH:null,bD:null,bA:null,bO:null,bU:null,bB:null,bmCount:0,_source:'football-data.org'}));
    _cache.set(ck,{ts:Date.now(),data:f});console.log(`[value] FDORG: ${f.length}`);return f;
  }catch(e){console.warn('[value] FDORG:',e.message);return[];}
}

async function fetchFromApiFootball(){
  if(!APIFB_KEY)return[];const ck='apifb';const ca=_cache.get(ck);
  if(ca&&Date.now()-ca.ts<CACHE_TTL)return ca.data;
  try{
    const today=new Date().toISOString().slice(0,10);
    const data=await fetchJSON(`https://apiv3.apifootball.com/?action=get_events&from=${today}&to=${today}&APIkey=${APIFB_KEY}`);
    if(!Array.isArray(data)||!data.length)return[];
    const f=data.slice(0,60).map(m=>({id:`apifb_${m.match_id}`,home:m.match_hometeam_name||'Home',away:m.match_awayteam_name||'Away',league:m.league_name||'Football',sport:'football',sportKey:'soccer',startTime:`${m.match_date}T${m.match_time||'12:00'}:00Z`,bH:parseFloat(m.odds?.['Odds_1'])>1.01?parseFloat(m.odds['Odds_1']):null,bD:parseFloat(m.odds?.['Odds_X'])>1.01?parseFloat(m.odds['Odds_X']):null,bA:parseFloat(m.odds?.['Odds_2'])>1.01?parseFloat(m.odds['Odds_2']):null,bO:null,bU:null,bB:null,bmCount:m.odds?1:0,_source:'apifootball.com'}));
    _cache.set(ck,{ts:Date.now(),data:f});console.log(`[value] APIFB: ${f.length}`);return f;
  }catch(e){console.warn('[value] APIFB:',e.message);return[];}
}

async function fetchFromOpenLigaDB(){
  const ck='openliga';const ca=_cache.get(ck);
  if(ca&&Date.now()-ca.ts<CACHE_TTL)return ca.data;
  try{
    const season=new Date().getFullYear()-(new Date().getMonth()<6?1:0);
    const data=await fetchJSON(`https://api.openligadb.de/getmatchdata/bl1/${season}`);
    if(!Array.isArray(data))return[];
    const now=Date.now();
    const f=data.filter(m=>!m.matchIsFinished&&new Date(m.matchDateTimeUTC).getTime()>now).slice(0,10).map(m=>({id:`ol_${m.matchID}`,home:m.team1?.teamName||'Home',away:m.team2?.teamName||'Away',league:'1. Bundesliga',sport:'football',sportKey:'soccer',startTime:m.matchDateTimeUTC,bH:null,bD:null,bA:null,bO:null,bU:null,bB:null,bmCount:0,_source:'openligadb'}));
    _cache.set(ck,{ts:Date.now(),data:f});console.log(`[value] OpenLiga: ${f.length}`);return f;
  }catch(e){console.warn('[value] OpenLiga:',e.message);return[];}
}

// ═══════════════════════════════════════════════════════════════════
//  Конфиг ClickHouse таблиц
// ═══════════════════════════════════════════════════════════════════
const SPORT_DB = {
  football:   { table:'betquant.football_matches',   home:'home_team',away:'away_team',scored:'home_goals',conceded:'away_goals',avgScored:1.35,formTable:'betquant.football_team_form' },
  hockey:     { table:'betquant.hockey_matches',     home:'home_team',away:'away_team',scored:'home_goals',conceded:'away_goals',avgScored:2.8,  formTable:'betquant.hockey_team_form' },
  basketball: { table:'betquant.basketball_matches', home:'home_team',away:'away_team',scored:'home_pts',  conceded:'away_pts',  avgScored:110,  fallback:'betquant.basketball_matches_v2' },
  tennis:     { table:'betquant.tennis_extended',    home:'winner',   away:'loser',    scored:'w_sets',    conceded:'l_sets',   avgScored:1.8 },
  baseball:   { table:'betquant.baseball_matches',   home:'home_team',away:'away_team',scored:'home_runs', conceded:'away_runs',avgScored:4.5 },
  rugby:      { table:'betquant.rugby_matches',      home:'home_team',away:'away_team',scored:'home_score',conceded:'away_score',avgScored:25 },
  volleyball: { table:'betquant.volleyball_matches', home:'home_team',away:'away_team',scored:'home_sets', conceded:'away_sets', avgScored:2.5 },
  waterpolo:  { table:'betquant.waterpolo_matches',  home:'home_team',away:'away_team',scored:'home_score',conceded:'away_score',avgScored:12 },
  cricket:    { table:'betquant.cricket_matches',    home:'team1',    away:'team2',    scored:'team1_runs',conceded:'team2_runs',avgScored:250 },
  nfl:        { table:'betquant.nfl_games',          home:'home_team',away:'away_team',scored:'home_score',conceded:'away_score',avgScored:24 },
  mma:        { table:'betquant.mma_matches',        home:'fighter1', away:'fighter2', scored:'toUInt8(1)',conceded:'toUInt8(0)',avgScored:1 },
  esports:    { table:'betquant.esports_matches',    home:'team1',    away:'team2',    scored:'team1_score',conceded:'team2_score',avgScored:1 },
};

// ── ClickHouse stats ──────────────────────────────────────────────────────
async function loadTeamStats(clickhouse, fixtures) {
  if (!clickhouse||!fixtures.length) return {};
  const stats = {};
  const bySport = {};
  for (const f of fixtures) {
    const s = f.sport||'football';
    if (!bySport[s]) bySport[s] = [];
    bySport[s].push(f);
  }
  for (const [sport, sportFixtures] of Object.entries(bySport)) {
    const db = SPORT_DB[sport]; if (!db) continue;
    const teams = [...new Set(sportFixtures.flatMap(f=>[f.homeResolved||f.home,f.awayResolved||f.away]))].filter(Boolean);
    if (!teams.length) continue;
    const list = teams.map(t=>`'${t.replace(/'/g,"''")}'`).join(',');
    const tables = [db.table, db.fallback].filter(Boolean);
    for (const table of tables) {
      try {
        const r = await clickhouse.query({
          query:`SELECT team,avg(scored)AS avg_scored,avg(conceded)AS avg_conceded,sum(n)AS matches FROM(SELECT ${db.home} AS team,toFloat64(${db.scored})AS scored,toFloat64(${db.conceded})AS conceded,1 AS n FROM ${table} WHERE ${db.home} IN(${list})AND date>=today()-365 UNION ALL SELECT ${db.away} AS team,toFloat64(${db.conceded})AS scored,toFloat64(${db.scored})AS conceded,1 AS n FROM ${table} WHERE ${db.away} IN(${list})AND date>=today()-365)GROUP BY team HAVING matches>=3`,
          format:'JSON'});
        const d = await r.json();
        for (const row of (d.data||[])) {
          stats[elo_key(row.team)]={scored:+row.avg_scored,conceded:+row.avg_conceded,n:+row.matches,sport};
          updateElo(row.team,'__avg__',+row.avg_scored,+row.avg_conceded);
        }
        console.log(`[value] stats ${sport}(${table}): ${d.data?.length||0} teams`);
        break;
      } catch(e){console.warn(`[value] stats ${sport} ${table}: ${e.message}`);}
    }
  }
  return stats;
}

async function loadH2HData(clickhouse, fixtures) {
  if (!clickhouse||!fixtures.length) return new Map();
  const h2hMap=new Map();
  const bySport={};
  for (const f of fixtures){const s=f.sport||'football';if(!bySport[s])bySport[s]=[];bySport[s].push(f);}
  for (const [sport,sportFixtures] of Object.entries(bySport)){
    const db=SPORT_DB[sport];if(!db)continue;
    const pairCond=sportFixtures.map(f=>{const h=(f.homeResolved||f.home).replace(/'/g,"''"),a=(f.awayResolved||f.away).replace(/'/g,"''");return`(${db.home}='${h}' AND ${db.away}='${a}') OR (${db.home}='${a}' AND ${db.away}='${h}')`;}).join(' OR ');
    const tables=[db.table,db.fallback].filter(Boolean);
    for (const table of tables){
      try{
        const r=await clickhouse.query({query:`SELECT ${db.home} AS home_team,${db.away} AS away_team,toFloat64(${db.scored})AS home_score,toFloat64(${db.conceded})AS away_score,date FROM ${table} WHERE (${pairCond})AND date<today() ORDER BY date DESC LIMIT 500`,format:'JSON'});
        const rows=(await r.json()).data||[];
        for(const f of sportFixtures){
          const h=f.homeResolved||f.home,a=f.awayResolved||f.away;
          const key=`${f.home}|${f.away}`;
          const pairRows=rows.filter(row=>(row.home_team===h&&row.away_team===a)||(row.home_team===a&&row.away_team===h)).slice(0,10);
          const results=pairRows.map(row=>{const iH=row.home_team===h;const my=iH?+row.home_score:+row.away_score,their=iH?+row.away_score:+row.home_score;return{home:row.home_team,away:row.away_team,home_goals:+row.home_score,away_goals:+row.away_score,date:row.date,result:my>their?'home':my<their?'away':'draw'};});
          h2hMap.set(key,{results,homeWins:results.filter(r=>r.result==='home').length,awayWins:results.filter(r=>r.result==='away').length,draws:results.filter(r=>r.result==='draw').length,avgGoals:results.length?+(results.reduce((s,r)=>s+r.home_goals+r.away_goals,0)/results.length).toFixed(2):0,total:results.length});
        }
        console.log(`[value] H2H ${sport}: ${rows.length} rows`);break;
      }catch(e){console.warn(`[value] H2H ${sport} ${table}: ${e.message}`);}
    }
  }
  return h2hMap;
}

async function loadFormData(clickhouse, fixtures) {
  if (!clickhouse||!fixtures.length) return new Map();
  const formMap=new Map();
  const bySport={};
  for(const f of fixtures){const s=f.sport||'football';const db=SPORT_DB[s];if(!db?.formTable)continue;if(!bySport[s])bySport[s]={db,teams:new Set()};bySport[s].teams.add(f.homeResolved||f.home);bySport[s].teams.add(f.awayResolved||f.away);}
  for(const [sport,{db,teams}] of Object.entries(bySport)){
    const tl=[...teams].filter(Boolean);if(!tl.length)continue;
    const list=tl.map(t=>`'${t.replace(/'/g,"''")}'`).join(',');
    try{
      const q=sport==='football'
        ?`SELECT team,argMax(form_5,date)AS form_5,argMax(form_10,date)AS form_10,argMax(pts_5,date)AS pts_5,argMax(pts_10,date)AS pts_10,argMax(season_wins,date)AS season_wins,argMax(season_draws,date)AS season_draws,argMax(season_losses,date)AS season_losses,argMax(season_goals_for,date)AS season_goals_for,argMax(season_goals_against,date)AS season_goals_against,argMax(season_xg_for,date)AS season_xg_for,argMax(season_xg_against,date)AS season_xg_against FROM ${db.formTable} WHERE team IN(${list}) GROUP BY team`
        :`SELECT team,argMax(form_5,date)AS form_5,argMax(form_10,date)AS form_10,argMax(pts_5,date)AS pts_5,argMax(pts_10,date)AS pts_10,argMax(season_wins,date)AS season_wins,argMax(season_losses,date)AS season_losses,argMax(season_goals_for,date)AS season_goals_for,argMax(season_goals_against,date)AS season_goals_against FROM ${db.formTable} WHERE team IN(${list}) GROUP BY team`;
      const r=await clickhouse.query({query:q,format:'JSON'});
      for(const row of (await r.json()).data||[]){
        const fd={form5:String(row.form_5||'').split(''),form10:String(row.form_10||'').split(''),pts5:+row.pts_5||0,pts10:+row.pts_10||0,seasonWins:+row.season_wins||0,seasonDraws:+row.season_draws||0,seasonLosses:+row.season_losses||0,seasonGoalsFor:+row.season_goals_for||0,seasonGoalsAgainst:+row.season_goals_against||0,seasonXgFor:+row.season_xg_for||0,seasonXgAgainst:+row.season_xg_against||0,sport};
        formMap.set(row.team,fd);
      }
      for(const f of fixtures.filter(x=>x.sport===sport)){const h=f.homeResolved||f.home,a=f.awayResolved||f.away;if(formMap.has(h)&&!formMap.has(f.home))formMap.set(f.home,formMap.get(h));if(formMap.has(a)&&!formMap.has(f.away))formMap.set(f.away,formMap.get(a));}
      console.log(`[value] Form ${sport}: ${formMap.size} teams`);
    }catch(e){console.warn(`[value] Form ${sport}: ${e.message}`);}
  }
  return formMap;
}

// ── team context ──────────────────────────────────────────────────────────
function buildTeamCtx(home,away,sport,formMap,stats,lH,lA){
  const getForm=t=>formMap.get(t)||formMap.get(resolver.resolve(t,sport));
  const getStat=t=>stats[elo_key(t)]||stats[elo_key(resolver.resolve(t,sport))];
  return{
    form(t,n=5){const fd=getForm(t);if(!fd)return Array(n).fill('D');const arr=fd.form10.length>=n?fd.form10:[...fd.form10,...fd.form5];return arr.slice(-n);},
    goalsScored(t,n=5){const fd=getForm(t);if(fd){const tot=fd.seasonWins+(fd.seasonDraws||0)+fd.seasonLosses;if(tot>0)return+(fd.seasonGoalsFor/tot).toFixed(2);}const st=getStat(t);if(st)return+st.scored.toFixed(2);return t===home?lH:lA;},
    goalsConceded(t,n=5){const fd=getForm(t);if(fd){const tot=fd.seasonWins+(fd.seasonDraws||0)+fd.seasonLosses;if(tot>0)return+(fd.seasonGoalsAgainst/tot).toFixed(2);}const st=getStat(t);if(st)return+st.conceded.toFixed(2);return t===home?lA:lH;},
    avgGoals(t){return this.goalsScored(t);},
    xG(t,n=5){const fd=getForm(t);if(fd?.seasonXgFor){const tot=fd.seasonWins+(fd.seasonDraws||0)+fd.seasonLosses;if(tot>0)return+(fd.seasonXgFor/tot).toFixed(2);}return this.goalsScored(t);},
    xGA(t,n=5){const fd=getForm(t);if(fd?.seasonXgAgainst){const tot=fd.seasonWins+(fd.seasonDraws||0)+fd.seasonLosses;if(tot>0)return+(fd.seasonXgAgainst/tot).toFixed(2);}return this.goalsConceded(t);},
    pointsScored(t,n=5){return this.goalsScored(t,n);},pointsConceded(t,n=5){return this.goalsConceded(t,n);},avgPoints(t){return this.goalsScored(t);},
    winRate(t,n=10){const fd=getForm(t);if(fd){const tot=fd.seasonWins+fd.seasonLosses;return tot>0?+(fd.seasonWins/tot).toFixed(2):0.5;}return 0.5;},
    elo(t){return getElo(resolver.resolve(t,sport));},rank(t){return 50;},
    homeWins(t,n=8){return getForm(t)?.seasonWins||0;},awayWins(t,n=8){return getForm(t)?.seasonWins||0;},
  };
}
function buildH2HCtx(home,away,h2hMap){const d=h2hMap.get(`${home}|${away}`);return d||{results:[],homeWins:0,awayWins:0,draws:0,avgGoals:0,total:0};}

// ── lambdas по спорту ─────────────────────────────────────────────────────
const DEFAULTS_BY_SPORT={football:[1.45,1.15],hockey:[2.9,2.7],basketball:[112,108],tennis:[1.8,1.2],baseball:[4.5,4.0],rugby:[26,22],volleyball:[2.5,2.2],waterpolo:[12,10],cricket:[260,240],nfl:[24,21],mma:[1,0],esports:[1.3,1.0]};
function lambdas(home,away,sport,stats){
  const db=SPORT_DB[sport];const avgS=db?.avgScored||1.35;
  const hk=elo_key(home),ak=elo_key(away),hrk=elo_key(resolver.resolve(home,sport)),ark=elo_key(resolver.resolve(away,sport));
  const hs=stats[hk]||stats[hrk],as_=stats[ak]||stats[ark];
  if(hs?.n>=3&&as_?.n>=3){const lH=+Math.max(avgS*(hs.scored/avgS)*(as_.conceded/avgS)*1.1,0.1).toFixed(3);const lA=+Math.max(avgS*(as_.scored/avgS)*(hs.conceded/avgS)*0.9,0.1).toFixed(3);return{lH,lA,src:'history'};}
  const[dH,dA]=DEFAULTS_BY_SPORT[sport]||[1.45,1.15];return{lH:dH,lA:dA,src:'default'};
}

// ── calcBets ──────────────────────────────────────────────────────────────
const LO={football:{homeWin:2.10,draw:3.40,awayWin:3.20,over25:1.90,under25:1.95,btts:1.85},hockey:{homeWin:1.90,draw:4.00,awayWin:2.10,over55:1.85,under55:1.95,btts:1.50},basketball:{homeWin:1.75,awayWin:2.10,over220:1.90,under220:1.90},tennis:{homeWin:1.60,awayWin:2.30},baseball:{homeWin:1.80,awayWin:2.05,over85:1.90,under85:1.90},rugby:{homeWin:1.75,draw:8.00,awayWin:2.10,over45:1.85},volleyball:{homeWin:1.70,awayWin:2.20},waterpolo:{homeWin:1.75,draw:5.00,awayWin:2.05},nfl:{homeWin:1.85,awayWin:1.95,over475:1.90},mma:{homeWin:1.70,awayWin:2.10},default:{homeWin:1.85,draw:3.50,awayWin:2.00,over25:1.90}};

function calcBets(fixture,stats,minEdge){
  const{home,away,sport,league,startTime,bH,bD,bA,bO,bU,bB}=fixture;
  const{lH,lA,src}=lambdas(home,away,sport,stats);
  const lo=LO[sport]||LO.default;
  const elo=eloProbs(resolver.resolve(home,sport),resolver.resolve(away,sport));
  const noDraw=['tennis','basketball','mma','nfl','baseball','esports','volleyball','waterpolo'];
  const hasDraw=!noDraw.includes(sport);
  let model;
  if(sport==='football'){const pois=aggregate(scoreMatrix(lH,lA));model={homeWin:+(pois.homeWin*.65+elo.homeWin*.35).toFixed(4),draw:+(pois.draw*.65+elo.draw*.35).toFixed(4),awayWin:+(pois.awayWin*.65+elo.awayWin*.35).toFixed(4),over25:+pois.over25.toFixed(4),under25:+(1-pois.over25).toFixed(4),btts:+pois.btts.toFixed(4)};}
  else if(sport==='hockey'){const pois=aggregate(scoreMatrix(lH,lA));const ov55=1-(Array.from({length:11},(_,h)=>Array.from({length:11},(_,a)=>h+a<5.5?poisson(h,lH)*poisson(a,lA):0).reduce((s,v)=>s+v,0)).reduce((s,v)=>s+v,0));model={homeWin:+(pois.homeWin*.6+elo.homeWin*.4).toFixed(4),draw:0.12,awayWin:+(pois.awayWin*.6+elo.awayWin*.4).toFixed(4),over55:+Math.max(ov55,0.05).toFixed(4),under55:+Math.max(1-ov55,0.05).toFixed(4),btts:+pois.btts.toFixed(4)};}
  else if(sport==='basketball'){const totalAvg=lH+lA;const overProb=1-1/(1+Math.exp(-(totalAvg-(bO?1/(bO>1?bO-1:1)*100:220))*0.03));model={homeWin:+(elo.homeWin*.5+(lH/(lH+lA))*.5).toFixed(4),draw:0,awayWin:+(elo.awayWin*.5+(lA/(lH+lA))*.5).toFixed(4),over25:+Math.min(Math.max(overProb,0.3),0.75).toFixed(4),under25:+Math.min(Math.max(1-overProb,0.3),0.75).toFixed(4)};}
  else if(sport==='tennis'){model={homeWin:+Math.min(Math.max(elo.homeWin+0.02,0.15),0.85).toFixed(4),draw:0,awayWin:+Math.min(Math.max(elo.awayWin-0.02,0.15),0.85).toFixed(4)};}
  else{model={homeWin:+(elo.homeWin*.5+(lH/(lH+lA+0.001))*.5).toFixed(4),draw:hasDraw?+elo.draw.toFixed(4):0,awayWin:+(elo.awayWin*.5+(lA/(lH+lA+0.001))*.5).toFixed(4),over25:0.52,under25:0.48};}

  const markets=[];
  if(model.homeWin>0)markets.push({key:'homeWin',prob:model.homeWin,odds:bH||lo.homeWin,bkOdds:!!bH});
  if(hasDraw&&model.draw>0)markets.push({key:'draw',prob:model.draw,odds:bD||lo.draw||3.5,bkOdds:!!bD});
  if(model.awayWin>0)markets.push({key:'awayWin',prob:model.awayWin,odds:bA||lo.awayWin,bkOdds:!!bA});
  if(sport==='hockey'){if(model.over55)markets.push({key:'over25',prob:model.over55,odds:bO||lo.over55||1.85,bkOdds:!!bO});if(model.under55)markets.push({key:'under25',prob:model.under55,odds:bU||lo.under55||1.95,bkOdds:!!bU});if(model.btts)markets.push({key:'btts',prob:model.btts,odds:bB||lo.btts||1.50,bkOdds:!!bB});}
  else if(sport==='football'){markets.push({key:'over25',prob:model.over25,odds:bO||lo.over25,bkOdds:!!bO});markets.push({key:'under25',prob:model.under25,odds:bU||lo.under25,bkOdds:!!bU});markets.push({key:'btts',prob:model.btts,odds:bB||lo.btts,bkOdds:!!bB});}
  else if(model.over25&&(bO||bU)){markets.push({key:'over25',prob:model.over25,odds:bO||lo.over25||1.90,bkOdds:!!bO});markets.push({key:'under25',prob:model.under25,odds:bU||lo.under25||1.90,bkOdds:!!bU});}

  const bets=[];const ko=startTime?new Date(startTime):null;
  for(const{key,prob,odds,bkOdds}of markets){
    if(!odds||odds<1.05||!prob)continue;const impl=1/odds;const edge=prob-impl;if(edge*100<minEdge)continue;
    const kelly=Math.max(0,(prob*(odds-1)-(1-prob))/(odds-1));
    bets.push({league,sport,sportKey:fixture.sportKey,match:`${home} vs ${away}`,home,away,homeResolved:fixture.homeResolved||home,awayResolved:fixture.awayResolved||away,resolverUsed:fixture.homeMatched||fixture.awayMatched,market:key,odds:+odds.toFixed(2),impliedProb:+(impl*100).toFixed(1),modelProb:+(prob*100).toFixed(1),edge:+(edge*100).toFixed(2),kelly:+(kelly*100*0.5).toFixed(1),lH,lA,lambdaSrc:src,oddsSource:bkOdds?'bookmaker':'estimated',startTime,kickoff:ko?ko.toLocaleString('ru',{month:'short',day:'numeric',hour:'2-digit',minute:'2-digit'}):'',daysToKickoff:ko?+((ko.getTime()-Date.now())/86400000).toFixed(1):null,bmCount:fixture.bmCount||0,dataSource:fixture._source||'unknown',matchedStrategies:[]});
  }
  return bets;
}

// ── applyStrategies ──────────────────────────────────────────────────────
const MARKET_MAP={homeWin:['home','homewin','1','home_win'],awayWin:['away','awaywin','2','away_win'],draw:['draw','x'],over25:['over','over25','over2.5','o2.5','over55'],under25:['under','under25','under2.5','u2.5','under55'],btts:['btts','yes','both']};
function marketMatches(bm,sm){if(!sm)return true;return(MARKET_MAP[bm]||[bm.toLowerCase()]).includes(String(sm).toLowerCase());}
function applyStrategies(bets,strategies,h2hMap,formMap,stats){
  if(!strategies?.length)return{bets,applied:false};
  const allBets=bets,passed=[];
  for(const bet of bets){
    const matchedNames=[];
    for(const s of strategies){
      const sId=String(s.id||s.name||''),sName=String(s.name||s.id||'Unnamed');
      if(s.sport&&s.sport!=='all'&&s.sport!=='any'&&s.sport!==bet.sport)continue;
      if(!s.code||!s.code.trim()){matchedNames.push({id:sId,name:sName});continue;}
      try{
        const _s=mkt=>allBets.find(b=>b.home===bet.home&&b.away===bet.away&&b.sport===bet.sport&&b.market===mkt);
        const _bH=_s('homeWin'),_bD=_s('draw'),_bA=_s('awayWin'),_bO=_s('over25'),_bU=_s('under25'),_bB=_s('btts');
        const matchCtx={team_home:bet.home,team_away:bet.away,league:bet.league,sport:bet.sport,date:bet.startTime||new Date().toISOString(),team_home_db:bet.homeResolved||bet.home,team_away_db:bet.awayResolved||bet.away,odds_home:_bH?_bH.odds:2.1,odds_draw:_bD?_bD.odds:3.4,odds_away:_bA?_bA.odds:3.2,odds_over:_bO?_bO.odds:1.9,odds_over25:_bO?_bO.odds:1.9,odds_under:_bU?_bU.odds:1.95,odds_btts:_bB?_bB.odds:1.85,lH:bet.lH||1.45,lA:bet.lA||1.15,prob_home:_bH?_bH.modelProb/100:0.45,prob_draw:_bD?_bD.modelProb/100:0.27,prob_away:_bA?_bA.modelProb/100:0.28,prob_over25:_bO?_bO.modelProb/100:0.52,prob_btts:_bB?_bB.modelProb/100:0.48,avg_points_home:bet.lH,avg_points_away:bet.lA,rank_home:50,rank_away:50};
        const teamCtx=buildTeamCtx(bet.home,bet.away,bet.sport,formMap,stats,bet.lH,bet.lA);
        const h2hCtx=buildH2HCtx(bet.home,bet.away,h2hMap);
        const marketCtx={value:(o,p)=>p-1/o,kelly:(o,p)=>Math.max(0,(p*(o-1)-(1-p))/(o-1)),implied:o=>1/o,edge:bet.edge/100,prob:bet.modelProb/100};
        // eslint-disable-next-line no-new-func
        const fn=new Function('match','team','h2h','market','"use strict";\n'+s.code+'\nif(typeof evaluate==="function"){var _r=evaluate(match,team,h2h,market);return _r;}return null;');
        const result=fn(matchCtx,teamCtx,h2hCtx,marketCtx);
        if(result&&result.signal===true&&marketMatches(bet.market,result.market))matchedNames.push({id:sId,name:sName});
      }catch(err){console.warn(`[value] Strat "${sName}" (${bet.sport}/${bet.market}): ${err.message}`);}
    }
    if(matchedNames.length>0){bet.matchedStrategies=matchedNames;passed.push(bet);}
  }
  passed.sort((a,b)=>b.edge-a.edge);
  return{bets:passed,applied:true};
}

function resolveFixtures(fixtures){
  return fixtures.map(f=>{const hR=resolver.resolve(f.home,f.sport),aR=resolver.resolve(f.away,f.sport);return{...f,homeResolved:hR,awayResolved:aR,homeMatched:hR!==f.home,awayMatched:aR!==f.away};});
}

async function saveFixturesToDB(pgPool,fixtures){
  if(!pgPool||!fixtures?.length)return;
  try{const fb=fixtures.filter(f=>f.sport==='football').slice(0,50);for(const f of fb){try{const hId=`vf_${(f.homeResolved||f.home||'').toLowerCase().replace(/[^a-z0-9]/g,'_').slice(0,40)}`;const aId=`vf_${(f.awayResolved||f.away||'').toLowerCase().replace(/[^a-z0-9]/g,'_').slice(0,40)}`;await pgPool.query(`INSERT INTO teams(id,name,sport_id)VALUES($1,$2,1)ON CONFLICT(id)DO NOTHING`,[hId,f.homeResolved||f.home]);await pgPool.query(`INSERT INTO teams(id,name,sport_id)VALUES($1,$2,1)ON CONFLICT(id)DO NOTHING`,[aId,f.awayResolved||f.away]);const mId=`vf_${f.id||`${hId}_${aId}_${(f.startTime||'').slice(0,10)}`}`.slice(0,100);const cId=f.league?.includes('Bundesliga')?'BL1':f.league?.includes('Premier')?'PL':f.league?.includes('La Liga')?'PD':f.league?.includes('Serie')?'SA':f.league?.includes('Ligue')?'FL1':f.league?.includes('Champions')?'CL':f.league?.includes('MLS')?'MLS':null;if(cId)await pgPool.query(`INSERT INTO matches(id,competition_id,season,home_team_id,away_team_id,scheduled_at,status,metadata)VALUES($1,$2,$3,$4,$5,$6,'SCHEDULED',$7)ON CONFLICT(id)DO UPDATE SET scheduled_at=EXCLUDED.scheduled_at`,[mId,cId,new Date().getFullYear().toString(),hId,aId,f.startTime||new Date().toISOString(),JSON.stringify({source:f._source,sport:f.sport})]);}catch(e){}}}catch(e){console.warn('[value] saveFixturesToDB:',e.message);}
}

// ═══════════════════════════════════════════════════════════════════
//  Главный обработчик
// ═══════════════════════════════════════════════════════════════════
const ALL_SPORTS=['football','basketball','hockey','tennis','baseball','rugby','volleyball','waterpolo','cricket','nfl','mma','esports'];

async function collectAllFixtures(sport, mode) {
  const sportList = sport==='all' ? ALL_SPORTS : [sport];
  const usedSources = [];
  let allFixtures = [];
  const seen = new Set();
  const dedup = (arr) => arr.filter(f=>{const k=`${f.sport}|${f.home}|${f.away}`;if(seen.has(k))return false;seen.add(k);return true;});

  // 1. ESPN — главный источник для всех спортов (без ключа!)
  try {
    const espnFixtures = await fetchFromESPN(sport);
    const deduped = dedup(espnFixtures);
    if (deduped.length) {
      allFixtures.push(...deduped);
      // Подсчёт по спортам для лога
      const byS = {};
      deduped.forEach(f=>{byS[f.sport]=(byS[f.sport]||0)+1;});
      const summary = Object.entries(byS).map(([s,n])=>`${s}:${n}`).join(',');
      usedSources.push(`ESPN(${deduped.length}: ${summary})`);
      console.log(`[value] ESPN total: ${deduped.length} [${summary}]`);
    }
  } catch(e) { console.warn('[value] ESPN:', e.message); }

  // 2. OddsAPI — коэффициенты поверх ESPN матчей
  if (ODDS_API_KEY && allFixtures.length > 0) {
    try {
      allFixtures = await fetchOddsForFixtures(allFixtures);
      const withOdds = allFixtures.filter(f=>f.bH||f.bA).length;
      if (withOdds) usedSources.push(`OddsAPI(${withOdds} odds)`);
    } catch(e) { console.warn('[value] OddsAPI overlay:', e.message); }
  }

  // 3. Дополнительные футбольные источники
  if (sportList.includes('football')) {
    if (FDORG_KEY) {
      const f=await fetchFromFootballDataOrg();const nw=dedup(f);
      if(nw.length){allFixtures.push(...nw);usedSources.push(`FDORG(${nw.length})`);}
    }
    if (APIFB_KEY) {
      const f=await fetchFromApiFootball();const nw=dedup(f);
      if(nw.length){allFixtures.push(...nw);usedSources.push(`APIFB(${nw.length})`);}
    }
    const fbCount = allFixtures.filter(f=>f.sport==='football').length;
    if (fbCount < 5) {
      const f=await fetchFromOpenLigaDB();const nw=dedup(f);
      if(nw.length){allFixtures.push(...nw);usedSources.push(`OpenLiga(${nw.length})`);}
    }
  }

  // Фильтрация по спорту и режиму
  if (sport !== 'all') allFixtures = allFixtures.filter(f=>f.sport===sport);
  const now = Date.now();
  allFixtures = allFixtures.filter(f=>{
    if(!f.startTime)return true;
    const t=new Date(f.startTime).getTime();
    return mode==='live'?t<=now+2*3600000:t>now;
  });

  return { allFixtures, usedSources };
}

async function scanHandler(req, res) {
  const{minEdge=3,sport='all',market='',strategies=[],mode='line'}=req.body;
  const clickhouse=req.app.locals.clickhouse;
  const pgPool=req.app.locals.pgPool;

  await resolver.init(clickhouse, pgPool);
  console.log(`[value/scan] sport=${sport} minEdge=${minEdge} strats=${strategies.length}`);

  const{allFixtures,usedSources}=await collectAllFixtures(sport,mode);

  if(!allFixtures.length){
    return res.json({bets:[],total:0,totalFixtures:0,source:'empty',sources:usedSources,error:false,message:'Нет матчей. ESPN и другие источники не вернули данных.'});
  }

  const fixtures=resolveFixtures(allFixtures);
  saveFixturesToDB(pgPool,fixtures).catch(()=>{});

  const[stats,h2hMap,formMap]=await Promise.all([
    loadTeamStats(clickhouse,fixtures),
    loadH2HData(clickhouse,fixtures),
    loadFormData(clickhouse,fixtures),
  ]);

  let allBets=fixtures.flatMap(f=>calcBets(f,stats,parseFloat(minEdge)));
  if(market)allBets=allBets.filter(b=>b.market===market);

  const{bets:final,applied}=applyStrategies(allBets,strategies,h2hMap,formMap,stats);

  // Статистика по спортам
  const sportBreakdown={};
  final.forEach(b=>{sportBreakdown[b.sport]=(sportBreakdown[b.sport]||0)+1;});

  console.log(`[value/scan] ${final.length} bets | sports: ${JSON.stringify(sportBreakdown)} | h2h:${h2hMap.size} form:${formMap.size}`);

  res.json({bets:final,total:final.length,totalFixtures:fixtures.length,source:usedSources[0]||'multi',sources:usedSources,stratApplied:applied,strategiesCount:strategies.length,lambdaFromHistory:Object.keys(stats).length,h2hPairs:h2hMap.size,formTeams:formMap.size,resolverStats:resolver.stats(),sportBreakdown,sport,market});
}

// ─── Routes ────────────────────────────────────────────────────────────────
router.post('/scan',scanHandler);
router.get('/scan',async(req,res)=>{req.body={minEdge:parseFloat(req.query.minEdge||3),sport:req.query.sport||'all',market:req.query.market||'',strategies:[],mode:req.query.mode||'line'};return scanHandler(req,res);});

router.get('/sources',(_req,res)=>{res.json({configured:{'espn':'always (no key)','odds-api':!!ODDS_API_KEY,'football-data.org':!!FDORG_KEY,'apifootball.com':!!APIFB_KEY,'openligadb':'always'},resolver:resolver.stats()});});

router.get('/debug',async(req,res)=>{
  const clickhouse=req.app.locals.clickhouse;const pgPool=req.app.locals.pgPool;
  const chTables={};
  if(clickhouse){for(const[sport,db]of Object.entries(SPORT_DB)){try{const r=await clickhouse.query({query:`SELECT count()AS n,max(date)AS last FROM ${db.table}`,format:'JSON'});const d=await r.json();const row=d.data?.[0];chTables[sport]={table:db.table,count:+row?.n||0,last:row?.last||null};}catch(e){chTables[sport]={table:db.table,error:e.message.slice(0,60)};}}}
  res.json({env:{ODDS_API_KEY:ODDS_API_KEY?ODDS_API_KEY.slice(0,8)+'...':'NOT SET',FDORG_KEY:FDORG_KEY?FDORG_KEY.slice(0,8)+'...':'NOT SET'},espnSports:Object.keys(ESPN_SPORTS),clickhouse:chTables,resolverStats:resolver.stats()});
});

router.post('/fixtures',async(req,res)=>{
  const{sport='all',mode='line'}=req.body||{};
  const clickhouse=req.app.locals.clickhouse;const pgPool=req.app.locals.pgPool;
  await resolver.init(clickhouse,pgPool);
  const{allFixtures,usedSources}=await collectAllFixtures(sport,mode);
  const fixtures=resolveFixtures(allFixtures);
  const stats=await loadTeamStats(clickhouse,fixtures);
  const result=fixtures.map(f=>{const{lH,lA,src}=lambdas(f.home,f.away,f.sport,stats);const ko=f.startTime?new Date(f.startTime).toLocaleString('ru',{month:'short',day:'numeric',hour:'2-digit',minute:'2-digit'}):'';return{id:f.id,home:f.home,away:f.away,homeResolved:f.homeResolved,awayResolved:f.awayResolved,resolverUsed:f.homeMatched||f.awayMatched,league:f.league,sport:f.sport,startTime:f.startTime,kickoff:ko,lH:+lH,lA:+lA,lambdaSrc:src,bmCount:f.bmCount||0,dataSource:f._source||'unknown',oddsHome:f.bH||null,oddsDraw:f.bD||null,oddsAway:f.bA||null};});
  res.json({fixtures:result,total:result.length,sources:usedSources});
});

router.post('/calculate',(req,res)=>{
  const{homeAttack=1,homeDefense=1,awayAttack=1,awayDefense=1,home='',away=''}=req.body;
  const lH=Math.max(0.1,homeAttack*awayDefense*1.45);const lA=Math.max(0.1,awayAttack*homeDefense*1.15);
  const mat=scoreMatrix(lH,lA);const top=[];mat.forEach((row,h)=>row.forEach((p,a)=>top.push({score:`${h}:${a}`,prob:+p.toFixed(4)})));top.sort((x,y)=>y.prob-x.prob);
  res.json({pois:{matrix:mat,topScores:top.slice(0,8)},elo:eloProbs(home,away),lH:+lH.toFixed(3),lA:+lA.toFixed(3)});
});

router.get('/elo',async(req,res)=>{
  const ch=req.app.locals.clickhouse;
  if(ch){try{const r=await ch.query({query:`SELECT home_team AS team,count()AS matches,countIf(home_goals>away_goals)AS wins,countIf(home_goals<away_goals)AS losses FROM betquant.football_matches WHERE date>=today()-365 GROUP BY home_team HAVING matches>=5 ORDER BY wins DESC LIMIT 50`,format:'JSON'});const d=await r.json();if(d.data?.length>3)return res.json({ratings:d.data.map(r=>({team:r.team,rating:Math.round(ELO_DEFAULT+(r.wins/r.matches-0.5)*400),matches:+r.matches,wins:+r.wins,losses:+r.losses})).sort((a,b)=>b.rating-a.rating),source:'clickhouse'});}catch(e){}}
  if(eloStore.size>3)return res.json({ratings:[...eloStore.entries()].map(([t,r])=>({team:t,rating:r})).sort((a,b)=>b.rating-a.rating).slice(0,30),source:'memory'});
  res.json({ratings:[],source:'none'});
});

module.exports = router;