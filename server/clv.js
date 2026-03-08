'use strict';
// BetQuant Pro — CLV Tracker
// Боевые данные по умолчанию. Тестовые — только при ?demo=true

const express = require('express');
const router  = express.Router();
const memStore = [];

function calcCLV(betOdds, closingOdds) {
  return +(((betOdds / closingOdds) - 1) * 100).toFixed(2);
}

function calcStats(bets) {
  const settled = bets.filter(b => b.closing_odds != null);
  if (!settled.length) return { avgClv:0, positiveClvPct:0, totalBets:bets.length, settledBets:0, winRate:0, totalPnl:0, clvTrend:[], interpretation:'Нет расчётных ставок для анализа CLV' };
  const clvs   = settled.map(b => calcCLV(+b.bet_odds, +b.closing_odds));
  const avgClv = +(clvs.reduce((s,v)=>s+v,0)/clvs.length).toFixed(2);
  const pos    = clvs.filter(v=>v>0).length;
  const pnl    = settled.reduce((s,b)=>s+(+b.pnl||0),0);
  const wins   = settled.filter(b=>b.result==='win').length;
  return {
    avgClv,
    positiveClvPct: +(pos/settled.length*100).toFixed(1),
    totalBets: bets.length, settledBets: settled.length,
    winRate: +(wins/settled.length*100).toFixed(1),
    totalPnl: +pnl.toFixed(2),
    clvTrend: clvs.slice(-20),
    interpretation: avgClv>2?'🟢 Отличный результат — вы стабильно бьёте рынок':avgClv>0?'🟡 Положительный CLV — продолжайте в том же духе':'🔴 Отрицательный CLV — пересмотрите подход к выбору ставок',
  };
}

function demoData() {
  return [
    {id:1,match_name:'Арсенал — Челси',market:'1X2',selection:'Арсенал',bet_odds:2.10,closing_odds:1.95,stake:20,result:'win',pnl:22.00,settled:true,bet_date:new Date(Date.now()-14*86400000).toISOString()},
    {id:2,match_name:'Бавария — Дортмунд',market:'Тотал',selection:'Больше 2.5',bet_odds:1.80,closing_odds:1.72,stake:15,result:'win',pnl:12.00,settled:true,bet_date:new Date(Date.now()-12*86400000).toISOString()},
    {id:3,match_name:'Реал Мадрид — Атлетико',market:'1X2',selection:'Реал',bet_odds:1.95,closing_odds:2.05,stake:10,result:'loss',pnl:-10.00,settled:true,bet_date:new Date(Date.now()-10*86400000).toISOString()},
    {id:4,match_name:'ПСЖ — Монако',market:'1X2',selection:'ПСЖ',bet_odds:1.55,closing_odds:1.48,stake:25,result:'win',pnl:13.75,settled:true,bet_date:new Date(Date.now()-8*86400000).toISOString()},
    {id:5,match_name:'Интер — Ювентус',market:'ОЗ',selection:'Да',bet_odds:1.75,closing_odds:1.68,stake:12,result:'win',pnl:9.00,settled:true,bet_date:new Date(Date.now()-6*86400000).toISOString()},
    {id:6,match_name:'Ливерпуль — Ман Сити',market:'1X2',selection:'Ничья',bet_odds:3.60,closing_odds:3.40,stake:8,result:'loss',pnl:-8.00,settled:true,bet_date:new Date(Date.now()-4*86400000).toISOString()},
    {id:7,match_name:'Барселона — Севилья',market:'Тотал',selection:'Больше 2.5',bet_odds:1.65,closing_odds:1.58,stake:20,result:'win',pnl:13.00,settled:true,bet_date:new Date(Date.now()-2*86400000).toISOString()},
    {id:8,match_name:'Наполи — Милан',market:'1X2',selection:'Наполи',bet_odds:2.20,closing_odds:2.10,stake:15,result:'win',pnl:18.00,settled:true,bet_date:new Date(Date.now()-1*86400000).toISOString()},
  ];
}

router.post('/bet', async (req, res) => {
  const pg = req.app.locals.pgPool;
  const { matchId, matchName, market, selection, betOdds, stake, betDate } = req.body;
  if (!matchName || !betOdds) return res.status(400).json({ error: 'Необходимо указать матч и коэффициент' });
  const bet = { match_id: matchId||null, match_name: matchName, market: market||'1X2', selection: selection||'', bet_odds: +betOdds, stake: +(stake||10), bet_date: betDate||new Date().toISOString(), settled: false };
  if (pg) {
    try {
      const r = await pg.query(`INSERT INTO clv_bets(match_id,match_name,market,selection,bet_odds,stake,bet_date) VALUES($1,$2,$3,$4,$5,$6,$7) RETURNING id`, [bet.match_id,bet.match_name,bet.market,bet.selection,bet.bet_odds,bet.stake,bet.bet_date]);
      return res.json({ ok:true, bet:{ ...bet, id:r.rows[0].id } });
    } catch(e) { console.warn('[CLV] pg insert:', e.message); }
  }
  bet.id = Date.now(); memStore.push(bet); res.json({ ok:true, bet });
});

router.put('/bet/:id/close', async (req, res) => {
  const pg = req.app.locals.pgPool;
  const id = +req.params.id;
  const { closingOdds, result } = req.body;
  if (!closingOdds) return res.status(400).json({ error: 'Необходимо указать closing odds' });
  if (pg) {
    try {
      const row = await pg.query('SELECT * FROM clv_bets WHERE id=$1', [id]);
      if (!row.rows[0]) return res.status(404).json({ error: 'Ставка не найдена' });
      const bO=+row.rows[0].bet_odds, st=+row.rows[0].stake, clvPct=calcCLV(bO,+closingOdds);
      const pnl=result==='win'?+(st*(bO-1)).toFixed(2):result==='loss'?-st:0;
      await pg.query(`UPDATE clv_bets SET closing_odds=$1,clv_pct=$2,result=$3,pnl=$4,settled=$5 WHERE id=$6`, [+closingOdds,clvPct,result||null,pnl,!!result,id]);
      return res.json({ ok:true, clvPct });
    } catch(e) { console.warn('[CLV] pg update:', e.message); }
  }
  const bet = memStore.find(b=>b.id===id);
  if (!bet) return res.status(404).json({ error: 'Ставка не найдена' });
  bet.closing_odds = +closingOdds; bet.clv_pct = calcCLV(bet.bet_odds,+closingOdds);
  if (result) { bet.result=result; bet.pnl=result==='win'?+(bet.stake*(bet.bet_odds-1)).toFixed(2):-bet.stake; bet.settled=true; }
  res.json({ ok:true, clvPct:bet.clv_pct });
});

router.get('/bets', async (req, res) => {
  const pg = req.app.locals.pgPool;
  if (req.query.demo === 'true') { const b=demoData(); return res.json({bets:b,stats:calcStats(b),source:'demo'}); }
  if (pg) {
    try { const r=await pg.query('SELECT * FROM clv_bets ORDER BY bet_date DESC LIMIT 500'); return res.json({bets:r.rows,stats:calcStats(r.rows),source:'postgresql'}); }
    catch(e) { console.warn('[CLV] pg select:', e.message); }
  }
  res.json({bets:memStore,stats:calcStats(memStore),source:'memory'});
});

router.get('/stats', async (req, res) => {
  const pg = req.app.locals.pgPool;
  if (req.query.demo === 'true') { const b=demoData(); return res.json({...calcStats(b),source:'demo'}); }
  if (pg) {
    try { const r=await pg.query('SELECT * FROM clv_bets WHERE settled=true'); return res.json({...calcStats(r.rows),source:'postgresql'}); }
    catch(e) { console.warn('[CLV] pg stats:', e.message); }
  }
  res.json({...calcStats(memStore.filter(b=>b.settled)),source:'memory'});
});

module.exports = router;