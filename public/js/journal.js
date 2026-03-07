const journal = {
  bets: [],
  refresh() {
    this.bets = JSON.parse(localStorage.getItem('bq_journal') || '[]');
    if (!this.bets.length) this.bets = this.getDemoData();
    this.render();
  },
  getDemoData() {
    return [
      {id:1,date:'2024-03-15',sport:'Football',match:'Arsenal vs Chelsea',market:'1X2',selection:'Home',odds:2.10,stake:20,result:'win',pnl:22.0,strategy:'Value Bet'},
      {id:2,date:'2024-03-16',sport:'Football',match:'Barcelona vs Atletico',market:'O/U 2.5',selection:'Over',odds:1.85,stake:15,result:'loss',pnl:-15.0,strategy:'xG Model'},
      {id:3,date:'2024-03-17',sport:'Tennis',match:'Djokovic vs Alcaraz',market:'Match Winner',selection:'Djokovic',odds:1.60,stake:25,result:'win',pnl:15.0,strategy:'Form'},
      {id:4,date:'2024-03-18',sport:'Basketball',match:'Lakers vs Celtics',market:'Spread',selection:'Celtics -3.5',odds:1.91,stake:20,result:'win',pnl:18.2,strategy:'ATS'},
      {id:5,date:'2024-03-19',sport:'Football',match:'Bayern vs Dortmund',market:'BTTS',selection:'Yes',odds:1.72,stake:10,result:'loss',pnl:-10.0,strategy:'Stats'},
    ];
  },
  render() {
    const wins=this.bets.filter(b=>b.result==='win').length;
    const losses=this.bets.filter(b=>b.result==='loss').length;
    const pnl=this.bets.reduce((s,b)=>s+b.pnl,0);
    const stake=this.bets.reduce((s,b)=>s+b.stake,0);
    const set=(id,v,cls='')=>{const el=document.getElementById(id);if(el){el.textContent=v;if(cls)el.className=cls;}};
    set('jTotal',this.bets.length); set('jWon',wins); set('jLost',losses);
    set('jPL',(pnl>=0?'+':'')+pnl.toFixed(2),pnl>=0?'positive':'negative');
    set('jROI',(pnl/stake*100).toFixed(1)+'%',pnl>=0?'positive':'negative');
    const container=document.getElementById('journalTable');
    if(container) container.innerHTML=makeTable(
      ['date','sport','match','market','selection','odds','stake',{label:'Result',key:'result'},{label:'P&L',key:'pnl'},'strategy'],
      this.bets.map(b=>({...b,result:`<span class="${b.result}">${b.result.toUpperCase()}</span>`,pnl:`<span class="${b.pnl>=0?'positive':'negative'}">${b.pnl>=0?'+':''}${b.pnl.toFixed(2)}</span>`}))
    );
  },
  addBet() { document.getElementById('addBetModal').style.display='flex'; },
  saveBet() {
    const bet = {
      id:Date.now(), date:document.getElementById('betDate').value, sport:document.getElementById('betSport').value,
      match:document.getElementById('betMatch').value, market:document.getElementById('betMarket').value,
      selection:document.getElementById('betSelection').value, odds:parseFloat(document.getElementById('betOdds').value),
      stake:parseFloat(document.getElementById('betStake').value), result:document.getElementById('betResult').value, strategy:'Manual'
    };
    const won=bet.result==='win', stake=bet.stake||0, odds=bet.odds||1;
    bet.pnl=won?stake*(odds-1):bet.result==='void'?0:-stake;
    this.bets.unshift(bet);
    localStorage.setItem('bq_journal',JSON.stringify(this.bets));
    document.getElementById('addBetModal').style.display='none';
    this.render();
  },
  export() {
    const csv=['date,sport,match,market,selection,odds,stake,result,pnl,strategy',...this.bets.map(b=>`${b.date},${b.sport},"${b.match}",${b.market},${b.selection},${b.odds},${b.stake},${b.result},${b.pnl},${b.strategy||''}`)].join('\n');
    const a=document.createElement('a');a.href='data:text/csv;charset=utf-8,'+encodeURIComponent(csv);a.download='betquant_journal.csv';a.click();
  }
};
