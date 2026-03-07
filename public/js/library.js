const library = {
  load() {
    const saved = JSON.parse(localStorage.getItem('bq_strategies') || '[]');
    const builtin = [
      { id:'b1', name:'Value Betting (Poisson)', desc:'Poisson model calculates home/away goal expectations, compares with market to find edge > 5%', roi:'+12.3%', bets:1847, sport:'Football', tags:['value','poisson','mathematical'], public:true },
      { id:'b2', name:'Over 2.5 xG Model', desc:'Bet Over 2.5 when combined team xG average exceeds 2.8, using FBref data', roi:'+7.8%', bets:923, sport:'Football', tags:['xg','totals','advanced'], public:true },
      { id:'b3', name:'Home Form Momentum', desc:'Back home teams with 4W in last 5 at home vs away teams with 2W- in last 5 away', roi:'+5.4%', bets:2134, sport:'Football', tags:['form','home','momentum'], public:true },
      { id:'b4', name:'ELO Rating Value', desc:'Custom ELO ratings updated after each match, bet when ELO probability beats market by 7%+', roi:'+9.1%', bets:1456, sport:'Football', tags:['elo','value','rating'], public:true },
      { id:'b5', name:'H2H Dominance', desc:'If team A won 4+ of last 5 H2H meetings, bet on them regardless of current form', roi:'+4.2%', bets:678, sport:'Football', tags:['h2h','historical'], public:true },
      { id:'b6', name:'ATP Serve Dominance', desc:'Back players with 65%+ 1st serve win rate against opponents with <55% return rate', roi:'+11.7%', bets:892, sport:'Tennis', tags:['serve','atp','stats'], public:true },
      ...saved.map(s=>({...s, roi:'?', bets:0, sport:'Custom', tags:[], public:false}))
    ];
    this.render(builtin);
  },
  
  render(strategies) {
    const filter = document.getElementById('libFilter')?.value || 'all';
    const search = (document.getElementById('libSearch')?.value || '').toLowerCase();
    let filtered = strategies;
    if (filter==='mine') filtered = filtered.filter(s=>!s.public);
    if (filter==='top') filtered = filtered.sort((a,b)=>parseFloat(b.roi)>parseFloat(a.roi)?1:-1);
    if (search) filtered = filtered.filter(s=>s.name.toLowerCase().includes(search)||s.desc.toLowerCase().includes(search));
    
    const container = document.getElementById('libraryGrid');
    if (!container) return;
    if (!filtered.length) { container.innerHTML='<div class="empty-state"><div class="empty-state-icon">📭</div>No strategies found</div>'; return; }
    container.innerHTML = filtered.map(s=>`
      <div class="library-card">
        <div class="library-card-title">${s.name} ${s.public?'<span class="chip">Public</span>':''}</div>
        <div class="library-card-desc">${s.desc}</div>
        <div class="library-card-stats">
          <span style="color:${parseFloat(s.roi)>0?'var(--green)':'var(--text2)'}">ROI: ${s.roi}</span>
          <span style="color:var(--text2)">Bets: ${s.bets.toLocaleString()}</span>
          <span style="color:var(--text2)">${s.sport}</span>
        </div>
        <div style="margin:8px 0">${(s.tags||[]).map(t=>`<span class="library-card-tag">${t}</span>`).join('')}</div>
        <div class="library-card-actions">
          <button class="ctrl-btn sm" onclick="library.load_strategy('${s.id}')">Load</button>
          <button class="ctrl-btn sm primary" onclick="library.backtest_strategy('${s.id}')">▶ Backtest</button>
        </div>
      </div>`).join('');
  },
  
  search(q) { this.load(); },
  
  load_strategy(id) {
    const strats = JSON.parse(localStorage.getItem('bq_strategies') || '[]');
    const s = strats.find(x=>x.id==id);
    if (s) { document.getElementById('strategyCode').value = s.code; document.getElementById('strategyName').value = s.name; app.showPanel('strategy'); strategyBuilder.showTab('code'); }
  },
  
  backtest_strategy(id) { app.showPanel('backtest'); }
};
