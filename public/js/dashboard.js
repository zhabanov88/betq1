// Dashboard module — generates demo data + real charts
const dashboard = {
  charts: {},
  
  refresh() {
    const days = parseInt(document.getElementById('dashPeriod')?.value || 30);
    const data = this.generateDemoData(days);
    this.updateKPIs(data);
    this.renderCharts(data);
    this.updateTicker(data);
  },
  
  generateDemoData(days) {
    const bets = [];
    let bankroll = 1000;
    const bankrollCurve = [bankroll];
    const leagues = ['EPL','La Liga','Bundesliga','Serie A','Ligue 1','CL'];
    const types = ['Home','Draw','Away','Over 2.5','Under 2.5','BTTS Yes'];
    const now = Date.now();
    
    for (let i = 0; i < days * 3; i++) {
      const date = new Date(now - (days - Math.floor(i/3)) * 86400000);
      const odds = 1.4 + Math.random() * 3;
      const stake = bankroll * 0.02;
      const win = Math.random() < (1/odds * 1.05 + 0.02);
      const pnl = win ? stake * (odds - 1) : -stake;
      bankroll = Math.max(0, bankroll + pnl);
      bets.push({ date, odds, stake, win, pnl, bankroll, league: leagues[Math.floor(Math.random()*leagues.length)], type: types[Math.floor(Math.random()*types.length)] });
      bankrollCurve.push(bankroll);
    }
    
    const wins = bets.filter(b=>b.win).length;
    const totalStaked = bets.reduce((s,b)=>s+b.stake,0);
    const totalPnL = bets.reduce((s,b)=>s+b.pnl,0);
    const roi = (totalPnL/totalStaked)*100;
    const avgOdds = bets.reduce((s,b)=>s+b.odds,0)/bets.length;
    
    // Drawdown
    let peak = 1000, maxDD = 0;
    const ddCurve = bankrollCurve.map(v => { if (v>peak) peak=v; const dd = (peak-v)/peak*100; if (dd>maxDD) maxDD=dd; return -dd; });
    
    // Sharpe
    const returns = bets.map(b=>b.pnl/b.stake);
    const avgR = returns.reduce((s,r)=>s+r,0)/returns.length;
    const stdR = Math.sqrt(returns.reduce((s,r)=>s+(r-avgR)**2,0)/returns.length);
    const sharpe = stdR > 0 ? (avgR/stdR)*Math.sqrt(252) : 0;
    
    // Monthly
    const monthly = {};
    bets.forEach(b => {
      const key = b.date.getFullYear()+'-'+(b.date.getMonth()+1).toString().padStart(2,'0');
      if (!monthly[key]) monthly[key] = 0;
      monthly[key] += b.pnl;
    });
    
    // By league
    const byLeague = {};
    bets.forEach(b => {
      if (!byLeague[b.league]) byLeague[b.league] = {w:0,l:0};
      if (b.win) byLeague[b.league].w++; else byLeague[b.league].l++;
    });
    
    // By type
    const byType = {};
    bets.forEach(b => {
      if (!byType[b.type]) byType[b.type] = {pnl:0,count:0};
      byType[b.type].pnl += b.pnl;
      byType[b.type].count++;
    });
    
    // Odds distribution
    const oddsBuckets = Array(10).fill(0);
    bets.forEach(b => { const idx = Math.min(Math.floor((b.odds-1)/0.4),9); oddsBuckets[idx]++; });
    
    // CLV fake data
    const clvData = bets.slice(0,30).map((b,i) => ({ x: i, y: (Math.random()-0.45)*5 }));
    
    return { bets, bankrollCurve, ddCurve, wins, totalStaked, totalPnL, roi, avgOdds, maxDD, sharpe, monthly, byLeague, byType, oddsBuckets, clvData, days };
  },
  
  updateKPIs(d) {
    const set = (id, val, color=null) => {
      const el = document.getElementById(id);
      if (el) {
        el.textContent = val;
        if (color) { el.classList.remove('positive','negative'); el.classList.add(color); }
      }
    };
    const n = d.bets.length || 1;
    const wr = d.wins / n;
    set('kpiBets', d.bets.length);
    set('kpiWinRate', formatNum(wr*100,1)+'%', wr > 0.5 ? 'positive' : wr < 0.4 ? 'negative' : null);
    set('kpiROI',    formatPct(d.roi),    d.roi > 0 ? 'positive' : d.roi < 0 ? 'negative' : null);
    set('kpiProfit', formatNum(d.totalPnL,2), d.totalPnL > 0 ? 'positive' : d.totalPnL < 0 ? 'negative' : null);
    set('kpiAvgOdds', formatOdds(d.avgOdds));
    set('kpiDrawdown', '-'+formatNum(d.maxDD,1)+'%', 'negative');
    set('kpiSharpe', formatNum(d.sharpe,2), d.sharpe > 1 ? 'positive' : d.sharpe < 0 ? 'negative' : null);
    set('kpiYield',  formatPct(d.roi),    d.roi > 0 ? 'positive' : d.roi < 0 ? 'negative' : null);
  },
  
  updateTicker(d) {
    const set = (id, val) => { const el=document.getElementById(id); if(el) el.textContent=val; };
    set('tickerBetCount', d.bets.length);
    set('tickerWinRate', formatNum(d.wins/Math.max(1,d.bets.length)*100,1)+'%');
    set('tickerROI', formatPct(d.roi));
    const clvAvg = d.clvData.reduce((s,c)=>s+c.y,0)/d.clvData.length;
    set('tickerKelly', formatPct(clvAvg));
    const roiEl = document.getElementById('tickerROI');
    if (roiEl) { roiEl.classList.remove('positive','negative'); roiEl.classList.add(d.roi>0?'positive':'negative'); }
  },
  
  renderCharts(d) {
    this.destroyAll();
    const C = Chart;
    const isDark = document.body.classList.contains('dark-mode');
    const grid = isDark ? 'rgba(255,255,255,0.05)' : 'rgba(0,0,0,0.05)';
    const textColor = isDark ? '#8892a4' : '#4a5568';
    
    const baseOpts = {
      responsive: true,
      maintainAspectRatio: false,
      plugins: { legend: { labels: { color: textColor, font:{size:10} } } },
      scales: {
        x: { ticks: { color: textColor, font:{size:9} }, grid: { color: grid } },
        y: { ticks: { color: textColor, font:{size:9} }, grid: { color: grid } }
      }
    };
    
    // Bankroll curve
    const labels = d.bankrollCurve.map((_,i) => i===0 ? 'Start' : i%10===0 ? 'Bet '+i : '');
    this.charts.bankroll = new C(document.getElementById('chartBankroll'), {
      type: 'line',
      data: {
        labels,
        datasets: [{
          label: 'Bankroll', data: d.bankrollCurve,
          borderColor: '#00d4ff', backgroundColor: 'rgba(0,212,255,0.06)',
          borderWidth: 2, pointRadius: 0, fill: true, tension: 0.3
        }]
      },
      options: { ...baseOpts, plugins: { ...baseOpts.plugins, tooltip: { callbacks: { label: c => '£' + c.parsed.y.toFixed(2) } } } }
    });
    
    // Odds distribution
    const buckLabels = Array.from({length:10},(_,i)=>(1+i*0.4).toFixed(1)+'-'+(1.4+i*0.4).toFixed(1));
    this.charts.oddsDistrib = new C(document.getElementById('chartOddsDistrib'), {
      type: 'bar',
      data: { labels: buckLabels, datasets: [{ label:'Bets', data: d.oddsBuckets, backgroundColor: 'rgba(0,212,255,0.6)', borderRadius: 3 }] },
      options: { ...baseOpts, plugins: { legend: { display: false } } }
    });
    
    // League W/L
    const lgs = Object.keys(d.byLeague);
    this.charts.leagueWL = new C(document.getElementById('chartLeagueWL'), {
      type: 'bar',
      data: { labels: lgs, datasets: [
        { label:'Win', data: lgs.map(l=>d.byLeague[l].w), backgroundColor:'rgba(0,230,118,0.7)', borderRadius:3 },
        { label:'Loss', data: lgs.map(l=>d.byLeague[l].l), backgroundColor:'rgba(255,69,96,0.7)', borderRadius:3 }
      ]},
      options: { ...baseOpts, scales: { ...baseOpts.scales, x: { ...baseOpts.scales.x, stacked: false } } }
    });
    
    // Monthly P&L
    const months = Object.keys(d.monthly).sort();
    const monthVals = months.map(m => d.monthly[m]);
    this.charts.monthlyPL = new C(document.getElementById('chartMonthlyPL'), {
      type: 'bar',
      data: { labels: months, datasets: [{ label:'P&L', data: monthVals, backgroundColor: monthVals.map(v=>v>0?'rgba(0,230,118,0.7)':'rgba(255,69,96,0.7)'), borderRadius: 3 }] },
      options: { ...baseOpts, plugins: { legend:{display:false} } }
    });
    
    // Bet types
    const types = Object.keys(d.byType);
    this.charts.betTypes = new C(document.getElementById('chartBetTypes'), {
      type: 'doughnut',
      data: { labels: types, datasets: [{ data: types.map(t=>d.byType[t].count), backgroundColor: ['#00d4ff','#00e676','#ffd740','#ff4560','#c084fc','#fb923c'] }] },
      options: { responsive:true, maintainAspectRatio:false, plugins: { legend: { position:'right', labels:{color:textColor,font:{size:10}} } } }
    });
    
    // CLV
    const clvColors = d.clvData.map(p=>p.y>=0?'rgba(0,230,118,0.8)':'rgba(255,69,96,0.8)');
    this.charts.clv = new C(document.getElementById('chartCLV'), {
      type: 'bar',
      data: { labels: d.clvData.map((_,i)=>'#'+i), datasets: [{ label:'CLV%', data: d.clvData.map(p=>p.y), backgroundColor: clvColors, borderRadius: 2 }] },
      options: { ...baseOpts, plugins: { legend:{display:false} } }
    });
  },
  
  destroyAll() {
    Object.values(this.charts).forEach(c => { try { c.destroy(); } catch(e){} });
    this.charts = {};
  }
};