// Monte Carlo Simulation Engine
const monteCarlo = {
  charts: {},
  
  async run() {
    const simCount      = parseInt(document.getElementById('mcSimCount')?.value) || 5000;
    const betsPerRun    = parseInt(document.getElementById('mcBetsPerRun')?.value) || 500;
    const startBankroll = parseFloat(document.getElementById('mcBankroll')?.value) || 1000;
    const winRate       = parseFloat(document.getElementById('mcWinRate')?.value) || 52;
    const avgOdds       = parseFloat(document.getElementById('mcAvgOdds')?.value) || 2.0;
    const stakePct      = parseFloat(document.getElementById('mcStake')?.value) || 2;
    const ruinThreshold = parseFloat(document.getElementById('mcRuinThreshold')?.value) || 50;

    const sumEl = document.getElementById('mcSummary');
    if (sumEl) sumEl.innerHTML = '<div style="color:var(--text3);padding:8px">⏳ Симуляция на сервере...</div>';

    const activeStrat = typeof backtestEngine !== 'undefined'
      ? (backtestEngine.activeStrategies || []).find(s => s.enabled) : null;

    try {
      const resp = await fetch('/api/bt/montecarlo', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          strategy: activeStrat ? { sport: activeStrat.sport, code: activeStrat.code } : null,
          cfg: { bankroll: startBankroll, dateFrom: '2020-01-01', dateTo: new Date().toISOString().slice(0,10) },
          mcCfg: { simCount, betsPerRun, ruinThreshold: ruinThreshold/100, winRate, avgOdds, stakePct },
        }),
      });
      const d = await resp.json();
      if (d.error) throw new Error(d.error);

      const { paths, finals, ruinByBet, percentiles: pc, avg, ruinProbability, realStats } = d;
      const { p5, p25, p50, p75, p95 } = pc;

      const srcNote = realStats?.tradesUsed > 10
        ? `<div style="font-size:11px;color:var(--text3);margin-bottom:8px">📊 На основе ${realStats.tradesUsed} реальных ставок (WR ${(realStats.winRate*100).toFixed(1)}%, avg odds ${realStats.avgOdds.toFixed(2)})</div>` : '';

      if (sumEl) sumEl.innerHTML = srcNote + `
        <div style="display:grid;grid-template-columns:1fr 1fr;gap:8px">
          <div><span style="color:var(--text3)">Медиана:</span><br><strong style="color:var(--accent)">${p50.toFixed(0)}</strong></div>
          <div><span style="color:var(--text3)">Среднее:</span><br><strong>${avg.toFixed(0)}</strong></div>
          <div><span style="color:var(--text3)">P5 (худшие 5%):</span><br><strong style="color:var(--red)">${p5.toFixed(0)}</strong></div>
          <div><span style="color:var(--text3)">P95 (лучшие 5%):</span><br><strong style="color:var(--green)">${p95.toFixed(0)}</strong></div>
          <div><span style="color:var(--text3)">Вер. руина:</span><br><strong style="color:${ruinProbability>20?'var(--red)':'var(--green)'}">${ruinProbability.toFixed(1)}%</strong></div>
          <div><span style="color:var(--text3)">ROI:</span><br><strong style="color:${avg>startBankroll?'var(--green)':'var(--red)'}">${((avg-startBankroll)/startBankroll*100).toFixed(1)}%</strong></div>
        </div>`;

      this.renderCharts(paths.slice(0,300), finals, ruinByBet, d.betsPerRun, d.simCount, startBankroll, p5, p25, p50, p75, p95);
    } catch(e) {
      if (sumEl) sumEl.innerHTML = `<div style="color:var(--red)">❌ ${e.message}</div>`;
      console.error('[MC]', e);
    }
  },
  
  renderCharts(paths, finals, ruinByBet, betsPerRun, simCount, start, p5, p25, p50, p75, p95) {
    this.destroyCharts();
    const textColor = document.body.classList.contains('dark-mode') ? '#8892a4' : '#4a5568';
    const gridColor = 'rgba(255,255,255,0.05)';
    const baseOpts = { responsive:true, maintainAspectRatio:false, plugins:{legend:{display:false}}, scales:{x:{ticks:{color:textColor,font:{size:8},maxTicksLimit:12},grid:{color:gridColor}},y:{ticks:{color:textColor,font:{size:9}},grid:{color:gridColor}}} };
    
    // Equity paths
    const labels = Array.from({length:betsPerRun+1},(_,i)=>i);
    const datasets = paths.slice(0,200).map(p => ({ data:p, borderColor:'rgba(0,212,255,0.08)', borderWidth:1, pointRadius:0, tension:0.1 }));
    // Add percentile lines
    const pcts = [p5,p25,p50,p75,p95];
    const colors = ['#ff4560','#ffd740','#00d4ff','#ffd740','#00e676'];
    const lbls = ['P5','P25','Median','P75','P95'];
    // Compute paths for each percentile
    const pctPaths = pcts.map((target) => {
      const closest = paths.reduce((best,p) => Math.abs(p[p.length-1]-target)<Math.abs(best[best.length-1]-target)?p:best, paths[0]);
      return closest;
    });
    pctPaths.forEach((p,i) => datasets.push({ data:p, borderColor:colors[i], borderWidth:2.5, pointRadius:0, label:lbls[i], tension:0.1 }));
    
    this.charts.paths = new Chart(document.getElementById('chartMCPaths'), {
      type:'line', data:{labels,datasets},
      options:{...baseOpts, plugins:{legend:{display:true,labels:{color:textColor,font:{size:10},filter:i=>i.datasetIndex>=paths.slice(0,200).length}}}}
    });
    
    // Final bankroll histogram
    const bins = 40, min = Math.min(...finals), max = Math.max(...finals);
    const binW = (max-min)/bins;
    const hist = Array(bins).fill(0);
    finals.forEach(v => { const i=Math.min(Math.floor((v-min)/binW),bins-1); hist[i]++; });
    const histLabels = Array.from({length:bins},(_,i)=>(min+i*binW).toFixed(0));
    this.charts.distrib = new Chart(document.getElementById('chartMCDistrib'), {
      type:'bar', data:{labels:histLabels, datasets:[{data:hist, backgroundColor:hist.map((_,i)=>i<bins*0.2?'rgba(255,69,96,0.7)':i>bins*0.8?'rgba(0,230,118,0.7)':'rgba(0,212,255,0.5)'), borderRadius:2}]},
      options:{...baseOpts,plugins:{legend:{display:false}}}
    });
    
    // Ruin probability curve
    const ruinPcts = ruinByBet.map(r=>(r/simCount*100));
    this.charts.ruin = new Chart(document.getElementById('chartMCRuin'), {
      type:'line', data:{labels:ruinByBet.map((_,i)=>i), datasets:[{data:ruinPcts, borderColor:'#ff4560', backgroundColor:'rgba(255,69,96,0.1)', borderWidth:2, pointRadius:0, fill:true}]},
      options:{...baseOpts, scales:{...baseOpts.scales,y:{...baseOpts.scales.y,max:100,ticks:{...baseOpts.scales.y.ticks,callback:v=>v+'%'}}}}
    });
  },
  
  destroyCharts() { Object.values(this.charts).forEach(c=>{try{c.destroy();}catch(e){}}); this.charts={}; }
};
