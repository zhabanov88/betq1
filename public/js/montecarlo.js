// Monte Carlo Simulation Engine
const monteCarlo = {
  charts: {},
  
  run() {
    const simCount = parseInt(document.getElementById('mcSimCount').value);
    const betsPerRun = parseInt(document.getElementById('mcBetsPerRun').value);
    const startBankroll = parseFloat(document.getElementById('mcBankroll').value);
    const winRate = parseFloat(document.getElementById('mcWinRate').value) / 100;
    const avgOdds = parseFloat(document.getElementById('mcAvgOdds').value);
    const stakePct = parseFloat(document.getElementById('mcStake').value) / 100;
    const ruinThreshold = parseFloat(document.getElementById('mcRuinThreshold').value) / 100;
    
    const allPaths = [];
    const finalBankrolls = [];
    let ruinCount = 0;
    const ruinByBet = Array(betsPerRun).fill(0);
    
    for (let s = 0; s < simCount; s++) {
      let bank = startBankroll;
      const path = [bank];
      let ruined = false;
      for (let b = 0; b < betsPerRun; b++) {
        const stake = bank * stakePct;
        const won = Math.random() < winRate;
        bank = Math.max(0, won ? bank + stake * (avgOdds - 1) : bank - stake);
        path.push(bank);
        if (!ruined && bank <= startBankroll * ruinThreshold) {
          ruined = true;
          ruinCount++;
          for (let rb = b; rb < betsPerRun; rb++) ruinByBet[rb]++;
        }
      }
      if (s < 500) allPaths.push(path);
      finalBankrolls.push(bank);
    }
    
    finalBankrolls.sort((a,b)=>a-b);
    const p5 = finalBankrolls[Math.floor(simCount*0.05)];
    const p25 = finalBankrolls[Math.floor(simCount*0.25)];
    const p50 = finalBankrolls[Math.floor(simCount*0.50)];
    const p75 = finalBankrolls[Math.floor(simCount*0.75)];
    const p95 = finalBankrolls[Math.floor(simCount*0.95)];
    const avg = finalBankrolls.reduce((s,v)=>s+v,0)/simCount;
    const ruinProbability = ruinCount/simCount*100;
    
    document.getElementById('mcSummary').innerHTML = `
      <div style="display:grid;grid-template-columns:1fr 1fr;gap:8px;">
        <div><span style="color:var(--text3)">Median Final:</span><br><strong style="color:var(--accent)">${p50.toFixed(0)}</strong></div>
        <div><span style="color:var(--text3)">Avg Final:</span><br><strong>${avg.toFixed(0)}</strong></div>
        <div><span style="color:var(--text3)">P5 (worst 5%):</span><br><strong style="color:var(--red)">${p5.toFixed(0)}</strong></div>
        <div><span style="color:var(--text3)">P95 (best 5%):</span><br><strong style="color:var(--green)">${p95.toFixed(0)}</strong></div>
        <div><span style="color:var(--text3)">Ruin Probability:</span><br><strong style="color:${ruinProbability>20?'var(--red)':'var(--green)'}">${ruinProbability.toFixed(1)}%</strong></div>
        <div><span style="color:var(--text3)">Expected ROI:</span><br><strong style="color:${avg>startBankroll?'var(--green)':'var(--red)'}">${((avg-startBankroll)/startBankroll*100).toFixed(1)}%</strong></div>
      </div>`;
    
    this.renderCharts(allPaths, finalBankrolls, ruinByBet, betsPerRun, simCount, startBankroll, p5, p25, p50, p75, p95);
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
