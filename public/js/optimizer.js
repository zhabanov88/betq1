// Parameter Optimizer
const optimizer = {
  params: [],
  charts: {},
  
  addParam() {
    const id = Date.now();
    this.params.push({ id, name:'minOdds', min:1.2, max:3.0, step:0.1 });
    this.renderParams();
  },
  
  renderParams() {
    const container = document.getElementById('optParamList');
    container.innerHTML = this.params.map((p,i) => `
      <div class="opt-param" id="opt-param-${p.id}">
        <div style="display:flex;gap:6px;align-items:center;margin-bottom:6px;">
          <input class="ctrl-input" style="flex:1" value="${p.name}" placeholder="param name" onchange="optimizer.params[${i}].name=this.value">
          <button class="filter-remove" onclick="optimizer.removeParam(${p.id})">×</button>
        </div>
        <div class="opt-param-range">
          <span>Min:</span><input type="number" class="ctrl-input-sm" style="width:60px" value="${p.min}" onchange="optimizer.params[${i}].min=+this.value">
          <span>Max:</span><input type="number" class="ctrl-input-sm" style="width:60px" value="${p.max}" onchange="optimizer.params[${i}].max=+this.value">
          <span>Step:</span><input type="number" class="ctrl-input-sm" style="width:60px" value="${p.step}" onchange="optimizer.params[${i}].step=+this.value">
        </div>
      </div>`).join('');
  },
  
  removeParam(id) {
    this.params = this.params.filter(p=>p.id!==id);
    this.renderParams();
  },
  
  async run() {
    if (!this.params.length) { this.addParam(); this.addParam(); }
    const method = document.getElementById('optMethod').value;
    const objective = document.getElementById('optObjective').value;
    const progress = document.getElementById('optProgress');
    const bar = document.getElementById('optProgressBar');
    const txt = document.getElementById('optProgressText');
    progress.style.display='';
    
    const p1 = this.params[0] || {name:'minOdds',min:1.3,max:3.0,step:0.1};
    const p2 = this.params[1] || {name:'minEdge',min:0,max:10,step:1};
    
    const results = [];
    const v1s = this.range(p1.min, p1.max, p1.step);
    const v2s = this.range(p2.min, p2.max, p2.step);
    const total = v1s.length * v2s.length;
    let done = 0;
    
    for (const v1 of v1s) {
      for (const v2 of v2s) {
        const roi = this.simulateRoi(v1, v2, objective);
        results.push({ [p1.name]: v1.toFixed(2), [p2.name]: v2.toFixed(2), roi: roi.toFixed(2), sharpe: (roi/10+Math.random()).toFixed(2), bets: Math.floor(50+Math.random()*200) });
        done++;
        if (done % 20 === 0) {
          bar.style.setProperty('--progress', (done/total*100)+'%');
          txt.textContent = `${method === 'genetic' ? 'Genetic Algorithm' : 'Grid Search'}: ${done}/${total} combinations`;
          await new Promise(r => setTimeout(r, 0));
        }
      }
    }
    
    results.sort((a,b)=>parseFloat(b.roi)-parseFloat(a.roi));
    bar.style.setProperty('--progress','100%');
    txt.textContent = `Complete — ${results.length} combinations evaluated`;
    setTimeout(() => { progress.style.display='none'; }, 1000);
    
    this.renderHeatmap(v1s, v2s, results, p1.name, p2.name, objective);
    this.renderResultsTable(results.slice(0,50), p1.name, p2.name);
  },
  
  range(min, max, step) {
    const r = [];
    for (let v=min; v<=max+step*0.001; v+=step) r.push(Math.round(v*1000)/1000);
    return r;
  },
  
  simulateRoi(v1, v2, objective) {
    // Simulated optimization landscape with realistic shape
    const peak1 = 1.8, peak2 = 4;
    const noise = (Math.random()-0.5)*3;
    const gaussian = Math.exp(-((v1-peak1)**2/0.8 + (v2-peak2)**2/8));
    return 8 * gaussian + noise - 1;
  },
  
  renderHeatmap(v1s, v2s, results, n1, n2, objective) {
    this.destroyCharts();
    const ctx = document.getElementById('chartOptSurface');
    const textColor = document.body.classList.contains('dark-mode') ? '#8892a4' : '#4a5568';
    
    const matrix = {};
    results.forEach(r => { matrix[`${r[n1]}_${r[n2]}`] = parseFloat(r.roi); });
    
    const max = Math.max(...results.map(r=>parseFloat(r.roi)));
    const min = Math.min(...results.map(r=>parseFloat(r.roi)));
    
    const datasets = v2s.map((v2,yi) => ({
      label: n2+'='+v2,
      data: v1s.map(v1 => matrix[`${v1.toFixed(2)}_${v2.toFixed(2)}`] || 0),
      borderColor: `hsl(${yi/v2s.length*200+180},70%,60%)`,
      backgroundColor: 'transparent',
      borderWidth: 1.5, pointRadius: 2, tension: 0.3
    }));
    
    this.charts.surface = new Chart(ctx, {
      type:'line',
      data:{ labels: v1s.map(v=>v.toFixed(2)), datasets },
      options:{ responsive:true, maintainAspectRatio:false, plugins:{legend:{labels:{color:textColor,font:{size:9},boxWidth:12}}}, scales:{x:{title:{display:true,text:n1,color:textColor},ticks:{color:textColor,font:{size:9}}},y:{title:{display:true,text:objective.toUpperCase(),color:textColor},ticks:{color:textColor,font:{size:9}}}} }
    });
  },
  
  renderResultsTable(results, n1, n2) {
    const container = document.getElementById('optResultsTable');
    const headers = [n1, n2, {label:'ROI %',key:'roi',color:true}, {label:'Sharpe',key:'sharpe'}, {label:'Bets',key:'bets'}];
    container.innerHTML = makeTable(headers, results.map((r,i) => ({...r, [n1]:`${i===0?'🏆 ':''}${r[n1]}`})));
  },
  
  destroyCharts() { Object.values(this.charts).forEach(c=>{try{c.destroy();}catch(e){}}); this.charts={}; }
};
