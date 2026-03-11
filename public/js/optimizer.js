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
    const method    = document.getElementById('optMethod')?.value    || 'grid';
    const objective = document.getElementById('optObjective')?.value || 'roi';
    const progress  = document.getElementById('optProgress');
    const txt       = document.getElementById('optProgressText');
    if (progress) progress.style.display = '';
    if (txt) txt.textContent = '⏳ Оптимизация на сервере...';

    const activeStrat = typeof backtestEngine !== 'undefined'
      ? (backtestEngine.activeStrategies || []).find(s => s.enabled) : null;

    if (!activeStrat) {
      alert('Сначала добавьте стратегию в Движке бэктеста');
      if (progress) progress.style.display = 'none';
      return;
    }

    try {
      const resp = await fetch('/api/bt/optimize', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          strategy: { name: activeStrat.name, sport: activeStrat.sport, code: activeStrat.code },
          params: this.params,
          cfg: { bankroll: 1000, dateFrom: '2020-01-01', dateTo: new Date().toISOString().slice(0,10) },
          method, objective, maxIter: 300,
        }),
      });
      const d = await resp.json();
      if (d.error) throw new Error(d.error);
      if (txt) txt.textContent = `✅ Готово: ${d.total} комбинаций`;
      if (progress) setTimeout(() => { progress.style.display = 'none'; }, 2000);

      // Обновляем таблицу результатов
      const container = document.getElementById('optResultsTable');
      if (container && d.results?.length) {
        const cols = Object.keys(d.results[0]);
        container.innerHTML = `<table class="data-table">
          <thead><tr>${cols.map(c => `<th>${c}</th>`).join('')}</tr></thead>
          <tbody>${d.results.slice(0,50).map((r,i) =>
            `<tr style="${i===0?'color:#00e676;font-weight:700':''}">${cols.map(c=>`<td>${r[c]}</td>`).join('')}</tr>`
          ).join('')}</tbody></table>`;
      }
      if (d.best) {
        let bestEl = document.getElementById('optBestResult');
        if (!bestEl) {
          bestEl = document.createElement('div');
          bestEl.id = 'optBestResult';
          if (container) container.parentNode.insertBefore(bestEl, container);
        }
        bestEl.innerHTML = `<div style="background:rgba(0,230,118,0.08);border:1px solid #00e67633;border-radius:8px;padding:12px;margin-bottom:12px">
          <div style="color:var(--green);font-weight:700;margin-bottom:8px">🏆 Лучшая комбинация</div>
          ${Object.entries(d.best).map(([k,v])=>
            `<div style="display:flex;justify-content:space-between;padding:2px 0"><span style="color:var(--text3)">${k}:</span><strong>${v}</strong></div>`
          ).join('')}</div>`;
      }
    } catch(e) {
      if (txt) txt.textContent = '❌ ' + e.message;
      if (progress) setTimeout(() => { progress.style.display = 'none'; }, 3000);
      console.error('[Optimizer]', e);
    }
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
