const alerts = {
  list: [],
  load() {
    this.list = JSON.parse(localStorage.getItem('bq_alerts') || '[]');
    if (!this.list.length) {
      this.list = [
        { id:1, name:'Value Bet > 8%', type:'value', threshold:8, active:true, triggered:false, channels:['browser'], created:'2024-03-01' },
        { id:2, name:'Odds Drop 15%+', type:'odds_drop', threshold:15, active:true, triggered:true, channels:['browser','telegram'], created:'2024-03-05' },
      ];
    }
    this.render();
  },
  render() {
    const container = document.getElementById('alertsList');
    if (!container) return;
    if (!this.list.length) { container.innerHTML = '<div class="empty-state"><div class="empty-state-icon">🔕</div>No alerts configured</div>'; return; }
    container.innerHTML = this.list.map(a => `
      <div class="alert-item ${a.triggered?'triggered':a.active?'active':''}">
        <span class="alert-icon">${{value:'💎',odds_drop:'📉',strategy_signal:'📊',line_open:'🚀',injury:'🏥'}[a.type]||'🔔'}</span>
        <div class="alert-info">
          <div class="alert-name">${a.name}</div>
          <div class="alert-desc">Type: ${a.type} | Threshold: ${a.threshold} | Channels: ${a.channels.join(', ')}</div>
          <div class="alert-desc" style="color:var(--text3);font-size:10px">Created: ${a.created}</div>
        </div>
        <div style="display:flex;gap:6px;flex-direction:column">
          <span class="chip ${a.active?'green':'red'}">${a.active?'Active':'Paused'}</span>
          ${a.triggered ? '<span class="chip yellow">Triggered!</span>' : ''}
          <button class="ctrl-btn sm" onclick="alerts.toggle(${a.id})">${a.active?'Pause':'Resume'}</button>
          <button class="ctrl-btn sm" style="color:var(--red)" onclick="alerts.delete(${a.id})">Delete</button>
        </div>
      </div>`).join('');
  },
  createAlert() {
    const div = document.getElementById('alertsCreate');
    if (div) div.style.display = div.style.display==='none'?'':'none';
  },
  save() {
    const name = document.getElementById('alertName')?.value;
    if (!name) return;
    this.list.unshift({ id:Date.now(), name, type:document.getElementById('alertType')?.value, threshold:parseFloat(document.getElementById('alertThreshold')?.value)||5, active:true, triggered:false, channels:['browser'], created:new Date().toISOString().split('T')[0] });
    localStorage.setItem('bq_alerts', JSON.stringify(this.list));
    this.render();
    const div = document.getElementById('alertsCreate');
    if (div) div.style.display = 'none';
  },
  toggle(id) {
    const a = this.list.find(x=>x.id===id);
    if (a) { a.active = !a.active; localStorage.setItem('bq_alerts',JSON.stringify(this.list)); this.render(); }
  },
  delete(id) {
    this.list = this.list.filter(x=>x.id!==id);
    localStorage.setItem('bq_alerts',JSON.stringify(this.list));
    this.render();
  }
};
