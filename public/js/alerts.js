'use strict';
// BetQuant Pro — Алерты (русский, реальные данные из API)
const alerts = {
  list: [],
  async load() {
    if(localStorage.getItem('bq_demo_mode')==='true'){
      this.list=[
        {id:1,name:'Value Bet > 8%',type:'value',threshold:8,active:true,triggered:false,channels:['browser'],created:'2024-03-01'},
        {id:2,name:'Падение коэффициента 15%+',type:'odds_drop',threshold:15,active:true,triggered:true,channels:['browser','telegram'],created:'2024-03-05'},
      ];
      this.render();return;
    }
    try{const r=await apiCall('/api/alerts');if(r&&r.alerts){this.list=r.alerts;this.render();return;}}catch(e){}
    this.list=JSON.parse(localStorage.getItem('bq_alerts')||'[]');this.render();
  },

  render() {
    const ct=document.getElementById('alertsList');if(!ct)return;
    if(!this.list.length){
      ct.innerHTML='<div class="empty-state" style="padding:48px;text-align:center"><div style="font-size:36px;margin-bottom:12px">🔕</div><div style="color:var(--text2);margin-bottom:6px">Нет настроенных алертов</div><div style="color:var(--text3);font-size:12px">Нажмите «+ Новый алерт» чтобы создать уведомление</div></div>';
      return;
    }
    const icons={value:'💎',odds_drop:'📉',strategy_signal:'📊',line_open:'🚀',injury:'🏥'};
    const names={value:'Value ставка',odds_drop:'Падение коэффициента',strategy_signal:'Сигнал стратегии',line_open:'Открытие линии',injury:'Травма'};
    ct.innerHTML=this.list.map(a=>`
      <div class="alert-item ${a.triggered?'triggered':a.active?'active':''}">
        <span class="alert-icon">${icons[a.type]||'🔔'}</span>
        <div class="alert-info">
          <div class="alert-name">${a.name}</div>
          <div class="alert-desc">Тип: ${names[a.type]||a.type} | Порог: ${a.threshold} | Каналы: ${(a.channels||[]).join(', ')}</div>
          <div class="alert-desc" style="color:var(--text3);font-size:10px">Создан: ${a.created||'—'}</div>
        </div>
        <div style="display:flex;gap:6px;flex-direction:column">
          <span class="chip ${a.active?'green':'red'}">${a.active?'Активен':'Пауза'}</span>
          ${a.triggered?'<span class="chip yellow">Сработал!</span>':''}
          <button class="ctrl-btn sm" onclick="alerts.toggle(${a.id})">${a.active?'Пауза':'Запустить'}</button>
          <button class="ctrl-btn sm" style="color:var(--red)" onclick="alerts.delete(${a.id})">Удалить</button>
        </div>
      </div>`).join('');
  },

  createAlert() {
    const ex=document.getElementById('alertsCreate');
    if(ex){ex.style.display=ex.style.display==='none'?'':'none';return;}
    const div=document.createElement('div');div.id='alertsCreate';div.className='alerts-create';div.style.marginBottom='12px';
    div.innerHTML=`<div style="font-weight:600;margin-bottom:10px">➕ Новый алерт</div>
      <div style="display:grid;grid-template-columns:1fr 1fr;gap:8px;margin-bottom:10px">
        <input class="ctrl-input" id="alertName" placeholder="Название алерта">
        <select class="ctrl-select" id="alertType">
          <option value="value">💎 Value ставка</option>
          <option value="odds_drop">📉 Падение коэффициента</option>
          <option value="strategy_signal">📊 Сигнал стратегии</option>
          <option value="line_open">🚀 Открытие линии</option>
        </select>
        <input type="number" class="ctrl-input" id="alertThreshold" placeholder="Порог (напр. 5)" step="0.5">
        <select class="ctrl-select" id="alertChannel"><option value="browser">Браузер</option><option value="telegram">Telegram</option></select>
      </div>
      <div style="display:flex;gap:8px">
        <button class="ctrl-btn primary" onclick="alerts.save()">💾 Сохранить</button>
        <button class="ctrl-btn" onclick="document.getElementById('alertsCreate').style.display='none'">Отмена</button>
      </div>`;
    document.querySelector('.alerts-layout')?.prepend(div);
  },

  async save() {
    const name=document.getElementById('alertName')?.value?.trim();if(!name){alert('Введите название');return;}
    const a={id:Date.now(),name,type:document.getElementById('alertType')?.value||'value',threshold:parseFloat(document.getElementById('alertThreshold')?.value)||5,active:true,triggered:false,channels:[document.getElementById('alertChannel')?.value||'browser'],created:new Date().toISOString().slice(0,10)};
    try{await apiCall('/api/alerts','POST',a);}catch(e){}
    this.list.unshift(a);localStorage.setItem('bq_alerts',JSON.stringify(this.list));this.render();
    document.getElementById('alertsCreate').style.display='none';
  },

  async toggle(id){const a=this.list.find(x=>x.id===id);if(!a)return;a.active=!a.active;try{await apiCall(`/api/alerts/${id}`,'PATCH',{active:a.active});}catch(e){}localStorage.setItem('bq_alerts',JSON.stringify(this.list));this.render();},
  async delete(id){if(!confirm('Удалить алерт?'))return;this.list=this.list.filter(x=>x.id!==id);try{await apiCall(`/api/alerts/${id}`,'DELETE');}catch(e){}localStorage.setItem('bq_alerts',JSON.stringify(this.list));this.render();},
};