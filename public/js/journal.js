'use strict';
// BetQuant Pro — Журнал ставок
// Реальные данные из PostgreSQL через API. Тестовые — только при bq_demo_mode=true.
const journal = {
  bets: [],

  async refresh() { await this.load(); },

  async load() {
    if (localStorage.getItem('bq_demo_mode') === 'true') {
      this.bets = this._demoData();
      this.render(); return;
    }
    try {
      const r = await apiCall('/api/journal/bets?limit=500');
      if (r && r.bets) { this.bets = r.bets; this.render(); return; }
    } catch(e) {}
    this.bets = JSON.parse(localStorage.getItem('bq_journal') || '[]');
    this.render();
  },

  _demoData() {
    return [
      {id:1,date:'2024-03-15',sport:'Футбол',match:'Арсенал — Челси',market:'1X2',selection:'П1',odds:2.10,stake:20,result:'win',pnl:22.0,strategy:'Value Bet'},
      {id:2,date:'2024-03-16',sport:'Футбол',match:'Барселона — Атлетико',market:'Тотал',selection:'Больше 2.5',odds:1.85,stake:15,result:'loss',pnl:-15.0,strategy:'xG Модель'},
      {id:3,date:'2024-03-17',sport:'Теннис',match:'Джокович — Алькарас',market:'Победитель',selection:'Джокович',odds:1.60,stake:25,result:'win',pnl:15.0,strategy:'Форма'},
      {id:4,date:'2024-03-18',sport:'Баскетбол',match:'Лейкерс — Селтикс',market:'Форa',selection:'Селтикс -3.5',odds:1.91,stake:20,result:'win',pnl:18.2,strategy:'ATS'},
      {id:5,date:'2024-03-19',sport:'Футбол',match:'Бавария — Дортмунд',market:'ОЗ',selection:'Да',odds:1.72,stake:10,result:'loss',pnl:-10.0,strategy:'Статистика'},
    ];
  },

  render() {
    const wins=this.bets.filter(b=>b.result==='win').length, losses=this.bets.filter(b=>b.result==='loss').length;
    const pnl=this.bets.reduce((s,b)=>s+(+b.pnl||0),0), stake=this.bets.reduce((s,b)=>s+(+b.stake||0),0);
    const set=(id,v,cls)=>{const el=document.getElementById(id);if(el){el.textContent=v;if(cls)el.className=cls;}};
    set('jTotal',this.bets.length);
    set('jWon',wins,'positive');
    set('jLost',losses,'negative');
    set('jPL',(pnl>=0?'+':'')+pnl.toFixed(2),pnl>=0?'positive':'negative');
    set('jROI',stake>0?(pnl/stake*100).toFixed(1)+'%':'—',pnl>=0?'positive':'negative');
    const ct=document.getElementById('journalTable');
    if(!ct)return;
    if(!this.bets.length){
      ct.innerHTML='<div class="empty-state" style="padding:48px;text-align:center"><div style="font-size:36px;margin-bottom:12px">📓</div><div style="color:var(--text2);margin-bottom:6px">Журнал пуст</div><div style="color:var(--text3);font-size:12px">Нажмите «+ Добавить ставку» чтобы начать</div></div>';
      return;
    }
    const resLabel={win:'Победа',loss:'Поражение',void:'Возврат'};
    ct.innerHTML=makeTable(
      [{label:'Дата',key:'date'},{label:'Спорт',key:'sport'},{label:'Матч',key:'match'},{label:'Рынок',key:'market'},{label:'Исход',key:'selection'},{label:'Коэф.',key:'odds'},{label:'Ставка',key:'stake'},{label:'Результат',key:'result'},{label:'P&L',key:'pnl',color:true},{label:'Стратегия',key:'strategy'}],
      this.bets.map(b=>({...b,
        result:`<span class="${b.result==='win'?'positive':b.result==='loss'?'negative':''}">${resLabel[b.result]||b.result||'—'}</span>`,
        pnl:`<span class="${+b.pnl>=0?'positive':'negative'}">${+b.pnl>=0?'+':''}${(+b.pnl).toFixed(2)}</span>`
      }))
    );
  },

  async addBet() {
    if(document.getElementById('journalAddModal')){document.getElementById('journalAddModal').style.display='flex';return;}
    const m=document.createElement('div');m.id='journalAddModal';m.className='modal';m.style.display='flex';
    m.onclick=e=>{if(e.target===m)m.style.display='none';};
    m.innerHTML=`<div class="modal-content" style="width:480px;max-width:95vw">
      <div class="modal-header"><span>➕ Новая ставка</span><button onclick="document.getElementById('journalAddModal').style.display='none'">×</button></div>
      <div style="padding:16px;display:grid;grid-template-columns:1fr 1fr;gap:10px">
        <div class="config-row" style="grid-column:1/-1"><label>Матч</label><input class="ctrl-input" id="jMatch" placeholder="Команда А — Команда Б"></div>
        <div class="config-row"><label>Дата</label><input type="date" class="ctrl-input" id="jDate" value="${new Date().toISOString().slice(0,10)}"></div>
        <div class="config-row"><label>Спорт</label><select class="ctrl-select" id="jSport"><option>Футбол</option><option>Теннис</option><option>Баскетбол</option><option>Хоккей</option><option>Прочее</option></select></div>
        <div class="config-row"><label>Рынок</label><input class="ctrl-input" id="jMarket" placeholder="1X2, Тотал..."></div>
        <div class="config-row"><label>Исход</label><input class="ctrl-input" id="jSelection" placeholder="П1, Больше 2.5..."></div>
        <div class="config-row"><label>Коэффициент</label><input type="number" step="0.01" class="ctrl-input" id="jOdds" placeholder="2.10"></div>
        <div class="config-row"><label>Ставка</label><input type="number" class="ctrl-input" id="jStake" placeholder="100"></div>
        <div class="config-row"><label>Результат</label><select class="ctrl-select" id="jResult"><option value="">— ожидает —</option><option value="win">Победа</option><option value="loss">Поражение</option><option value="void">Возврат</option></select></div>
        <div class="config-row" style="grid-column:1/-1"><label>Стратегия</label><input class="ctrl-input" id="jStrategy" placeholder="Название стратегии"></div>
      </div>
      <div style="padding:0 16px 16px;display:flex;gap:8px;justify-content:flex-end">
        <button class="ctrl-btn" onclick="document.getElementById('journalAddModal').style.display='none'">Отмена</button>
        <button class="ctrl-btn primary" onclick="journal.saveBet()">💾 Сохранить</button>
      </div>
    </div>`;
    document.body.appendChild(m);
  },

  async saveBet() {
    const g=id=>document.getElementById(id)?.value||'';
    const odds=parseFloat(g('jOdds'))||0, stake=parseFloat(g('jStake'))||0, result=g('jResult');
    const pnl=result==='win'?+(stake*(odds-1)).toFixed(2):result==='loss'?-stake:0;
    const bet={date:g('jDate'),sport:g('jSport'),match:g('jMatch'),market:g('jMarket'),selection:g('jSelection'),odds,stake,result,pnl,strategy:g('jStrategy')};
    if(!bet.match||!bet.odds){alert('Заполните матч и коэффициент');return;}
    try{const r=await apiCall('/api/journal/bets','POST',bet);if(r&&r.ok){document.getElementById('journalAddModal').style.display='none';await this.load();return;}}catch(e){}
    const saved=JSON.parse(localStorage.getItem('bq_journal')||'[]');bet.id=Date.now();saved.unshift(bet);
    localStorage.setItem('bq_journal',JSON.stringify(saved));this.bets=saved;this.render();
    document.getElementById('journalAddModal').style.display='none';
  },

  async export() {
    if(!this.bets.length){alert('Журнал пуст');return;}
    const h=['Дата','Спорт','Матч','Рынок','Исход','Коэф','Ставка','Результат','P&L','Стратегия'];
    const rows=this.bets.map(b=>[b.date,b.sport,b.match,b.market,b.selection,b.odds,b.stake,b.result,b.pnl,b.strategy||''].map(v=>`"${String(v??'').replace(/"/g,'""')}"`).join(','));
    const a=Object.assign(document.createElement('a'),{href:'data:text/csv;charset=utf-8,\uFEFF'+encodeURIComponent([h.join(','),...rows].join('\n')),download:`журнал_ставок_${new Date().toISOString().slice(0,10)}.csv`});
    a.click();
  },
};