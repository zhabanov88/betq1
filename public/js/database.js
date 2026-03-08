'use strict';
// BetQuant Pro — База данных (русский, без демо-данных по умолчанию)
const db = {
  currentTable:'matches', page:1, pageSize:50,

  async refresh(){this.showTable('matches');this.loadCounts();},

  async loadCounts(){
    for(const t of ['matches','odds','team_stats','xg_data','lineups']){
      try{const r=await apiCall('/api/db/count/'+t);const el=document.getElementById('dbCount-'+t);if(el)el.textContent=r?(+r.count).toLocaleString('ru-RU'):'—';}catch(e){}
    }
  },

  async showTable(name){
    this.currentTable=name;
    document.querySelectorAll('.db-table-item').forEach(el=>el.classList.toggle('active',el.textContent.trim().startsWith(name)));
    const ct=document.getElementById('dbTableContent');if(!ct)return;
    ct.innerHTML='<div class="empty-state" style="padding:40px;text-align:center"><div style="font-size:24px;margin-bottom:8px">⏳</div><div style="color:var(--text3)">Загрузка...</div></div>';
    try{
      const r=await apiCall(`/api/db/table/${name}?page=${this.page}&limit=${this.pageSize}`);
      if(r&&r.rows&&r.rows.length){ct.innerHTML=this.renderTable(r.rows,r.columns);this.renderPagination(r.total);}
      else ct.innerHTML=this.renderEmpty(name);
    }catch(e){ct.innerHTML=this.renderEmpty(name);}
  },

  renderTable(rows,cols){
    if(!rows.length)return this.renderEmpty(this.currentTable);
    const headers=cols||Object.keys(rows[0]);
    let html='<table class="data-table"><thead><tr>';
    headers.forEach(h=>html+=`<th onclick="db.sort('${h}')">${h}</th>`);
    html+='</tr></thead><tbody>';
    rows.forEach(row=>{html+='<tr>';headers.forEach(h=>{const v=row[h];html+=`<td>${v!==null&&v!==undefined?v:'—'}</td>`;});html+='</tr>';});
    return html+'</tbody></table>';
  },

  renderEmpty(name){
    const names={matches:'матчей',odds:'коэффициентов',team_stats:'статистики команд',xg_data:'xG данных',lineups:'составов'};
    return `<div class="empty-state" style="padding:60px;text-align:center">
      <div style="font-size:36px;margin-bottom:12px">📭</div>
      <div style="color:var(--text2);font-size:14px;margin-bottom:8px">Нет данных для таблицы «${names[name]||name}»</div>
      <div style="color:var(--text3);font-size:12px">Подключите ClickHouse и запустите ETL для загрузки данных</div>
      <button class="ctrl-btn" style="margin-top:12px" onclick="app.showPanel('scraper')">🕷 Перейти к сборщику данных</button>
    </div>`;
  },

  renderPagination(total){
    const pg=document.getElementById('dbPagination');if(!pg||!total)return;
    const tp=Math.ceil(total/this.pageSize);
    pg.innerHTML=`<span style="color:var(--text3);font-size:12px">Всего: ${total.toLocaleString('ru-RU')} записей</span>
      <div style="display:flex;gap:4px;align-items:center">
        <button class="ctrl-btn sm" onclick="db.prevPage()" ${this.page<=1?'disabled':''}>‹</button>
        <span style="font-size:12px;color:var(--text2)">${this.page} / ${tp}</span>
        <button class="ctrl-btn sm" onclick="db.nextPage()" ${this.page>=tp?'disabled':''}>›</button>
      </div>`;
  },

  prevPage(){if(this.page>1){this.page--;this.showTable(this.currentTable);}},
  nextPage(){this.page++;this.showTable(this.currentTable);},
  sort(){this.showTable(this.currentTable);},

  async export(){
    try{
      const r=await apiCall(`/api/db/table/${this.currentTable}?page=1&limit=10000`);
      if(!r||!r.rows||!r.rows.length){alert('Нет данных для экспорта');return;}
      const h=r.columns||Object.keys(r.rows[0]);
      const csv=[h.join(','),...r.rows.map(row=>h.map(k=>`"${String(row[k]??'').replace(/"/g,'""')}"`).join(','))].join('\n');
      Object.assign(document.createElement('a'),{href:'data:text/csv;charset=utf-8,\uFEFF'+encodeURIComponent(csv),download:`${this.currentTable}_${new Date().toISOString().slice(0,10)}.csv`}).click();
    }catch(e){alert('Ошибка экспорта: '+e.message);}
  },

  openQuery(){document.getElementById('sqlModal').style.display='flex';},
  closeQuery(){document.getElementById('sqlModal').style.display='none';},

  async runQuery(){
    const sql=document.getElementById('sqlQuery')?.value?.trim();if(!sql)return;
    const ct=document.getElementById('sqlResult');if(ct)ct.innerHTML='<div style="color:var(--text3);padding:8px">⏳ Выполняется...</div>';
    try{
      const r=await apiCall('/api/db/query','POST',{sql});
      if(r&&r.rows)ct.innerHTML=this.renderTable(r.rows,r.columns);
      else ct.innerHTML=`<div style="color:var(--red);padding:8px">${r?.error||'Ошибка выполнения запроса'}</div>`;
    }catch(e){if(ct)ct.innerHTML=`<div style="color:var(--red);padding:8px">Ошибка: ${e.message}</div>`;}
  },

  search(){this.showTable(this.currentTable);},
  filter(){this.showTable(this.currentTable);},
};