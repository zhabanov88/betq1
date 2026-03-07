// Database Panel
const db = {
  currentTable: 'matches',
  page: 1, pageSize: 50,
  
  async refresh() {
    this.showTable('matches');
    this.loadCounts();
  },
  
  async loadCounts() {
    const tables = ['matches','odds','team_stats','xg_data','lineups'];
    for (const t of tables) {
      try {
        const r = await apiCall('/api/db/count/' + t);
        const el = document.getElementById('dbCount-' + t);
        if (el) el.textContent = r ? r.count.toLocaleString() : '—';
      } catch(e) {}
    }
  },
  
  async showTable(name) {
    this.currentTable = name;
    document.querySelectorAll('.db-table-item').forEach(el => el.classList.toggle('active', el.textContent.trim().startsWith(name)));
    const container = document.getElementById('dbTableContent');
    container.innerHTML = '<div class="empty-state"><div class="spinning">⬡</div><br>Loading...</div>';
    
    try {
      const r = await apiCall(`/api/db/table/${name}?page=${this.page}&limit=${this.pageSize}`);
      if (r && r.rows && r.rows.length) {
        container.innerHTML = this.renderTable(r.rows, r.columns);
        this.renderPagination(r.total);
      } else {
        container.innerHTML = this.renderDemoTable(name);
      }
    } catch(e) {
      container.innerHTML = this.renderDemoTable(name);
    }
  },
  
  renderTable(rows, cols) {
    if (!rows.length) return '<div class="empty-state"><div class="empty-state-icon">📭</div>No data</div>';
    const headers = cols || Object.keys(rows[0]);
    let html = '<table class="data-table"><thead><tr>';
    headers.forEach(h => html += '<th onclick="db.sort(\'' + h + '\')">' + h + '</th>');
    html += '</tr></thead><tbody>';
    rows.forEach(row => {
      html += '<tr>';
      headers.forEach(h => { const v=row[h]; html += '<td>' + (v!==null&&v!==undefined?v:'—') + '</td>'; });
      html += '</tr>';
    });
    html += '</tbody></table>';
    return html;
  },
  
  renderDemoTable(name) {
    const demos = {
      matches: {
        cols:['id','date','league','home_team','away_team','home_goals','away_goals','result'],
        rows:[
          {id:1,date:'2024-03-15',league:'EPL',home_team:'Arsenal',away_team:'Chelsea',home_goals:2,away_goals:1,result:'H'},
          {id:2,date:'2024-03-15',league:'La Liga',home_team:'Barcelona',away_team:'Real Madrid',home_goals:1,away_goals:3,result:'A'},
          {id:3,date:'2024-03-16',league:'Bundesliga',home_team:'Bayern',away_team:'Dortmund',home_goals:0,away_goals:0,result:'D'},
          {id:4,date:'2024-03-16',league:'Serie A',home_team:'Juventus',away_team:'Milan',home_goals:2,away_goals:2,result:'D'},
          {id:5,date:'2024-03-17',league:'EPL',home_team:'Liverpool',away_team:'Man City',home_goals:1,away_goals:2,result:'A'},
        ]
      },
      odds: {
        cols:['match_id','bookmaker','market','odds_home','odds_draw','odds_away','timestamp'],
        rows:[
          {match_id:1,bookmaker:'Pinnacle',market:'1X2',odds_home:2.10,odds_draw:3.40,odds_away:3.50,timestamp:'2024-03-14 10:00'},
          {match_id:1,bookmaker:'Bet365',market:'1X2',odds_home:2.05,odds_draw:3.30,odds_away:3.60,timestamp:'2024-03-14 10:05'},
          {match_id:1,bookmaker:'Pinnacle',market:'1X2',odds_home:1.95,odds_draw:3.50,odds_away:3.80,timestamp:'2024-03-15 08:00'},
          {match_id:2,bookmaker:'Pinnacle',market:'1X2',odds_home:1.80,odds_draw:3.80,odds_away:4.50,timestamp:'2024-03-14 11:00'},
        ]
      },
      team_stats: {
        cols:['team','league','season','matches','wins','draws','losses','gf','ga','xg','xga','ppg'],
        rows:[
          {team:'Arsenal',league:'EPL',season:'2023/24',matches:30,wins:19,draws:5,losses:6,gf:71,ga:28,xg:62.4,xga:25.1,ppg:2.07},
          {team:'Man City',league:'EPL',season:'2023/24',matches:30,wins:20,draws:4,losses:6,gf:68,ga:34,xg:65.2,xga:27.8,ppg:2.13},
          {team:'Liverpool',league:'EPL',season:'2023/24',matches:30,wins:18,draws:6,losses:6,gf:69,ga:38,xg:60.1,xga:31.2,ppg:2.00},
        ]
      },
      xg_data: {
        cols:['match_id','team','xg','shots','shots_on_target','deep_completions'],
        rows:[
          {match_id:1,team:'Arsenal',xg:1.87,shots:14,shots_on_target:6,deep_completions:12},
          {match_id:1,team:'Chelsea',xg:0.92,shots:8,shots_on_target:3,deep_completions:7},
          {match_id:2,team:'Barcelona',xg:1.45,shots:16,shots_on_target:5,deep_completions:15},
        ]
      },
      lineups: {
        cols:['match_id','team','player','position','rating','minutes'],
        rows:[
          {match_id:1,team:'Arsenal',player:'Raya',position:'GK',rating:7.2,minutes:90},
          {match_id:1,team:'Arsenal',player:'Saka',position:'RW',rating:8.1,minutes:90},
          {match_id:1,team:'Arsenal',player:'Ødegaard',position:'CAM',rating:7.8,minutes:86},
        ]
      }
    };
    const demo = demos[name] || demos.matches;
    return this.renderTable(demo.rows, demo.cols) + '<p style="color:var(--text3);font-size:10px;padding:8px">Demo data — connect database to see real records</p>';
  },
  
  renderPagination(total) {
    const pages = Math.ceil(total/this.pageSize);
    const container = document.getElementById('dbPagination');
    if (!container) return;
    container.innerHTML = `
      <button class="ctrl-btn sm" onclick="db.page=Math.max(1,db.page-1);db.showTable(db.currentTable)" ${this.page<=1?'disabled':''}>← Prev</button>
      <span>Page ${this.page} / ${pages || 1} (${total?.toLocaleString()||'?'} rows)</span>
      <button class="ctrl-btn sm" onclick="db.page=Math.min(${pages},db.page+1);db.showTable(db.currentTable)" ${this.page>=pages?'disabled':''}>Next →</button>`;
  },
  
  async openQuery() { document.getElementById('sqlModal').style.display='flex'; },
  closeQuery() { document.getElementById('sqlModal').style.display='none'; },
  
  async runQuery() {
    const sql = document.getElementById('sqlQuery').value;
    const container = document.getElementById('sqlResult');
    container.innerHTML = '<div class="empty-state"><div class="spinning">⬡</div></div>';
    const r = await apiCall('/api/db/query', 'POST', { sql });
    if (r && r.rows) container.innerHTML = this.renderTable(r.rows, r.columns);
    else container.innerHTML = '<div style="color:var(--red);padding:10px">' + (r?.error||'Query failed — demo mode') + '</div>';
  },
  
  search(q) { if (q.length>1||!q) this.showTable(this.currentTable); },
  filter() { this.showTable(this.currentTable); },
  sort(col) {},
  export() { alert('Export: connect to server for CSV download'); }
};
