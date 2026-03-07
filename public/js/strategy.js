// Strategy Builder
const strategyBuilder = {
  filters: { market:[], stats:[], odds:[] },
  
  addFilter(type) {
    const id = Date.now();
    const opts = {
      market: [['league','League'],['sport','Sport'],['home_away','Home/Away'],['season','Season'],['round','Round']],
      stats: [['form','Team Form (last N)'],['goals_scored','Goals Scored avg'],['goals_conceded','Goals Conceded avg'],['xg','xG avg'],['xga','xGA avg'],['h2h_wins','H2H Wins'],['clean_sheets','Clean Sheets %']],
      odds: [['odds_home','Home Odds'],['odds_draw','Draw Odds'],['odds_away','Away Odds'],['odds_over','Over Odds'],['odds_under','Under Odds'],['odds_btts','BTTS Odds'],['implied_prob','Implied Probability']]
    };
    const filter = { id, type, field: opts[type][0][0], op:'>', value:'' };
    this.filters[type].push(filter);
    this.renderFilters(type, opts[type]);
    this.updateCodePreview();
  },
  
  renderFilters(type, opts) {
    const container = document.getElementById(type+'Filters');
    const opsList = ['>', '<', '>=', '<=', '==', '!=', 'contains', 'in'];
    container.innerHTML = this.filters[type].map((f,i) => `
      <div class="filter-row">
        <select class="ctrl-select-sm" onchange="strategyBuilder.filters['${type}'][${i}].field=this.value;strategyBuilder.updateCodePreview()">
          ${(opts||[]).map(([v,l])=>`<option value="${v}"${f.field===v?' selected':''}>${l}</option>`).join('')}
        </select>
        <select class="ctrl-select-sm" style="width:50px" onchange="strategyBuilder.filters['${type}'][${i}].op=this.value;strategyBuilder.updateCodePreview()">
          ${opsList.map(op=>`<option${f.op===op?' selected':''}>${op}</option>`).join('')}
        </select>
        <input class="ctrl-input-sm" style="width:60px" value="${f.value}" placeholder="value" oninput="strategyBuilder.filters['${type}'][${i}].value=this.value;strategyBuilder.updateCodePreview()">
        <button class="filter-remove" onclick="strategyBuilder.removeFilter('${type}',${f.id})">×</button>
      </div>`).join('');
  },
  
  removeFilter(type, id) {
    this.filters[type] = this.filters[type].filter(f=>f.id!==id);
    const opts = { market:[['league','League'],['sport','Sport']], stats:[['form','Form']], odds:[['odds_home','Home Odds']] };
    this.renderFilters(type, opts[type]);
    this.updateCodePreview();
  },
  
  updateCodePreview() {
    const lines = ['function evaluate(match, team, h2h, market) {', '  // Auto-generated from visual builder'];
    const allFilters = [...this.filters.market, ...this.filters.stats, ...this.filters.odds];
    if (allFilters.length) {
      lines.push('  const conditions = [');
      allFilters.forEach(f => {
        if (!f.value) return;
        let expr = '';
        if (['league','sport'].includes(f.field)) expr = `match.${f.field} ${f.op} '${f.value}'`;
        else if (f.field.startsWith('odds_')) expr = `match.${f.field} ${f.op} ${f.value}`;
        else if (f.field === 'form') expr = `team.form(match.team_home,5).filter(r=>r==='W').length ${f.op} ${f.value}`;
        else expr = `/* ${f.field} ${f.op} ${f.value} */`;
        lines.push(`    ${expr},`);
      });
      lines.push('  ];', '  if (!conditions.every(Boolean)) return null;');
    }
    lines.push('  // Add signal logic here', '  return { signal: true, market: \'home\', stake: 1, prob: 0.5 };', '}');
    const el = document.getElementById('strategyCode');
    if (el && !el.value.trim()) el.value = lines.join('\n');
  },
  
  showTab(tab) {
    document.querySelectorAll('.strat-tab').forEach((b,i) => {
      const tabs = ['visual','code','docs'];
      b.classList.toggle('active', tabs[i]===tab);
    });
    document.querySelectorAll('.strat-tab-content').forEach((c,i) => {
      const tabs = ['stratTabVisual','stratTabCode','stratTabDocs'];
      if (tabs[i]) c.classList.toggle('active', c.id===('stratTab'+tab.charAt(0).toUpperCase()+tab.slice(1)));
    });
    if (tab==='docs') this.renderDocs();
  },
  
  renderDocs() {
    document.getElementById('strategyDocs').innerHTML = `
    <h3>Available Variables</h3>
    <p><code>match.team_home</code>, <code>match.team_away</code> — team names<br>
    <code>match.league</code>, <code>match.country</code>, <code>match.date</code><br>
    <code>match.odds_home</code>, <code>match.odds_draw</code>, <code>match.odds_away</code><br>
    <code>match.odds_over</code>, <code>match.odds_under</code> — O/U 2.5<br>
    <code>match.odds_btts</code> — BTTS Yes odds<br>
    <code>match.prob_home</code>, <code>match.prob_draw</code>, <code>match.prob_away</code> — implied probs</p>
    
    <h3>Team API</h3>
    <p><code>team.form(name, n)</code> → ['W','D','L',...] last N results<br>
    <code>team.goalsScored(name, n)</code> → avg goals scored last N<br>
    <code>team.goalsConceded(name, n)</code> → avg conceded last N<br>
    <code>team.xG(name, n)</code> → avg xG last N<br>
    <code>team.xGA(name, n)</code> → avg xG Against last N</p>
    
    <h3>H2H API</h3>
    <p><code>h2h.results</code> → array of last 5 H2H matches<br>
    Each result: <code>{ home, away, home_goals, away_goals, result }</code></p>
    
    <h3>Market API</h3>
    <p><code>market.implied(odds)</code> → implied probability (1/odds)<br>
    <code>market.value(odds, myProb)</code> → edge (myProb - implied)<br>
    <code>market.kelly(odds, myProb)</code> → Kelly fraction</p>
    
    <h3>Return Format</h3>
    <p><code>{ signal: true, market: 'home'|'draw'|'away'|'over'|'under'|'btts', stake: 1, prob: 0.55 }</code><br>
    Return <code>null</code> or <code>{ signal: false }</code> to skip the match.</p>`;
  },
  
  async save() {
    const name = document.getElementById('strategyName').value || 'Unnamed Strategy';
    const code = document.getElementById('strategyCode').value;
    const strategies = JSON.parse(localStorage.getItem('bq_strategies') || '[]');
    strategies.unshift({ id: Date.now(), name, code, created: new Date().toISOString(), tags: [] });
    localStorage.setItem('bq_strategies', JSON.stringify(strategies));
    alert('Strategy "' + name + '" saved!');
    library.load();
  },
  
  runBacktest() {
    const code = document.getElementById('strategyCode').value;
    document.getElementById('btInlineCode').value = code;
    app.showPanel('backtest');
    backtestEngine.run();
  },
  
  formatCode() {
    const el = document.getElementById('strategyCode');
    try { el.value = js_beautify ? js_beautify(el.value) : el.value; } catch(e){}
  },
  
  validateCode() {
    const code = document.getElementById('strategyCode').value;
    try {
      new Function('match','team','h2h','market', code.replace(/^function evaluate[^{]*{/,'').replace(/}$/,''));
      alert('✅ Code is valid!');
    } catch(e) { alert('❌ Syntax error: ' + e.message); }
  }
};
