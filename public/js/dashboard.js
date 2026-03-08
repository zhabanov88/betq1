'use strict';
// BetQuant Pro — Дашборд
// Боевые данные из API. Тестовые — только при bq_demo_mode=true в настройках.
const dashboard = {
  charts: {},

  async refresh() {
    const days = parseInt(document.getElementById('dashPeriod')?.value || 30);
    const data = await this.loadData(days);
    this.updateKPIs(data);
    this.renderCharts(data);
    this.updateTicker(data);
  },

  async loadData(days) {
    if (localStorage.getItem('bq_demo_mode') === 'true') {
      return this._calcStats(this._generateDemo(days), days);
    }
    try {
      const r = await apiCall(`/api/journal/bets?days=${days}&limit=2000`);
      if (r && r.bets && r.bets.length) {
        const bets = r.bets.map(b => ({
          ...b,
          date:   new Date(b.date || b.bet_date || b.created_at),
          win:    b.result === 'win',
          odds:   +b.odds || +b.bet_odds || 1,
          stake:  +b.stake || 10,
          pnl:    +b.pnl || 0,
          league: b.league || b.sport || '—',
          type:   b.market || b.bet_type || '1X2',
        }));
        return this._calcStats(bets, days);
      }
    } catch(e) {}
    return { bets:[], bankrollCurve:[0], ddCurve:[0], wins:0, totalStaked:0, totalPnL:0, roi:0, avgOdds:0, maxDD:0, sharpe:0, monthly:{}, byLeague:{}, byType:{}, oddsBuckets:Array(10).fill(0), clvData:[], days, empty:true };
  },

  _generateDemo(days) {
    const bets=[], leagues=['АПЛ','Ла Лига','Бундеслига','Серия А','Лига 1','ЛЧ'], types=['П1','X','П2','Больш.2.5','Меньш.2.5','ОЗ'];
    let bank=1000;
    for(let i=0;i<days*3;i++){
      const odds=1.4+Math.random()*3, stake=bank*0.02, win=Math.random()<(1/odds*1.05+0.02), pnl=win?stake*(odds-1):-stake;
      bank=Math.max(0,bank+pnl);
      bets.push({date:new Date(Date.now()-(days-Math.floor(i/3))*86400000),odds,stake,win,pnl,bank,league:leagues[i%leagues.length],type:types[i%types.length]});
    }
    return bets;
  },

  _calcStats(bets, days) {
    let bank=1000; const bc=[bank];
    bets.forEach(b=>{bank=Math.max(0,bank+b.pnl);bc.push(bank);});
    const wins=bets.filter(b=>b.win).length, ts=bets.reduce((s,b)=>s+b.stake,0), tp=bets.reduce((s,b)=>s+b.pnl,0);
    const roi=ts>0?tp/ts*100:0, avgO=bets.length?bets.reduce((s,b)=>s+b.odds,0)/bets.length:0;
    let peak=bc[0],mxdd=0;
    const dd=bc.map(v=>{if(v>peak)peak=v;const d=peak>0?(peak-v)/peak*100:0;if(d>mxdd)mxdd=d;return -d;});
    const rets=bets.map(b=>b.stake>0?b.pnl/b.stake:0), avgR=rets.length?rets.reduce((s,r)=>s+r,0)/rets.length:0;
    const stdR=rets.length>1?Math.sqrt(rets.reduce((s,r)=>s+(r-avgR)**2,0)/rets.length):0;
    const sharpe=stdR>0?(avgR/stdR)*Math.sqrt(252):0;
    const monthly={},byLeague={},byType={},ob=Array(10).fill(0);
    bets.forEach(b=>{
      const k=b.date.getFullYear()+'-'+String(b.date.getMonth()+1).padStart(2,'0');
      monthly[k]=(monthly[k]||0)+b.pnl;
      if(!byLeague[b.league])byLeague[b.league]={w:0,l:0};
      if(b.win)byLeague[b.league].w++;else byLeague[b.league].l++;
      if(!byType[b.type])byType[b.type]={pnl:0,count:0};
      byType[b.type].pnl+=b.pnl;byType[b.type].count++;
      ob[Math.min(Math.floor((b.odds-1)/0.4),9)]++;
    });
    const clvData=bets.slice(0,50).map((b,i)=>({x:i,y:b.clv_pct!=null?+b.clv_pct:0}));
    return {bets,bankrollCurve:bc,ddCurve:dd,wins,totalStaked:ts,totalPnL:tp,roi,avgOdds:avgO,maxDD:mxdd,sharpe,monthly,byLeague,byType,oddsBuckets:ob,clvData,days};
  },

  updateKPIs(d) {
    const set=(id,val,cls)=>{const el=document.getElementById(id);if(!el)return;el.textContent=val;if(cls){el.classList.remove('positive','negative');el.classList.add(cls);}};
    const wr=d.wins/Math.max(1,d.bets.length);
    set('kpiBets',d.bets.length);
    set('kpiWinRate',formatNum(wr*100,1)+'%',wr>0.5?'positive':wr<0.4?'negative':null);
    set('kpiROI',formatPct(d.roi),d.roi>0?'positive':d.roi<0?'negative':null);
    set('kpiProfit',formatNum(d.totalPnL,2),d.totalPnL>0?'positive':d.totalPnL<0?'negative':null);
    set('kpiAvgOdds',formatOdds(d.avgOdds));
    set('kpiDrawdown','-'+formatNum(d.maxDD,1)+'%','negative');
    set('kpiSharpe',formatNum(d.sharpe,2),d.sharpe>1?'positive':d.sharpe<0?'negative':null);
    set('kpiYield',formatPct(d.roi),d.roi>0?'positive':d.roi<0?'negative':null);
  },

  updateTicker(d) {
    const set=(id,v)=>{const el=document.getElementById(id);if(el)el.textContent=v;};
    set('tickerBetCount',d.bets.length);
    set('tickerWinRate',formatNum(d.wins/Math.max(1,d.bets.length)*100,1)+'%');
    set('tickerROI',formatPct(d.roi));
    const ca=d.clvData.length?d.clvData.reduce((s,c)=>s+c.y,0)/d.clvData.length:0;
    set('tickerKelly',formatPct(ca));
    const re=document.getElementById('tickerROI');
    if(re){re.classList.remove('positive','negative');re.classList.add(d.roi>0?'positive':'negative');}
  },

  renderCharts(d) {
    this.destroyAll();
    const IDS=['chartBankroll','chartOddsDistrib','chartLeagueWL','chartMonthlyPL','chartBetTypes','chartCLV'];
    if(d.empty){
      IDS.forEach(id=>{
        const c=document.getElementById(id); if(!c||!c.parentElement)return;
        c.style.display='none';
        if(!c.parentElement.querySelector('.dash-empty')){
          const m=document.createElement('div');m.className='dash-empty';
          m.style.cssText='display:flex;align-items:center;justify-content:center;min-height:80px;';
          m.innerHTML='<span style="color:var(--text3);font-size:12px">📭 Нет данных — добавьте ставки в журнал</span>';
          c.parentElement.appendChild(m);
        }
      });
      return;
    }
    IDS.forEach(id=>{const c=document.getElementById(id);if(c){c.style.display='';const o=c.parentElement?.querySelector('.dash-empty');if(o)o.remove();}});
    const C=Chart,dk=document.body.classList.contains('dark-mode');
    const grid=dk?'rgba(255,255,255,0.05)':'rgba(0,0,0,0.05)',tc=dk?'#8892a4':'#4a5568';
    const base={responsive:true,maintainAspectRatio:false,plugins:{legend:{labels:{color:tc,font:{size:10}}}},scales:{x:{ticks:{color:tc,font:{size:9}},grid:{color:grid}},y:{ticks:{color:tc,font:{size:9}},grid:{color:grid}}}};

    this.charts.bankroll=new C(document.getElementById('chartBankroll'),{type:'line',data:{labels:d.bankrollCurve.map((_,i)=>i===0?'Старт':i%10===0?''+i:''),datasets:[{label:'Банкролл',data:d.bankrollCurve,borderColor:'#00d4ff',backgroundColor:'rgba(0,212,255,0.06)',borderWidth:2,pointRadius:0,fill:true,tension:0.3}]},options:{...base,plugins:{...base.plugins,tooltip:{callbacks:{label:c=>'₽'+c.parsed.y.toFixed(2)}}}}});

    this.charts.oddsDistrib=new C(document.getElementById('chartOddsDistrib'),{type:'bar',data:{labels:Array.from({length:10},(_,i)=>(1+i*0.4).toFixed(1)+'–'+(1.4+i*0.4).toFixed(1)),datasets:[{label:'Ставки',data:d.oddsBuckets,backgroundColor:'rgba(0,212,255,0.6)',borderRadius:3}]},options:{...base,plugins:{legend:{display:false}}}});

    const lgs=Object.keys(d.byLeague);
    this.charts.leagueWL=new C(document.getElementById('chartLeagueWL'),{type:'bar',data:{labels:lgs,datasets:[{label:'Победы',data:lgs.map(l=>d.byLeague[l].w),backgroundColor:'rgba(0,230,118,0.7)',borderRadius:3},{label:'Поражения',data:lgs.map(l=>d.byLeague[l].l),backgroundColor:'rgba(255,69,96,0.7)',borderRadius:3}]},options:base});

    const months=Object.keys(d.monthly).sort(),mv=months.map(m=>d.monthly[m]);
    this.charts.monthlyPL=new C(document.getElementById('chartMonthlyPL'),{type:'bar',data:{labels:months,datasets:[{label:'P&L',data:mv,backgroundColor:mv.map(v=>v>0?'rgba(0,230,118,0.7)':'rgba(255,69,96,0.7)'),borderRadius:3}]},options:{...base,plugins:{legend:{display:false}}}});

    const types=Object.keys(d.byType);
    this.charts.betTypes=new C(document.getElementById('chartBetTypes'),{type:'doughnut',data:{labels:types,datasets:[{data:types.map(t=>d.byType[t].count),backgroundColor:['#00d4ff','#00e676','#ffd740','#ff4560','#c084fc','#fb923c']}]},options:{responsive:true,maintainAspectRatio:false,plugins:{legend:{position:'right',labels:{color:tc,font:{size:10}}}}}});

    const cc=d.clvData.map(p=>p.y>=0?'rgba(0,230,118,0.8)':'rgba(255,69,96,0.8)');
    this.charts.clv=new C(document.getElementById('chartCLV'),{type:'bar',data:{labels:d.clvData.map((_,i)=>'#'+i),datasets:[{label:'CLV%',data:d.clvData.map(p=>p.y),backgroundColor:cc,borderRadius:2}]},options:{...base,plugins:{legend:{display:false}}}});
  },

  destroyAll() { Object.values(this.charts).forEach(c=>{try{c.destroy();}catch(e){}});this.charts={}; },
};