// Charts panel + Stats engine + Value Finder + Walk-Forward + Odds History
const oddsChart = {
  charts: {},
  load() {
    const team = document.getElementById('chartTeamSearch')?.value || 'Arsenal';
    document.getElementById('oddsChartTitle').textContent = team + ' — Odds Movement';
    this.renderMovement(team);
    this.renderComparison(team);
    this.renderSharpPublic();
    this.renderOpenClose();
  },
  renderMovement(team) {
    const n = 20;
    const labels = Array.from({length:n},(_,i)=>`-${n-i}d`);
    const homeBase=2.0, drawBase=3.4, awayBase=3.2;
    const homeData=Array.from({length:n},(_,i)=>homeBase + Math.sin(i/3)*0.3 - i*0.01 + (Math.random()-0.5)*0.15);
    const drawData=Array.from({length:n},(_,i)=>drawBase + Math.cos(i/4)*0.2 + (Math.random()-0.5)*0.1);
    const awayData=Array.from({length:n},(_,i)=>awayBase + Math.sin(i/3.5+1)*0.25 + i*0.005 + (Math.random()-0.5)*0.15);
    const textColor=document.body.classList.contains('dark-mode')?'#8892a4':'#4a5568';
    const gridColor='rgba(255,255,255,0.05)';
    if(this.charts.movement) this.charts.movement.destroy();
    this.charts.movement=new Chart(document.getElementById('chartOddsMovement'),{
      type:'line',
      data:{labels,datasets:[
        {label:'Home',data:homeData,borderColor:'#00d4ff',borderWidth:2,pointRadius:3,tension:0.3},
        {label:'Draw',data:drawData,borderColor:'#ffd740',borderWidth:2,pointRadius:3,tension:0.3},
        {label:'Away',data:awayData,borderColor:'#fb923c',borderWidth:2,pointRadius:3,tension:0.3}
      ]},
      options:{responsive:true,maintainAspectRatio:false,plugins:{legend:{labels:{color:textColor,font:{size:10}}}},scales:{x:{ticks:{color:textColor,font:{size:9}},grid:{color:gridColor}},y:{ticks:{color:textColor,font:{size:9}},grid:{color:gridColor}}}}
    });
    const books=['Pinnacle','Bet365','Betway','William Hill','Betfair','Unibet','1xBet'];
    const container=document.getElementById('oddsComparisonTable');
    container.innerHTML='<table class="data-table"><thead><tr><th>Bookmaker</th><th>Home</th><th>Draw</th><th>Away</th><th>Margin%</th></tr></thead><tbody>'+
      books.map(b=>{
        const h=(homeBase+0.1+Math.random()*0.3-0.15).toFixed(2);
        const d=(drawBase+0.1+Math.random()*0.3-0.15).toFixed(2);
        const a=(awayData[awayData.length-1]+0.1+Math.random()*0.3-0.15).toFixed(2);
        const margin=((1/h+1/d+1/a-1)*100).toFixed(1);
        return `<tr><td>${b}</td><td>${h}</td><td>${d}</td><td>${a}</td><td>${margin}%</td></tr>`;
      }).join('')+'</tbody></table>';
  },
  renderSharpPublic() {
    const textColor=document.body.classList.contains('dark-mode')?'#8892a4':'#4a5568';
    const gridColor='rgba(255,255,255,0.05)';
    const labels=['Home','Draw','Away'];
    if(this.charts.sharp) this.charts.sharp.destroy();
    this.charts.sharp=new Chart(document.getElementById('chartSharpPublic'),{
      type:'bar',
      data:{labels,datasets:[
        {label:'Sharp %',data:[62,18,20],backgroundColor:'rgba(0,212,255,0.7)',borderRadius:3},
        {label:'Public %',data:[38,35,27],backgroundColor:'rgba(192,132,252,0.7)',borderRadius:3}
      ]},
      options:{responsive:true,maintainAspectRatio:false,plugins:{legend:{labels:{color:textColor,font:{size:10}}}},scales:{x:{ticks:{color:textColor},grid:{color:gridColor}},y:{ticks:{color:textColor,callback:v=>v+'%'},grid:{color:gridColor},max:100}}}
    });
  },
  renderOpenClose() {
    const n=15;
    const textColor=document.body.classList.contains('dark-mode')?'#8892a4':'#4a5568';
    const gridColor='rgba(255,255,255,0.05)';
    const open=Array.from({length:n},()=>1.5+Math.random()*2);
    const close=open.map(v=>v*(0.9+Math.random()*0.2));
    if(this.charts.openclose) this.charts.openclose.destroy();
    this.charts.openclose=new Chart(document.getElementById('chartOpenClose'),{
      type:'scatter',
      data:{datasets:[{label:'Open vs Close',data:open.map((v,i)=>({x:v,y:close[i]})),backgroundColor:open.map((v,i)=>close[i]<v?'rgba(0,230,118,0.8)':'rgba(255,69,96,0.8)'),pointRadius:5}]},
      options:{responsive:true,maintainAspectRatio:false,plugins:{legend:{labels:{color:textColor,font:{size:10}}}},scales:{x:{title:{display:true,text:'Opening Odds',color:textColor},ticks:{color:textColor},grid:{color:gridColor}},y:{title:{display:true,text:'Closing Odds',color:textColor},ticks:{color:textColor},grid:{color:gridColor}}}}
    });
  }
};

const statsEngine = {
  charts:{},
  load() {
    this.renderLeagueTable();
    this.renderGoalsChart();
    this.renderHomeAwayChart();
    this.renderXGChart();
  },
  renderLeagueTable() {
    const teams=[{t:'Arsenal',p:30,w:19,d:5,l:6,gf:71,ga:28,gd:43,pts:62,xg:62.4,xga:25.1},{t:'Man City',p:30,w:20,d:4,l:6,gf:68,ga:34,gd:34,pts:64,xg:65.2,xga:27.8},{t:'Liverpool',p:30,w:18,d:6,l:6,gf:69,ga:38,gd:31,pts:60,xg:60.1,xga:31.2},{t:'Chelsea',p:30,w:12,d:9,l:9,gf:52,ga:43,gd:9,pts:45,xg:50.2,xga:42.1},{t:'Spurs',p:30,w:11,d:7,l:12,gf:49,ga:55,gd:-6,pts:40,xg:46.1,xga:51.3}];
    document.getElementById('statsLeagueTable').innerHTML=makeTable(['t','p','w','d','l','gf','ga','gd','pts','xg','xga'],teams);
  },
  renderGoalsChart() {
    const textColor=document.body.classList.contains('dark-mode')?'#8892a4':'#4a5568';
    const gridColor='rgba(255,255,255,0.05)';
    const labels=['0','1','2','3','4','5','6+'];
    const data=[8,22,28,21,13,6,2];
    if(this.charts.goals) this.charts.goals.destroy();
    this.charts.goals=new Chart(document.getElementById('chartStatsGoals'),{type:'bar',data:{labels,datasets:[{label:'Matches',data,backgroundColor:'rgba(0,212,255,0.7)',borderRadius:3}]},options:{responsive:true,maintainAspectRatio:false,plugins:{legend:{display:false}},scales:{x:{ticks:{color:textColor},grid:{color:gridColor}},y:{ticks:{color:textColor},grid:{color:gridColor}}}}});
  },
  renderHomeAwayChart() {
    const textColor=document.body.classList.contains('dark-mode')?'#8892a4':'#4a5568';
    const gridColor='rgba(255,255,255,0.05)';
    if(this.charts.homeaway) this.charts.homeaway.destroy();
    this.charts.homeaway=new Chart(document.getElementById('chartStatsHomeAway'),{type:'doughnut',data:{labels:['Home Win','Draw','Away Win'],datasets:[{data:[46,27,27],backgroundColor:['rgba(0,212,255,0.8)','rgba(255,215,64,0.8)','rgba(251,146,60,0.8)']}]},options:{responsive:true,maintainAspectRatio:false,plugins:{legend:{labels:{color:textColor,font:{size:10}}}}}});
  },
  renderXGChart() {
    const textColor=document.body.classList.contains('dark-mode')?'#8892a4':'#4a5568';
    const gridColor='rgba(255,255,255,0.05)';
    const teams=['Arsenal','Man City','Liverpool','Chelsea','Spurs'];
    const xg=[62.4,65.2,60.1,50.2,46.1];
    const actual=[71,68,69,52,49];
    if(this.charts.xg) this.charts.xg.destroy();
    this.charts.xg=new Chart(document.getElementById('chartStatsXG'),{type:'bar',data:{labels:teams,datasets:[{label:'xG',data:xg,backgroundColor:'rgba(0,212,255,0.6)',borderRadius:3},{label:'Actual Goals',data:actual,backgroundColor:'rgba(0,230,118,0.6)',borderRadius:3}]},options:{responsive:true,maintainAspectRatio:false,plugins:{legend:{labels:{color:textColor,font:{size:10}}}},scales:{x:{ticks:{color:textColor},grid:{color:gridColor}},y:{ticks:{color:textColor},grid:{color:gridColor}}}}});
  }
};

const oddsHistory = {
  async load() {
    const search=document.getElementById('ohSearch')?.value||'';
    const container=document.getElementById('oddsHistoryTable');
    const rows=Array.from({length:20},(_,i)=>({
      date:`2024-03-${(i+1).toString().padStart(2,'0')}`,
      match:'Arsenal vs '+['Chelsea','City','Liverpool','Spurs','Everton'][i%5],
      market:['1X2','O/U 2.5','BTTS','AH'][i%4],
      open_home:(1.5+Math.random()).toFixed(2),close_home:(1.5+Math.random()).toFixed(2),
      open_draw:(3.0+Math.random()*0.8).toFixed(2),close_draw:(3.0+Math.random()*0.8).toFixed(2),
      open_away:(2.5+Math.random()*1.5).toFixed(2),close_away:(2.5+Math.random()*1.5).toFixed(2),
      movement:((Math.random()-0.5)*10).toFixed(1)+'%',result:['H','D','A'][Math.floor(Math.random()*3)]
    }));
    container.innerHTML=makeTable(['date','match','market','open_home','close_home','open_draw','close_draw','open_away','close_away','movement','result'],rows);
  }
};