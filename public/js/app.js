// ── Глобальный fetch interceptor ──────────────────────────────────────────
// Добавляет credentials:include + X-Auth-Token для всех /api/ запросов
(function() {
  const _fetch = window.fetch;
  window.fetch = function(url, opts = {}) {
    if (typeof url === 'string' && url.startsWith('/api/')) {
      const token = localStorage.getItem('bq_token') || '';
      opts = {
        ...opts,
        credentials: 'include',
        headers: {
          ...(opts.headers || {}),
          ...(token ? { 'X-Auth-Token': token } : {}),
        },
      };
    }
    return _fetch(url, opts);
  };
})();

// BetQuant Pro - Core Application
const app = {
  currentSport: 'football',
  currentPanel: 'dashboard',
  settings: {},
  
  init() {
    this.settings = JSON.parse(localStorage.getItem('bq_settings') || '{}');
    const savedTheme = localStorage.getItem('bq_theme') || 'dark';
    if (savedTheme === 'light') document.body.classList.replace('dark-mode', 'light-mode');
    const token = localStorage.getItem('bq_token');
    if (token) { this.enterApp(localStorage.getItem('bq_username') || 'User'); }
  },
  
  async login() {
    const u = document.getElementById('loginUsername').value.trim();
    const p = document.getElementById('loginPassword').value;
    const status = document.getElementById('loginStatus');
    if (!u || !p) { status.textContent = 'Введи логин и пароль'; return; }
    status.textContent = 'Вход...';
    try {
      const r = await fetch('/api/auth/login', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        credentials: 'include',   // ← отправляем session cookie
        body: JSON.stringify({ username: u, password: p }),
      });
      const d = await r.json();
      if (r.ok) {
        localStorage.setItem('bq_token', 'session');
        localStorage.setItem('bq_username', u);
        this.enterApp(u);
      } else {
        status.textContent = d.error || 'Неверный логин или пароль';
        status.style.color = 'var(--red, #ff4560)';
      }
    } catch(e) {
      status.textContent = 'Сервер недоступен — ' + e.message;
      status.style.color = 'var(--red, #ff4560)';
    }
  },
  
  loginAsGuest() {
    localStorage.setItem('bq_token', 'demo');
    localStorage.setItem('bq_username', 'Demo User');
    this.enterApp('Demo User');
  },
  
  enterApp(username) {
    document.getElementById('loginScreen').style.display = 'none';
    document.getElementById('appContainer').classList.remove('hidden');
    document.getElementById('navUsername').textContent = username;
    this.showPanel('dashboard');
    dashboard.refresh();
    scraper.initSourcesGrid();
    library.load();
    journal.refresh();
    alerts.load();
    aiStrategy.init();
    backtestEngine.init();
    document.addEventListener('keydown', e => { if (e.key === 'Escape') this.closeModals(); });
  },
  
  logout() {
    localStorage.removeItem('bq_token');
    localStorage.removeItem('bq_username');
    document.getElementById('loginScreen').style.display = 'flex';
    document.getElementById('appContainer').classList.add('hidden');
  },
  
  showPanel(name) {
    document.querySelectorAll('.panel').forEach(p => p.classList.remove('active'));
    document.querySelectorAll('.sidebar-btn').forEach(b => b.classList.remove('active'));
    const panel = document.getElementById('panel-' + name);
    if (panel) panel.classList.add('active');
    const btn = document.querySelector('.sidebar-btn[data-panel="' + name + '"]');
    if (btn) btn.classList.add('active');
    this.currentPanel = name;
    if (name === 'database') db.refresh();
    if (name === 'stats') statsEngine.load();
    if (name === 'library') library.load();
    if (name === 'value') valueFinder.init();
  },
  
  setSport(sport) {
    this.currentSport = sport;
    document.querySelectorAll('.sport-btn').forEach(b => b.classList.toggle('active', b.dataset.sport === sport));
  },
  
  toggleTheme() {
    if (document.body.classList.contains('dark-mode')) {
      document.body.classList.replace('dark-mode', 'light-mode');
      localStorage.setItem('bq_theme', 'light');
    } else {
      document.body.classList.replace('light-mode', 'dark-mode');
      localStorage.setItem('bq_theme', 'dark');
    }
  },
  
  openSettings() {
    document.getElementById('settingsModal').style.display = 'flex';
    this.loadSettingsUI();
  },

  loadSettingsUI() {
    const s = this.settings;
    // Restore provider selection
    const providerSel = document.getElementById('settingsProvider');
    if (providerSel) {
      providerSel.value = s.last_provider || 'anthropic';
      this.onSettingsProviderChange();
    }
    // DB / notifications
    ['settingsCHHost','settingsPGHost','settingsTelegramToken','settingsTelegramChatId'].forEach(id => {
      const key = id.replace('settings', '').replace(/^./, c => c.toLowerCase());
      const el = document.getElementById(id);
      if (el) el.value = s[key] || '';
    });
  },

  // Called when provider changes inside Settings modal
  onSettingsProviderChange() {
    const provider = document.getElementById('settingsProvider')?.value || 'anthropic';
    const cfg = aiStrategy.PROVIDERS[provider];
    if (!cfg) return;
    const s = this.settings;

    // API key row
    const keyRow   = document.getElementById('settingsApiKeyRow');
    const keyLabel = document.getElementById('settingsApiKeyLabel');
    const keyInput = document.getElementById('settingsApiKey');
    if (keyRow)   keyRow.style.display = cfg.noKey ? 'none' : '';
    if (keyLabel) keyLabel.textContent = provider === 'anthropic' ? 'API Key' : cfg.label + ' Key';
    if (keyInput) {
      keyInput.placeholder = cfg.keyPlaceholder || '';
      keyInput.value = s[`apiKey_${provider}`] || '';
    }

    // Base URL row (local / custom)
    const urlRow   = document.getElementById('settingsBaseUrlRow');
    const urlInput = document.getElementById('settingsBaseUrl');
    if (urlRow) urlRow.style.display = (cfg.noKey || cfg.customUrl) ? '' : 'none';
    if (urlInput) {
      const defaults = { ollama: 'http://localhost:11434', lmstudio: 'http://localhost:1234', custom: '' };
      urlInput.placeholder = defaults[provider] || 'http://...';
      urlInput.value = s.baseUrl || defaults[provider] || '';
    }

    // Model select
    const modelSel = document.getElementById('settingsModel');
    if (modelSel) {
      modelSel.innerHTML = cfg.models.map(m => `<option value="${m.id}">${m.label}</option>`).join('');
      const saved = s[`model_${provider}`];
      if (saved) modelSel.value = saved;
    }

    // Custom model field
    const customRow = document.getElementById('settingsCustomModelRow');
    const customInput = document.getElementById('settingsCustomModel');
    if (customRow)  customRow.style.display = provider === 'custom' ? '' : 'none';
    if (customInput) customInput.value = s.customModel || '';

    // Hint
    const hint = document.getElementById('settingsProviderHint');
    if (hint) hint.textContent = cfg.keyHint || '';
  },

  saveSettings() {
    const provider = document.getElementById('settingsProvider')?.value || 'anthropic';
    const s = JSON.parse(localStorage.getItem('bq_settings') || '{}');

    // Save key for this specific provider
    const keyVal = document.getElementById('settingsApiKey')?.value || '';
    if (keyVal) s[`apiKey_${provider}`] = keyVal;

    s.last_provider = provider;
    s.baseUrl       = document.getElementById('settingsBaseUrl')?.value  || '';
    s.customModel   = document.getElementById('settingsCustomModel')?.value || '';
    s[`model_${provider}`] = document.getElementById('settingsModel')?.value || '';

    // DB / notifications
    s.chHost         = document.getElementById('settingsCHHost')?.value  || '';
    s.pgHost         = document.getElementById('settingsPGHost')?.value  || '';
    s.telegramToken  = document.getElementById('settingsTelegramToken')?.value || '';
    s.telegramChatId = document.getElementById('settingsTelegramChatId')?.value || '';

    this.settings = s;
    localStorage.setItem('bq_settings', JSON.stringify(s));

    // Sync AI panel provider selector
    const panelProvider = document.getElementById('aiProvider');
    if (panelProvider) { panelProvider.value = provider; aiStrategy.onProviderChange(); }

    document.getElementById('settingsModal').style.display = 'none';
  },
  
  openDataSources() {
    document.getElementById('dataSourcesModal').style.display = 'flex';
    document.getElementById('dataSourcesContent').innerHTML = this.getDataSourcesHTML();
  },
  closeModals() { document.querySelectorAll('.modal').forEach(m => m.style.display = 'none'); },
  
  getDataSourcesHTML() {
    return `<div class="data-sources-guide">
<div class="ds-section"><h3>⚽ Football / Soccer</h3>
<table class="ds-table"><tr><th>Source</th><th>Data</th><th>Coverage</th><th>Cost</th></tr>
<tr><td><strong>football-data.co.uk</strong></td><td>Results + odds (B365, Pinnacle, etc), basic stats</td><td>30+ leagues, 1993–now</td><td>Free CSV</td></tr>
<tr><td><strong>OpenFootball (GitHub)</strong></td><td>Fixtures, results, standings</td><td>50+ leagues, 2012–now</td><td>Open Source</td></tr>
<tr><td><strong>FBref / StatsBomb</strong></td><td>xG, xA, shots, progressive passes, pressures</td><td>Top 6 leagues, 2017–now</td><td>Free/Paid API</td></tr>
<tr><td><strong>Understat</strong></td><td>xG per shot, match shot maps</td><td>Top 6 leagues, 2014–now</td><td>Free scrape</td></tr>
<tr><td><strong>Betfair Exchange API</strong></td><td>Exchange prices, BSP, traded volumes</td><td>Global, live + historical</td><td>Free API</td></tr>
<tr><td><strong>OddsPortal</strong></td><td>Historical odds from 50+ bookmakers</td><td>Global, 2005–now</td><td>Free scrape</td></tr>
<tr><td><strong>Pinnacle API</strong></td><td>Sharp odds, live lines, line movement</td><td>Global, realtime</td><td>Paid API</td></tr>
<tr><td><strong>API-Football</strong></td><td>Fixtures, stats, odds, predictions</td><td>1000+ leagues</td><td>Freemium</td></tr>
<tr><td><strong>Transfermarkt</strong></td><td>Market values, injuries, squads</td><td>Global, 2000–now</td><td>Free scrape</td></tr>
</table></div>
<div class="ds-section"><h3>🎾 Tennis</h3>
<table class="ds-table"><tr><th>Source</th><th>Data</th><th>Coverage</th><th>Cost</th></tr>
<tr><td><strong>Jeff Sackmann / tennis_atp (GitHub)</strong></td><td>Match results, rankings, serve/return stats</td><td>ATP 1968–now, WTA 1920–now</td><td>Open Source</td></tr>
<tr><td><strong>tennis-data.co.uk</strong></td><td>Odds + match results</td><td>ATP+WTA, 2000–now</td><td>Free CSV</td></tr>
</table></div>
<div class="ds-section"><h3>🏀 Basketball (NBA/EuroLeague)</h3>
<table class="ds-table"><tr><th>Source</th><th>Data</th><th>Coverage</th><th>Cost</th></tr>
<tr><td><strong>NBA Stats API (unofficial)</strong></td><td>Box scores, play-by-play, advanced stats</td><td>NBA 1946–now</td><td>Free</td></tr>
<tr><td><strong>Basketball-Reference</strong></td><td>Full historical records</td><td>1946–now</td><td>Free scrape</td></tr>
<tr><td><strong>The Odds API</strong></td><td>Odds from 40+ books</td><td>Multi-sport, realtime</td><td>Freemium</td></tr>
</table></div>
<div class="ds-section"><h3>🏒 Hockey (NHL)</h3>
<table class="ds-table"><tr><th>Source</th><th>Data</th><th>Coverage</th><th>Cost</th></tr>
<tr><td><strong>NHL Official API</strong></td><td>Play-by-play, game events, stats</td><td>NHL 1917–now</td><td>Free</td></tr>
<tr><td><strong>Natural Stat Trick</strong></td><td>Corsi, Fenwick, xG, zone entries</td><td>2007–now</td><td>Free scrape</td></tr>
</table></div>
<div class="ds-section"><h3>🤖 Collection Architecture</h3>
<div class="ds-note">
<strong>Recommended Stack:</strong><br>
• <strong>Python + Playwright/Scrapy</strong> → scrape Transfermarkt, FBref, SofaScore, OddsPortal<br>
• <strong>aiohttp + asyncio</strong> → NBA/NHL official APIs, API-Football<br>
• <strong>ClickHouse</strong> → store odds time-series (ultra-fast for backtests over millions of rows)<br>
• <strong>PostgreSQL</strong> → relational: teams, leagues, players, strategies, users<br>
• <strong>Apache Airflow</strong> → schedule daily/hourly pipelines<br>
• <strong>Redis</strong> → cache live odds, deduplication<br><br>
All ready-to-use collectors in <code>/server/data-collectors/</code> directory.
</div></div>
</div>`;
  }
};

function formatNum(n, dec=1) { if (n===null||n===undefined||isNaN(n)) return '—'; return Number(n).toFixed(dec); }
function formatPct(n) { if (!n&&n!==0) return '—'; return (n>=0?'+':'') + Number(n).toFixed(1) + '%'; }
function formatOdds(n) { if (!n) return '—'; return Number(n).toFixed(2); }
function colorize(el, val) { el.classList.remove('positive','negative'); if (val>0) el.classList.add('positive'); else if (val<0) el.classList.add('negative'); }

function makeTable(headers, rows, opts={}) {
  if (!rows.length) return '<div class="empty-state"><div class="empty-state-icon">📭</div>No data</div>';
  let html = '<table class="data-table"><thead><tr>';
  headers.forEach(h => html += '<th>' + (typeof h==='object'?h.label:h) + '</th>');
  html += '</tr></thead><tbody>';
  rows.forEach(row => {
    html += '<tr>';
    headers.forEach(h => {
      const key = typeof h==='object' ? h.key : h.toLowerCase().replace(/ /g,'_');
      const val = row[key];
      const cls = typeof h==='object' && h.color ? ' class="' + (val>0?'positive':'negative') + '"' : '';
      html += '<td' + cls + '>' + (val !== undefined && val !== null ? val : '—') + '</td>';
    });
    html += '</tr>';
  });
  html += '</tbody></table>';
  return html;
}

async function apiCall(endpoint, method='GET', body=null) {
  const opts = { method, headers: {'Content-Type':'application/json'} };
  if (body) opts.body = JSON.stringify(body);
  try {
    const r = await fetch(endpoint, opts);
    if (r.status === 401) { app.logout(); return null; }
    return await r.json();
  } catch(e) { return null; }
}

document.addEventListener('DOMContentLoaded', () => app.init());