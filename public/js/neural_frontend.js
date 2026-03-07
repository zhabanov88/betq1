'use strict';
// ═══════════════════════════════════════════════════════════════════════════
//  BetQuant Neural Networks Panel
//  Автообучаемые нейросети • Веса с объяснениями • Стратегии
// ═══════════════════════════════════════════════════════════════════════════

const neuralPanel = {
  activeSport:    'football',
  statusData:     {},
  weightsData:    {},
  strategiesData: {},
  chartInstances: {},

  // ── Init ────────────────────────────────────────────────────────────────
  async init() {
    await this.loadStatus();
    this.renderSportTabs();
    this.renderStatus();
    this.bindEvents();
  },

  bindEvents() {
    document.addEventListener('click', e => {
      if (e.target.closest('[data-neural-sport]')) {
        const sport = e.target.closest('[data-neural-sport]').dataset.neuralSport;
        this.setActiveSport(sport);
      }
    });
  },

  async setActiveSport(sport) {
    this.activeSport = sport;
    document.querySelectorAll('[data-neural-sport]').forEach(el => {
      el.classList.toggle('nn-tab-active', el.dataset.neuralSport === sport);
    });
    await this.loadWeights(sport);
    await this.loadStrategies(sport);
    this.renderWeights(sport);
    this.renderStrategies(sport);
  },

  // ── API ─────────────────────────────────────────────────────────────────
  async api(path, opts = {}) {
    const r = await fetch('/api/neural' + path, {
      headers: { 'Content-Type': 'application/json' },
      ...opts,
    });
    return r.json();
  },

  async loadStatus() {
    try {
      const d = await this.api('/status');
      this.statusData = d.status || {};
    } catch (e) { this.statusData = {}; }
  },

  async loadWeights(sport) {
    try {
      this.weightsData[sport] = await this.api(`/weights/${sport}`);
    } catch (e) { this.weightsData[sport] = null; }
  },

  async loadStrategies(sport) {
    try {
      this.strategiesData[sport] = await this.api(`/strategy/${sport}`);
    } catch (e) { this.strategiesData[sport] = null; }
  },

  // ── Train ────────────────────────────────────────────────────────────────
  async trainSport(sport) {
    const btn = document.querySelector(`[data-train-sport="${sport}"]`);
    if (btn) { btn.disabled = true; btn.textContent = '⏳ Обучение...'; }
    this.showToast(`🧠 Обучение ${sport}...`, 'info');

    try {
      const d = await this.api(`/train/${sport}`, { method: 'POST' });
      if (d.ok) {
        this.showToast(`✅ ${sport} готово — точность ${d.accuracy}%`, 'success');
        await this.loadStatus();
        this.renderStatus();
        if (sport === this.activeSport || sport === 'all') {
          await this.setActiveSport(this.activeSport);
        }
      }
    } catch (e) {
      this.showToast('❌ Ошибка обучения', 'error');
    } finally {
      if (btn) { btn.disabled = false; btn.textContent = '▶ Обучить'; }
    }
  },

  // ── Render status cards ──────────────────────────────────────────────────
  renderStatus() {
    const container = document.getElementById('nn-status-grid');
    if (!container) return;

    const sports = Object.entries(this.statusData);
    if (!sports.length) {
      container.innerHTML = '<div class="nn-empty">Статус недоступен — запустите обучение</div>';
      return;
    }

    container.innerHTML = sports.map(([sport, info]) => `
      <div class="nn-status-card ${info.trained ? 'trained' : 'untrained'}" onclick="neuralPanel.setActiveSport('${sport}')">
        <div class="nn-status-icon">${this.sportIcon(sport)}</div>
        <div class="nn-status-info">
          <div class="nn-status-name">${info.label}</div>
          <div class="nn-status-meta">
            ${info.trained
              ? `<span class="nn-badge green">✓ Обучена</span>
                 <span class="nn-badge blue">${info.accuracy}% точность</span>
                 <span class="nn-badge grey">${info.rowsUsed} матчей</span>`
              : `<span class="nn-badge red">Не обучена</span>`
            }
          </div>
          ${info.trained ? `<div class="nn-loss-mini" id="loss-mini-${sport}"></div>` : ''}
        </div>
        <div class="nn-status-actions">
          <button class="nn-btn sm" data-train-sport="${sport}" onclick="event.stopPropagation();neuralPanel.trainSport('${sport}')">
            ${info.trained ? '🔄 Переобучить' : '▶ Обучить'}
          </button>
        </div>
      </div>
    `).join('');

    // Mini loss charts
    sports.forEach(([sport, info]) => {
      if (info.trained && info.lossHistory?.length) {
        setTimeout(() => this.drawMiniLoss(`loss-mini-${sport}`, info.lossHistory), 50);
      }
    });
  },

  // ── Render sport tabs ────────────────────────────────────────────────────
  renderSportTabs() {
    const container = document.getElementById('nn-sport-tabs');
    if (!container) return;
    const sports = ['football','hockey','tennis','basketball'];
    container.innerHTML = sports.map(s => `
      <button class="nn-tab ${s === this.activeSport ? 'nn-tab-active' : ''}" data-neural-sport="${s}">
        ${this.sportIcon(s)} ${this.sportName(s)}
      </button>
    `).join('');
  },

  // ── Render weights ───────────────────────────────────────────────────────
  renderWeights(sport) {
    const container = document.getElementById('nn-weights-panel');
    if (!container) return;

    const data = this.weightsData[sport];
    if (!data || data.error) {
      container.innerHTML = `
        <div class="nn-not-trained">
          <div class="nn-not-trained-icon">🧠</div>
          <div>Модель для <b>${this.sportName(sport)}</b> ещё не обучена.</div>
          <button class="nn-btn primary" onclick="neuralPanel.trainSport('${sport}')">▶ Обучить сейчас</button>
        </div>`;
      return;
    }

    const maxW = Math.max(...data.inputImportance.map(f => f.weight));

    container.innerHTML = `
      <div class="nn-weights-header">
        <div class="nn-weights-title">
          ${data.label} — Веса нейросети
          <span class="nn-badge green">${data.accuracy}% точность</span>
          <span class="nn-badge grey">${data.rowsUsed} матчей</span>
        </div>
        <div class="nn-arch">Архитектура: [${data.architecture.join(' → ')}]</div>
      </div>

      <div class="nn-weights-layout">
        <!-- Feature importance -->
        <div class="nn-section">
          <div class="nn-section-title">📊 Важность признаков (входные веса)</div>
          <div class="nn-section-sub">Чем больше вес — тем сильнее влияние на предсказание</div>
          <div class="nn-feature-list">
            ${data.inputImportance.map((f, i) => `
              <div class="nn-feature-row">
                <div class="nn-feature-rank">${i + 1}</div>
                <div class="nn-feature-label">
                  <span class="nn-feature-name">${f.feature}</span>
                  <span class="nn-feature-group nn-group-${f.group}">${f.group}</span>
                </div>
                <div class="nn-feature-bar-wrap">
                  <div class="nn-feature-bar nn-group-bar-${f.group}" style="width:${(f.weight/maxW*100).toFixed(1)}%"></div>
                </div>
                <div class="nn-feature-val">${f.weight.toFixed(4)}</div>
              </div>
            `).join('')}
          </div>
        </div>

        <!-- Group importance -->
        <div class="nn-section">
          <div class="nn-section-title">🏷️ Важность по группам</div>
          <div class="nn-groups">
            ${data.groups.map(g => `
              <div class="nn-group-item">
                <span class="nn-group-name nn-group-${g.group}">${g.group}</span>
                <div class="nn-group-bar-wrap">
                  <div class="nn-group-bar-fill nn-group-bar-${g.group}" 
                       style="width:${(g.total / data.groups[0].total * 100).toFixed(1)}%"></div>
                </div>
                <span class="nn-group-total">${g.total.toFixed(3)}</span>
              </div>
            `).join('')}
          </div>
        </div>

        <!-- Target explanations -->
        <div class="nn-section nn-full-width">
          <div class="nn-section-title">🎯 Объяснение предсказаний по целевым переменным</div>
          <div class="nn-targets-grid">
            ${data.targetExplanations.map(t => `
              <div class="nn-target-card">
                <div class="nn-target-label">${t.label}</div>
                <div class="nn-target-features">
                  Топ-признаки:
                  ${t.topFeatures.slice(0, 3).map(f =>
                    `<span class="nn-feat-pill">${f.name}</span>`
                  ).join('')}
                </div>
                <div class="nn-target-explanation">${t.explanation}</div>
              </div>
            `).join('')}
          </div>
        </div>
      </div>

      <!-- Network graph -->
      <div class="nn-section nn-full-width">
        <div class="nn-section-title">🔗 Визуализация архитектуры сети</div>
        <canvas id="nn-arch-canvas" width="700" height="220"></canvas>
      </div>
    `;

    setTimeout(() => this.drawArchitecture('nn-arch-canvas', data.architecture), 50);
  },

  // ── Render strategies ────────────────────────────────────────────────────
  renderStrategies(sport) {
    const container = document.getElementById('nn-strategies-panel');
    if (!container) return;

    const data = this.strategiesData[sport];
    if (!data || data.error || !data.strategies?.length) {
      container.innerHTML = `<div class="nn-empty">Обучите модель для генерации стратегий</div>`;
      return;
    }

    container.innerHTML = `
      <div class="nn-strat-header">
        <div class="nn-strat-title">🚀 Нейросетевые стратегии — ${data.label}</div>
        <button class="nn-btn sm" onclick="neuralPanel.exportStrategies('${sport}')">📥 Экспорт</button>
      </div>
      <div class="nn-strat-grid">
        ${data.strategies.map(s => `
          <div class="nn-strat-card nn-type-${s.type}">
            <div class="nn-strat-top">
              <span class="nn-strat-name">${s.name}</span>
              <span class="nn-strat-roi">ROI +${s.expectedROI}%</span>
            </div>
            <div class="nn-strat-condition">📌 ${s.condition}</div>
            ${s.keyFeatures ? `
              <div class="nn-strat-features">
                Ключевые признаки: ${s.keyFeatures.map(f=>`<span class="nn-feat-pill sm">${f}</span>`).join('')}
              </div>` : ''}
            <div class="nn-strat-explanation">${s.explanation}</div>
            <div class="nn-strat-nonstd">
              <span class="nn-nonstd-badge">💡 Нестандартно</span>
              ${s.nonStandard}
            </div>
            <div class="nn-strat-footer">
              <div class="nn-conf-wrap">
                <span>Уверенность</span>
                <div class="nn-conf-bar"><div class="nn-conf-fill" style="width:${s.confidence}%"></div></div>
                <span>${s.confidence}%</span>
              </div>
              <button class="nn-btn xs" onclick="neuralPanel.sendToBacktest('${s.id}', '${sport}')">→ Бэктест</button>
            </div>
          </div>
        `).join('')}
      </div>
    `;
  },

  // ── Canvas: mini loss chart ──────────────────────────────────────────────
  drawMiniLoss(containerId, history) {
    const el = document.getElementById(containerId);
    if (!el) return;
    el.innerHTML = `<canvas width="160" height="30"></canvas>`;
    const canvas = el.querySelector('canvas');
    const ctx    = canvas.getContext('2d');
    const min    = Math.min(...history);
    const max    = Math.max(...history);
    const range  = max - min || 1;
    const pts    = history.map((v, i) => ({
      x: (i / (history.length - 1)) * 160,
      y: 28 - ((v - min) / range) * 26,
    }));
    ctx.strokeStyle = '#4ade80';
    ctx.lineWidth   = 1.5;
    ctx.beginPath();
    pts.forEach((p, i) => i === 0 ? ctx.moveTo(p.x, p.y) : ctx.lineTo(p.x, p.y));
    ctx.stroke();
  },

  // ── Canvas: network architecture ────────────────────────────────────────
  drawArchitecture(canvasId, layers) {
    const canvas = document.getElementById(canvasId);
    if (!canvas) return;
    const ctx   = canvas.getContext('2d');
    const W     = canvas.width;
    const H     = canvas.height;
    const cols  = layers.length;
    const maxN  = Math.min(Math.max(...layers), 10);
    const colW  = W / cols;
    const nodeR = 8;

    ctx.clearRect(0, 0, W, H);

    const positions = layers.map((n, col) => {
      const visible = Math.min(n, 10);
      const spacing = H / (visible + 1);
      return Array.from({ length: visible }, (_, row) => ({
        x: col * colW + colW / 2,
        y: spacing * (row + 1),
      }));
    });

    // Draw connections (sample)
    ctx.globalAlpha = 0.12;
    ctx.strokeStyle = '#818cf8';
    ctx.lineWidth   = 0.8;
    for (let l = 0; l < positions.length - 1; l++) {
      const from = positions[l].slice(0, 5);
      const to   = positions[l + 1].slice(0, 5);
      from.forEach(f => to.forEach(t => {
        ctx.beginPath(); ctx.moveTo(f.x, f.y); ctx.lineTo(t.x, t.y); ctx.stroke();
      }));
    }
    ctx.globalAlpha = 1;

    // Draw nodes
    const colors = ['#4ade80', '#818cf8', '#818cf8', '#f59e0b'];
    positions.forEach((col, ci) => {
      col.forEach(({ x, y }) => {
        ctx.beginPath();
        ctx.arc(x, y, nodeR, 0, Math.PI * 2);
        ctx.fillStyle = colors[ci] || '#818cf8';
        ctx.fill();
      });
      // Label
      ctx.fillStyle = '#94a3b8';
      ctx.font      = '11px monospace';
      ctx.textAlign = 'center';
      const label = ci === 0 ? `Вход\n${layers[ci]}`
                  : ci === positions.length - 1 ? `Выход\n${layers[ci]}`
                  : `Скрытый\n${layers[ci]}`;
      label.split('\n').forEach((line, li) => ctx.fillText(line, col[0]?.x || 0, H - 20 + li * 14));
    });
  },

  // ── Export strategies ────────────────────────────────────────────────────
  exportStrategies(sport) {
    const data = this.strategiesData[sport];
    if (!data) return;
    const blob = new Blob([JSON.stringify(data.strategies, null, 2)], { type: 'application/json' });
    const url  = URL.createObjectURL(blob);
    const a    = Object.assign(document.createElement('a'), { href: url, download: `nn_strategies_${sport}.json` });
    a.click(); URL.revokeObjectURL(url);
  },

  sendToBacktest(stratId, sport) {
    this.showToast(`✅ Стратегия ${stratId} отправлена в бэктест`, 'success');
    // Hook into existing backtest engine if available
    if (typeof backtestEngine !== 'undefined' && backtestEngine.addStrategyFromNeural) {
      backtestEngine.addStrategyFromNeural(stratId, sport);
    }
  },

  // ── Helpers ──────────────────────────────────────────────────────────────
  sportIcon(s) {
    return { football:'⚽', hockey:'🏒', tennis:'🎾', basketball:'🏀' }[s] || '🎯';
  },
  sportName(s) {
    return { football:'Футбол', hockey:'Хоккей', tennis:'Теннис', basketball:'Баскетбол' }[s] || s;
  },

  showToast(msg, type = 'info') {
    const colors = { info: '#818cf8', success: '#4ade80', error: '#f87171', warning: '#f59e0b' };
    const t = Object.assign(document.createElement('div'), {
      textContent: msg,
      style: `position:fixed;top:20px;right:20px;z-index:9999;padding:10px 18px;
              border-radius:8px;font-size:13px;font-weight:600;
              background:${colors[type]};color:#0f172a;
              box-shadow:0 4px 20px rgba(0,0,0,0.4);`,
    });
    document.body.appendChild(t);
    setTimeout(() => t.remove(), 3000);
  },
};

// Auto-init when panel is shown
if (document.readyState === 'loading') {
  document.addEventListener('DOMContentLoaded', () => {
    if (document.getElementById('nn-status-grid')) neuralPanel.init();
  });
} else {
  if (document.getElementById('nn-status-grid')) neuralPanel.init();
}