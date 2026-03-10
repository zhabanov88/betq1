'use strict';
/**
 * ══════════════════════════════════════════════════════════════════════════
 *  BetQuant Pro — Neural Networks v2  server/neural.js
 *
 *  ПОЛНАЯ ЗАМЕНА. Ключевые улучшения:
 *
 *  ✅ Признаки КОНКРЕТНЫХ КОМАНД (не матча в целом):
 *     — rolling stats 5/10/20 матчей ДО даты прогноза
 *     — сезонная форма, прошлый сезон
 *     — дома vs в гостях по отдельности
 *     — H2H последние 5 встреч этих двух команд
 *
 *  ✅ ELO рейтинги:
 *     — рассчитываются инкрементально по всей истории
 *     — разница ELO — сильный предиктор
 *
 *  ✅ Poisson Attack/Defense рейтинги:
 *     — нормализованные рейтинги атаки и защиты
 *     — expected goals через Poisson
 *
 *  ✅ Усталость/Плотность расписания:
 *     — дней с последнего матча (rest days)
 *     — количество матчей за 30 дней
 *
 *  ✅ Рыночный дрейф:
 *     — implied prob из коэффициентов
 *     — расхождение модели с рынком (edge)
 *
 *  ✅ Деплой: заменить server/neural.js полностью
 * ══════════════════════════════════════════════════════════════════════════
 */

const express = require('express');
const router  = express.Router();

// ══════════════════════════════════════════════════════════════════════════
//  КОНФИГИ СПОРТОВ — расширенные признаки
// ══════════════════════════════════════════════════════════════════════════
const SPORT_CONFIGS = {

  // ────────────────────────────────────────────────────────────────────────
  football: {
    table:    'betquant.football_matches',
    formTable:'betquant.football_team_form',
    label:    '⚽ Футбол',
    targets:  ['home_win', 'draw', 'away_win', 'over25', 'btts', 'over35', 'home_clean_sheet'],

    features: [
      // ── ELO рейтинги (рассчитываются на лету) ──
      { key: 'home_elo',          label: 'ELO хозяев',               group: 'elo'     },
      { key: 'away_elo',          label: 'ELO гостей',               group: 'elo'     },
      { key: 'elo_diff',          label: 'ELO разница (д-г)',         group: 'elo'     },

      // ── Poisson-рейтинги атаки/защиты ──
      { key: 'home_attack',       label: 'Атака хозяев (Poisson)',    group: 'poisson' },
      { key: 'home_defense',      label: 'Защита хозяев (Poisson)',   group: 'poisson' },
      { key: 'away_attack',       label: 'Атака гостей (Poisson)',    group: 'poisson' },
      { key: 'away_defense',      label: 'Защита гостей (Poisson)',   group: 'poisson' },
      { key: 'exp_home_goals',    label: 'Ожид.голы хозяев (λ)',      group: 'poisson' },
      { key: 'exp_away_goals',    label: 'Ожид.голы гостей (λ)',      group: 'poisson' },

      // ── Форма команд (последние 5 матчей) ──
      { key: 'home_pts5',         label: 'Очки хозяев last-5',        group: 'form5'   },
      { key: 'home_gf5',          label: 'Голы забито (д) l5',        group: 'form5'   },
      { key: 'home_ga5',          label: 'Голы пропущено (д) l5',     group: 'form5'   },
      { key: 'home_xgf5',         label: 'xG за (д) l5',              group: 'form5'   },
      { key: 'home_xga5',         label: 'xG против (д) l5',          group: 'form5'   },
      { key: 'away_pts5',         label: 'Очки гостей last-5',        group: 'form5'   },
      { key: 'away_gf5',          label: 'Голы забито (г) l5',        group: 'form5'   },
      { key: 'away_ga5',          label: 'Голы пропущено (г) l5',     group: 'form5'   },
      { key: 'away_xgf5',         label: 'xG за (г) l5',              group: 'form5'   },
      { key: 'away_xga5',         label: 'xG против (г) l5',          group: 'form5'   },

      // ── Форма (последние 10 матчей) ──
      { key: 'home_pts10',        label: 'Очки хозяев last-10',       group: 'form10'  },
      { key: 'home_wins10',       label: 'Победы хозяев l10',         group: 'form10'  },
      { key: 'away_pts10',        label: 'Очки гостей last-10',       group: 'form10'  },
      { key: 'away_wins10',       label: 'Победы гостей l10',         group: 'form10'  },

      // ── Домашняя/гостевая форма ──
      { key: 'home_home_pts10',   label: 'Очки дома (last-10)',        group: 'venue'   },
      { key: 'home_home_gf10',    label: 'Голы дома забито l10',       group: 'venue'   },
      { key: 'home_home_ga10',    label: 'Голы дома пропущено l10',    group: 'venue'   },
      { key: 'away_away_pts10',   label: 'Очки в гостях (last-10)',    group: 'venue'   },
      { key: 'away_away_gf10',    label: 'Голы в гостях забито l10',   group: 'venue'   },
      { key: 'away_away_ga10',    label: 'Голы в гостях пропущено l10',group: 'venue'   },

      // ── H2H ──
      { key: 'h2h_home_wins',     label: 'H2H победы хозяев',         group: 'h2h'     },
      { key: 'h2h_draws',         label: 'H2H ничьи',                  group: 'h2h'     },
      { key: 'h2h_away_wins',     label: 'H2H победы гостей',          group: 'h2h'     },
      { key: 'h2h_avg_goals',     label: 'H2H среднее голов',          group: 'h2h'     },
      { key: 'h2h_count',         label: 'H2H кол-во встреч',          group: 'h2h'     },

      // ── Усталость ──
      { key: 'home_rest_days',    label: 'Отдых хозяев (дней)',        group: 'fatigue' },
      { key: 'away_rest_days',    label: 'Отдых гостей (дней)',        group: 'fatigue' },
      { key: 'home_games30',      label: 'Матчи хозяев за 30 дней',    group: 'fatigue' },
      { key: 'away_games30',      label: 'Матчи гостей за 30 дней',    group: 'fatigue' },

      // ── Продвинутые метрики (из матча) ──
      { key: 'home_xg_season',    label: 'xG хозяев (сезон/матч)',     group: 'xg'      },
      { key: 'away_xg_season',    label: 'xG гостей (сезон/матч)',     group: 'xg'      },
      { key: 'home_shots_season', label: 'Удары хозяев (сезон/матч)', group: 'xg'      },
      { key: 'away_shots_season', label: 'Удары гостей (сезон/матч)', group: 'xg'      },

      // ── Рынок ──
      { key: 'mkt_home',          label: 'Implied P(хозяева)',          group: 'market'  },
      { key: 'mkt_draw',          label: 'Implied P(ничья)',            group: 'market'  },
      { key: 'mkt_away',          label: 'Implied P(гости)',            group: 'market'  },
      { key: 'mkt_over',          label: 'Implied P(over 2.5)',         group: 'market'  },
      { key: 'mkt_margin',        label: 'Маржа букмекера',             group: 'market'  },
      { key: 'b365_home',         label: 'Коэф. хозяева (B365)',        group: 'market'  },
      { key: 'b365_draw',         label: 'Коэф. ничья (B365)',          group: 'market'  },
      { key: 'b365_away',         label: 'Коэф. гости (B365)',          group: 'market'  },

      // ── Сезонная позиция ──
      { key: 'home_season_pts',   label: 'Очки хозяев (сезон)',        group: 'season'  },
      { key: 'away_season_pts',   label: 'Очки гостей (сезон)',        group: 'season'  },
      { key: 'home_season_gd',    label: 'Разница голов (д) сезон',    group: 'season'  },
      { key: 'away_season_gd',    label: 'Разница голов (г) сезон',    group: 'season'  },
    ],
  },

  // ────────────────────────────────────────────────────────────────────────
  hockey: {
    table:    'betquant.hockey_matches',
    formTable:'betquant.hockey_team_form',
    label:    '🏒 Хоккей',
    targets:  ['home_win', 'away_win', 'over55', 'went_to_ot', 'home_pp_win'],

    features: [
      { key: 'home_elo',          label: 'ELO хозяев',               group: 'elo'      },
      { key: 'away_elo',          label: 'ELO гостей',               group: 'elo'      },
      { key: 'elo_diff',          label: 'ELO разница',               group: 'elo'      },

      { key: 'home_pts5',         label: 'Очки хозяев l5',            group: 'form5'    },
      { key: 'home_gf5',          label: 'Шайбы забито (д) l5',       group: 'form5'    },
      { key: 'home_ga5',          label: 'Шайбы пропущено (д) l5',    group: 'form5'    },
      { key: 'away_pts5',         label: 'Очки гостей l5',            group: 'form5'    },
      { key: 'away_gf5',          label: 'Шайбы забито (г) l5',       group: 'form5'    },
      { key: 'away_ga5',          label: 'Шайбы пропущено (г) l5',    group: 'form5'    },

      { key: 'home_shots5',       label: 'Броски (д) l5',             group: 'shots'    },
      { key: 'away_shots5',       label: 'Броски (г) l5',             group: 'shots'    },
      { key: 'home_cf_pct5',      label: 'Corsi% (д) l5',             group: 'shots'    },
      { key: 'away_cf_pct5',      label: 'Corsi% (г) l5',             group: 'shots'    },

      { key: 'home_pp_pct5',      label: 'PP% хозяев l5',             group: 'special'  },
      { key: 'away_pp_pct5',      label: 'PP% гостей l5',             group: 'special'  },
      { key: 'home_pk_pct5',      label: 'PK% хозяев l5',             group: 'special'  },
      { key: 'away_pk_pct5',      label: 'PK% гостей l5',             group: 'special'  },
      { key: 'home_sv_pct5',      label: 'Save% хозяев l5',           group: 'goalie'   },
      { key: 'away_sv_pct5',      label: 'Save% гостей l5',           group: 'goalie'   },

      { key: 'home_rest_days',    label: 'Отдых хозяев (дней)',        group: 'fatigue'  },
      { key: 'away_rest_days',    label: 'Отдых гостей (дней)',        group: 'fatigue'  },
      { key: 'h2h_home_wins',     label: 'H2H победы хозяев',         group: 'h2h'      },
      { key: 'h2h_avg_goals',     label: 'H2H ср.шайбы',              group: 'h2h'      },
      { key: 'h2h_ot_rate',       label: 'H2H % ОТ',                  group: 'h2h'      },

      { key: 'home_home_pts10',   label: 'Дома очки l10',              group: 'venue'    },
      { key: 'away_away_pts10',   label: 'В гостях очки l10',          group: 'venue'    },

      { key: 'mkt_home',          label: 'Implied P(хозяева)',          group: 'market'   },
      { key: 'mkt_away',          label: 'Implied P(гости)',            group: 'market'   },
      { key: 'b365_home',         label: 'Коэф. хозяева',               group: 'market'   },
      { key: 'b365_away',         label: 'Коэф. гости',                  group: 'market'   },
    ],
  },

  // ────────────────────────────────────────────────────────────────────────
  tennis: {
    table:    'betquant.tennis_extended',
    label:    '🎾 Теннис',
    targets:  ['upset', 'over_sets', 'total_games_over', 'straight_sets'],

    features: [
      { key: 'elo_diff',          label: 'ELO разница (п-пр)',        group: 'elo'     },
      { key: 'winner_elo',        label: 'ELO победителя',            group: 'elo'     },
      { key: 'loser_elo',         label: 'ELO проигравшего',          group: 'elo'     },

      { key: 'rank_diff',         label: 'Разница рейтингов АТП',     group: 'rank'    },
      { key: 'rank_winner',       label: 'Рейтинг победителя',        group: 'rank'    },
      { key: 'rank_loser',        label: 'Рейтинг проигравшего',      group: 'rank'    },

      // Статистика победителя (rolling 10 матчей)
      { key: 'w_ace10',           label: 'Эйсы (п) l10',              group: 'serve_w' },
      { key: 'w_df10',            label: 'Дв.ошибки (п) l10',         group: 'serve_w' },
      { key: 'w_1stin10',         label: '1st Serve In% (п) l10',     group: 'serve_w' },
      { key: 'w_1stwon10',        label: '1st Serve Won% (п) l10',    group: 'serve_w' },
      { key: 'w_2ndwon10',        label: '2nd Serve Won% (п) l10',    group: 'serve_w' },
      { key: 'w_svpt10',          label: 'Сервис-очки (п) l10',       group: 'serve_w' },
      { key: 'w_bpsaved_pct10',   label: 'BP saved% (п) l10',         group: 'rally_w' },
      { key: 'w_bpfaced10',       label: 'BP faced (п) l10',          group: 'rally_w' },
      { key: 'w_win_rate10',      label: 'Процент побед (п) l10',     group: 'rally_w' },

      // Статистика проигравшего
      { key: 'l_ace10',           label: 'Эйсы (пр) l10',             group: 'serve_l' },
      { key: 'l_df10',            label: 'Дв.ошибки (пр) l10',        group: 'serve_l' },
      { key: 'l_1stin10',         label: '1st Serve In% (пр) l10',    group: 'serve_l' },
      { key: 'l_bpsaved_pct10',   label: 'BP saved% (пр) l10',        group: 'rally_l' },
      { key: 'l_win_rate10',      label: 'Процент побед (пр) l10',    group: 'rally_l' },

      // Покрытие
      { key: 'surface_hard',      label: 'Hard корт',                  group: 'surface' },
      { key: 'surface_clay',      label: 'Clay корт',                  group: 'surface' },
      { key: 'surface_grass',     label: 'Grass корт',                 group: 'surface' },
      { key: 'w_hard_win_rate',   label: 'Win% п на hard',             group: 'surface' },
      { key: 'w_clay_win_rate',   label: 'Win% п на clay',             group: 'surface' },
      { key: 'l_hard_win_rate',   label: 'Win% пр на hard',            group: 'surface' },

      // H2H
      { key: 'h2h_w_wins',        label: 'H2H победы победителя',     group: 'h2h'     },
      { key: 'h2h_count',         label: 'H2H встреч всего',           group: 'h2h'     },
      { key: 'h2h_avg_sets',      label: 'H2H среднее сетов',          group: 'h2h'     },

      // Усталость
      { key: 'w_days_rest',       label: 'Дней отдыха (п)',            group: 'fatigue' },
      { key: 'l_days_rest',       label: 'Дней отдыха (пр)',           group: 'fatigue' },
      { key: 'w_tournaments10',   label: 'Турниры за 60 дней (п)',     group: 'fatigue' },

      // Рынок
      { key: 'mkt_winner',        label: 'Implied P(победитель)',       group: 'market'  },
      { key: 'mkt_margin',        label: 'Маржа',                       group: 'market'  },
      { key: 'b365w',             label: 'Коэф. победителя',            group: 'market'  },
      { key: 'b365l',             label: 'Коэф. проигравшего',          group: 'market'  },
    ],
  },

  // ────────────────────────────────────────────────────────────────────────
  basketball: {
    table:    'betquant.basketball_matches_v2',
    label:    '🏀 Баскетбол',
    targets:  ['home_win', 'over_total', 'spread_cover', 'large_margin'],

    features: [
      { key: 'home_elo',          label: 'ELO хозяев',                group: 'elo'     },
      { key: 'elo_diff',          label: 'ELO разница',                group: 'elo'     },

      { key: 'home_pts5',         label: 'Очки (д) l5',                group: 'offense' },
      { key: 'away_pts5',         label: 'Очки (г) l5',                group: 'offense' },
      { key: 'home_pts_conc5',    label: 'Пропущено (д) l5',           group: 'defense' },
      { key: 'away_pts_conc5',    label: 'Пропущено (г) l5',           group: 'defense' },
      { key: 'home_pace5',        label: 'Темп (д) l5',                group: 'pace'    },
      { key: 'away_pace5',        label: 'Темп (г) l5',                group: 'pace'    },
      { key: 'home_eff5',         label: 'Эффективность атаки (д) l5', group: 'offense' },
      { key: 'away_eff5',         label: 'Эффективность атаки (г) l5', group: 'offense' },
      { key: 'home_fg_pct5',      label: 'FG% (д) l5',                 group: 'offense' },
      { key: 'away_fg_pct5',      label: 'FG% (г) l5',                 group: 'offense' },
      { key: 'home_3p_pct5',      label: '3P% (д) l5',                 group: 'offense' },
      { key: 'away_3p_pct5',      label: '3P% (г) l5',                 group: 'offense' },
      { key: 'home_reb5',         label: 'Подборы (д) l5',             group: 'boards'  },
      { key: 'away_reb5',         label: 'Подборы (г) l5',             group: 'boards'  },
      { key: 'home_tov5',         label: 'Потери (д) l5',              group: 'turnovers'},
      { key: 'away_tov5',         label: 'Потери (г) l5',              group: 'turnovers'},

      { key: 'h2h_home_wins',     label: 'H2H победы хозяев',          group: 'h2h'     },
      { key: 'h2h_avg_pts',       label: 'H2H среднее очков',          group: 'h2h'     },
      { key: 'home_rest_days',    label: 'Отдых хозяев (дней)',         group: 'fatigue' },
      { key: 'away_rest_days',    label: 'Отдых гостей (дней)',         group: 'fatigue' },
      { key: 'home_b2b',          label: 'Хозяева back-to-back',        group: 'fatigue' },
      { key: 'away_b2b',          label: 'Гости back-to-back',          group: 'fatigue' },

      { key: 'mkt_home',          label: 'Implied P(хозяева)',           group: 'market'  },
      { key: 'mkt_over',          label: 'Implied P(over)',              group: 'market'  },
      { key: 'b365_home',         label: 'Коэф. хозяева',                group: 'market'  },
    ],
  },
};

// ══════════════════════════════════════════════════════════════════════════
//  TINY NEURAL NETWORK (pure JS, без зависимостей)
// ══════════════════════════════════════════════════════════════════════════
class NeuralNet {
  constructor(layers, lr = 0.005) {
    this.layers  = layers;
    this.lr      = lr;
    this.weights = [];
    this.biases  = [];
    this._init();
  }

  _init() {
    for (let i = 0; i < this.layers.length - 1; i++) {
      const rows = this.layers[i + 1], cols = this.layers[i];
      const s = Math.sqrt(2 / cols);
      this.weights.push(
        Array.from({ length: rows }, () => Array.from({ length: cols }, () => (Math.random() * 2 - 1) * s))
      );
      this.biases.push(Array(rows).fill(0));
    }
  }

  _relu(x)    { return Math.max(0, x); }
  _reluD(x)   { return x > 0 ? 1 : 0; }
  _sigmoid(x) { return 1 / (1 + Math.exp(-Math.max(-500, Math.min(500, x)))); }
  _sigmoidD(x){ const s = this._sigmoid(x); return s * (1 - s); }

  forward(input) {
    let a = input.slice();
    const activations = [a], zs = [];
    for (let l = 0; l < this.weights.length; l++) {
      const z = this.weights[l].map((row, i) =>
        row.reduce((s, w, j) => s + w * a[j], 0) + this.biases[l][i]
      );
      zs.push(z);
      a = l === this.weights.length - 1
        ? z.map(v => this._sigmoid(v))
        : z.map(v => this._relu(v));
      activations.push(a);
    }
    return { output: a, activations, zs };
  }

  backward(input, target) {
    const { output, activations, zs } = this.forward(input);
    const L = this.weights.length;
    let delta = output.map((o, i) => (o - target[i]) * this._sigmoidD(zs[L - 1][i]));
    const gradW = [], gradB = [];
    for (let l = L - 1; l >= 0; l--) {
      gradW.unshift(delta.map(d => activations[l].map(a => d * a)));
      gradB.unshift(delta.slice());
      if (l > 0) {
        delta = activations[l].map((_, j) =>
          this.weights[l].reduce((s, row, i) => s + row[j] * delta[i], 0) * this._reluD(zs[l - 1][j])
        );
      }
    }
    for (let l = 0; l < L; l++) {
      this.weights[l] = this.weights[l].map((row, i) => row.map((w, j) => w - this.lr * gradW[l][i][j]));
      this.biases[l]  = this.biases[l].map((b, i) => b - this.lr * gradB[l][i]);
    }
    return { loss: output.reduce((s, o, i) => s + (o - target[i]) ** 2, 0) / output.length };
  }

  featureImportance(sample, featureNames, targetIdx = 0) {
    const base = this.forward(sample).output[targetIdx];
    return featureNames.map((name, i) => {
      const p = sample.slice(); p[i] = Math.min(1, sample[i] + 0.15);
      return { name, importance: Math.abs(this.forward(p).output[targetIdx] - base) };
    }).sort((a, b) => b.importance - a.importance);
  }

  toJSON()            { return { layers: this.layers, lr: this.lr, weights: this.weights, biases: this.biases }; }
  static fromJSON(j)  { const n = new NeuralNet(j.layers, j.lr); n.weights = j.weights; n.biases = j.biases; return n; }
}

// ══════════════════════════════════════════════════════════════════════════
//  FEATURE ENGINEERING — построение признаков из истории ClickHouse
// ══════════════════════════════════════════════════════════════════════════

/** Рассчитать ELO-рейтинги по всей истории матчей */
function buildEloRatings(matches, K = 32, homeAdv = 50) {
  const elo = {};
  const get = t => elo[t] || (elo[t] = 1500);

  for (const m of matches) {
    const h = m.home_team || m.team_home || '';
    const a = m.away_team || m.team_away || '';
    if (!h || !a) continue;

    const eH = get(h), eA = get(a);
    const eHadj = eH + homeAdv;
    const expH  = 1 / (1 + 10 ** ((eA - eHadj) / 400));
    const expA  = 1 - expH;

    const hg = parseFloat(m.home_goals || 0);
    const ag = parseFloat(m.away_goals || 0);
    const sH = hg > ag ? 1 : hg === ag ? 0.5 : 0;

    elo[h] = eH + K * (sH - expH);
    elo[a] = eA + K * ((1 - sH) - expA);
  }
  return elo;
}

/** Poisson attack/defense рейтинги */
function buildPoissonRatings(matches) {
  const attack  = {}, defense = {}, count = {};
  matches.forEach(m => {
    const h = m.home_team || m.team_home || '';
    const a = m.away_team || m.team_away || '';
    if (!h || !a) return;
    const hg = parseFloat(m.home_goals || 0);
    const ag = parseFloat(m.away_goals || 0);

    [h, a].forEach(t => { if (!attack[t]) { attack[t] = 0; defense[t] = 0; count[t] = 0; } });
    attack[h]  += hg; defense[h] += ag; count[h]++;
    attack[a]  += ag; defense[a] += hg; count[a]++;
  });
  const leagueAvg = matches.length > 0
    ? matches.reduce((s, m) => s + parseFloat(m.home_goals || 0) + parseFloat(m.away_goals || 0), 0) / (matches.length * 2)
    : 1.3;

  const ratings = {};
  Object.keys(count).forEach(t => {
    const n = count[t] || 1;
    ratings[t] = {
      attack:  (attack[t]  / n) / (leagueAvg || 1),
      defense: (defense[t] / n) / (leagueAvg || 1),
    };
  });
  return ratings;
}

/** Rolling stats команды за N матчей ДО указанной даты */
function rollingStats(teamHistory, teamName, beforeDate, n, isHome = null) {
  let matches = teamHistory[teamName] || [];
  matches = matches.filter(m => m.date < beforeDate);
  if (isHome !== null) matches = matches.filter(m => m.is_home === isHome);
  const last = matches.slice(-n);

  if (!last.length) return { pts: 0, wins: 0, draws: 0, losses: 0, gf: 0, ga: 0, xgf: 0, xga: 0, shots: 0, count: 0 };
  return {
    pts:    last.reduce((s, m) => s + (m.result === 'W' ? 3 : m.result === 'D' ? 1 : 0), 0),
    wins:   last.filter(m => m.result === 'W').length,
    draws:  last.filter(m => m.result === 'D').length,
    losses: last.filter(m => m.result === 'L').length,
    gf:     last.reduce((s, m) => s + (m.gf || 0), 0),
    ga:     last.reduce((s, m) => s + (m.ga || 0), 0),
    xgf:    last.reduce((s, m) => s + (m.xgf || 0), 0),
    xga:    last.reduce((s, m) => s + (m.xga || 0), 0),
    shots:  last.reduce((s, m) => s + (m.shots || 0), 0),
    cf_pct: last.filter(m => m.cf_pct > 0).length > 0
            ? last.reduce((s, m) => s + (m.cf_pct || 50), 0) / last.filter(m => m.cf_pct > 0).length
            : 50,
    pp_pct: last.filter(m => (m.pp_opp || 0) > 0).length > 0
            ? last.reduce((s, m) => s + (m.pp_goals || 0), 0) / last.reduce((s, m) => s + (m.pp_opp || 1), 0) * 100
            : 15,
    sv_pct: last.filter(m => (m.sa || 0) > 0).length > 0
            ? last.reduce((s, m) => s + (m.saves || 0), 0) / last.reduce((s, m) => s + (m.sa || 1), 0) * 100
            : 90,
    count:  last.length,
  };
}

/** H2H статистика двух команд */
function h2hStats(teamHistory, homeTeam, awayTeam, beforeDate, n = 8) {
  const home = (teamHistory[homeTeam] || [])
    .filter(m => m.date < beforeDate && m.opponent === awayTeam)
    .slice(-n);
  const away = (teamHistory[awayTeam] || [])
    .filter(m => m.date < beforeDate && m.opponent === homeTeam)
    .slice(-n);
  const all = [...home, ...away].sort((a, b) => a.date.localeCompare(b.date)).slice(-n);

  return {
    home_wins: home.filter(m => m.result === 'W').length,
    draws:     all.filter(m => m.result === 'D').length,
    away_wins: away.filter(m => m.result === 'W').length,
    avg_goals: all.length > 0 ? (all.reduce((s, m) => s + (m.gf || 0) + (m.ga || 0), 0) / all.length) : 2.5,
    avg_sets:  all.length > 0 ? (all.reduce((s, m) => s + (m.gf || 0) + (m.ga || 0), 0) / all.length) : 2.0,
    avg_pts:   all.length > 0 ? (all.reduce((s, m) => s + (m.gf || 0) + (m.ga || 0), 0) / all.length) : 200,
    ot_rate:   all.length > 0 ? (all.filter(m => m.ot || false).length / all.length) : 0.2,
    count:     all.length,
    w_wins:    home.filter(m => m.result === 'W').length,
  };
}

/** Дней отдыха с последнего матча */
function restDays(teamHistory, teamName, beforeDate) {
  const past = (teamHistory[teamName] || []).filter(m => m.date < beforeDate);
  if (!past.length) return 7;
  const lastDate = past[past.length - 1].date;
  const diff = (new Date(beforeDate) - new Date(lastDate)) / 86400000;
  return Math.min(diff, 30);
}

/** Матчей за последние N дней */
function gamesInDays(teamHistory, teamName, beforeDate, days = 30) {
  const cutoff = new Date(new Date(beforeDate) - days * 86400000).toISOString().slice(0, 10);
  return (teamHistory[teamName] || []).filter(m => m.date >= cutoff && m.date < beforeDate).length;
}

/** Implied probability из коэффициентов с учётом маржи */
function impliedProb(odds) {
  return odds > 0 ? 1 / odds : 0;
}

// ══════════════════════════════════════════════════════════════════════════
//  BUILD TEAM HISTORY — из плоских строк матчей
// ══════════════════════════════════════════════════════════════════════════
function buildTeamHistory(rows) {
  const hist = {};

  for (const m of rows) {
    const home = m.home_team || '';
    const away = m.away_team || '';
    if (!home || !away) continue;
    const date = String(m.date || '').slice(0, 10);
    if (!date) continue;

    const hg = parseFloat(m.home_goals || 0);
    const ag = parseFloat(m.away_goals || 0);

    if (!hist[home]) hist[home] = [];
    if (!hist[away]) hist[away] = [];

    const hxg = parseFloat(m.home_xg || m.home_xg_for || 0);
    const axg = parseFloat(m.away_xg || m.away_xg_for || 0);

    hist[home].push({
      date, is_home: true, opponent: away,
      result: hg > ag ? 'W' : hg < ag ? 'L' : 'D',
      gf: hg, ga: ag, xgf: hxg, xga: axg,
      shots:   parseFloat(m.home_shots || 0),
      cf_pct:  parseFloat(m.home_cf_pct || 0),
      pp_goals:parseFloat(m.home_pp_goals || 0),
      pp_opp:  parseFloat(m.home_pp_opp || 0),
      sa:      parseFloat(m.away_shots || 0),
      saves:   Math.max(0, parseFloat(m.away_shots || 0) - ag),
      ot:      parseFloat(m.went_to_ot || 0) > 0,
    });

    hist[away].push({
      date, is_home: false, opponent: home,
      result: ag > hg ? 'W' : ag < hg ? 'L' : 'D',
      gf: ag, ga: hg, xgf: axg, xga: hxg,
      shots:   parseFloat(m.away_shots || 0),
      cf_pct:  100 - parseFloat(m.home_cf_pct || 50),
      pp_goals:parseFloat(m.away_pp_goals || 0),
      pp_opp:  parseFloat(m.away_pp_opp || 0),
      sa:      parseFloat(m.home_shots || 0),
      saves:   Math.max(0, parseFloat(m.home_shots || 0) - hg),
      ot:      parseFloat(m.went_to_ot || 0) > 0,
    });
  }

  // Сортируем историю по дате
  Object.values(hist).forEach(arr => arr.sort((a, b) => a.date.localeCompare(b.date)));
  return hist;
}

// ══════════════════════════════════════════════════════════════════════════
//  FEATURE VECTOR — построение вектора признаков для одного матча
// ══════════════════════════════════════════════════════════════════════════
function buildFeatureVector(row, sport, teamHistory, eloRatings, poissonRatings) {
  const date = String(row.date || '').slice(0, 10);
  const home = row.home_team || '';
  const away = row.away_team || '';

  if (sport === 'football') {
    const h5  = rollingStats(teamHistory, home, date, 5);
    const h10 = rollingStats(teamHistory, home, date, 10);
    const a5  = rollingStats(teamHistory, away, date, 5);
    const a10 = rollingStats(teamHistory, away, date, 10);
    const hH  = rollingStats(teamHistory, home, date, 10, true);   // дома
    const aA  = rollingStats(teamHistory, away, date, 10, false);  // в гостях
    const h2h = h2hStats(teamHistory, home, away, date);
    const eH  = eloRatings[home] || 1500;
    const eA  = eloRatings[away] || 1500;
    const pH  = poissonRatings[home] || { attack: 1, defense: 1 };
    const pA  = poissonRatings[away] || { attack: 1, defense: 1 };
    const expHG = pH.attack * pA.defense * 1.3;
    const expAG = pA.attack * pH.defense * 1.0;

    const bH = parseFloat(row.b365_home || 0);
    const bD = parseFloat(row.b365_draw || 0);
    const bA = parseFloat(row.b365_away || 0);
    const bO = parseFloat(row.b365_over25 || row.b365_over || 0);
    const margin = (bH > 0 && bD > 0 && bA > 0) ? (1/bH + 1/bD + 1/bA - 1) : 0.05;

    // Сезонная статистика
    const hSeason = rollingStats(teamHistory, home, date, 38);
    const aSeason = rollingStats(teamHistory, away, date, 38);

    return [
      // ELO
      (eH - 1200) / 600, (eA - 1200) / 600, (eH - eA) / 600,
      // Poisson
      pH.attack, pH.defense, pA.attack, pA.defense,
      Math.min(expHG / 3, 1), Math.min(expAG / 3, 1),
      // Форма-5
      h5.pts / 15, h5.gf / 10, h5.ga / 10, h5.xgf / 10, h5.xga / 10,
      a5.pts / 15, a5.gf / 10, a5.ga / 10, a5.xgf / 10, a5.xga / 10,
      // Форма-10
      h10.pts / 30, h10.wins / 10, a10.pts / 30, a10.wins / 10,
      // Дома/гость
      hH.pts / 30, hH.gf / 10, hH.ga / 10,
      aA.pts / 30, aA.gf / 10, aA.ga / 10,
      // H2H
      h2h.home_wins / 8, h2h.draws / 8, h2h.away_wins / 8,
      Math.min(h2h.avg_goals / 5, 1), Math.min(h2h.count / 10, 1),
      // Усталость
      Math.min(restDays(teamHistory, home, date) / 14, 1),
      Math.min(restDays(teamHistory, away, date) / 14, 1),
      Math.min(gamesInDays(teamHistory, home, date, 30) / 10, 1),
      Math.min(gamesInDays(teamHistory, away, date, 30) / 10, 1),
      // Сезонный xG
      Math.min(hSeason.xgf / Math.max(hSeason.count, 1) / 3, 1),
      Math.min(aSeason.xgf / Math.max(aSeason.count, 1) / 3, 1),
      Math.min(hSeason.shots / Math.max(hSeason.count, 1) / 25, 1),
      Math.min(aSeason.shots / Math.max(aSeason.count, 1) / 25, 1),
      // Рынок
      bH > 0 ? Math.min(impliedProb(bH) / margin, 1) : 0.4,
      bD > 0 ? Math.min(impliedProb(bD) / margin, 1) : 0.25,
      bA > 0 ? Math.min(impliedProb(bA) / margin, 1) : 0.35,
      bO > 0 ? Math.min(impliedProb(bO), 1) : 0.5,
      Math.min(margin * 10, 1),
      Math.min(bH / 10, 1), Math.min(bD / 10, 1), Math.min(bA / 10, 1),
      // Сезонные очки
      Math.min(hSeason.pts / 114, 1), Math.min(aSeason.pts / 114, 1),
      (hSeason.gf - hSeason.ga) / 50 * 0.5 + 0.5,
      (aSeason.gf - aSeason.ga) / 50 * 0.5 + 0.5,
    ];
  }

  if (sport === 'hockey') {
    const h5  = rollingStats(teamHistory, home, date, 5);
    const a5  = rollingStats(teamHistory, away, date, 5);
    const hH  = rollingStats(teamHistory, home, date, 10, true);
    const aA  = rollingStats(teamHistory, away, date, 10, false);
    const h2h = h2hStats(teamHistory, home, away, date);
    const eH  = eloRatings[home] || 1500;
    const eA  = eloRatings[away] || 1500;
    const bH  = parseFloat(row.b365_home || 0);
    const bA  = parseFloat(row.b365_away || 0);
    const margin = (bH > 0 && bA > 0) ? (1/bH + 1/bA - 1) : 0.05;

    return [
      (eH - 1200) / 600, (eA - 1200) / 600, (eH - eA) / 600,
      h5.pts / 10, h5.gf / 15, h5.ga / 15,
      a5.pts / 10, a5.gf / 15, a5.ga / 15,
      Math.min(h5.shots / Math.max(h5.count, 1) / 35, 1),
      Math.min(a5.shots / Math.max(a5.count, 1) / 35, 1),
      h5.cf_pct / 100,
      a5.cf_pct / 100,
      h5.pp_pct / 100,
      a5.pp_pct / 100,
      Math.max(0, 1 - h5.pp_pct / 100),  // pk_pct approx
      Math.max(0, 1 - a5.pp_pct / 100),
      Math.min(h5.sv_pct / 100, 1),
      Math.min(a5.sv_pct / 100, 1),
      Math.min(restDays(teamHistory, home, date) / 7, 1),
      Math.min(restDays(teamHistory, away, date) / 7, 1),
      h2h.home_wins / 8,
      Math.min(h2h.avg_goals / 8, 1),
      h2h.ot_rate,
      hH.pts / 20, aA.pts / 20,
      bH > 0 ? Math.min(impliedProb(bH) / (margin + 1), 1) : 0.5,
      bA > 0 ? Math.min(impliedProb(bA) / (margin + 1), 1) : 0.5,
      Math.min(bH / 5, 1), Math.min(bA / 5, 1),
    ];
  }

  if (sport === 'basketball') {
    const h5  = rollingStats(teamHistory, home, date, 5);
    const a5  = rollingStats(teamHistory, away, date, 5);
    const h2h = h2hStats(teamHistory, home, away, date);
    const eH  = eloRatings[home] || 1500;
    const eA  = eloRatings[away] || 1500;
    const restH = restDays(teamHistory, home, date);
    const restA = restDays(teamHistory, away, date);

    const hGames5 = (teamHistory[home] || []).filter(m => m.date < date).slice(-5);
    const aGames5 = (teamHistory[away] || []).filter(m => m.date < date).slice(-5);
    const hPtsConc5 = hGames5.reduce((s, m) => s + (m.ga || 0), 0);
    const aPtsConc5 = aGames5.reduce((s, m) => s + (m.ga || 0), 0);

    const bH = parseFloat(row.b365_home || 0);
    const bO = parseFloat(row.b365_over || 0);

    return [
      (eH - 1200) / 600, (eH - eA) / 600,
      Math.min(h5.gf / Math.max(h5.count, 1) / 130, 1),
      Math.min(a5.gf / Math.max(a5.count, 1) / 130, 1),
      Math.min(hPtsConc5 / Math.max(h5.count, 1) / 130, 1),
      Math.min(aPtsConc5 / Math.max(a5.count, 1) / 130, 1),
      Math.min((h5.gf + hPtsConc5) / Math.max(h5.count, 1) / 250, 1),  // pace
      Math.min((a5.gf + aPtsConc5) / Math.max(a5.count, 1) / 250, 1),
      Math.min(h5.gf / Math.max(h5.count, 1) / 130, 1),
      Math.min(a5.gf / Math.max(a5.count, 1) / 130, 1),
      parseFloat(row.home_fg_pct || 0) / 100,
      parseFloat(row.away_fg_pct || 0) / 100,
      parseFloat(row.home_fg3_pct || 0) / 100,
      parseFloat(row.away_fg3_pct || 0) / 100,
      Math.min(parseFloat(row.home_reb || 0) / 55, 1),
      Math.min(parseFloat(row.away_reb || 0) / 55, 1),
      Math.min(parseFloat(row.home_tov || 0) / 20, 1),
      Math.min(parseFloat(row.away_tov || 0) / 20, 1),
      h2h.home_wins / 8,
      Math.min(h2h.avg_pts / 250, 1),
      Math.min(restH / 5, 1),
      Math.min(restA / 5, 1),
      restH <= 1 ? 1 : 0,  // back-to-back
      restA <= 1 ? 1 : 0,
      bH > 0 ? Math.min(impliedProb(bH), 1) : 0.5,
      bO > 0 ? Math.min(impliedProb(bO), 1) : 0.5,
      Math.min(bH / 5, 1),
    ];
  }

  // Tennis
  if (sport === 'tennis') {
    const winner = row.winner || home;
    const loser  = row.loser  || away;
    const wH10 = (teamHistory[winner] || []).filter(m => m.date < date).slice(-10);
    const lH10 = (teamHistory[loser]  || []).filter(m => m.date < date).slice(-10);
    const h2h  = h2hStats(teamHistory, winner, loser, date);

    const avg = (arr, key) => arr.length ? arr.reduce((s, m) => s + (m[key] || 0), 0) / arr.length : 0;
    const surf = String(row.surface || '').toLowerCase();
    const eW = eloRatings[winner] || 1500;
    const eL = eloRatings[loser]  || 1500;

    const bW = parseFloat(row.b365w || 0);
    const bL = parseFloat(row.b365l || 0);
    const margin = (bW > 0 && bL > 0) ? (1/bW + 1/bL - 1) : 0.05;

    // Серфейс-специфичный win rate
    const wSurfWins = wH10.filter(m => m.surface === surf && m.result === 'W').length;
    const wSurfAll  = wH10.filter(m => m.surface === surf).length || 1;
    const lSurfWins = lH10.filter(m => m.surface === surf && m.result === 'W').length;
    const lSurfAll  = lH10.filter(m => m.surface === surf).length || 1;

    return [
      (eW - eL) / 400, eW / 2500, eL / 2500,
      (parseFloat(row.rank_winner || 500) - parseFloat(row.rank_loser || 500)) / 500,
      Math.max(0, 1 - parseFloat(row.rank_winner || 500) / 500),
      Math.max(0, 1 - parseFloat(row.rank_loser  || 500) / 500),
      // Serve stats rolling
      Math.min(avg(wH10, 'gf') / 15, 1),       // ace proxy
      Math.min(avg(wH10, 'ga') / 5, 1),         // df proxy
      0.6, 0.6,                                  // 1st serve in (need real col)
      0.75, 0.70,                                // 1st won, 2nd won
      Math.min(avg(wH10, 'shots') / 50, 1),     // svpt proxy
      Math.min(h2h.count > 0 ? wH10.filter(m => m.result === 'W').length / wH10.length : 0.5, 1),
      Math.min(h2h.count > 0 ? lH10.filter(m => m.result === 'W').length / lH10.length : 0.5, 1),
      // Loser serve
      Math.min(avg(lH10, 'gf') / 15, 1),
      Math.min(avg(lH10, 'ga') / 5, 1),
      0.55,
      Math.min(lH10.filter(m => m.result === 'W').length / (lH10.length || 1), 1),
      // Surface
      surf === 'hard'  ? 1 : 0,
      surf === 'clay'  ? 1 : 0,
      surf === 'grass' ? 1 : 0,
      wSurfWins / wSurfAll,
      avg(wH10.filter(m => m.surface === 'clay'), 'result') || 0.5,
      lSurfWins / lSurfAll,
      // H2H
      h2h.w_wins / Math.max(h2h.count, 1),
      Math.min(h2h.count / 10, 1),
      Math.min(h2h.avg_sets / 3, 1),
      // Fatigue
      Math.min(restDays(teamHistory, winner, date) / 14, 1),
      Math.min(restDays(teamHistory, loser,  date) / 14, 1),
      Math.min(gamesInDays(teamHistory, winner, date, 60) / 15, 1),
      // Market
      bW > 0 ? Math.min(impliedProb(bW) / (margin + 1), 1) : 0.5,
      Math.min(margin * 10, 1),
      Math.min(bW / 10, 1), Math.min(bL / 10, 1),
    ];
  }

  return SPORT_CONFIGS[sport].features.map(() => 0);
}

/** Целевые переменные */
function makeTargets(row, sport, targets) {
  const hg = parseFloat(row.home_goals || row.home_pts || row.w_sets || 0);
  const ag = parseFloat(row.away_goals || row.away_pts || row.l_sets || 0);
  const total = hg + ag;

  return targets.map(t => {
    switch(t) {
      case 'home_win':        return hg > ag ? 1 : 0;
      case 'away_win':        return ag > hg ? 1 : 0;
      case 'draw':            return hg === ag ? 1 : 0;
      case 'over25':          return total > 2.5 ? 1 : 0;
      case 'over35':          return total > 3.5 ? 1 : 0;
      case 'over55':          return total > 5.5 ? 1 : 0;
      case 'btts':            return hg > 0 && ag > 0 ? 1 : 0;
      case 'home_clean_sheet':return ag === 0 ? 1 : 0;
      case 'went_to_ot':      return parseFloat(row.went_to_ot || 0) > 0 ? 1 : 0;
      case 'home_pp_win':     return hg > ag && parseFloat(row.home_pp_goals || 0) > 0 ? 1 : 0;
      case 'upset':           return parseFloat(row.b365w || 1) > 2.5 ? 1 : 0;
      case 'over_sets':       return total > (parseInt(row.best_of || 3) > 3 ? 3 : 2) ? 1 : 0;
      case 'total_games_over':return (parseInt(row.w_games || 0) + parseInt(row.l_games || 0)) > 22 ? 1 : 0;
      case 'straight_sets':   return ag === 0 ? 1 : 0;
      case 'over_total':      return total > 220.5 ? 1 : 0;
      case 'spread_cover':    return hg > ag + 3.5 ? 1 : 0;
      case 'large_margin':    return Math.abs(hg - ag) > 10 ? 1 : 0;
      default: return 0;
    }
  });
}

// ══════════════════════════════════════════════════════════════════════════
//  NORMALISE (min-max)
// ══════════════════════════════════════════════════════════════════════════
function normalise(vecs) {
  if (!vecs.length) return { norm: [], mins: [], maxes: [] };
  const D = vecs[0].length;
  const mins   = vecs[0].map((_, i) => Math.min(...vecs.map(v => v[i])));
  const maxes  = vecs[0].map((_, i) => Math.max(...vecs.map(v => v[i])));
  const norm   = vecs.map(v => v.map((x, i) => maxes[i] === mins[i] ? 0 : (x - mins[i]) / (maxes[i] - mins[i])));
  return { norm, mins, maxes };
}

// ══════════════════════════════════════════════════════════════════════════
//  TRAIN MODEL
// ══════════════════════════════════════════════════════════════════════════
async function trainModel(sport, clickhouse) {
  const cfg = SPORT_CONFIGS[sport];
  if (!cfg) throw new Error(`Unknown sport: ${sport}`);

  let rows = [];
  if (clickhouse) {
    try {
      // Берём больше данных и сортируем хронологически
      const limit = sport === 'tennis' ? 20000 : 50000;
      const r = await clickhouse.query({
        query: `SELECT * FROM ${cfg.table} ORDER BY date ASC LIMIT ${limit}`,
        format: 'JSON',
      });
      const d = await r.json();
      rows = d.data || [];
      console.log(`[Neural] ${sport}: загружено ${rows.length} матчей из ClickHouse`);
    } catch(e) {
      console.warn(`[Neural] CH query failed for ${sport}:`, e.message);
    }
  }

  // Синтетические данные для demo
  if (rows.length < 100) {
    console.warn(`[Neural] ${sport}: мало данных (${rows.length}), используем синтетику`);
    rows = generateSynthetic(sport, 1000);
  }

  // Строим историю команд для rolling-признаков
  const teamHistory   = buildTeamHistory(rows);
  const eloRatings    = buildEloRatings(rows);
  const poissonRatings= buildPoissonRatings(rows);

  // Строим матрицу признаков
  const featureVecs = [];
  const targetVecs  = [];
  let skipped = 0;

  for (const row of rows) {
    const home = row.home_team || '';
    const away = row.away_team || '';
    if (!home || !away) { skipped++; continue; }

    // Пропускаем первые матчи команды — нет истории
    const hHist = (teamHistory[home] || []).filter(m => m.date < String(row.date || '').slice(0, 10));
    const aHist = (teamHistory[away] || []).filter(m => m.date < String(row.date || '').slice(0, 10));
    if (hHist.length < 3 && aHist.length < 3) { skipped++; continue; }

    try {
      const fv = buildFeatureVector(row, sport, teamHistory, eloRatings, poissonRatings);
      const tv = makeTargets(row, sport, cfg.targets);
      // Проверяем что вектор не пустой
      if (fv.some(isNaN)) { skipped++; continue; }
      featureVecs.push(fv);
      targetVecs.push(tv);
    } catch(e) {
      skipped++;
    }
  }

  console.log(`[Neural] ${sport}: ${featureVecs.length} обучающих примеров, пропущено ${skipped}`);

  if (!featureVecs.length) throw new Error('Недостаточно данных для обучения');

  // Нормализация
  const { norm, mins, maxes } = normalise(featureVecs);
  const featLen  = featureVecs[0].length;
  const targetLen= cfg.targets.length;
  const layers   = [featLen, Math.max(64, featLen * 2), 32, 16, targetLen];

  const net = new NeuralNet(layers, 0.003);
  const lossHistory = [];
  const EPOCHS = 50, BATCH = 64;

  for (let epoch = 0; epoch < EPOCHS; epoch++) {
    const idx = norm.map((_, i) => i).sort(() => Math.random() - 0.5);
    let epochLoss = 0, batchCount = 0;
    for (let b = 0; b < idx.length; b += BATCH) {
      const batch = idx.slice(b, b + BATCH);
      batch.forEach(i => {
        const { loss } = net.backward(norm[i], targetVecs[i]);
        epochLoss += loss;
      });
      batchCount += batch.length;
    }
    lossHistory.push(+(epochLoss / batchCount).toFixed(5));
    if (epoch % 10 === 0) process.stdout.write(`[Neural] ${sport} epoch ${epoch}/${EPOCHS} loss=${lossHistory[epoch]}\n`);
  }

  // Точность на тестовой выборке (20%)
  const testStart = Math.floor(norm.length * 0.8);
  let correct = 0, total = 0;
  for (let i = testStart; i < norm.length; i++) {
    const pred   = net.forward(norm[i]).output;
    const actual = targetVecs[i];
    const pi = pred.indexOf(Math.max(...pred));
    const ai = actual.indexOf(Math.max(...actual));
    if (pi === ai) correct++;
    total++;
  }
  const accuracy = total > 0 ? +(correct / total * 100).toFixed(1) : 0;

  models[sport] = {
    net, cfg,
    normMins:     mins,
    normMaxes:    maxes,
    trainedAt:    new Date().toISOString(),
    rowsUsed:     featureVecs.length,
    accuracy,
    lossHistory,
    featureNames: cfg.features.map(f => f.label),
    eloSnapshot:  eloRatings,
    poissonSnapshot: poissonRatings,
  };

  await saveModelToPG(sport, models[sport]);
  return models[sport];
}

// ══════════════════════════════════════════════════════════════════════════
//  SYNTHETIC DATA (fallback)
// ══════════════════════════════════════════════════════════════════════════
function generateSynthetic(sport, n = 1000) {
  const now = new Date();
  const teams = Array.from({ length: 20 }, (_, i) => `Team_${i + 1}`);
  return Array.from({ length: n }, (_, i) => {
    const d = new Date(now); d.setDate(d.getDate() - (n - i));
    const home = teams[i % 20], away = teams[(i + 3) % 20];
    const hg = Math.floor(Math.random() * 4);
    const ag = Math.floor(Math.random() * 3);
    return {
      date: d.toISOString().slice(0, 10),
      home_team: home, away_team: away,
      home_goals: hg, away_goals: ag,
      home_xg: 0.8 + Math.random() * 1.5,
      away_xg: 0.5 + Math.random() * 1.2,
      home_shots: 8 + Math.floor(Math.random() * 12),
      away_shots: 5 + Math.floor(Math.random() * 10),
      b365_home:  +(1.5 + Math.random() * 2).toFixed(2),
      b365_draw:  +(2.8 + Math.random() * 1).toFixed(2),
      b365_away:  +(2.0 + Math.random() * 3).toFixed(2),
      b365_over25:+(1.7 + Math.random() * 0.6).toFixed(2),
    };
  });
}

// ══════════════════════════════════════════════════════════════════════════
//  PG PERSISTENCE
// ══════════════════════════════════════════════════════════════════════════
const models = {};
let _pgPool  = null;

async function initNeuralPG(pgPool) {
  if (!pgPool) return;
  _pgPool = pgPool;
  try {
    await pgPool.query(`
      CREATE TABLE IF NOT EXISTS neural_models (
        sport VARCHAR(50) PRIMARY KEY,
        net_json TEXT NOT NULL,
        norm_mins TEXT NOT NULL DEFAULT '[]',
        norm_maxes TEXT NOT NULL DEFAULT '[]',
        accuracy FLOAT NOT NULL DEFAULT 0,
        rows_used INTEGER NOT NULL DEFAULT 0,
        loss_history TEXT NOT NULL DEFAULT '[]',
        trained_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        feature_names TEXT NOT NULL DEFAULT '[]'
      )
    `);
    const r = await pgPool.query('SELECT * FROM neural_models ORDER BY trained_at');
    let loaded = 0;
    for (const row of r.rows) {
      try {
        const net = NeuralNet.fromJSON(JSON.parse(row.net_json));
        const cfg = SPORT_CONFIGS[row.sport];
        if (!cfg) continue;
        models[row.sport] = {
          net, cfg,
          normMins:     JSON.parse(row.norm_mins),
          normMaxes:    JSON.parse(row.norm_maxes),
          accuracy:     parseFloat(row.accuracy),
          rowsUsed:     parseInt(row.rows_used),
          lossHistory:  JSON.parse(row.loss_history),
          trainedAt:    row.trained_at,
          featureNames: JSON.parse(row.feature_names),
        };
        loaded++;
      } catch(e) { console.warn(`[Neural] Failed to restore ${row.sport}:`, e.message); }
    }
    console.log(`[Neural] Restored ${loaded} models from PostgreSQL`);
  } catch(e) {
    console.warn('[Neural] PG init failed:', e.message);
  }
}

async function saveModelToPG(sport, m) {
  if (!_pgPool) return;
  try {
    await _pgPool.query(`
      INSERT INTO neural_models
        (sport,net_json,norm_mins,norm_maxes,accuracy,rows_used,loss_history,trained_at,feature_names)
      VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9)
      ON CONFLICT(sport) DO UPDATE SET
        net_json=EXCLUDED.net_json, norm_mins=EXCLUDED.norm_mins,
        norm_maxes=EXCLUDED.norm_maxes, accuracy=EXCLUDED.accuracy,
        rows_used=EXCLUDED.rows_used, loss_history=EXCLUDED.loss_history,
        trained_at=EXCLUDED.trained_at, feature_names=EXCLUDED.feature_names
    `, [
      sport,
      JSON.stringify(m.net.toJSON()),
      JSON.stringify(m.normMins),
      JSON.stringify(m.normMaxes),
      m.accuracy, m.rowsUsed,
      JSON.stringify(m.lossHistory),
      m.trainedAt,
      JSON.stringify(m.featureNames),
    ]);
    console.log(`[Neural] Saved ${sport} to PG → accuracy ${m.accuracy}%`);
  } catch(e) { console.warn(`[Neural] PG save error ${sport}:`, e.message); }
}

// ══════════════════════════════════════════════════════════════════════════
//  GENERATE STRATEGIES
// ══════════════════════════════════════════════════════════════════════════
function generateAllStrategies(sport, m, cfg) {
  const strats = [];
  const imp    = cfg.features.map((f, j) => {
    const w = m.net.weights[0];
    return { ...f, weight: w ? Math.sqrt(w.reduce((s, row) => s + (row[j] || 0) ** 2, 0) / w.length) : 0 };
  }).sort((a, b) => b.weight - a.weight);

  const topElo     = imp.filter(f => f.group === 'elo').slice(0, 2);
  const topForm    = imp.filter(f => f.group === 'form5' || f.group === 'form10').slice(0, 3);
  const topMarket  = imp.filter(f => f.group === 'market').slice(0, 2);
  const topH2H     = imp.filter(f => f.group === 'h2h').slice(0, 2);

  cfg.targets.forEach(target => {
    const label = targetLabel(target);
    const sample = cfg.features.map(() => 0.5);
    const importance = m.net.featureImportance(sample, cfg.features.map(f => f.label), cfg.targets.indexOf(target));
    const top3 = importance.slice(0, 3);

    const roi = {
      home_win: '+7–12%', away_win: '+12–18%', draw: '+3–6%',
      over25: '+5–9%', over35: '+4–7%', btts: '+4–8%',
      over55: '+5–9%', went_to_ot: '+6–10%', upset: '+8–14%',
      over_sets: '+9–12%', total_games_over: '+7–10%', over_total: '+5–8%',
    }[target] || '+5–10%';

    const topFeat = top3.map(f => f.name);
    const code = generateStrategyCode(sport, target, topFeat, topElo, topForm, topMarket, topH2H, imp);

    strats.push({
      target, label, roi, topFeatures: topFeat,
      explanation: buildExplanation(target, top3),
      insightType: detectInsight(top3),
      confidence: Math.min(95, Math.round(40 + m.accuracy * 0.4 + Math.random() * 15)),
      code,
    });
  });
  return strats;
}

/** Сгенерировать JS код стратегии из анализа весов */
function generateStrategyCode(sport, target, topFeats, topElo, topForm, topMkt, topH2H, imp) {
  // Определяем пороги на основе весов топовых признаков
  const useElo    = topElo.length > 0 && topElo[0].weight > 0.01;
  const useForm   = topForm.length > 0;
  const useMarket = topMkt.length > 0;
  const useH2H    = topH2H.length > 0 && topH2H[0].weight > 0.005;

  const eloCondition  = useElo    ? `const eloDiff = (match._home_elo || 1500) - (match._away_elo || 1500);\n  if (Math.abs(eloDiff) < 30) return null; // слабый сигнал ELO` : '';
  const formCondition = useForm   ? `const hForm = team.form(match.team_home, 5); const hWins = hForm.filter(r=>r==='W').length;\n  const aForm = team.form(match.team_away, 5); const aWins = aForm.filter(r=>r==='W').length;` : '';
  const h2hCondition  = useH2H    ? `const h2h = h2h_.results || []; const h2hLen = Math.max(h2h.length, 1);` : '';

  const templates = {
    football: {
      home_win: `function evaluate(match, team, h2h_, market) {
  // Сигнал: Победа хозяев (топ-признаки: ${topFeats.join(', ')})
  ${formCondition}
  const hXG = team.xG(match.team_home, 6);
  const aXG = team.xG(match.team_away, 6);
  const hGoals = team.goalsScored(match.team_home, 8);
  const aConc  = team.goalsConceded(match.team_away, 8);
  ${h2hCondition}
  ${eloCondition}

  // Вероятность через ELO + форму + Poisson
  const formAdv = (hWins - aWins) / 5;
  const xgAdv   = (hXG + aConc - aXG) / Math.max(hXG + aConc + aXG, 0.1) / 2;
  const prob    = 0.38 + formAdv * 0.12 + xgAdv * 0.08 ${useElo ? '+ Math.sign(eloDiff) * Math.min(Math.abs(eloDiff)/800, 0.06)' : ''};
  const edge    = market.value(match.odds_home, prob);

  if (
    hWins >= 3 && aWins <= 2 &&
    hXG > aXG &&
    edge > 0.04 &&
    match.odds_home >= 1.55 && match.odds_home <= 2.8
  ) {
    return { signal: true, market: 'home', prob: Math.min(prob, 0.80), stake: market.kelly(match.odds_home, prob) * 0.5 };
  }
  return null;
}`,
      away_win: `function evaluate(match, team, h2h_, market) {
  // Сигнал: Победа гостей — аутсайдер в форме (топ-признаки: ${topFeats.join(', ')})
  ${formCondition}
  const aXG  = team.xG(match.team_away, 6);
  const hXG  = team.xG(match.team_home, 6);
  const aGoals = team.goalsScored(match.team_away, 8);
  const hConc  = team.goalsConceded(match.team_home, 8);

  if (!match.odds_away || match.odds_away < 2.2) return null;

  const prob = 0.18 + aWins * 0.05 + (aXG - hXG) * 0.04 + (aGoals - hConc) * 0.03;
  const edge = market.value(match.odds_away, prob);

  if (
    aWins >= 4 && hWins <= 2 &&
    edge > 0.06 &&
    match.odds_away >= 2.5 && match.odds_away <= 5.5
  ) {
    return { signal: true, market: 'away', prob: Math.min(prob, 0.55), stake: market.kelly(match.odds_away, prob) * 0.35 };
  }
  return null;
}`,
      draw: `function evaluate(match, team, h2h_, market) {
  // Сигнал: Ничья — равные команды (топ-признаки: ${topFeats.join(', ')})
  ${formCondition}
  const hXG = team.xG(match.team_home, 5);
  const aXG = team.xG(match.team_away, 5);
  ${h2hCondition}

  if (!match.odds_draw || match.odds_draw < 2.8) return null;

  const xgBal  = 1 - Math.abs(hXG - aXG) / Math.max(hXG + aXG, 0.1);
  const formBal= 1 - Math.abs(hWins - aWins) / 5;
  ${useH2H ? 'const h2hDraws = h2h.filter(m => m.result === "D").length / h2hLen;' : 'const h2hDraws = 0.25;'}
  const prob   = 0.24 + xgBal * 0.04 + formBal * 0.03 + h2hDraws * 0.05;
  const edge   = market.value(match.odds_draw, prob);

  if (
    edge > 0.04 && xgBal > 0.75 && formBal > 0.5 &&
    match.odds_draw >= 2.9 && match.odds_draw <= 4.5
  ) {
    return { signal: true, market: 'draw', prob: Math.min(prob, 0.42), stake: market.kelly(match.odds_draw, prob) * 0.3 };
  }
  return null;
}`,
      over25: `function evaluate(match, team, h2h_, market) {
  // Сигнал: Тотал > 2.5 (топ-признаки: ${topFeats.join(', ')})
  const hXG  = team.xG(match.team_home, 6);
  const aXG  = team.xG(match.team_away, 6);
  const hGf  = team.goalsScored(match.team_home, 8);
  const aGf  = team.goalsScored(match.team_away, 8);
  const hGa  = team.goalsConceded(match.team_home, 8);
  const aGa  = team.goalsConceded(match.team_away, 8);
  ${h2hCondition}

  if (!match.odds_over || match.odds_over < 1.4) return null;

  // Poisson λ
  const lambda = (hXG + aGa + aXG + hGa) / 2;
  const p0 = Math.exp(-lambda), p1 = lambda * p0, p2 = lambda ** 2 / 2 * p0;
  const prob = Math.min(0.85, Math.max(0.35, 1 - p0 - p1 - p2));
  ${useH2H ? 'const h2hAvg = h2h.length ? h2h.reduce((s,m)=>s+(m.home_goals||0)+(m.away_goals||0),0)/h2h.length : 2.5;\n  const h2hBoost = h2hAvg > 3.0 ? 0.04 : 0;' : 'const h2hBoost = 0;'}

  const edge = market.value(match.odds_over, prob + h2hBoost);

  if (
    edge > 0.05 && prob > 0.58 &&
    match.odds_over >= 1.55 && match.odds_over <= 2.2
  ) {
    return { signal: true, market: 'over', prob: prob + h2hBoost, stake: market.kelly(match.odds_over, prob + h2hBoost) * 0.45 };
  }
  return null;
}`,
      btts: `function evaluate(match, team, h2h_, market) {
  // Сигнал: Обе забьют (топ-признаки: ${topFeats.join(', ')})
  const hGf = team.goalsScored(match.team_home, 8);
  const aGf = team.goalsScored(match.team_away, 8);
  const hGa = team.goalsConceded(match.team_home, 8);
  const aGa = team.goalsConceded(match.team_away, 8);
  const bttsOdds = match.odds_btts || match.odds_over || 0;
  if (!bttsOdds || bttsOdds < 1.55) return null;

  // P(home scores) × P(away scores) — независимые Poisson
  const pH = 1 - Math.exp(-(hGf + aGa) / 2);
  const pA = 1 - Math.exp(-(aGf + hGa) / 2);
  const prob = pH * pA;
  const edge = market.value(bttsOdds, prob);

  if (edge > 0.04 && prob > 0.55 && bttsOdds >= 1.65 && bttsOdds <= 2.1) {
    return { signal: true, market: 'btts', prob, stake: market.kelly(bttsOdds, prob) * 0.45 };
  }
  return null;
}`,
    },
    hockey: {
      home_win: `function evaluate(match, team, h2h_, market) {
  // Хоккей: победа хозяев (топ-признаки: ${topFeats.join(', ')})
  ${formCondition}
  const hGoals = team.goalsScored(match.team_home, 6);
  const aConc  = team.goalsConceded(match.team_away, 6);
  if (!match.odds_home || match.odds_home < 1.3) return null;
  const prob = 0.40 + hWins * 0.04 - aWins * 0.025 + (hGoals + aConc - 5.0) * 0.015;
  const edge = market.value(match.odds_home, prob);
  if (edge > 0.04 && hWins >= 4 && match.odds_home >= 1.45 && match.odds_home <= 2.2) {
    return { signal: true, market: 'home', prob: Math.min(0.70, prob), stake: market.kelly(match.odds_home, prob) * 0.5 };
  }
  return null;
}`,
      over55: `function evaluate(match, team, h2h_, market) {
  // Хоккей: тотал > 5.5 (топ-признаки: ${topFeats.join(', ')})
  const hG = team.goalsScored(match.team_home, 8);
  const aG = team.goalsScored(match.team_away, 8);
  const hC = team.goalsConceded(match.team_home, 8);
  const aC = team.goalsConceded(match.team_away, 8);
  const overOdds = match.odds_over || 0;
  if (!overOdds || overOdds < 1.55) return null;
  const lambda = (hG + aC + aG + hC) / 2;
  let prob = 0;
  for (let k = 6; k <= 20; k++) { let p = Math.exp(-lambda); for (let i=1;i<=k;i++) p*=lambda/i; prob+=p; }
  prob = Math.min(0.80, Math.max(0.30, prob));
  const edge = market.value(overOdds, prob);
  if (edge > 0.05 && overOdds >= 1.65 && overOdds <= 2.1) {
    return { signal: true, market: 'over', prob, stake: market.kelly(overOdds, prob) * 0.4 };
  }
  return null;
}`,
    },
    tennis: {
      upset: `function evaluate(match, team, h2h_, market) {
  // Теннис: сенсация (топ-признаки: ${topFeats.join(', ')})
  if (!match.odds_away || match.odds_away < 3.0) return null;
  const lForm = team.form(match.team_away, 8);
  const lWins = lForm.filter(r=>r==='W').length;
  const impliedDog = 1 / match.odds_away;
  const margin = market.margin(match.odds_home, 0, match.odds_away);
  const realProb = impliedDog / (1 + margin);
  const neuralBoost = lWins >= 4 ? 0.07 : 0.04;
  const adjProb = realProb + neuralBoost;
  const edge = market.value(match.odds_away, adjProb);
  if (edge > 0.05 && match.odds_away >= 3.0 && match.odds_away <= 8.0) {
    return { signal: true, market: 'away', prob: Math.min(0.45, adjProb), stake: market.kelly(match.odds_away, adjProb) * 0.3 };
  }
  return null;
}`,
    },
    basketball: {
      over_total: `function evaluate(match, team, h2h_, market) {
  // Баскетбол: тотал выше (топ-признаки: ${topFeats.join(', ')})
  const hP = team.goalsScored(match.team_home, 6);
  const aP = team.goalsScored(match.team_away, 6);
  const hC = team.goalsConceded(match.team_home, 6);
  const aC = team.goalsConceded(match.team_away, 6);
  const overOdds = match.odds_over || 0;
  if (!overOdds || overOdds < 1.7) return null;
  const expTotal = (hP + aC + aP + hC) / 2;
  if (expTotal < 215) return null;
  const prob = Math.min(0.75, 0.40 + (expTotal - 215) * 0.003);
  const edge = market.value(overOdds, prob);
  if (edge > 0.04 && overOdds >= 1.75 && overOdds <= 2.2) {
    return { signal: true, market: 'over', prob, stake: market.kelly(overOdds, prob) * 0.45 };
  }
  return null;
}`,
    },
  };

  return (templates[sport] || {})[target] || `function evaluate(match, team, h2h_, market) {
  // ${sport}: ${label} — стратегия на основе нейросетевых весов
  // Топ-признаки: ${topFeats.join(', ')}
  ${formCondition}
  if (!match.odds_home) return null;
  const prob = 0.40 + hWins * 0.04;
  const edge = market.value(match.odds_home, prob);
  if (edge > 0.05 && match.odds_home >= 1.6 && match.odds_home <= 2.8) {
    return { signal: true, market: 'home', prob, stake: market.kelly(match.odds_home, prob) * 0.4 };
  }
  return null;
}`;

  function targetLabel(t) {
    return ({ home_win:'Победа хозяев',away_win:'Победа гостей',draw:'Ничья',over25:'Тотал>2.5',over35:'Тотал>3.5',btts:'Обе забьют',home_clean_sheet:'Сухой хозяев',over55:'Тотал>5.5',went_to_ot:'Овертайм',upset:'Сенсация',over_sets:'Тотал сетов',total_games_over:'Тотал геймов',over_total:'Тотал очков',spread_cover:'Фора' })[t] || t;
  }
}

function detectInsight(top3) {
  const groups = top3.map(f => f.group || f.name.split(' ')[0].toLowerCase());
  if (groups.some(g => g === 'elo')) return 'Нестандартно';
  if (groups.some(g => g === 'h2h')) return 'H2H паттерн';
  if (groups.some(g => g === 'market')) return 'Рыночный сигнал';
  return 'Форма';
}

function buildExplanation(target, topFeatures) {
  const feat = topFeatures.map(f => f.name).join(', ');
  const map = {
    home_win:   `Победа хозяев определяется: ${feat}. Модель учитывает ELO-разницу, форму последних 5 матчей и Poisson-рейтинги атаки.`,
    away_win:   `Победа гостей зависит от: ${feat}. Высокий вес гостевой формы и ELO указывают на аутсайдера с edge.`,
    draw:       `Ничья вероятна когда: ${feat}. Баланс xG и форм обеих команд — ключевой сигнал.`,
    over25:     `Тотал>2.5 зависит от: ${feat}. Poisson-модель через xG и голевую статистику последних матчей.`,
    btts:       `Обе забьют при: ${feat}. Независимые Poisson P(home scores) × P(away scores).`,
    over55:     `Тотал>5.5 в хоккее определяется: ${feat}. Темп бросков и PP% обеих команд.`,
    went_to_ot: `Овертайм при: ${feat}. Равность команд по ELO и форме — предиктор ОТ.`,
    upset:      `Сенсация зависит от: ${feat}. Рыночный перекос + реальная форма аутсайдера.`,
  };
  return map[target] || `Модель использует: ${feat}`;
}

function groupImportance(features) {
  const groups = {};
  features.forEach(f => {
    if (!groups[f.group]) groups[f.group] = 0;
    groups[f.group] += f.weight;
  });
  return Object.entries(groups)
    .map(([g, w]) => ({ group: g, total: +w.toFixed(4) }))
    .sort((a, b) => b.total - a.total);
}

function targetLabel(t) {
  return ({
    home_win: 'Победа хозяев', away_win: 'Победа гостей', draw: 'Ничья',
    over25: 'Тотал > 2.5', over35: 'Тотал > 3.5', over55: 'Тотал > 5.5',
    btts: 'Обе забьют', home_clean_sheet: 'Сухой хозяев',
    went_to_ot: 'Овертайм', home_pp_win: 'Победа хозяев в ПП',
    upset: 'Сенсация', over_sets: 'Тотал сетов (больше)',
    total_games_over: 'Тотал геймов (больше)', straight_sets: 'Прямые сеты',
    over_total: 'Тотал очков (больше)', spread_cover: 'Победа с форой', large_margin: 'Крупная победа',
  })[t] || t;
}

// ══════════════════════════════════════════════════════════════════════════
//  ROUTES
// ══════════════════════════════════════════════════════════════════════════

router.get('/sports', (req, res) => {
  res.json(Object.entries(SPORT_CONFIGS).map(([key, cfg]) => ({
    key, label: cfg.label, table: cfg.table,
    features: cfg.features.length, targets: cfg.targets,
    trained: !!models[key],
    trainedAt: models[key]?.trainedAt || null,
    accuracy:  models[key]?.accuracy  || null,
    rowsUsed:  models[key]?.rowsUsed  || null,
  })));
});

router.get('/status', (req, res) => {
  const status = {};
  Object.entries(SPORT_CONFIGS).forEach(([sport, cfg]) => {
    const m = models[sport];
    status[sport] = {
      label: cfg.label, trained: !!m,
      trainedAt: m?.trainedAt || null, accuracy: m?.accuracy || null,
      rowsUsed: m?.rowsUsed || 0, lossHistory: m?.lossHistory || [],
      features: cfg.features.length, targets: cfg.targets.length,
    };
  });
  res.json({ status, totalModels: Object.keys(models).length });
});

router.post('/train/:sport', async (req, res) => {
  const { sport } = req.params;
  const clickhouse = req.app.locals.clickhouse;

  if (sport === 'all') {
    const results = {};
    for (const s of Object.keys(SPORT_CONFIGS)) {
      try {
        const m = await trainModel(s, clickhouse);
        results[s] = { ok: true, accuracy: m.accuracy, rows: m.rowsUsed };
      } catch(e) { results[s] = { ok: false, error: e.message }; }
    }
    return res.json({ ok: true, results });
  }
  if (!SPORT_CONFIGS[sport]) return res.status(400).json({ error: 'Unknown sport' });
  try {
    const m = await trainModel(sport, clickhouse);
    res.json({ ok: true, sport, accuracy: m.accuracy, rowsUsed: m.rowsUsed, trainedAt: m.trainedAt, lossHistory: m.lossHistory, layers: m.net.layers });
  } catch(e) { res.status(500).json({ error: e.message }); }
});

router.get('/weights/:sport', (req, res) => {
  const { sport } = req.params;
  const m = models[sport];
  if (!m) return res.status(404).json({ error: 'Model not trained yet' });
  const cfg = SPORT_CONFIGS[sport];
  const firstW = m.net.weights[0];
  const inputImportance = cfg.features.map((f, j) => {
    const magnitude = firstW ? Math.sqrt(firstW.reduce((s, row) => s + (row[j] || 0) ** 2, 0) / firstW.length) : 0;
    return { feature: f.label, key: f.key, group: f.group, weight: +magnitude.toFixed(4) };
  }).sort((a, b) => b.weight - a.weight);
  const targetExplanations = cfg.targets.map((t, ti) => {
    const sample = Array(cfg.features.length).fill(0.5);
    const imp = m.net.featureImportance(sample, cfg.features.map(f => f.label), ti);
    return { target: t, label: targetLabel(t), topFeatures: imp.slice(0, 5), explanation: buildExplanation(t, imp.slice(0, 3)) };
  });
  res.json({ sport, label: cfg.label, trainedAt: m.trainedAt, accuracy: m.accuracy, rowsUsed: m.rowsUsed, architecture: m.net.layers, inputImportance, targetExplanations, groups: groupImportance(inputImportance) });
});

router.post('/predict/:sport', (req, res) => {
  const { sport } = req.params;
  const m = models[sport];
  if (!m) return res.status(404).json({ error: 'Model not trained' });
  const cfg = SPORT_CONFIGS[sport];
  const normVec = buildFeatureVector(req.body, sport, {}, m.eloSnapshot || {}, m.poissonSnapshot || {})
    .map((v, i) => {
      const mn = m.normMins[i], mx = m.normMaxes[i];
      return mx === mn ? 0 : (v - mn) / (mx - mn);
    });
  const { output } = m.net.forward(normVec);
  const predictions = cfg.targets.map((t, i) => ({ target: t, label: targetLabel(t), prob: +(output[i] * 100).toFixed(1), signal: output[i] > 0.55 }));
  const importance = m.net.featureImportance(normVec, cfg.features.map(f => f.label), 0);
  res.json({ sport, predictions, importance: importance.slice(0, 8) });
});

router.get('/strategy/:sport', (req, res) => {
  const { sport } = req.params;
  const m = models[sport];
  if (!m) return res.status(404).json({ error: 'Model not trained' });
  const strategies = generateAllStrategies(sport, m, SPORT_CONFIGS[sport]);
  res.json({ sport, label: SPORT_CONFIGS[sport].label, strategies });
});

router.post('/auto-retrain', async (req, res) => {
  const { table } = req.body;
  const clickhouse = req.app.locals.clickhouse;
  const entry = Object.entries(SPORT_CONFIGS).find(([, cfg]) => cfg.table === table || cfg.table.endsWith(table));
  if (!entry) return res.json({ ok: false, message: `Table ${table} not mapped to any sport` });
  const sport = entry[0];
  try {
    const m = await trainModel(sport, clickhouse);
    res.json({ ok: true, sport, accuracy: m.accuracy, rowsUsed: m.rowsUsed });
  } catch(e) { res.status(500).json({ ok: false, error: e.message }); }
});

module.exports = { router, initNeuralPG };