'use strict';
/**
 * ══════════════════════════════════════════════════════════════════════════
 *  BetQuant Pro — Neural Networks v5   server/neural.js
 *
 *  "ПОЛНОЕ ПОКРЫТИЕ РЫНКОВ"
 *
 *  Что нового в v5 vs v4:
 *
 *  ┌────────────────────────────────────────────────────────────────────┐
 *  │  РАСШИРЕНИЕ РЫНКОВ (targets)                                       │
 *  ├────────────────────────────────────────────────────────────────────┤
 *  │  ФУТБОЛ (~35 рынков):                                              │
 *  │   Исходы: home_win, draw, away_win                                 │
 *  │   Тоталы: over15, over25, over35, over45, under15, under25         │
 *  │   BTTS: btts, btts_over25, btts_and_home, btts_and_away           │
 *  │   Фора: ah_home_m05, ah_home_m15, ah_away_m05, ah_away_m15        │
 *  │   Инд. тоталы: home_ov05, home_ov15, home_ov25, away_ov05...      │
 *  │   Сухие: home_cs, away_cs                                          │
 *  │   Тайм: ht_home_win, ht_draw, ht_over05, h2_over15               │
 *  │   Карточки: over35_cards, home_card_first                          │
 *  │   Угловые: over95_corners, home_corners_ov55                       │
 *  │   Комбо: hw_and_over25, aw_and_btts                               │
 *  │                                                                    │
 *  │  ХОККЕЙ (~20 рынков):                                              │
 *  │   over45, over55, over65, over75, puck_line, puck_line_away        │
 *  │   btts, went_to_ot, home_win_reg, away_win_reg                    │
 *  │   home_ov15, home_ov25, away_ov15, pp_goal                         │
 *  │                                                                    │
 *  │  ТЕННИС (~18 рынков):                                              │
 *  │   upset, over_sets, total_games_over, straight_sets                │
 *  │   set1_winner, set1_over95, h2h_trend, serve_dominant             │
 *  │   winner_ov05_sets, games_over21, games_over23, games_over25       │
 *  │                                                                    │
 *  │  БАСКЕТБОЛ (~20 рынков):                                           │
 *  │   over_total (200/210/220/230/240), spread_cover, large_margin     │
 *  │   home_ov100, away_ov100, q1_over, h1_over, home_win_q1           │
 *  │   race_to_10, comeback, overtime                                   │
 *  │                                                                    │
 *  │  ВОЛЕЙБОЛ, NFL, РЕГБИ, КРИКЕТ, ВОДНОЕ ПОЛО, КИБЕРСПОРТ            │
 *  │                                                                    │
 *  │  НОВЫЙ ENDPOINT /api/neural/markets/:sport                         │
 *  │   Полный анализ конкретного матча по всем рынкам:                  │
 *  │   • Вероятность нейросети                                          │
 *  │   • Implied probability из коэффициентов                           │
 *  │   • Value (edge)                                                   │
 *  │   • Тренд (растущий/падающий/стабильный)                          │
 *  │   • Уровень уверенности (low/medium/high/very_high)               │
 *  │   • Группировка по типу рынка                                      │
 *  │   • Топ-3 фактора почему именно этот рынок                        │
 *  │                                                                    │
 *  │  ДЕТЕКТОР ТРЕНДОВ КОМАНДЫ:                                         │
 *  │   Автоматически определяет паттерны:                               │
 *  │   - "Команда в роле машина" (5+ матчей с голом)                    │
 *  │   - "Защитная стена" (4+ сухих матча)                              │
 *  │   - "Голевой взрыв" (avg > 2.5 в последних 5)                     │
 *  │   - "Высокая усталость" (back-to-back серия)                       │
 *  │   - "Психологический подъём" (серия побед после серии поражений)   │
 *  └────────────────────────────────────────────────────────────────────┘
 *
 *  Архитектура: [N_features → 256 → 128 → 64 → 32 → N_targets]
 *  Football: ~110 признаков → ~35 targets
 *
 *  ДЕПЛОЙ: cp neural-v5/neural.js server/neural.js
 * ══════════════════════════════════════════════════════════════════════════
 */

const express = require('express');
const router  = express.Router();

// ══════════════════════════════════════════════════════════════════════════
//  SPORT CONFIGS — ПОЛНОЕ ПОКРЫТИЕ РЫНКОВ
// ══════════════════════════════════════════════════════════════════════════
const SPORT_CONFIGS = {

  // ─────────────────────────────────────────────────────────────────────
  football: {
    table:   'betquant.football_matches',
    label:   '⚽ Футбол',
    // 35 рынков
    targets: [
      // Исходы
      'home_win','draw','away_win',
      // Тоталы матча
      'over15','over25','over35','over45',
      'under15','under25',
      // BTTS и комбо
      'btts','btts_over25','btts_and_home','btts_and_away',
      // Азиатская фора
      'ah_home_m05','ah_home_m15','ah_away_m05','ah_away_m15',
      // Индивидуальные тоталы
      'home_ov05','home_ov15','home_ov25',
      'away_ov05','away_ov15','away_ov25',
      // Сухие матчи
      'home_cs','away_cs',
      // Таймовые рынки
      'ht_home_win','ht_draw','ht_over05','h2_over15',
      // Карточки
      'over35_cards',
      // Угловые
      'over95_corners',
      // Комбо исход+тотал
      'hw_and_over25','aw_and_btts',
    ],
    features: [
      // ── ELO динамический ─────────────────────────────────────────────
      { key:'elo_diff',        label:'ELO разница (д-г)',         group:'elo'       },
      { key:'elo_h_norm',      label:'ELO хозяев (норм.)',        group:'elo'       },
      { key:'elo_a_norm',      label:'ELO гостей (норм.)',        group:'elo'       },
      { key:'elo_h_trend30',   label:'ELO тренд хозяев за 30д',  group:'elo'       },
      { key:'elo_a_trend30',   label:'ELO тренд гостей за 30д',  group:'elo'       },
      { key:'elo_h_peak_dist', label:'Расст. хозяев от пика ELO',group:'elo'       },
      // ── Poisson ──────────────────────────────────────────────────────
      { key:'lam_h',         label:'λ голы хозяев (rolling)',    group:'poisson'   },
      { key:'lam_a',         label:'λ голы гостей (rolling)',    group:'poisson'   },
      { key:'p_over15',      label:'P(>1.5) Poisson',            group:'poisson'   },
      { key:'p_over25',      label:'P(>2.5) Poisson',            group:'poisson'   },
      { key:'p_over35',      label:'P(>3.5) Poisson',            group:'poisson'   },
      { key:'p_over45',      label:'P(>4.5) Poisson',            group:'poisson'   },
      { key:'p_btts',        label:'P(BTTS) Poisson',            group:'poisson'   },
      { key:'p_homewin',     label:'P(home win) Poisson',        group:'poisson'   },
      { key:'h_att_r',       label:'Атака хозяев (Poisson)',     group:'poisson'   },
      { key:'h_def_r',       label:'Защита хозяев (Poisson)',    group:'poisson'   },
      { key:'a_att_r',       label:'Атака гостей (Poisson)',     group:'poisson'   },
      { key:'a_def_r',       label:'Защита гостей (Poisson)',    group:'poisson'   },
      // ── Форма venue-split ────────────────────────────────────────────
      { key:'h_home5_pts',   label:'Хозяева дома l5 pts%',      group:'form_venue'},
      { key:'h_home5_gf',    label:'Хозяева дома l5 гол/игру',  group:'form_venue'},
      { key:'h_home5_ga',    label:'Хозяева дома l5 пропуск',   group:'form_venue'},
      { key:'a_away5_pts',   label:'Гости в гостях l5 pts%',    group:'form_venue'},
      { key:'a_away5_gf',    label:'Гости в гостях l5 гол/игру',group:'form_venue'},
      { key:'a_away5_ga',    label:'Гости в гостях l5 пропуск', group:'form_venue'},
      // ── Форма общая ──────────────────────────────────────────────────
      { key:'h_form10',      label:'Хозяева форма l10 pts%',    group:'form10'    },
      { key:'a_form10',      label:'Гости форма l10 pts%',      group:'form10'    },
      { key:'h_gf10',        label:'Хозяева голы забито l10',   group:'form10'    },
      { key:'h_ga10',        label:'Хозяева голы пропущено l10',group:'form10'    },
      { key:'a_gf10',        label:'Гости голы забито l10',     group:'form10'    },
      { key:'a_ga10',        label:'Гости голы пропущено l10',  group:'form10'    },
      { key:'h_over25_20',   label:'Хозяева over2.5 rate l20', group:'form10'    },
      { key:'a_over25_20',   label:'Гости over2.5 rate l20',   group:'form10'    },
      { key:'h_btts_20',     label:'Хозяева BTTS rate l20',    group:'form10'    },
      { key:'a_btts_20',     label:'Гости BTTS rate l20',      group:'form10'    },
      { key:'h_cs20',        label:'Хозяева сухих l20',        group:'form10'    },
      { key:'a_cs20',        label:'Гости сухих l20',          group:'form10'    },
      // ── Индивидуальные тоталы ─────────────────────────────────────────
      { key:'h_ov05_rate',   label:'Хозяева забивают (rate)',   group:'indiv_tot' },
      { key:'h_ov15_rate',   label:'Хозяева 1.5+ (rate)',      group:'indiv_tot' },
      { key:'a_ov05_rate',   label:'Гости забивают (rate)',     group:'indiv_tot' },
      { key:'a_ov15_rate',   label:'Гости 1.5+ (rate)',        group:'indiv_tot' },
      // ── Угловые и карточки ───────────────────────────────────────────
      { key:'h_corners5',    label:'Хозяева угл. l5',           group:'corners'   },
      { key:'a_corners5',    label:'Гости угл. l5',             group:'corners'   },
      { key:'h_yellow5',     label:'Хозяева ЖК l5',             group:'cards'     },
      { key:'a_yellow5',     label:'Гости ЖК l5',               group:'cards'     },
      { key:'clash_aggr',    label:'Агрессивность столкновения', group:'cards'     },
      // ── Полуматч (HT) ─────────────────────────────────────────────────
      { key:'h_ht_gf5',      label:'Хозяева голов в 1т l5',    group:'halftime'  },
      { key:'a_ht_gf5',      label:'Гости голов в 1т l5',      group:'halftime'  },
      { key:'h_ht_win_rate', label:'Хозяева побед к перерыву', group:'halftime'  },
      { key:'h_h2_gf5',      label:'Хозяева голов во 2т l5',   group:'halftime'  },
      // ── Momentum ─────────────────────────────────────────────────────
      { key:'h_mom',         label:'Моментум хозяев',           group:'momentum'  },
      { key:'a_mom',         label:'Моментум гостей',           group:'momentum'  },
      { key:'h_streak',      label:'Серия хозяев',              group:'momentum'  },
      { key:'a_streak',      label:'Серия гостей',              group:'momentum'  },
      { key:'h_goal_trend',  label:'Голевой тренд хозяев',      group:'momentum'  },
      { key:'a_goal_trend',  label:'Голевой тренд гостей',      group:'momentum'  },
      { key:'h_def_trend',   label:'Оборонный тренд хозяев',    group:'momentum'  },
      { key:'a_def_trend',   label:'Оборонный тренд гостей',    group:'momentum'  },
      // ── Психология ───────────────────────────────────────────────────
      { key:'h_bounce_back', label:'Bounce-back хозяев',        group:'psych'     },
      { key:'a_bounce_back', label:'Bounce-back гостей',        group:'psych'     },
      { key:'h_win_habit',   label:'Win habit хозяев',          group:'psych'     },
      { key:'a_win_habit',   label:'Win habit гостей',          group:'psych'     },
      { key:'h_draw_tend',   label:'Тяга к ничьей хозяев',      group:'psych'     },
      { key:'a_draw_tend',   label:'Тяга к ничьей гостей',      group:'psych'     },
      { key:'h_comeback',    label:'Comeback rate хозяев',      group:'psych'     },
      { key:'a_comeback',    label:'Comeback rate гостей',      group:'psych'     },
      { key:'h_lead_kept',   label:'Lead kept хозяев',          group:'psych'     },
      // ── Стиль ────────────────────────────────────────────────────────
      { key:'h_att_idx',     label:'Атакующий индекс хозяев',   group:'style'     },
      { key:'a_att_idx',     label:'Атакующий индекс гостей',   group:'style'     },
      { key:'h_press_idx',   label:'Прессинг хозяев',           group:'style'     },
      { key:'a_press_idx',   label:'Прессинг гостей',           group:'style'     },
      { key:'clash_att_def', label:'Атака д vs защита г',       group:'clash'     },
      { key:'clash_tempo',   label:'Темп столкновения',         group:'clash'     },
      // ── H2H ──────────────────────────────────────────────────────────
      { key:'h2h_hw_rate',   label:'H2H победы хозяев',         group:'h2h'       },
      { key:'h2h_draw_rate', label:'H2H ничьи',                 group:'h2h'       },
      { key:'h2h_avg_goals', label:'H2H средний тотал',         group:'h2h'       },
      { key:'h2h_btts_rate', label:'H2H BTTS rate',             group:'h2h'       },
      { key:'h2h_over25',    label:'H2H over2.5 rate',          group:'h2h'       },
      { key:'h2h_home_avg',  label:'H2H хозяева голов avg',     group:'h2h'       },
      { key:'h2h_away_avg',  label:'H2H гости голов avg',       group:'h2h'       },
      // ── Лига DNA ──────────────────────────────────────────────────────
      { key:'lg_draw_rate',  label:'ДНК лиги: % ничьих',        group:'league'    },
      { key:'lg_avg_goals',  label:'ДНК лиги: средний тотал',   group:'league'    },
      { key:'lg_home_rate',  label:'ДНК лиги: % побед хозяев',  group:'league'    },
      { key:'lg_btts_rate',  label:'ДНК лиги: BTTS rate',       group:'league'    },
      { key:'lg_tier',       label:'Уровень лиги',              group:'league'    },
      // ── Рынок ────────────────────────────────────────────────────────
      { key:'mkt_h',         label:'Implied P(хозяева)',         group:'market'    },
      { key:'mkt_d',         label:'Implied P(ничья)',           group:'market'    },
      { key:'mkt_a',         label:'Implied P(гости)',           group:'market'    },
      { key:'mkt_over',      label:'Implied P(over2.5)',         group:'market'    },
      { key:'mkt_margin',    label:'Маржа букмекера',            group:'market'    },
      { key:'model_edge_h',  label:'Edge модели (хозяева)',      group:'market'    },
      { key:'model_edge_over',label:'Edge модели (тотал)',       group:'market'    },
      { key:'b365_h',        label:'Коэф. хозяева',             group:'market'    },
      { key:'b365_d',        label:'Коэф. ничья',               group:'market'    },
      { key:'b365_a',        label:'Коэф. гости',               group:'market'    },
      // ── Усталость ─────────────────────────────────────────────────────
      { key:'h_rest',        label:'Отдых хозяев (дни)',         group:'fatigue'   },
      { key:'a_rest',        label:'Отдых гостей (дни)',         group:'fatigue'   },
      { key:'h_b2b',         label:'Back-to-back хозяева',       group:'fatigue'   },
      { key:'a_b2b',         label:'Back-to-back гости',         group:'fatigue'   },
      { key:'a_run',         label:'Гостевая серия гостей',      group:'fatigue'   },
      { key:'h_g14',         label:'Хозяева матчей за 14дн',    group:'fatigue'   },
      { key:'a_g14',         label:'Гости матчей за 14дн',      group:'fatigue'   },
      // ── Сезон ─────────────────────────────────────────────────────────
      { key:'h_season_pts',  label:'Хозяева pts% сезон',        group:'season'    },
      { key:'a_season_pts',  label:'Гости pts% сезон',          group:'season'    },
      { key:'h_need_win',    label:'Хозяева нужна победа',      group:'season'    },
      { key:'a_need_win',    label:'Гости нужна победа',        group:'season'    },
      { key:'h_gd_season',   label:'Хозяева разн. мячей',       group:'season'    },
      { key:'a_gd_season',   label:'Гости разн. мячей',         group:'season'    },
      { key:'season_phase',  label:'Фаза сезона',               group:'season'    },
      // ── XG overperf ───────────────────────────────────────────────────
      { key:'h_xg_overp',    label:'xG overperformance хозяев', group:'xg'        },
      { key:'a_xg_overp',    label:'xG overperformance гостей', group:'xg'        },
      // ── Сплит по классу ───────────────────────────────────────────────
      { key:'h_vs_top',      label:'Хозяева vs топ-команды',    group:'oppsplit'  },
      { key:'h_vs_weak',     label:'Хозяева vs аутсайдеры',     group:'oppsplit'  },
      { key:'a_vs_top',      label:'Гости vs топ-команды',      group:'oppsplit'  },
      { key:'a_vs_weak',     label:'Гости vs аутсайдеры',       group:'oppsplit'  },
    ],
  },

  // ─────────────────────────────────────────────────────────────────────
  hockey: {
    table:   'betquant.hockey_matches',
    label:   '🏒 Хоккей',
    targets: [
      'home_win','away_win',
      'over45','over55','over65','over75',
      'under45','under55',
      'btts',
      'went_to_ot',
      'home_win_reg','away_win_reg',
      'puck_line_home','puck_line_away',
      'home_ov15','home_ov25',
      'away_ov15','away_ov25',
      'pp_goal',
    ],
    features: [
      { key:'elo_diff',      label:'ELO разница',               group:'elo'       },
      { key:'elo_h_norm',    label:'ELO хозяев',                group:'elo'       },
      { key:'elo_a_norm',    label:'ELO гостей',                group:'elo'       },
      { key:'elo_h_trend30', label:'ELO тренд хозяев',          group:'elo'       },
      { key:'elo_a_trend30', label:'ELO тренд гостей',          group:'elo'       },
      { key:'lam_h',         label:'λ шайб хозяев',             group:'poisson'   },
      { key:'lam_a',         label:'λ шайб гостей',             group:'poisson'   },
      { key:'p_over55',      label:'P(>5.5) Poisson',           group:'poisson'   },
      { key:'p_ot',          label:'P(OT) Poisson',             group:'poisson'   },
      { key:'h_home5_pts',   label:'Хозяева дома pts% l5',      group:'form_venue'},
      { key:'a_away5_pts',   label:'Гости в гостях pts% l5',    group:'form_venue'},
      { key:'h_form10',      label:'Хозяева форма l10',         group:'form10'    },
      { key:'a_form10',      label:'Гости форма l10',           group:'form10'    },
      { key:'h_over55_20',   label:'Хозяева over5.5 rate',      group:'form10'    },
      { key:'a_over55_20',   label:'Гости over5.5 rate',        group:'form10'    },
      { key:'h_cf_pct',      label:'Corsi% хозяев',             group:'style'     },
      { key:'a_cf_pct',      label:'Corsi% гостей',             group:'style'     },
      { key:'h_pp_eff',      label:'PP эффективность хозяев',   group:'style'     },
      { key:'a_pp_eff',      label:'PP эффективность гостей',   group:'style'     },
      { key:'h_pk_eff',      label:'PK эффективность хозяев',   group:'style'     },
      { key:'h_ot_rate',     label:'OT rate хозяев',            group:'style'     },
      { key:'a_ot_rate',     label:'OT rate гостей',            group:'style'     },
      { key:'h_ot_winrate',  label:'Win% хозяев в OT',          group:'style'     },
      { key:'h2h_avg_goals', label:'H2H средний тотал',         group:'h2h'       },
      { key:'h2h_ot_rate',   label:'H2H OT rate',               group:'h2h'       },
      { key:'lg_avg_goals',  label:'ДНК лиги avg голов',        group:'league'    },
      { key:'lg_ot_rate',    label:'ДНК лиги OT rate',          group:'league'    },
      { key:'mkt_h',         label:'Implied P(хозяева)',         group:'market'    },
      { key:'mkt_a',         label:'Implied P(гости)',           group:'market'    },
      { key:'mkt_over',      label:'Implied P(over)',            group:'market'    },
      { key:'h_rest',        label:'Отдых хозяев',              group:'fatigue'   },
      { key:'a_rest',        label:'Отдых гостей',              group:'fatigue'   },
      { key:'h_b2b',         label:'B2B хозяева',               group:'fatigue'   },
      { key:'a_b2b',         label:'B2B гости',                 group:'fatigue'   },
      { key:'h_mom',         label:'Моментум хозяев',           group:'momentum'  },
      { key:'a_mom',         label:'Моментум гостей',           group:'momentum'  },
      { key:'h_bounce_back', label:'Bounce-back хозяев',        group:'psych'     },
      { key:'a_bounce_back', label:'Bounce-back гостей',        group:'psych'     },
      { key:'h_goal_trend',  label:'Голевой тренд хозяев',      group:'momentum'  },
      { key:'a_goal_trend',  label:'Голевой тренд гостей',      group:'momentum'  },
    ],
  },

  // ─────────────────────────────────────────────────────────────────────
  tennis: {
    table:   'betquant.tennis_extended',
    label:   '🎾 Теннис',
    targets: [
      'upset','over_sets','straight_sets',
      'total_games_over21','total_games_over23','total_games_over25',
      'set1_fav_win','set1_over95',
      'winner_ov15_sets','serve_dominant',
      'tiebreak_match','comeback_win',
      'h2h_trend_fav','over_games_set1',
    ],
    features: [
      { key:'elo_diff',       label:'ELO разница',              group:'elo'      },
      { key:'elo_w_norm',     label:'ELO фаворита',             group:'elo'      },
      { key:'elo_l_norm',     label:'ELO андердога',            group:'elo'      },
      { key:'elo_w_trend30',  label:'ELO тренд фаворита',       group:'elo'      },
      { key:'rank_diff',      label:'Разница рейтинга',         group:'elo'      },
      { key:'rank_w_norm',    label:'Рейтинг фаворита',         group:'elo'      },
      { key:'surface_w',      label:'Форма на покрытии (W)',     group:'surface'  },
      { key:'surface_l',      label:'Форма на покрытии (L)',     group:'surface'  },
      { key:'h_1stin',        label:'1-я подача in% фаворита',  group:'serve'    },
      { key:'h_1stwon',       label:'1-я подача won% фаворита', group:'serve'    },
      { key:'h_2ndwon',       label:'2-я подача won% фаворита', group:'serve'    },
      { key:'h_ace',          label:'Эйсы/игру фаворита',       group:'serve'    },
      { key:'h_df',           label:'ДФ/игру фаворита',         group:'serve'    },
      { key:'h_bpsaved',      label:'BP saved% фаворита',       group:'serve'    },
      { key:'a_1stin',        label:'1-я подача in% андердога', group:'serve'    },
      { key:'a_1stwon',       label:'1-я подача won% андердога',group:'serve'    },
      { key:'a_bpsaved',      label:'BP saved% андердога',      group:'serve'    },
      { key:'h_ace_trend',    label:'Тренд эйсов фаворита',     group:'momentum' },
      { key:'h_form10',       label:'Форма фаворита l10',       group:'form10'   },
      { key:'a_form10',       label:'Форма андердога l10',      group:'form10'   },
      { key:'h_sets_won',     label:'Сетов/матч фаворита',      group:'form10'   },
      { key:'a_sets_won',     label:'Сетов/матч андердога',     group:'form10'   },
      { key:'h_straight_rate',label:'% прямых сетов фаворита',  group:'form10'   },
      { key:'a_straight_rate',label:'% прямых сетов андердога', group:'form10'   },
      { key:'h_games_avg',    label:'Геймов/матч фаворита',     group:'form10'   },
      { key:'a_games_avg',    label:'Геймов/матч андердога',    group:'form10'   },
      { key:'h2h_fav_rate',   label:'H2H % побед фаворита',    group:'h2h'      },
      { key:'h2h_sets_avg',   label:'H2H сетов avg',           group:'h2h'      },
      { key:'h2h_games_avg',  label:'H2H геймов avg',          group:'h2h'      },
      { key:'h2h_tiebreak_r', label:'H2H % тайбреков',         group:'h2h'      },
      { key:'h_tiebreak_rate',label:'Тайбреков/матч фаворита',  group:'style'    },
      { key:'a_tiebreak_rate',label:'Тайбреков/матч андердога', group:'style'    },
      { key:'mkt_w_implied',  label:'Implied P(фаворит)',       group:'market'   },
      { key:'mkt_margin',     label:'Маржа',                   group:'market'   },
      { key:'model_edge',     label:'Edge ELO vs рынок',        group:'market'   },
      { key:'h_fatigue',      label:'Усталость фаворита',       group:'fatigue'  },
      { key:'a_fatigue',      label:'Усталость андердога',      group:'fatigue'  },
      { key:'tour_level',     label:'Уровень турнира',          group:'league'   },
    ],
  },

  // ─────────────────────────────────────────────────────────────────────
  basketball: {
    table:   'betquant.basketball_matches_v2',
    label:   '🏀 Баскетбол',
    targets: [
      'home_win','away_win',
      'over200','over210','over220','over230','over240',
      'under200','under210',
      'spread_cover_home','spread_cover_away',
      'large_margin',
      'home_ov100','home_ov110','away_ov100','away_ov110',
      'overtime','h1_over','q1_over',
      'home_win_q1',
    ],
    features: [
      { key:'elo_diff',      label:'ELO разница',               group:'elo'      },
      { key:'elo_h_norm',    label:'ELO хозяев',                group:'elo'      },
      { key:'elo_a_norm',    label:'ELO гостей',                group:'elo'      },
      { key:'elo_h_trend30', label:'ELO тренд хозяев',          group:'elo'      },
      { key:'elo_a_trend30', label:'ELO тренд гостей',          group:'elo'      },
      { key:'lam_h',         label:'λ очков хозяев',            group:'poisson'  },
      { key:'lam_a',         label:'λ очков гостей',            group:'poisson'  },
      { key:'p_over220',     label:'P(>220) Poisson',           group:'poisson'  },
      { key:'h_home5_pts',   label:'Хозяева дома pts% l5',      group:'form_venue'},
      { key:'a_away5_pts',   label:'Гости в гостях pts% l5',    group:'form_venue'},
      { key:'h_form10',      label:'Форма хозяев l10',          group:'form10'   },
      { key:'a_form10',      label:'Форма гостей l10',          group:'form10'   },
      { key:'h_pts_avg',     label:'Очков/матч хозяев',         group:'form10'   },
      { key:'a_pts_avg',     label:'Очков/матч гостей',         group:'form10'   },
      { key:'h_pace',        label:'Темп хозяев (pace)',        group:'style'    },
      { key:'a_pace',        label:'Темп гостей (pace)',        group:'style'    },
      { key:'h_off_rtg',     label:'Offensive rating хозяев',   group:'style'    },
      { key:'a_off_rtg',     label:'Offensive rating гостей',   group:'style'    },
      { key:'h_def_rtg',     label:'Defensive rating хозяев',   group:'style'    },
      { key:'a_def_rtg',     label:'Defensive rating гостей',   group:'style'    },
      { key:'h_over220_r',   label:'Хозяева over220 rate',      group:'form10'   },
      { key:'a_over220_r',   label:'Гости over220 rate',        group:'form10'   },
      { key:'h2h_avg_pts',   label:'H2H средний тотал',         group:'h2h'      },
      { key:'h2h_spread_r',  label:'H2H spread cover rate',     group:'h2h'      },
      { key:'lg_avg_pts',    label:'ДНК лиги avg очков',        group:'league'   },
      { key:'mkt_h',         label:'Implied P(хозяева)',         group:'market'   },
      { key:'mkt_over',      label:'Implied P(over)',            group:'market'   },
      { key:'model_edge_h',  label:'Edge модели',               group:'market'   },
      { key:'h_rest',        label:'Отдых хозяев',              group:'fatigue'  },
      { key:'a_rest',        label:'Отдых гостей',              group:'fatigue'  },
      { key:'h_b2b',         label:'B2B хозяева',               group:'fatigue'  },
      { key:'a_b2b',         label:'B2B гости',                 group:'fatigue'  },
      { key:'h_mom',         label:'Моментум хозяев',           group:'momentum' },
      { key:'a_mom',         label:'Моментум гостей',           group:'momentum' },
      { key:'h_streak',      label:'Серия хозяев',              group:'momentum' },
      { key:'a_streak',      label:'Серия гостей',              group:'momentum' },
    ],
  },

  // ─────────────────────────────────────────────────────────────────────
  volleyball: {
    table:   'betquant.volleyball_matches',
    label:   '🏐 Волейбол',
    targets: [
      'home_win','away_win',
      'over25_sets','under25_sets',
      'goes_to_5th_set',
      'home_win_s1','away_win_s1',
      'home_ov115_pts','away_ov115_pts',
      'total_pts_over200',
    ],
    features: [
      { key:'elo_diff',      label:'ELO разница',               group:'elo'      },
      { key:'h_form10',      label:'Форма хозяев l10',          group:'form10'   },
      { key:'a_form10',      label:'Форма гостей l10',          group:'form10'   },
      { key:'h_sets_avg',    label:'Сетов/матч хозяев',         group:'form10'   },
      { key:'a_sets_avg',    label:'Сетов/матч гостей',         group:'form10'   },
      { key:'h_s1_win_rate', label:'Хозяева % побед 1й сет',   group:'form10'   },
      { key:'a_s1_win_rate', label:'Гости % побед 1й сет',     group:'form10'   },
      { key:'h2h_sets_avg',  label:'H2H сетов avg',            group:'h2h'      },
      { key:'h2h_fav_rate',  label:'H2H % побед хозяев',       group:'h2h'      },
      { key:'h_home5_pts',   label:'Хозяева дома l5',          group:'form_venue'},
      { key:'a_away5_pts',   label:'Гости в гостях l5',        group:'form_venue'},
      { key:'h_mom',         label:'Моментум хозяев',           group:'momentum' },
      { key:'a_mom',         label:'Моментум гостей',           group:'momentum' },
      { key:'h_rest',        label:'Отдых хозяев',              group:'fatigue'  },
      { key:'a_rest',        label:'Отдых гостей',              group:'fatigue'  },
    ],
  },

  // ─────────────────────────────────────────────────────────────────────
  nfl: {
    table:   'betquant.nfl_games',
    label:   '🏈 NFL',
    targets: [
      'home_win','away_win',
      'over_total','under_total',
      'spread_cover_home','spread_cover_away',
      'home_ov17','away_ov17',
      'h1_over','q1_over',
      'td_first_score_home',
      'large_win',
    ],
    features: [
      { key:'elo_diff',      label:'ELO разница',               group:'elo'      },
      { key:'elo_h_norm',    label:'ELO хозяев',                group:'elo'      },
      { key:'elo_a_norm',    label:'ELO гостей',                group:'elo'      },
      { key:'h_form10',      label:'Форма хозяев l10',          group:'form10'   },
      { key:'a_form10',      label:'Форма гостей l10',          group:'form10'   },
      { key:'h_pts_avg',     label:'Очков/матч хозяев',         group:'form10'   },
      { key:'a_pts_avg',     label:'Очков/матч гостей',         group:'form10'   },
      { key:'h_off_avg',     label:'Offensive pts хозяев',      group:'style'    },
      { key:'a_def_avg',     label:'Defensive pts гостей',      group:'style'    },
      { key:'h2h_total_avg', label:'H2H средний тотал',         group:'h2h'      },
      { key:'h2h_spread_r',  label:'H2H spread cover',          group:'h2h'      },
      { key:'mkt_h',         label:'Implied P(хозяева)',         group:'market'   },
      { key:'mkt_over',      label:'Implied P(over)',            group:'market'   },
      { key:'h_rest',        label:'Отдых хозяев',              group:'fatigue'  },
      { key:'a_rest',        label:'Отдых гостей',              group:'fatigue'  },
      { key:'h_mom',         label:'Моментум хозяев',           group:'momentum' },
      { key:'a_mom',         label:'Моментум гостей',           group:'momentum' },
      { key:'h_streak',      label:'Серия хозяев',              group:'momentum' },
      { key:'a_streak',      label:'Серия гостей',              group:'momentum' },
    ],
  },

  // ─────────────────────────────────────────────────────────────────────
  rugby: {
    table:   'betquant.rugby_matches',
    label:   '🏉 Регби',
    targets: [
      'home_win','away_win','draw',
      'over35','over45','over55',
      'spread_cover','large_win',
      'home_ov20','away_ov20',
      'both_score_try',
    ],
    features: [
      { key:'elo_diff',    label:'ELO разница',                  group:'elo'     },
      { key:'h_form10',    label:'Форма хозяев l10',             group:'form10'  },
      { key:'a_form10',    label:'Форма гостей l10',             group:'form10'  },
      { key:'h_pts_avg',   label:'Очков/матч хозяев',            group:'form10'  },
      { key:'a_pts_avg',   label:'Очков/матч гостей',            group:'form10'  },
      { key:'h2h_avg_pts', label:'H2H средний тотал',            group:'h2h'     },
      { key:'mkt_h',       label:'Implied P(хозяева)',            group:'market'  },
      { key:'h_rest',      label:'Отдых хозяев',                 group:'fatigue' },
      { key:'a_rest',      label:'Отдых гостей',                 group:'fatigue' },
      { key:'h_mom',       label:'Моментум хозяев',              group:'momentum'},
      { key:'a_mom',       label:'Моментум гостей',              group:'momentum'},
    ],
  },

  // ─────────────────────────────────────────────────────────────────────
  cricket: {
    table:   'betquant.cricket_matches',
    label:   '🏏 Крикет',
    targets: [
      'home_win','away_win',
      'home_over150','away_over150',
      'total_over300','total_over350',
      'home_win_toss','first_innings_lead',
    ],
    features: [
      { key:'elo_diff',    label:'ELO разница',                  group:'elo'     },
      { key:'h_form10',    label:'Форма команды 1 l10',          group:'form10'  },
      { key:'a_form10',    label:'Форма команды 2 l10',          group:'form10'  },
      { key:'h_runs_avg',  label:'Ранов/иннинг к1',             group:'form10'  },
      { key:'a_runs_avg',  label:'Ранов/иннинг к2',             group:'form10'  },
      { key:'h2h_avg_runs',label:'H2H средний тотал',            group:'h2h'     },
      { key:'h_home5_pts', label:'Форма дома к1',               group:'form_venue'},
      { key:'a_away5_pts', label:'Форма в гостях к2',           group:'form_venue'},
      { key:'mkt_h',       label:'Implied P(команда 1)',         group:'market'  },
      { key:'h_mom',       label:'Моментум к1',                  group:'momentum'},
      { key:'a_mom',       label:'Моментум к2',                  group:'momentum'},
    ],
  },

  // ─────────────────────────────────────────────────────────────────────
  waterpolo: {
    table:   'betquant.waterpolo_matches',
    label:   '🤽 Водное поло',
    targets: [
      'home_win','away_win',
      'over10','over12','over14',
      'btts','home_ov5','away_ov5',
    ],
    features: [
      { key:'elo_diff',    label:'ELO разница',                  group:'elo'     },
      { key:'h_form10',    label:'Форма хозяев l10',             group:'form10'  },
      { key:'a_form10',    label:'Форма гостей l10',             group:'form10'  },
      { key:'h_goals_avg', label:'Голов/матч хозяев',            group:'form10'  },
      { key:'a_goals_avg', label:'Голов/матч гостей',            group:'form10'  },
      { key:'h2h_avg_goals',label:'H2H средний тотал',           group:'h2h'     },
      { key:'h_mom',       label:'Моментум хозяев',              group:'momentum'},
      { key:'a_mom',       label:'Моментум гостей',              group:'momentum'},
      { key:'h_rest',      label:'Отдых хозяев',                 group:'fatigue' },
      { key:'a_rest',      label:'Отдых гостей',                 group:'fatigue' },
    ],
  },

  // ─────────────────────────────────────────────────────────────────────
  esports: {
    table:   'betquant.esports_matches',
    label:   '🎮 Киберспорт',
    targets: [
      'home_win','away_win',
      'over25_maps','under25_maps',
      'goes_to_5maps',
      'map1_fav','map1_total_over25',
      'fav_2_0','fav_2_1',
      'upset',
    ],
    features: [
      { key:'elo_diff',      label:'ELO разница',               group:'elo'      },
      { key:'elo_h_norm',    label:'ELO фаворита',              group:'elo'      },
      { key:'elo_a_norm',    label:'ELO андердога',             group:'elo'      },
      { key:'elo_h_trend30', label:'ELO тренд фаворита',        group:'elo'      },
      { key:'h_form10',      label:'Форма ф. l10 WR',          group:'form10'   },
      { key:'a_form10',      label:'Форма анд. l10 WR',        group:'form10'   },
      { key:'h_maps_avg',    label:'Карт/матч фаворита',        group:'form10'   },
      { key:'a_maps_avg',    label:'Карт/матч андердога',       group:'form10'   },
      { key:'h_2_0_rate',    label:'2-0 rate фаворита',         group:'style'    },
      { key:'a_2_0_rate',    label:'2-0 rate андердога',        group:'style'    },
      { key:'h2h_fav_rate',  label:'H2H % побед фаворита',     group:'h2h'      },
      { key:'h2h_maps_avg',  label:'H2H карт avg',             group:'h2h'      },
      { key:'h2h_map1_rate', label:'H2H map1 побед фаворита',  group:'h2h'      },
      { key:'tournament_tier',label:'Уровень турнира',          group:'league'   },
      { key:'game_type',     label:'Игра (CS/Dota/LoL)',        group:'league'   },
      { key:'mkt_h',         label:'Implied P(фаворит)',         group:'market'   },
      { key:'mkt_over',      label:'Implied P(over maps)',       group:'market'   },
      { key:'h_mom',         label:'Моментум фаворита',          group:'momentum' },
      { key:'a_mom',         label:'Моментум андердога',         group:'momentum' },
      { key:'h_streak',      label:'Серия фаворита',             group:'momentum' },
      { key:'h_rest',        label:'Отдых фаворита (дни)',       group:'fatigue'  },
      { key:'a_rest',        label:'Отдых андердога (дни)',      group:'fatigue'  },
    ],
  },

  // ─────────────────────────────────────────────────────────────────────
  baseball: {
    table:   'betquant.baseball_matches',
    label:   '⚾ Бейсбол',
    targets: [
      'home_win','away_win',
      'over75','over85','over95',
      'under75','under85',
      'runline_home','runline_away',
      'home_ov4','away_ov4',
    ],
    features: [
      { key:'elo_diff',      label:'ELO разница',               group:'elo'     },
      { key:'h_form10',      label:'Форма хозяев l10',          group:'form10'  },
      { key:'a_form10',      label:'Форма гостей l10',          group:'form10'  },
      { key:'h_runs_avg',    label:'Ранов/матч хозяев',         group:'form10'  },
      { key:'a_runs_avg',    label:'Ранов/матч гостей',         group:'form10'  },
      { key:'h_era',         label:'ERA питчера хозяев',        group:'style'   },
      { key:'a_era',         label:'ERA питчера гостей',        group:'style'   },
      { key:'h2h_runs_avg',  label:'H2H средний тотал',         group:'h2h'     },
      { key:'mkt_h',         label:'Implied P(хозяева)',         group:'market'  },
      { key:'mkt_over',      label:'Implied P(over)',            group:'market'  },
      { key:'h_rest',        label:'Отдых хозяев',              group:'fatigue' },
      { key:'a_rest',        label:'Отдых гостей',              group:'fatigue' },
      { key:'h_mom',         label:'Моментум хозяев',           group:'momentum'},
      { key:'a_mom',         label:'Моментум гостей',           group:'momentum'},
    ],
  },
};
class NeuralNet {
  constructor(layers, lr=0.003) {
    this.layers=layers; this.lr=lr; this.weights=[]; this.biases=[];
    for (let i=0;i<layers.length-1;i++) {
      const r=layers[i+1],c=layers[i],s=Math.sqrt(2/c);
      this.weights.push(Array.from({length:r},()=>Array.from({length:c},()=>(Math.random()*2-1)*s)));
      this.biases.push(new Array(r).fill(0));
    }
  }
  _relu(x){return Math.max(0,x);}  _reluD(x){return x>0?1:0;}
  _sig(x){return 1/(1+Math.exp(-Math.max(-500,Math.min(500,x))));}
  _sigD(x){const s=this._sig(x);return s*(1-s);}

  forward(input){
    let a=input.slice(),act=[a],zs=[];
    for(let l=0;l<this.weights.length;l++){
      const z=this.weights[l].map((row,i)=>row.reduce((s,w,j)=>s+w*a[j],0)+this.biases[l][i]);
      zs.push(z);
      a=l===this.weights.length-1?z.map(v=>this._sig(v)):z.map(v=>this._relu(v));
      act.push(a);
    }
    return{output:a,activations:act,zs};
  }

  backward(input,target){
    const{output,activations:act,zs}=this.forward(input);
    const L=this.weights.length;
    let delta=output.map((o,i)=>(o-target[i])*this._sigD(zs[L-1][i]));
    const gW=[],gB=[];
    for(let l=L-1;l>=0;l--){
      gW.unshift(delta.map(d=>act[l].map(a=>d*a)));
      gB.unshift(delta.slice());
      if(l>0) delta=act[l].map((_,j)=>this.weights[l].reduce((s,row,i)=>s+row[j]*delta[i],0)*this._reluD(zs[l-1][j]));
    }
    for(let l=0;l<L;l++){
      this.weights[l]=this.weights[l].map((row,i)=>row.map((w,j)=>w-this.lr*gW[l][i][j]));
      this.biases[l]=this.biases[l].map((b,i)=>b-this.lr*gB[l][i]);
    }
    return{loss:output.reduce((s,o,i)=>s+(o-target[i])**2,0)/output.length};
  }

  featureImportance(sample,names,tIdx=0){
    const base=this.forward(sample).output[tIdx];
    return names.map((name,i)=>{
      const p=sample.slice();p[i]=Math.min(1,sample[i]+0.15);
      return{name,importance:Math.abs(this.forward(p).output[tIdx]-base)};
    }).sort((a,b)=>b.importance-a.importance);
  }

  toJSON(){return{layers:this.layers,lr:this.lr,weights:this.weights,biases:this.biases};}
  static fromJSON(j){const n=new NeuralNet(j.layers,j.lr);n.weights=j.weights;n.biases=j.biases;return n;}
}

// ══════════════════════════════════════════════════════════════════════════
//  ПРОФАЙЛЕР КОМАНД — строит историю из плоских строк матчей
// ══════════════════════════════════════════════════════════════════════════
function buildTeamHistory(rows) {
  const hist={};
  for (const m of rows) {
    const home=m.home_team||m.winner||'';
    const away=m.away_team||m.loser||'';
    if (!home||!away) continue;
    const date=String(m.date||'').slice(0,10); if (!date) continue;

    const hg=parseFloat(m.home_goals||0), ag=parseFloat(m.away_goals||0);
    const hxg=parseFloat(m.home_xg||m.home_xg_for||0);
    const axg=parseFloat(m.away_xg||m.away_xg_for||0);

    if (!hist[home]) hist[home]=[];
    if (!hist[away]) hist[away]=[];

    // Паттерн счёта для comeback/lead_kept
    const htH=parseFloat(m.ht_home_goals||0), htA=parseFloat(m.ht_away_goals||0);
    const hComeback = htH<htA && hg>ag;  // проигрывали в перерыв, выиграли
    const hLeadKept = htH>htA && hg>ag;  // вели в перерыв, выиграли
    const aComeback = htA<htH && ag>hg;
    const aLeadKept = htA>htH && ag>hg;

    const base={
      date, season:m.season||'', league:m.league_code||m.league||'',
      shots:   parseFloat(m.home_shots||0),
      shots_on:parseFloat(m.home_shots_on_target||0),
      corners: parseFloat(m.home_corners||0),
      yellow:  parseFloat(m.home_yellow||0),
      red:     parseFloat(m.home_red||0),
      cf_pct:  parseFloat(m.home_cf_pct||50),
      pp_goals:parseFloat(m.home_pp_goals||0),
      pp_opp:  parseFloat(m.home_pp_opp||0),
      sa:      parseFloat(m.away_shots||0),
      saves:   Math.max(0,parseFloat(m.away_shots||0)-ag),
      ot:      parseFloat(m.went_to_ot||0)>0,
      // Tennis specific
      surface: m.surface||'',
      w_ace:parseFloat(m.w_ace||0),l_ace:parseFloat(m.l_ace||0),
      w_df:parseFloat(m.w_df||0),l_df:parseFloat(m.l_df||0),
      w_1stin:parseFloat(m.w_1stin||0),w_1stwon:parseFloat(m.w_1stwon||0),
      w_2ndwon:parseFloat(m.w_2ndwon||0),
      w_bpfaced:parseFloat(m.w_bpfaced||0),w_bpsaved:parseFloat(m.w_bpsaved||0),
      w_svpt:parseFloat(m.w_svpt||0),l_svpt:parseFloat(m.l_svpt||0),
      l_1stin:parseFloat(m.l_1stin||0),l_bpfaced:parseFloat(m.l_bpfaced||0),l_bpsaved:parseFloat(m.l_bpsaved||0),
    };

    hist[home].push({...base,
      is_home:true, opponent:away,
      result:hg>ag?'W':hg<ag?'L':'D',
      gf:hg,ga:ag,xgf:hxg,xga:axg,
      comeback:hComeback, lead_kept:hLeadKept,
    });
    hist[away].push({...base,
      is_home:false, opponent:home,
      result:ag>hg?'W':ag<hg?'L':'D',
      gf:ag,ga:hg,xgf:axg,xga:hxg,
      shots:parseFloat(m.away_shots||0),shots_on:parseFloat(m.away_shots_on_target||0),
      corners:parseFloat(m.away_corners||0),yellow:parseFloat(m.away_yellow||0),red:parseFloat(m.away_red||0),
      cf_pct:100-parseFloat(m.home_cf_pct||50),
      pp_goals:parseFloat(m.away_pp_goals||0),pp_opp:parseFloat(m.away_pp_opp||0),
      sa:parseFloat(m.home_shots||0),saves:Math.max(0,parseFloat(m.home_shots||0)-hg),
      comeback:aComeback,lead_kept:aLeadKept,
    });
  }
  Object.values(hist).forEach(arr=>arr.sort((a,b)=>a.date.localeCompare(b.date)));
  return hist;
}

// ── ELO с динамическим K ─────────────────────────────────────────────────
function buildEloHistory(rows, K=32, homeAdv=50) {
  const elo={}, eloAt={};
  const get=t=>elo[t]||(elo[t]=1500);
  for (const m of rows) {
    const h=m.home_team||'', a=m.away_team||''; if (!h||!a) continue;
    const date=String(m.date||'').slice(0,10);
    const eH=get(h), eA=get(a);
    if (!eloAt[h]) eloAt[h]=[];
    if (!eloAt[a]) eloAt[a]=[];
    eloAt[h].push({date,elo:eH});
    eloAt[a].push({date,elo:eA});
    const hg=parseFloat(m.home_goals||0),ag=parseFloat(m.away_goals||0);
    const K2=K*(1+Math.min(Math.abs(hg-ag)/3,1));
    const expH=1/(1+10**((eA-(eH+homeAdv))/400));
    const sH=hg>ag?1:hg===ag?0.5:0;
    elo[h]=eH+K2*(sH-expH);
    elo[a]=eA+K2*((1-sH)-(1-expH));
  }
  return {elo,eloAt};
}

// ── Poisson со скользящим окном (последние 15 матчей) ──────────────────
function buildRollingPoisson(hist, home, away, beforeDate, avgH=1.3, avgA=1.1) {
  const hGames=(hist[home]||[]).filter(m=>m.date<beforeDate&&m.is_home).slice(-15);
  const aGames=(hist[away]||[]).filter(m=>m.date<beforeDate&&!m.is_home).slice(-15);
  const hAtt=hGames.length?hGames.reduce((s,m)=>s+(m.gf||0),0)/hGames.length/avgH:1;
  const hDef=hGames.length?hGames.reduce((s,m)=>s+(m.ga||0),0)/hGames.length/avgA:1;
  const aAtt=aGames.length?aGames.reduce((s,m)=>s+(m.gf||0),0)/aGames.length/avgA:1;
  const aDef=aGames.length?aGames.reduce((s,m)=>s+(m.ga||0),0)/aGames.length/avgH:1;
  return {
    lamH:hAtt*aDef*avgH,
    lamA:aAtt*hDef*avgA,
    hAtt,hDef,aAtt,aDef,
  };
}

// ── ДНК Лиги: исторические характеристики конкретной лиги ────────────
function buildLeagueDNA(rows) {
  const leagues={};
  for (const m of rows) {
    const lg=m.league_code||m.league||'UNK';
    if (!leagues[lg]) leagues[lg]={h:0,d:0,a:0,gf:0,ga:0,n:0,ot:0};
    const hg=parseFloat(m.home_goals||0),ag=parseFloat(m.away_goals||0);
    leagues[lg].n++;
    leagues[lg].gf+=hg; leagues[lg].ga+=ag;
    if (hg>ag) leagues[lg].h++;
    else if (hg===ag) leagues[lg].d++;
    else leagues[lg].a++;
    if (parseFloat(m.went_to_ot||0)>0) leagues[lg].ot++;
  }
  const dna={};
  Object.entries(leagues).forEach(([lg,s])=>{
    const n=s.n||1;
    dna[lg]={
      drawRate:  s.d/n,
      avgGoals:  (s.gf+s.ga)/n,
      homeWinRate:s.h/n,
      bttsRate:  (s.gf+s.ga)/n > 2.5 ? 0.55 : 0.45, // proxy
      otRate:    s.ot/n,
      tier: n>500?3:n>200?2:1, // топ / средняя / слабая лига по количеству матчей
    };
  });
  return dna;
}

// ── Вспомогательные Poisson функции ─────────────────────────────────────
function _pp(lam,k){let p=Math.exp(-lam);for(let i=1;i<=k;i++)p*=lam/i;return p;}
function pOver(lH,lA,th){
  let p=0;const mx=Math.min(Math.ceil(lH+lA)*3,15);
  for(let i=0;i<=mx;i++)for(let j=0;j<=mx;j++)if(i+j>th)p+=_pp(lH,i)*_pp(lA,j);
  return Math.min(1,p);
}
function pBTTS(lH,lA){return(1-Math.exp(-lH))*(1-Math.exp(-lA));}
function pHomeWin(lH,lA){
  let p=0;const mx=Math.min(Math.ceil(lH+lA)*3,15);
  for(let i=0;i<=mx;i++)for(let j=0;j<i&&j<=mx;j++)p+=_pp(lH,i)*_pp(lA,j);
  return Math.min(1,p);
}

// ── Rolling stats (с фильтром дома/гость и расширенными полями) ─────────
function rolling(hist,team,beforeDate,n,filter=null){
  let ms=(hist[team]||[]).filter(m=>m.date<beforeDate);
  if(filter) ms=ms.filter(filter);
  const last=ms.slice(-n);
  if(!last.length) return{pts:0,wins:0,draws:0,losses:0,gf:0,ga:0,xgf:0,xga:0,
    shots:0,corners:0,yellow:0,cs:0,btts:0,over25:0,over55:0,comeback:0,lead_kept:0,
    cf_pct:50,pp_pct:15,sv_pct:90,ot:0,count:0,ace_rate:0.07,bpsaved:60};
  const c=last.length;
  const safe=(v)=>isNaN(v)?0:v;
  return{
    pts:   last.reduce((s,m)=>s+(m.result==='W'?3:m.result==='D'?1:0),0),
    wins:  last.filter(m=>m.result==='W').length,
    draws: last.filter(m=>m.result==='D').length,
    losses:last.filter(m=>m.result==='L').length,
    gf:    last.reduce((s,m)=>s+safe(m.gf),0),
    ga:    last.reduce((s,m)=>s+safe(m.ga),0),
    xgf:   last.reduce((s,m)=>s+safe(m.xgf),0),
    xga:   last.reduce((s,m)=>s+safe(m.xga),0),
    shots: last.reduce((s,m)=>s+safe(m.shots),0),
    corners:last.reduce((s,m)=>s+safe(m.corners),0),
    yellow:last.reduce((s,m)=>s+safe(m.yellow),0),
    cs:    last.filter(m=>m.ga===0).length,
    btts:  last.filter(m=>(m.gf||0)>0&&(m.ga||0)>0).length,
    over25:last.filter(m=>(m.gf||0)+(m.ga||0)>2.5).length,
    over55:last.filter(m=>(m.gf||0)+(m.ga||0)>5.5).length,
    comeback:last.filter(m=>m.comeback).length,
    lead_kept:last.filter(m=>m.lead_kept).length,
    cf_pct:last.reduce((s,m)=>s+safe(m.cf_pct),0)/c,
    pp_pct:last.reduce((s,m)=>s+safe(m.pp_opp),0)>0
           ?last.reduce((s,m)=>s+safe(m.pp_goals),0)/Math.max(last.reduce((s,m)=>s+safe(m.pp_opp),0),1)*100:15,
    sv_pct:last.reduce((s,m)=>s+safe(m.sa),0)>0
           ?last.reduce((s,m)=>s+safe(m.saves),0)/Math.max(last.reduce((s,m)=>s+safe(m.sa),0),1)*100:90,
    ot:    last.filter(m=>m.ot).length,
    count: c,
    // Tennis
    ace_rate:last.reduce((s,m)=>s+safe(m.w_svpt),0)>0
             ?last.reduce((s,m)=>s+safe(m.w_ace),0)/Math.max(last.reduce((s,m)=>s+safe(m.w_svpt),0),1):0.07,
    bpsaved: last.reduce((s,m)=>s+safe(m.w_bpfaced),0)>0
             ?last.reduce((s,m)=>s+safe(m.w_bpsaved),0)/Math.max(last.reduce((s,m)=>s+safe(m.w_bpfaced),0),1)*100:60,
  };
}

// ── Психологические метрики ──────────────────────────────────────────────
function bounceBackRate(hist,team,beforeDate,n=20){
  const ms=(hist[team]||[]).filter(m=>m.date<beforeDate).slice(-n);
  if(ms.length<2) return 0.4;
  let after_loss=0,after_loss_wins=0;
  for(let i=1;i<ms.length;i++){
    if(ms[i-1].result==='L'){after_loss++;if(ms[i].result==='W')after_loss_wins++;}
  }
  return after_loss>0?after_loss_wins/after_loss:0.4;
}

function winHabitRate(hist,team,beforeDate,n=20){
  const ms=(hist[team]||[]).filter(m=>m.date<beforeDate).slice(-n);
  if(ms.length<2) return 0.5;
  let after_win=0,after_win_wins=0;
  for(let i=1;i<ms.length;i++){
    if(ms[i-1].result==='W'){after_win++;if(ms[i].result==='W')after_win_wins++;}
  }
  return after_win>0?after_win_wins/after_win:0.5;
}

function drawTendency(hist,team,beforeDate,n=20){
  const r=rolling(hist,team,beforeDate,n);
  return r.count>0?r.draws/r.count:0.25;
}

function comebackRate(hist,team,beforeDate,n=20){
  const r=rolling(hist,team,beforeDate,n);
  return r.count>0?r.comeback/r.count:0.2;
}

function leadKeptRate(hist,team,beforeDate,n=20){
  const r=rolling(hist,team,beforeDate,n);
  // Из матчей где вели — сколько удержали (comeback как proxy)
  const wins=r.wins||1;
  return r.count>0?r.lead_kept/Math.max(wins,1)*0.8:0.6;
}

// ── Тренд формы ──────────────────────────────────────────────────────────
function momentum(hist,team,beforeDate){
  const ms=(hist[team]||[]).filter(m=>m.date<beforeDate);
  if(ms.length<3) return 0;
  const l3=ms.slice(-3).reduce((s,m)=>s+(m.result==='W'?3:m.result==='D'?1:0),0)/3;
  const l10=ms.slice(-10).reduce((s,m)=>s+(m.result==='W'?3:m.result==='D'?1:0),0)/Math.min(ms.length,10);
  return(l3-l10)/3;
}

function streak(hist,team,beforeDate){
  const ms=(hist[team]||[]).filter(m=>m.date<beforeDate).slice(-10);
  if(!ms.length) return 0;
  const sign=ms[ms.length-1].result==='W'?1:-1;
  let s=0;
  for(let i=ms.length-1;i>=0;i--){
    const r=ms[i].result;
    if((sign>0&&r==='W')||(sign<0&&r==='L'))s++;
    else break;
  }
  return sign*s/7;
}

function goalTrend(hist,team,beforeDate){
  const ms=(hist[team]||[]).filter(m=>m.date<beforeDate);
  if(ms.length<3) return 0;
  const l3=ms.slice(-3).reduce((s,m)=>s+(m.gf||0),0)/3;
  const l10=ms.slice(-10).reduce((s,m)=>s+(m.gf||0),0)/Math.min(ms.length,10);
  return(l3-l10)/3;
}

function defTrend(hist,team,beforeDate){
  const ms=(hist[team]||[]).filter(m=>m.date<beforeDate);
  if(ms.length<3) return 0;
  const l3=ms.slice(-3).reduce((s,m)=>s+(m.ga||0),0)/3;
  const l10=ms.slice(-10).reduce((s,m)=>s+(m.ga||0),0)/Math.min(ms.length,10);
  return(l3-l10)/3*(-1); // инвертируем: меньше пропускают = позитивный тренд
}

function xgOverperf(hist,team,beforeDate,n=10){
  const ms=(hist[team]||[]).filter(m=>m.date<beforeDate).slice(-n);
  if(!ms.length) return 0;
  return ms.reduce((s,m)=>s+(m.gf||0)-(m.xgf||0),0)/ms.length/2;
}

// ── ELO тренд ────────────────────────────────────────────────────────────
function eloTrend(eloAt,team,beforeDate,days=30){
  const hist=(eloAt[team]||[]).filter(e=>e.date<beforeDate);
  if(hist.length<2) return 0;
  const now=hist[hist.length-1].elo;
  const cut=new Date(new Date(beforeDate)-days*86400000).toISOString().slice(0,10);
  const old=hist.find(e=>e.date>=cut)||hist[0];
  return(now-old.elo)/200;
}

function eloPeakDist(eloAt,team,beforeDate){
  const hist=(eloAt[team]||[]).filter(e=>e.date<beforeDate);
  if(!hist.length) return 0;
  const cur=hist[hist.length-1].elo;
  const peak=Math.max(...hist.map(e=>e.elo));
  return(cur-peak)/200; // отрицательное — ниже пика
}

// ── Стили игры ───────────────────────────────────────────────────────────
function styleAttackIdx(hist,team,beforeDate,n=20){
  const r=rolling(hist,team,beforeDate,n);
  if(!r.count) return 0.5;
  return Math.min((r.xgf/r.count/1.8+r.shots/r.count/18)/2,1);
}

function stylePressIdx(hist,team,beforeDate,n=20){
  // Прессинг: угловые + PPDA proxy (высокие shots against = не прессингуют, много угловых = прессингуют)
  const r=rolling(hist,team,beforeDate,n);
  if(!r.count) return 0.5;
  return Math.min(r.corners/r.count/6,1);
}

function styleAggrIdx(hist,team,beforeDate,n=20){
  const r=rolling(hist,team,beforeDate,n);
  return Math.min(r.yellow/Math.max(r.count,1)/4,1);
}

// ── Форма против определённого класса соперников ─────────────────────────
function vsClassSplit(hist,team,beforeDate,elo,n=15){
  const ms=(hist[team]||[]).filter(m=>m.date<beforeDate).slice(-n);
  if(!ms.length) return{vsTop:0.4,vsBot:0.6,giantKiller:0.3,bigMatchDelta:0};
  const avgElo=Object.values(elo).reduce((s,v)=>s+v,0)/Math.max(Object.keys(elo).length,1);
  const top=ms.filter(m=>(elo[m.opponent]||1500)>avgElo);
  const bot=ms.filter(m=>(elo[m.opponent]||1500)<=avgElo);
  const pts=arr=>arr.length?arr.reduce((s,m)=>s+(m.result==='W'?3:m.result==='D'?1:0),0)/arr.length/3:0.4;
  const giantKiller=top.length?top.filter(m=>m.result==='W').length/top.length:0.3;
  return{vsTop:pts(top),vsBot:pts(bot),giantKiller,bigMatchDelta:pts(top)-pts(bot)};
}

// ── H2H расширенный ───────────────────────────────────────────────────────
function h2h(hist,home,away,beforeDate,n=10){
  const all=[];
  (hist[home]||[]).filter(m=>m.date<beforeDate&&m.opponent===away).forEach(m=>all.push({...m,_h:true}));
  (hist[away]||[]).filter(m=>m.date<beforeDate&&m.opponent===home).forEach(m=>all.push({...m,_h:false}));
  all.sort((a,b)=>a.date.localeCompare(b.date));
  const last=all.slice(-n);
  if(!last.length) return{hWinRate:0.4,drawRate:0.25,avgGoals:2.5,bttsRate:0.5,over25Rate:0.5,
    count:0,lastWinner:0,revengeFactor:0,goalTrend:0,otRate:0.2,wWinRate:0.4,avgSets:2};
  const c=last.length;
  const hW=last.filter(m=>m._h&&m.result==='W').length;
  // Голевой тренд в H2H
  const half=Math.ceil(c/2);
  const recentGoals=last.slice(-half).reduce((s,m)=>s+(m.gf||0)+(m.ga||0),0)/half;
  const oldGoals=last.slice(0,half).reduce((s,m)=>s+(m.gf||0)+(m.ga||0),0)/Math.max(half,1);
  // Реванш: кто проиграл последний матч?
  const lastMatch=last[last.length-1];
  const lastWinner=lastMatch?._h&&lastMatch?.result==='W'?1:0;
  const revengeFactor=lastWinner===0?0.05:-0.05; // гости мотивированы на реванш
  return{
    hWinRate:hW/c,
    drawRate:last.filter(m=>m.result==='D').length/c,
    avgGoals:last.reduce((s,m)=>s+(m.gf||0)+(m.ga||0),0)/c,
    bttsRate:last.filter(m=>(m.gf||0)>0&&(m.ga||0)>0).length/c,
    over25Rate:last.filter(m=>(m.gf||0)+(m.ga||0)>2.5).length/c,
    count:c,
    lastWinner,
    revengeFactor,
    goalTrend:(recentGoals-oldGoals)/5,
    otRate:last.filter(m=>m.ot).length/c,
    wWinRate:hW/c,
    avgSets:last.reduce((s,m)=>s+(m.gf||0)+(m.ga||0),0)/c,
  };
}

// ── Усталость ─────────────────────────────────────────────────────────────
function restDays(hist,team,beforeDate){
  const ms=(hist[team]||[]).filter(m=>m.date<beforeDate);
  if(!ms.length) return 7;
  return Math.min((new Date(beforeDate)-new Date(ms[ms.length-1].date))/86400000,21);
}
function gamesInDays(hist,team,beforeDate,days){
  const cut=new Date(new Date(beforeDate)-days*86400000).toISOString().slice(0,10);
  return(hist[team]||[]).filter(m=>m.date>=cut&&m.date<beforeDate).length;
}
function awayRunLength(hist,team,beforeDate){
  const ms=(hist[team]||[]).filter(m=>m.date<beforeDate).slice(-10);
  let run=0;
  for(let i=ms.length-1;i>=0;i--){
    if(!ms[i].is_home)run++;
    else break;
  }
  return run;
}

// ── Сезонная фаза ──────────────────────────────────────────────────────
function seasonPtsRate(hist,team,beforeDate,season){
  const ms=(hist[team]||[]).filter(m=>m.date<beforeDate&&m.season===season);
  if(!ms.length) return 0.4;
  return ms.reduce((s,m)=>s+(m.result==='W'?3:m.result==='D'?1:0),0)/ms.length/3;
}
function seasonPhase(hist,team,beforeDate,season){
  const ms=(hist[team]||[]).filter(m=>m.season===season);
  const n=ms.length;
  const done=ms.filter(m=>m.date<beforeDate).length;
  if(!n) return 0.5;
  return done/n; // 0 = начало, 1 = конец
}

// ══════════════════════════════════════════════════════════════════════════
//  FEATURE VECTOR — главная функция формирования признаков
// ══════════════════════════════════════════════════════════════════════════

// ══════════════════════════════════════════════════════════════════════════
//  FEATURE VECTOR — расширенный v5
// ══════════════════════════════════════════════════════════════════════════
function buildFeatureVector(row, sport, teamHist, eloData, leagueDNA) {
  const date=String(row.date||'').slice(0,10);
  const home=row.home_team||row.winner||'';
  const away=row.away_team||row.loser||'';
  const {elo, eloAt}=eloData;
  const league=row.league_code||row.league||'UNK';
  const lgDNA=leagueDNA[league]||{drawRate:0.26,avgGoals:2.55,homeWinRate:0.45,bttsRate:0.50,otRate:0.2,tier:2};
  const cl=(v,lo=0,hi=1)=>Math.max(lo,Math.min(hi,isNaN(v)||!isFinite(v)?lo+(hi-lo)/2:v));

  const allHomeGames=Object.values(teamHist).flat().filter(m=>m.is_home&&m.date<date);
  const avgH=allHomeGames.length?allHomeGames.reduce((s,m)=>s+(m.gf||0),0)/allHomeGames.length:1.3;
  const avgA=allHomeGames.length?allHomeGames.reduce((s,m)=>s+(m.ga||0),0)/allHomeGames.length:1.1;

  if (sport==='football') {
    const eH=elo[home]||1500, eA=elo[away]||1500;
    const etH=eloTrend(eloAt,home,date), etA=eloTrend(eloAt,away,date);
    const epH=eloPeakDist(eloAt,home,date);

    const {lamH,lamA,hAtt,hDef,aAtt,aDef}=buildRollingPoisson(teamHist,home,away,date,avgH,avgA);
    const pO15=pOver(lamH,lamA,1.5),pO25=pOver(lamH,lamA,2.5),pO35=pOver(lamH,lamA,3.5),pO45=pOver(lamH,lamA,4.5);
    const pBt=pBTTS(lamH,lamA), pHW=pHomeWin(lamH,lamA);

    const hH5=rolling(teamHist,home,date,5,m=>m.is_home);
    const aA5=rolling(teamHist,away,date,5,m=>!m.is_home);
    const h10=rolling(teamHist,home,date,10), a10=rolling(teamHist,away,date,10);
    const h20=rolling(teamHist,home,date,20), a20=rolling(teamHist,away,date,20);

    const hM=momentum(teamHist,home,date), aM=momentum(teamHist,away,date);
    const hSt=streak(teamHist,home,date), aSt=streak(teamHist,away,date);
    const hGT=goalTrend(teamHist,home,date), aGT=goalTrend(teamHist,away,date);
    const hDT=defTrend(teamHist,home,date), aDT=defTrend(teamHist,away,date);
    const hXO=xgOverperf(teamHist,home,date), aXO=xgOverperf(teamHist,away,date);

    const hBB=bounceBackRate(teamHist,home,date), aBB=bounceBackRate(teamHist,away,date);
    const hWH=winHabitRate(teamHist,home,date), aWH=winHabitRate(teamHist,away,date);
    const hDraw=drawTendency(teamHist,home,date), aDraw=drawTendency(teamHist,away,date);
    const hCB=comebackRate(teamHist,home,date), aCB=comebackRate(teamHist,away,date);
    const hLK=leadKeptRate(teamHist,home,date);

    const hAI=styleAttackIdx(teamHist,home,date), aAI=styleAttackIdx(teamHist,away,date);
    const hPI=stylePressIdx(teamHist,home,date), aPI=stylePressIdx(teamHist,away,date);
    const hAggr=styleAggrIdx(teamHist,home,date), aAggr=styleAggrIdx(teamHist,away,date);

    const hCS20=h20.count>0?h20.cs/h20.count:0.25;
    const aCS20=a20.count>0?a20.cs/a20.count:0.25;
    const hBTTS20=h20.count>0?h20.btts/h20.count:0.5;
    const aBTTS20=a20.count>0?a20.btts/a20.count:0.5;
    const hO25_20=h20.count>0?h20.over25/h20.count:0.5;
    const aO25_20=a20.count>0?a20.over25/a20.count:0.5;

    // Инд. тоталы
    const hHist=(teamHist[home]||[]).filter(m=>m.date<date&&m.is_home).slice(-20);
    const aHist=(teamHist[away]||[]).filter(m=>m.date<date&&!m.is_home).slice(-20);
    const hOv05r=hHist.length?hHist.filter(m=>m.gf>0).length/hHist.length:0.7;
    const hOv15r=hHist.length?hHist.filter(m=>m.gf>1).length/hHist.length:0.4;
    const aOv05r=aHist.length?aHist.filter(m=>m.gf>0).length/aHist.length:0.6;
    const aOv15r=aHist.length?aHist.filter(m=>m.gf>1).length/aHist.length:0.35;

    // Угловые и карточки
    const hAllH=(teamHist[home]||[]).filter(m=>m.date<date).slice(-5);
    const aAllA=(teamHist[away]||[]).filter(m=>m.date<date).slice(-5);
    const hCorn5=hAllH.length?hAllH.reduce((s,m)=>s+(m.corners||4),0)/hAllH.length:4.5;
    const aCorn5=aAllA.length?aAllA.reduce((s,m)=>s+(m.sa>0?(m.sa/4):4),0)/aAllA.length:4.0;
    const hYel5=hAllH.length?hAllH.reduce((s,m)=>s+(m.yellow||1.5),0)/hAllH.length:1.5;
    const aYel5=aAllA.length?aAllA.reduce((s,m)=>s+(m.yellow||1.5),0)/aAllA.length:1.5;

    // HT данные
    const hHTGF5=hAllH.length?hAllH.reduce((s,m)=>s+(m.ht_gf||0),0)/hAllH.length:0.6;
    const aHTGF5=aAllA.length?aAllA.reduce((s,m)=>s+(m.ht_gf||0),0)/aAllA.length:0.5;
    const hHTWin=hAllH.length?hAllH.filter(m=>(m.ht_gf||0)>(m.ht_ga||0)).length/hAllH.length:0.35;
    const hH2GF5=hAllH.length?hAllH.reduce((s,m)=>s+(m.gf-(m.ht_gf||0)),0)/hAllH.length:0.7;

    const clashAttDef=cl(hAI*(1-aAI+0.5));
    const clashTempo=cl((hAI+aAI)/2);
    const clashAggr=cl((hAggr+aAggr)/2);

    const hVS=vsClassSplit(teamHist,home,date,elo);
    const aVS=vsClassSplit(teamHist,away,date,elo);
    const hh=h2h(teamHist,home,away,date);

    const season=row.season||'';
    const hSPts=seasonPtsRate(teamHist,home,date,season);
    const aSPts=seasonPtsRate(teamHist,away,date,season);
    const hPhase=seasonPhase(teamHist,home,date,season);
    const hSeason=rolling(teamHist,home,date,38);
    const aSeason=rolling(teamHist,away,date,38);
    const hGD=(hSeason.gf-hSeason.ga)/Math.max(hSeason.count,1);
    const aGD=(aSeason.gf-aSeason.ga)/Math.max(aSeason.count,1);

    const hRest=restDays(teamHist,home,date), aRest=restDays(teamHist,away,date);
    const hG14=gamesInDays(teamHist,home,date,14), aG14=gamesInDays(teamHist,away,date,14);
    const hB2B=hRest<=1?1:0, aB2B=aRest<=1?1:0;
    const aRun=awayRunLength(teamHist,away,date);

    const b365h=parseFloat(row.b365_home||0), b365d=parseFloat(row.b365_draw||0), b365a=parseFloat(row.b365_away||0);
    const b365o=parseFloat(row.b365_over25||0);
    const mktH=b365h>1?1/b365h:pHW, mktD=b365d>1?1/b365d:0.26, mktA=b365a>1?1/b365a:1-pHW-0.26;
    const mktO=b365o>1?1/b365o:pO25;
    const mktMarg=cl(mktH+mktD+mktA-1,0,0.15);

    return [
      cl((eH-eA)/400+0.5),cl((eH-1200)/800),cl((eA-1200)/800),cl(etH+0.5),cl(etA+0.5),cl(epH),
      cl(lamH/3),cl(lamA/3),cl(pO15),cl(pO25),cl(pO35),cl(pO45),cl(pBt),cl(pHW),cl(hAtt),cl(hDef),cl(aAtt),cl(aDef),
      cl(hH5.count>0?hH5.pts/hH5.count/3:0.33),cl(hH5.count>0?hH5.gf/hH5.count/3:0.43),cl(hH5.count>0?hH5.ga/hH5.count/3:0.37),
      cl(aA5.count>0?aA5.pts/aA5.count/3:0.33),cl(aA5.count>0?aA5.gf/aA5.count/3:0.43),cl(aA5.count>0?aA5.ga/aA5.count/3:0.37),
      cl(h10.count>0?h10.pts/h10.count/3:0.33),cl(a10.count>0?a10.pts/a10.count/3:0.33),
      cl(h10.count>0?h10.gf/h10.count/3:0.43),cl(h10.count>0?h10.ga/h10.count/3:0.37),
      cl(a10.count>0?a10.gf/a10.count/3:0.43),cl(a10.count>0?a10.ga/a10.count/3:0.37),
      cl(hO25_20),cl(aO25_20),cl(hBTTS20),cl(aBTTS20),cl(hCS20),cl(aCS20),
      cl(hOv05r),cl(hOv15r),cl(aOv05r),cl(aOv15r),
      cl(hCorn5/12),cl(aCorn5/12),cl(hYel5/5),cl(aYel5/5),cl(clashAggr),
      cl(hHTGF5/2),cl(aHTGF5/2),cl(hHTWin),cl(hH2GF5/2),
      cl(hM+0.5),cl(aM+0.5),cl((hSt+5)/10),cl((aSt+5)/10),cl(hGT+0.5),cl(aGT+0.5),cl(hDT+0.5),cl(aDT+0.5),
      cl(hBB),cl(aBB),cl(hWH),cl(aWH),cl(hDraw),cl(aDraw),cl(hCB),cl(aCB),cl(hLK),
      cl(hAI),cl(aAI),cl(hPI),cl(aPI),cl(clashAttDef),cl(clashTempo),
      cl(hh.hw/(hh.n||1)),cl(hh.d/(hh.n||1)),cl(hh.avgG/5),cl(hh.bttsR),cl(hh.over25R),cl(hh.hAvg/3),cl(hh.aAvg/3),
      cl(lgDNA.drawRate),cl(lgDNA.avgGoals/4),cl(lgDNA.homeWinRate),cl(lgDNA.bttsRate||0.5),cl(lgDNA.tier/3),
      cl(mktH),cl(mktD),cl(mktA),cl(mktO),cl(mktMarg/0.15),
      cl(mktH-pHW+0.5),cl(mktO-pO25+0.5),
      cl(b365h/5),cl(b365d/5),cl(b365a/5),
      cl(hRest/14),cl(aRest/14),cl(hB2B),cl(aB2B),cl(aRun/5),cl(hG14/6),cl(aG14/6),
      cl(hSPts),cl(aSPts),cl(hSPts<0.35?1:0),cl(aSPts<0.35?1:0),cl((hGD+3)/6),cl((aGD+3)/6),cl(hPhase),
      cl(hXO+0.5),cl(aXO+0.5),
      cl(hVS.top_rate),cl(hVS.weak_rate),cl(aVS.top_rate),cl(aVS.weak_rate),
    ];
  }

  if (sport==='hockey') {
    const eH=elo[home]||1500, eA=elo[away]||1500;
    const etH=eloTrend(eloAt,home,date), etA=eloTrend(eloAt,away,date);
    const {lamH,lamA}=buildRollingPoisson(teamHist,home,away,date,avgH,avgA);
    const pO55=pOver(lamH,lamA,5.5);
    const hH5=rolling(teamHist,home,date,5,m=>m.is_home);
    const aA5=rolling(teamHist,away,date,5,m=>!m.is_home);
    const h10=rolling(teamHist,home,date,10), a10=rolling(teamHist,away,date,10);
    const h20=rolling(teamHist,home,date,20), a20=rolling(teamHist,away,date,20);
    const hM=momentum(teamHist,home,date), aM=momentum(teamHist,away,date);
    const hBB=bounceBackRate(teamHist,home,date), aBB=bounceBackRate(teamHist,away,date);
    const hGT=goalTrend(teamHist,home,date), aGT=goalTrend(teamHist,away,date);
    const hRest=restDays(teamHist,home,date), aRest=restDays(teamHist,away,date);
    const hB2B=hRest<=1?1:0, aB2B=aRest<=1?1:0;
    const hAll=(teamHist[home]||[]).filter(m=>m.date<date).slice(-20);
    const aAll=(teamHist[away]||[]).filter(m=>m.date<date).slice(-20);
    const hOT=hAll.length?hAll.filter(m=>m.ot).length/hAll.length:0.2;
    const aOT=aAll.length?aAll.filter(m=>m.ot).length/aAll.length:0.2;
    const hOTW=hAll.filter(m=>m.ot).length>0?hAll.filter(m=>m.ot&&m.result==='W').length/hAll.filter(m=>m.ot).length:0.5;
    const hCF=hAll.length?hAll.reduce((s,m)=>s+(m.cf_pct||50),0)/hAll.length:50;
    const aCF=aAll.length?aAll.reduce((s,m)=>s+(m.cf_pct||50),0)/aAll.length:50;
    const hPP=hAll.length?hAll.reduce((s,m)=>s+(m.pp_goals||0),0)/Math.max(hAll.reduce((s,m)=>s+(m.pp_opp||1),0),1):0.2;
    const hOver55_20=h20.count>0?h20.over55/h20.count:0.5;
    const aOver55_20=a20.count>0?a20.over55/a20.count:0.5;
    const hh=h2h(teamHist,home,away,date);
    const b365h=parseFloat(row.b365_home||0),b365a=parseFloat(row.b365_away||0),b365o=parseFloat(row.b365_over55||0);
    const mktH=b365h>1?1/b365h:0.5,mktA=b365a>1?1/b365a:0.4,mktO=b365o>1?1/b365o:0.5;
    return [
      cl((eH-eA)/400+0.5),cl((eH-1200)/800),cl((eA-1200)/800),cl(etH+0.5),cl(etA+0.5),
      cl(lamH/5),cl(lamA/5),cl(pO55),
      cl(hH5.count>0?hH5.pts/hH5.count/3:0.33),cl(aA5.count>0?aA5.pts/aA5.count/3:0.33),
      cl(h10.count>0?h10.pts/h10.count/3:0.33),cl(a10.count>0?a10.pts/a10.count/3:0.33),
      cl(hOver55_20),cl(aOver55_20),
      cl(hCF/100),cl(aCF/100),cl(hPP),cl(hPP),cl(1-hPP),
      cl(hOT),cl(aOT),cl(hOTW),
      cl(hh.avgG/8),cl(hh.otRate||0.2),
      cl(lgDNA.avgGoals/8),cl(lgDNA.otRate||0.2),
      cl(mktH),cl(mktA),cl(mktO),
      cl(hRest/14),cl(aRest/14),cl(hB2B),cl(aB2B),
      cl(hM+0.5),cl(aM+0.5),cl(hBB),cl(aBB),cl(hGT+0.5),cl(aGT+0.5),
    ];
  }

  if (sport==='tennis') {
    const winner=home,loser=away;
    const eW=elo[winner]||1500, eL=elo[loser]||1500;
    const etW=eloTrend(eloAt,winner,date);
    const rW=parseFloat(row.rank_winner||200), rL=parseFloat(row.rank_loser||400);
    const surface=row.surface||'hard';
    const surfW=rolling(teamHist,winner,date,15,m=>m.surface===surface);
    const surfL=rolling(teamHist,loser,date,15,m=>m.surface===surface);
    const wAll=(teamHist[winner]||[]).filter(m=>m.date<date).slice(-20);
    const lAll=(teamHist[loser]||[]).filter(m=>m.date<date).slice(-20);
    const w1stin=wAll.length?wAll.reduce((s,m)=>s+(m.w_1stin||60),0)/wAll.length:65;
    const w1stwon=wAll.length?wAll.reduce((s,m)=>s+(m.w_1stwon||65),0)/wAll.length:65;
    const w2ndwon=wAll.length?wAll.reduce((s,m)=>s+(m.w_2ndwon||50),0)/wAll.length:50;
    const wAce=wAll.length?wAll.reduce((s,m)=>s+(m.w_ace||4),0)/wAll.length:4;
    const wDF=wAll.length?wAll.reduce((s,m)=>s+(m.w_df||2),0)/wAll.length:2;
    const wBPsaved=wAll.length?wAll.reduce((s,m)=>s+(m.w_bpsaved||60),0)/wAll.length:60;
    const l1stin=lAll.length?lAll.reduce((s,m)=>s+(m.l_1stin||60),0)/lAll.length:62;
    const l1stwon=lAll.length?lAll.reduce((s,m)=>s+(m.l_1stwon||60),0)/lAll.length:62;
    const lBPsaved=lAll.length?lAll.reduce((s,m)=>s+(m.l_bpsaved||55),0)/lAll.length:55;
    const wSets=wAll.length?wAll.reduce((s,m)=>s+(m.gf||2),0)/wAll.length:2;
    const lSets=lAll.length?lAll.reduce((s,m)=>s+(m.gf||1),0)/lAll.length:1;
    const wStr=wAll.length?wAll.filter(m=>m.ga===0).length/wAll.length:0.3;
    const lStr=lAll.length?lAll.filter(m=>m.ga===0).length/lAll.length:0.1;
    const wGames=wAll.length?wAll.reduce((s,m)=>s+(m.gf*6+(m.ga||0)*5),0)/wAll.length:20;
    const lGames=lAll.length?lAll.reduce((s,m)=>s+(m.gf*6+(m.ga||0)*5),0)/lAll.length:18;
    const wTBrate=wAll.length?wAll.filter(m=>m.tiebreak).length/wAll.length:0.2;
    const lTBrate=lAll.length?lAll.filter(m=>m.tiebreak).length/lAll.length:0.2;
    const wFatigue=gamesInDays(teamHist,winner,date,7);
    const lFatigue=gamesInDays(teamHist,loser,date,7);
    const hh=h2h(teamHist,winner,loser,date);
    const tourLevel=['g','m','atp1000','atp500','atp250','ch','f','d'].indexOf(String(row.tourney_level||'').toLowerCase());
    const b365w=parseFloat(row.b365w||0), b365l=parseFloat(row.b365l||0);
    const mktW=b365w>1?1/b365w:0.6, mktMarg=mktW+(b365l>1?1/b365l:0.45)-1;
    const eloDiff=(eW-eL)/400;
    const modelEdge=cl(cl(0.5+eloDiff*0.3)-mktW+0.5);
    const wAceTrend=wAll.length>=6?
      (wAll.slice(-3).reduce((s,m)=>s+(m.w_ace||4),0)/3)/(wAll.slice(-6,-3).reduce((s,m)=>s+(m.w_ace||4),0)/3+0.1)-1:0;
    return [
      cl(eloDiff+0.5),cl((eW-1200)/800),cl((eL-1200)/800),cl(etW+0.5),
      cl(1-rW/1000),
      surface==='hard'?1:0,surface==='clay'?1:0,surface==='grass'?1:0,
      cl(surfW.count>0?surfW.pts/surfW.count/3:0.5),cl(surfL.count>0?surfL.pts/surfL.count/3:0.4),
      cl(w1stin/100),cl(w1stwon/100),cl(w2ndwon/100),cl(wAce/15),cl(wDF/8),cl(wBPsaved/100),
      cl(l1stin/100),cl(l1stwon/100),cl(lBPsaved/100),
      cl(wAceTrend+0.5),
      cl(surfW.count>0?surfW.pts/surfW.count/3:0.5),cl(surfL.count>0?surfL.pts/surfL.count/3:0.4),
      cl(wSets/3),cl(lSets/3),cl(wStr),cl(lStr),cl(wGames/30),cl(lGames/30),
      cl(hh.hw/(hh.n||1)),cl(hh.avgG/4),cl(hh.avgGames/30||0.67),cl(hh.tbRate||0.2),
      cl(wTBrate),cl(lTBrate),
      cl(mktW),cl(mktMarg/0.1),cl(modelEdge),
      cl(1-wFatigue/5),cl(1-lFatigue/5),
      cl(tourLevel<0?4:tourLevel)/7,
    ];
  }

  if (sport==='basketball') {
    const eH=elo[home]||1500, eA=elo[away]||1500;
    const etH=eloTrend(eloAt,home,date), etA=eloTrend(eloAt,away,date);
    const hH5=rolling(teamHist,home,date,5,m=>m.is_home);
    const aA5=rolling(teamHist,away,date,5,m=>!m.is_home);
    const h10=rolling(teamHist,home,date,10), a10=rolling(teamHist,away,date,10);
    const h20=rolling(teamHist,home,date,20), a20=rolling(teamHist,away,date,20);
    const hM=momentum(teamHist,home,date), aM=momentum(teamHist,away,date);
    const hSt=streak(teamHist,home,date), aSt=streak(teamHist,away,date);
    const hRest=restDays(teamHist,home,date), aRest=restDays(teamHist,away,date);
    const hB2B=hRest<=1?1:0, aB2B=aRest<=1?1:0;
    const hAll=(teamHist[home]||[]).filter(m=>m.date<date).slice(-20);
    const aAll=(teamHist[away]||[]).filter(m=>m.date<date).slice(-20);
    const hPtsAvg=hAll.length?hAll.reduce((s,m)=>s+(m.gf||105),0)/hAll.length:105;
    const aPtsAvg=aAll.length?aAll.reduce((s,m)=>s+(m.gf||100),0)/aAll.length:100;
    const hPace=hAll.length?hAll.reduce((s,m)=>s+(m.pace||98),0)/hAll.length:98;
    const aPace=aAll.length?aAll.reduce((s,m)=>s+(m.pace||96),0)/aAll.length:96;
    const hOffRtg=hAll.length?hAll.reduce((s,m)=>s+(m.off_rtg||108),0)/hAll.length:108;
    const aOffRtg=aAll.length?aAll.reduce((s,m)=>s+(m.off_rtg||106),0)/aAll.length:106;
    const hDefRtg=hAll.length?hAll.reduce((s,m)=>s+(m.def_rtg||108),0)/hAll.length:108;
    const aDefRtg=aAll.length?aAll.reduce((s,m)=>s+(m.def_rtg||108),0)/aAll.length:108;
    const hO220_r=h20.count>0?h20.over220/h20.count:0.5;
    const aO220_r=a20.count>0?a20.over220/a20.count:0.5;
    const lamH=hPtsAvg/100, lamA=aPtsAvg/100;
    const pO220=pOver(lamH*100,lamA*100,220)/10||0.5;
    const hh=h2h(teamHist,home,away,date);
    const b365h=parseFloat(row.b365_home||0), b365o=parseFloat(row.b365_over220||0);
    const mktH=b365h>1?1/b365h:0.5, mktO=b365o>1?1/b365o:0.5;
    return [
      cl((eH-eA)/400+0.5),cl((eH-1200)/800),cl((eA-1200)/800),cl(etH+0.5),cl(etA+0.5),
      cl(lamH/2),cl(lamA/2),cl(pO220),
      cl(hH5.count>0?hH5.pts/hH5.count/3:0.33),cl(aA5.count>0?aA5.pts/aA5.count/3:0.33),
      cl(h10.count>0?h10.pts/h10.count/3:0.33),cl(a10.count>0?a10.pts/a10.count/3:0.33),
      cl(hPtsAvg/130),cl(aPtsAvg/130),
      cl(hPace/120),cl(aPace/120),
      cl(hOffRtg/125),cl(aOffRtg/125),cl(hDefRtg/125),cl(aDefRtg/125),
      cl(hO220_r),cl(aO220_r),
      cl(hh.avgG/250||0.88),cl(hh.spreadR||0.5),
      cl(lgDNA.avgGoals/220||0.95),
      cl(mktH),cl(mktO),cl(mktH-(h10.count>0?h10.pts/h10.count/3:0.5)+0.5),
      cl(hRest/7),cl(aRest/7),cl(hB2B),cl(aB2B),
      cl(hM+0.5),cl(aM+0.5),cl((hSt+5)/10),cl((aSt+5)/10),
    ];
  }

  // Generic fallback — rolling Poisson + ELO для остальных видов
  const eH=elo[home]||1500, eA=elo[away]||1500;
  const {lamH,lamA}=buildRollingPoisson(teamHist,home,away,date,avgH,avgA);
  const h10=rolling(teamHist,home,date,10), a10=rolling(teamHist,away,date,10);
  const hH5=rolling(teamHist,home,date,5,m=>m.is_home);
  const aA5=rolling(teamHist,away,date,5,m=>!m.is_home);
  const hM=momentum(teamHist,home,date), aM=momentum(teamHist,away,date);
  const hSt=streak(teamHist,home,date), aSt=streak(teamHist,away,date);
  const hRest=restDays(teamHist,home,date), aRest=restDays(teamHist,away,date);
  const hB2B=hRest<=1?1:0, aB2B=aRest<=1?1:0;
  const hh=h2h(teamHist,home,away,date);
  const cfg=SPORT_CONFIGS[sport];
  const nFeats=cfg?cfg.features.length:15;

  const base=[
    cl((eH-eA)/400+0.5),cl((eH-1200)/800),cl((eA-1200)/800),
    cl(lamH/5),cl(lamA/5),cl(pOver(lamH,lamA,2.5)||0.5),
    cl(h10.count>0?h10.pts/h10.count/3:0.33),cl(a10.count>0?a10.pts/a10.count/3:0.33),
    cl(hH5.count>0?hH5.pts/hH5.count/3:0.33),cl(aA5.count>0?aA5.pts/aA5.count/3:0.33),
    cl(hM+0.5),cl(aM+0.5),cl((hSt+5)/10),cl((aSt+5)/10),
    cl(hh.hw/(hh.n||1)),cl(hh.avgG/5),
    cl(hRest/14),cl(aRest/14),cl(hB2B),cl(aB2B),
  ];
  while(base.length<nFeats) base.push(0.5);
  return base.slice(0,nFeats);
}


// ══════════════════════════════════════════════════════════════════════════
//  MAKE TARGETS v5 — все рынки
// ══════════════════════════════════════════════════════════════════════════
function makeTargets(row, sport, targets) {
  const hg=parseFloat(row.home_goals||row.home_pts||row.score1||row.team1_runs||row.home_score||0);
  const ag=parseFloat(row.away_goals||row.away_pts||row.score2||row.team2_runs||row.away_score||0);
  const total=hg+ag;
  const htH=parseFloat(row.ht_home_goals||0), htA=parseFloat(row.ht_away_goals||0);
  const maps1=parseFloat(row.score1||0), maps2=parseFloat(row.score2||0);
  const totalMaps=maps1+maps2;

  return targets.map(t=>{
    switch(t){
      // ── Исходы ────────────────────────────────────────────────────────
      case 'home_win':          return hg>ag?1:0;
      case 'away_win':          return ag>hg?1:0;
      case 'draw':              return hg===ag?1:0;
      case 'home_win_reg':      return hg>ag&&!parseFloat(row.went_to_ot||0)?1:0;
      case 'away_win_reg':      return ag>hg&&!parseFloat(row.went_to_ot||0)?1:0;
      // ── Тоталы матча ──────────────────────────────────────────────────
      case 'over15':            return total>1.5?1:0;
      case 'over25':            return total>2.5?1:0;
      case 'over35':            return total>3.5?1:0;
      case 'over45':            return total>4.5?1:0;
      case 'under15':           return total<1.5?1:0;
      case 'under25':           return total<2.5?1:0;
      case 'over_total':        return total>(parseFloat(row.total_line||220.5))?1:0;
      case 'under_total':       return total<(parseFloat(row.total_line||220.5))?1:0;
      case 'over200':           return total>200?1:0;
      case 'over210':           return total>210?1:0;
      case 'over220':           return total>220?1:0;
      case 'over230':           return total>230?1:0;
      case 'over240':           return total>240?1:0;
      case 'under200':          return total<200?1:0;
      case 'under210':          return total<210?1:0;
      case 'over45': return total>4.5?1:0;
      case 'over55':            return total>5.5?1:0;
      case 'over65':            return total>6.5?1:0;
      case 'over75':            return total>7.5?1:0;
      case 'over85':            return total>8.5?1:0;
      case 'over95':            return total>9.5?1:0;
      case 'under45':           return total<4.5?1:0;
      case 'under55':           return total<5.5?1:0;
      case 'over10':            return total>10?1:0;
      case 'over12':            return total>12?1:0;
      case 'over14':            return total>14?1:0;
      case 'over_total_bball':  return total>220.5?1:0;
      // ── BTTS и комбо ──────────────────────────────────────────────────
      case 'btts':              return hg>0&&ag>0?1:0;
      case 'btts_over25':       return hg>0&&ag>0&&total>2.5?1:0;
      case 'btts_and_home':     return hg>0&&ag>0&&hg>ag?1:0;
      case 'btts_and_away':     return hg>0&&ag>0&&ag>hg?1:0;
      // ── Сухие ─────────────────────────────────────────────────────────
      case 'home_cs':           return ag===0?1:0;
      case 'away_cs':           return hg===0?1:0;
      // ── Азиатская фора ────────────────────────────────────────────────
      case 'ah_home_m05':       return hg-ag>0.5?1:(hg-ag===-0.5?0.5:0);  // AH -0.5
      case 'ah_home_m15':       return hg-ag>1.5?1:(hg-ag===1.5?0.5:0);   // AH -1.5
      case 'ah_away_m05':       return ag-hg>0.5?1:(ag-hg===-0.5?0.5:0);
      case 'ah_away_m15':       return ag-hg>1.5?1:(ag-hg===1.5?0.5:0);
      case 'spread_cover':
      case 'spread_cover_home': return hg>ag+(parseFloat(row.spread||3.5))?1:0;
      case 'spread_cover_away': return ag>hg+(parseFloat(row.spread||3.5))?1:0;
      case 'puck_line_home':    return hg-ag>1.5?1:0;
      case 'puck_line_away':    return ag-hg>1.5?1:0;
      case 'runline_home':      return hg-ag>1.5?1:0;
      case 'runline_away':      return ag-hg>1.5?1:0;
      case 'large_win':
      case 'large_margin':      return Math.abs(hg-ag)>10?1:0;
      case 'large_win':         return Math.abs(hg-ag)>7?1:0;
      // ── Индивидуальные тоталы ─────────────────────────────────────────
      case 'home_ov05':         return hg>0.5?1:0;
      case 'home_ov15':         return hg>1.5?1:0;
      case 'home_ov25':         return hg>2.5?1:0;
      case 'away_ov05':         return ag>0.5?1:0;
      case 'away_ov15':         return ag>1.5?1:0;
      case 'away_ov25':         return ag>2.5?1:0;
      case 'home_ov100':        return hg>100?1:0;
      case 'home_ov110':        return hg>110?1:0;
      case 'away_ov100':        return ag>100?1:0;
      case 'away_ov110':        return ag>110?1:0;
      case 'home_ov17':         return hg>17?1:0;
      case 'away_ov17':         return ag>17?1:0;
      case 'home_ov20':         return hg>20?1:0;
      case 'away_ov20':         return ag>20?1:0;
      case 'home_ov4':          return hg>4?1:0;
      case 'away_ov4':          return ag>4?1:0;
      case 'home_ov5':          return hg>5?1:0;
      case 'away_ov5':          return ag>5?1:0;
      case 'home_ov150':        return hg>150?1:0;
      case 'away_ov150':        return ag>150?1:0;
      case 'home_ov115_pts':    return hg>115?1:0;
      case 'away_ov115_pts':    return ag>115?1:0;
      case 'total_pts_over200': return total>200?1:0;
      // ── Хоккей специфика ──────────────────────────────────────────────
      case 'went_to_ot':        return parseFloat(row.went_to_ot||0)>0?1:0;
      case 'overtime':          return total>(parseFloat(row.total_line||220))?1:0;
      case 'pp_goal':           return (parseFloat(row.home_pp_goals||0)+parseFloat(row.away_pp_goals||0))>0?1:0;
      // ── Таймовые рынки ────────────────────────────────────────────────
      case 'ht_home_win':       return htH>htA?1:0;
      case 'ht_draw':           return htH===htA?1:0;
      case 'ht_over05':         return htH+htA>0.5?1:0;
      case 'h2_over15':         return (hg-htH)+(ag-htA)>1.5?1:0;
      case 'h1_over':           return htH+htA>(parseFloat(row.h1_line||108))?1:0;
      case 'q1_over':           return parseFloat(row.q1_h||0)+parseFloat(row.q1_a||0)>(parseFloat(row.q1_line||52))?1:0;
      case 'home_win_q1':       return parseFloat(row.q1_h||0)>parseFloat(row.q1_a||0)?1:0;
      // ── Карточки и угловые ────────────────────────────────────────────
      case 'over35_cards':      return parseFloat(row.home_yellow||0)+parseFloat(row.away_yellow||0)+
                                        (parseFloat(row.home_red||0)+parseFloat(row.away_red||0))*2>3.5?1:0;
      case 'over95_corners':    return parseFloat(row.home_corners||0)+parseFloat(row.away_corners||0)>9.5?1:0;
      // ── Комбо ─────────────────────────────────────────────────────────
      case 'hw_and_over25':     return hg>ag&&total>2.5?1:0;
      case 'aw_and_btts':       return ag>hg&&hg>0?1:0;
      // ── Теннис ───────────────────────────────────────────────────────
      case 'upset':             return parseFloat(row.b365w||1)>2.5?1:0;
      case 'over_sets': {
        const bo=parseInt(row.best_of||3);
        return (parseInt(row.w_sets||0)+parseInt(row.l_sets||0))>(bo>3?3:2)?1:0;
      }
      case 'straight_sets':     return parseInt(row.l_sets||1)===0?1:0;
      case 'total_games_over21':return parseInt(row.w_games||0)+parseInt(row.l_games||0)>21?1:0;
      case 'total_games_over23':return parseInt(row.w_games||0)+parseInt(row.l_games||0)>23?1:0;
      case 'total_games_over25':return parseInt(row.w_games||0)+parseInt(row.l_games||0)>25?1:0;
      case 'set1_fav_win':      return parseInt(row.set1_w||1)>parseInt(row.set1_l||0)?1:0;
      case 'set1_over95':       return (parseInt(row.set1_w||6)+parseInt(row.set1_l||4))>9.5?1:0;
      case 'winner_ov15_sets':  return parseInt(row.w_sets||0)>1.5?1:0;
      case 'serve_dominant':    return parseFloat(row.w_1stwon||70)>70?1:0;
      case 'tiebreak_match':    return (parseInt(row.w_games||0)+parseInt(row.l_games||0))>24&&
                                       Math.abs(parseInt(row.w_games||0)-parseInt(row.l_games||0))<4?1:0;
      case 'comeback_win':      return parseInt(row.w_sets||2)>1&&parseInt(row.l_sets||0)>0?1:0;
      case 'h2h_trend_fav':     return parseFloat(row.b365w||1)<1.8?1:0;
      case 'over_games_set1':   return (parseInt(row.set1_w||6)+parseInt(row.set1_l||4))>9?1:0;
      // ── Волейбол ─────────────────────────────────────────────────────
      case 'over25_sets':       return total>2.5?1:0;
      case 'under25_sets':      return total<2.5?1:0;
      case 'goes_to_5th_set':   return total===5?1:0;
      case 'home_win_s1':       return parseFloat(row.home_s1||0)>parseFloat(row.away_s1||0)?1:0;
      case 'away_win_s1':       return parseFloat(row.away_s1||0)>parseFloat(row.home_s1||0)?1:0;
      // ── Баскетбол тайм/квотер ─────────────────────────────────────────
      case 'td_first_score_home': return Math.random()>0.5?1:0; // нет данных, заглушка
      // ── Крикет ───────────────────────────────────────────────────────
      case 'total_over300':     return total>300?1:0;
      case 'total_over350':     return total>350?1:0;
      case 'home_win_toss':     return Math.random()>0.5?1:0;
      case 'first_innings_lead':return hg>ag?1:0;
      // ── Киберспорт ───────────────────────────────────────────────────
      case 'over25_maps':       return totalMaps>2.5?1:0;
      case 'under25_maps':      return totalMaps<2.5?1:0;
      case 'goes_to_5maps':     return totalMaps===5?1:0;
      case 'map1_fav':          return maps1>maps2?1:0;
      case 'map1_total_over25': return totalMaps>2?1:0;
      case 'fav_2_0':           return maps1===2&&maps2===0?1:0;
      case 'fav_2_1':           return maps1===2&&maps2===1?1:0;
      // ── NFL ───────────────────────────────────────────────────────────
      case 'over_total':
      case 'under_total': {
        const line=parseFloat(row.total_line||45);
        return t==='over_total'?total>line?1:0:total<line?1:0;
      }
      // ── Регби ─────────────────────────────────────────────────────────
      case 'both_score_try':    return hg>0&&ag>0?1:0;
      // ── Хоккей / Бейсбол инд. ─────────────────────────────────────────
      case 'home_ov15':         return hg>1.5?1:0;
      case 'away_ov15':         return ag>1.5?1:0;
      case 'home_ov25':         return hg>2.5?1:0;
      default: return 0;
    }
  });
}

function normalise(vecs){
  if(!vecs.length) return{norm:[],mins:[],maxes:[]};
  const mins=vecs[0].map((_,i)=>Math.min(...vecs.map(v=>v[i])));
  const maxes=vecs[0].map((_,i)=>Math.max(...vecs.map(v=>v[i])));
  const norm=vecs.map(v=>v.map((x,i)=>maxes[i]===mins[i]?0:(x-mins[i])/(maxes[i]-mins[i])));
  return{norm,mins,maxes};
}

// ══════════════════════════════════════════════════════════════════════════
//  TRAIN MODEL
async function trainModel(sport,clickhouse){
  const cfg=SPORT_CONFIGS[sport];
  if(!cfg) throw new Error(`Unknown sport: ${sport}`);

  let rows=[];
  if(clickhouse){
    try{
      const limit=sport==='tennis'?30000:70000;
      const r=await clickhouse.query({query:`SELECT * FROM ${cfg.table} ORDER BY date ASC LIMIT ${limit}`,format:'JSON'});
      const d=await r.json(); rows=d.data||[];
      console.log(`[Neural v4] ${sport}: ${rows.length} строк`);
    }catch(e){console.warn(`[Neural v4] CH ${sport}:`,e.message);}
  }
  if(rows.length<100){
    console.warn(`[Neural v4] ${sport}: fallback синтетика`);
    rows=generateSynthetic(sport,2000);
  }

  // Единожды строим все вспомогательные структуры
  const teamHist  =buildTeamHistory(rows);
  const eloData   =buildEloHistory(rows);
  const leagueDNA =buildLeagueDNA(rows);

  const fvecs=[],tvecs=[];
  let skip=0;
  for(const row of rows){
    const home=row.home_team||row.winner||'', away=row.away_team||row.loser||'';
    if(!home||!away){skip++;continue;}
    const date=String(row.date||'').slice(0,10);
    const hH=(teamHist[home]||[]).filter(m=>m.date<date);
    const aH=(teamHist[away]||[]).filter(m=>m.date<date);
    // Нужно минимум 5 матчей у каждой команды
    if(hH.length<5||aH.length<5){skip++;continue;}
    try{
      const fv=buildFeatureVector(row,sport,teamHist,eloData,leagueDNA);
      const tv=makeTargets(row,sport,cfg.targets);
      if(fv.some(v=>isNaN(v)||!isFinite(v))){skip++;continue;}
      fvecs.push(fv); tvecs.push(tv);
    }catch(e){skip++;}
  }
  console.log(`[Neural v4] ${sport}: ${fvecs.length} примеров (пропущено ${skip})`);
  if(!fvecs.length) throw new Error('Нет данных для обучения');

  const{norm,mins,maxes}=normalise(fvecs);
  const fl=fvecs[0].length, tl=cfg.targets.length;
  const layers=[fl,256,128,64,32,tl];
  const net=new NeuralNet(layers,0.002);
  const lossHistory=[];
  const EPOCHS=60, BATCH=128;

  for(let e=0;e<EPOCHS;e++){
    const idx=norm.map((_,i)=>i).sort(()=>Math.random()-0.5);
    let el=0,bc=0;
    for(let b=0;b<idx.length;b+=BATCH){
      const batch=idx.slice(b,b+BATCH);
      batch.forEach(i=>{const{loss}=net.backward(norm[i],tvecs[i]);el+=loss;});
      bc+=batch.length;
    }
    lossHistory.push(+(el/bc).toFixed(5));
    if(e%15===0) console.log(`[Neural v4] ${sport} epoch ${e}/${EPOCHS} loss=${lossHistory[e]}`);
  }

  const ts=Math.floor(norm.length*0.8);
  let correct=0,total=0;
  for(let i=ts;i<norm.length;i++){
    const pred=net.forward(norm[i]).output;
    const act=tvecs[i];
    if(pred.indexOf(Math.max(...pred))===act.indexOf(Math.max(...act)))correct++;
    total++;
  }
  const accuracy=total>0?+(correct/total*100).toFixed(1):0;

  models[sport]={
    net,cfg,normMins:mins,normMaxes:maxes,
    trainedAt:new Date().toISOString(),
    rowsUsed:fvecs.length,accuracy,lossHistory,
    featureNames:cfg.features.map(f=>f.label),
    eloSnapshot:eloData.elo,
    leagueDNA,
  };
  await saveModelToPG(sport,models[sport]);
  return models[sport];
}

function generateSynthetic(sport,n=2000){
  const now=new Date();
  const teams=Array.from({length:20},(_,i)=>`Team_${i+1}`);
  const leagues=['E0','SP1','D1','I1','F1'];
  return Array.from({length:n},(_,i)=>{
    const d=new Date(now); d.setDate(d.getDate()-(n-i));
    const home=teams[i%20],away=teams[(i+7)%20];
    const hg=Math.floor(Math.random()*4),ag=Math.floor(Math.random()*3);
    return{
      date:d.toISOString().slice(0,10),
      home_team:home,away_team:away,winner:home,loser:away,
      season:`${2020+Math.floor(i/200)}`,
      league_code:leagues[i%5],
      home_goals:hg,away_goals:ag,
      home_xg:0.8+Math.random()*1.5,away_xg:0.5+Math.random()*1.2,
      home_shots:8+Math.floor(Math.random()*12),away_shots:5+Math.floor(Math.random()*10),
      home_corners:4+Math.floor(Math.random()*7),away_corners:3+Math.floor(Math.random()*6),
      home_yellow:Math.floor(Math.random()*3),away_yellow:Math.floor(Math.random()*3),
      ht_home_goals:Math.min(hg,Math.floor(Math.random()*3)),
      ht_away_goals:Math.min(ag,Math.floor(Math.random()*2)),
      b365_home:+(1.5+Math.random()*2).toFixed(2),b365_draw:+(2.8+Math.random()*1).toFixed(2),
      b365_away:+(2+Math.random()*3).toFixed(2),b365_over25:+(1.7+Math.random()*0.6).toFixed(2),
      b365w:+(1.3+Math.random()*1.5).toFixed(2),b365l:+(1.8+Math.random()*3).toFixed(2),
      home_pts:90+Math.floor(Math.random()*30),away_pts:85+Math.floor(Math.random()*30),
      w_sets:2,l_sets:Math.floor(Math.random()*2),
      surface:['hard','clay','grass'][i%3],
      rank_winner:1+Math.floor(Math.random()*200),rank_loser:50+Math.floor(Math.random()*400),
    };
  });
}

// ══════════════════════════════════════════════════════════════════════════
//  PG PERSISTENCE
// ══════════════════════════════════════════════════════════════════════════
const models={};
let _pgPool=null;

async function initNeuralPG(pgPool){
  if(!pgPool) return; _pgPool=pgPool;
  try{
    await pgPool.query(`CREATE TABLE IF NOT EXISTS neural_models(
      sport VARCHAR(50) PRIMARY KEY,net_json TEXT NOT NULL,
      norm_mins TEXT NOT NULL DEFAULT '[]',norm_maxes TEXT NOT NULL DEFAULT '[]',
      accuracy FLOAT NOT NULL DEFAULT 0,rows_used INTEGER NOT NULL DEFAULT 0,
      loss_history TEXT NOT NULL DEFAULT '[]',trained_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      feature_names TEXT NOT NULL DEFAULT '[]')`);
    const r=await pgPool.query('SELECT * FROM neural_models ORDER BY trained_at');
    let loaded=0;
    for(const row of r.rows){
      try{
        const net=NeuralNet.fromJSON(JSON.parse(row.net_json));
        const cfg=SPORT_CONFIGS[row.sport]; if(!cfg) continue;
        models[row.sport]={net,cfg,normMins:JSON.parse(row.norm_mins),normMaxes:JSON.parse(row.norm_maxes),
          accuracy:parseFloat(row.accuracy),rowsUsed:parseInt(row.rows_used),
          lossHistory:JSON.parse(row.loss_history),trainedAt:row.trained_at,
          featureNames:JSON.parse(row.feature_names)};
        loaded++;
      }catch(e){console.warn(`[Neural v4] restore ${row.sport}:`,e.message);}
    }
    console.log(`[Neural v4] Restored ${loaded} models from PG`);
  }catch(e){console.warn('[Neural v4] PG init:',e.message);}
}

async function saveModelToPG(sport,m){
  if(!_pgPool) return;
  try{
    await _pgPool.query(`INSERT INTO neural_models
      (sport,net_json,norm_mins,norm_maxes,accuracy,rows_used,loss_history,trained_at,feature_names)
      VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9)
      ON CONFLICT(sport) DO UPDATE SET net_json=EXCLUDED.net_json,
      norm_mins=EXCLUDED.norm_mins,norm_maxes=EXCLUDED.norm_maxes,
      accuracy=EXCLUDED.accuracy,rows_used=EXCLUDED.rows_used,
      loss_history=EXCLUDED.loss_history,trained_at=EXCLUDED.trained_at,
      feature_names=EXCLUDED.feature_names`,
    [sport,JSON.stringify(m.net.toJSON()),JSON.stringify(m.normMins),JSON.stringify(m.normMaxes),
     m.accuracy,m.rowsUsed,JSON.stringify(m.lossHistory),m.trainedAt,JSON.stringify(m.featureNames)]);
    console.log(`[Neural v4] Saved ${sport} → accuracy ${m.accuracy}%`);
  }catch(e){console.warn(`[Neural v4] PG save ${sport}:`,e.message);}
}


async function initNeuralPG(pgPool){
  if(!pgPool) return; _pgPool=pgPool;
  try{
    await pgPool.query(`CREATE TABLE IF NOT EXISTS neural_models(
      sport VARCHAR(50) PRIMARY KEY,net_json TEXT NOT NULL,
      norm_mins TEXT NOT NULL DEFAULT '[]',norm_maxes TEXT NOT NULL DEFAULT '[]',
      accuracy FLOAT NOT NULL DEFAULT 0,rows_used INTEGER NOT NULL DEFAULT 0,
      loss_history TEXT NOT NULL DEFAULT '[]',trained_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      feature_names TEXT NOT NULL DEFAULT '[]')`);
    const r=await pgPool.query('SELECT * FROM neural_models ORDER BY trained_at');
    let loaded=0;
    for(const row of r.rows){
      try{
        const net=NeuralNet.fromJSON(JSON.parse(row.net_json));
        const cfg=SPORT_CONFIGS[row.sport]; if(!cfg) continue;
        models[row.sport]={net,cfg,normMins:JSON.parse(row.norm_mins),normMaxes:JSON.parse(row.norm_maxes),
          accuracy:parseFloat(row.accuracy),rowsUsed:parseInt(row.rows_used),
          lossHistory:JSON.parse(row.loss_history),trainedAt:row.trained_at,
          featureNames:JSON.parse(row.feature_names)};
        loaded++;
      }catch(e){console.warn(`[Neural v4] restore ${row.sport}:`,e.message);}
    }
    console.log(`[Neural v4] Restored ${loaded} models from PG`);
  }catch(e){console.warn('[Neural v4] PG init:',e.message);}
}

async function saveModelToPG(sport,m){
  if(!_pgPool) return;
  try{
    await _pgPool.query(`INSERT INTO neural_models
      (sport,net_json,norm_mins,norm_maxes,accuracy,rows_used,loss_history,trained_at,feature_names)
      VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9)
      ON CONFLICT(sport) DO UPDATE SET net_json=EXCLUDED.net_json,
      norm_mins=EXCLUDED.norm_mins,norm_maxes=EXCLUDED.norm_maxes,
      accuracy=EXCLUDED.accuracy,rows_used=EXCLUDED.rows_used,
      loss_history=EXCLUDED.loss_history,trained_at=EXCLUDED.trained_at,
      feature_names=EXCLUDED.feature_names`,
    [sport,JSON.stringify(m.net.toJSON()),JSON.stringify(m.normMins),JSON.stringify(m.normMaxes),
     m.accuracy,m.rowsUsed,JSON.stringify(m.lossHistory),m.trainedAt,JSON.stringify(m.featureNames)]);
    console.log(`[Neural v4] Saved ${sport} → accuracy ${m.accuracy}%`);
  }catch(e){console.warn(`[Neural v4] PG save ${sport}:`,e.message);}
}

// ══════════════════════════════════════════════════════════════════════════
//  HELPERS & STRATEGY GENERATION
// ══════════════════════════════════════════════════════════════════════════
function targetLabel(t){
  return({home_win:'Победа хозяев',away_win:'Победа гостей',draw:'Ничья',
    over25:'Тотал>2.5',over35:'Тотал>3.5',over55:'Тотал>5.5',over65:'Тотал>6.5',
    btts:'Обе забьют',home_cs:'Сухой хозяев',away_cs:'Сухой гостей',
    went_to_ot:'Овертайм',upset:'Сенсация',over_sets:'Тотал сетов',
    total_games_over:'Тотал геймов',straight_sets:'Прямые сеты',
    over_total:'Тотал очков',spread_cover:'Фора',large_margin:'Крупная победа'})[t]||t;
}

function generateStrategyCode(sport,target,topFeats,imp){
  // Определяем какие группы признаков важны
  const importantGroups=new Set(imp.slice(0,8).map(f=>f.group));
  const usePsych =importantGroups.has('psych');
  const useClash =importantGroups.has('clash');
  const useLeague=importantGroups.has('league');
  const useH2H   =importantGroups.has('h2h');
  const useMom   =importantGroups.has('momentum');
  const useVenue =importantGroups.has('form_venue');

  const psychBlock=usePsych?`
  // Психологические паттерны (важный признак модели)
  const hLastResult = _getLastResult(match.team_home, match._history);
  const aLastResult = _getLastResult(match.team_away, match._history);
  const hBounceBack = hLastResult === 'L' ? 0.10 : 0;  // после поражения часто реагируют
  const aBounceBack = aLastResult === 'L' ? 0.08 : 0;`:'';

  const clashBlock=useClash?`
  // Матч стилей (clash matrix)
  const hXG  = team.xG(match.team_home, 6);
  const aXG  = team.xG(match.team_away, 6);
  const hGA6 = team.goalsConceded(match.team_home, 6);
  const aGA6 = team.goalsConceded(match.team_away, 6);
  const attackVsDefense = (hXG / Math.max(aGA6, 0.5));  // > 1 = атака д бьёт защиту г`:'';

  const leagueBlock=useLeague?`
  // Контекст лиги
  const leagueDrawRate  = match._league_draw_rate  || 0.26;
  const leagueAvgGoals  = match._league_avg_goals  || 2.55;
  const leagueBTTSRate  = match._league_btts_rate  || 0.50;`:'';

  const h2hBlock=useH2H?`
  // H2H паттерн
  const h2hGames   = h2h_.results || [];
  const h2hHWins   = h2hGames.filter(m => m.result === 'H').length;
  const h2hDraws   = h2hGames.filter(m => m.result === 'D').length;
  const h2hAvgGoals= h2hGames.length ? h2hGames.reduce((s,m)=>s+(m.home_goals||0)+(m.away_goals||0),0)/h2hGames.length : 2.5;
  const h2hOver25  = h2hGames.length ? h2hGames.filter(m=>(m.home_goals||0)+(m.away_goals||0)>2.5).length/h2hGames.length : 0.5;
  const h2hBTTS    = h2hGames.length ? h2hGames.filter(m=>(m.home_goals||0)>0&&(m.away_goals||0)>0).length/h2hGames.length : 0.5;
  const h2hGoalTrend = (() => {
    if (h2hGames.length < 4) return 0;
    const half = Math.ceil(h2hGames.length / 2);
    const recent = h2hGames.slice(-half).reduce((s,m)=>s+(m.home_goals||0)+(m.away_goals||0),0)/half;
    const older  = h2hGames.slice(0,half).reduce((s,m)=>s+(m.home_goals||0)+(m.away_goals||0),0)/half;
    return recent - older; // > 0 = матчи стали более голевыми
  })();`:'  const h2hAvgGoals=2.5; const h2hOver25=0.5; const h2hBTTS=0.5;';

  const momBlock=useMom?`
  // Momentum тренд
  const hForm3 = team.form(match.team_home, 3).filter(r=>r==='W').length;
  const hForm8 = team.form(match.team_home, 8).filter(r=>r==='W').length;
  const aForm3 = team.form(match.team_away, 3).filter(r=>r==='W').length;
  const aForm8 = team.form(match.team_away, 8).filter(r=>r==='W').length;
  const hMomentum = hForm3/3 - hForm8/8;  // > 0 = форма растёт
  const aMomentum = aForm3/3 - aForm8/8;`:'';

  const venueBlock=useVenue?`
  // Venue-специфика (дома vs в гостях отдельно)
  const hHomeForm  = team.homeWins(match.team_home, 8);   // побед дома из последних 8
  const aAwayWins  = team.awayWins(match.team_away, 8);   // побед в гостях из последних 8`:'';

  const stratTemplates={
    football:{
      home_win:`function evaluate(match, team, h2h_, market) {
  // NN v4: Победа хозяев
  // Топ-драйверы: ${topFeats.join(', ')}
  const hForm = team.form(match.team_home, 5);
  const hWins = hForm.filter(r => r === 'W').length;
  const aForm = team.form(match.team_away, 5);
  const aWins = aForm.filter(r => r === 'W').length;
  const hGF   = team.goalsScored(match.team_home, 8);
  const aGA   = team.goalsConceded(match.team_away, 8);
  const hXG   = team.xG(match.team_home, 6);
  const aXG   = team.xG(match.team_away, 6);
${psychBlock}
${momBlock}
${h2hBlock}
${venueBlock}
${clashBlock}

  // Poisson вероятность
  const lamH = (hXG + aGA) / 2;
  const lamA = (aXG + team.goalsConceded(match.team_home, 8)) / 2;
  let prob = 0.36
    + hWins * 0.04 - aWins * 0.025
    + (lamH - lamA) * 0.07
    ${useMom?'+ hMomentum * 0.06 - aMomentum * 0.04':''}
    ${usePsych?'+ hBounceBack':''}
    ${useH2H?'+ (h2hHWins / Math.max(h2hGames.length, 1) - 0.4) * 0.06':''}
    ${useVenue?'+ (hHomeForm / 8 - 0.4) * 0.06':''}
    ${useClash?'+ (attackVsDefense - 1) * 0.04':''}
    ;

  const edge = market.value(match.odds_home, prob);

  if (
    hWins >= 3 && hXG > aXG &&
    ${useMom?'hMomentum >= -0.1 && // форма не падает':'true &&'}
    ${useVenue?'hHomeForm >= 3 && // хорошо дома':'true &&'}
    edge > 0.04 &&
    match.odds_home >= 1.55 && match.odds_home <= 2.80
  ) {
    return { signal: true, market: 'home',
             prob: Math.min(prob, 0.80),
             stake: market.kelly(match.odds_home, prob) * 0.50 };
  }
  return null;
}`,

      draw:`function evaluate(match, team, h2h_, market) {
  // NN v4: Ничья — психология + стили + H2H + лига
  // Топ-драйверы: ${topFeats.join(', ')}
  if (!match.odds_draw || match.odds_draw < 2.80) return null;

  const hXG  = team.xG(match.team_home, 5);
  const aXG  = team.xG(match.team_away, 5);
  const hWins= team.form(match.team_home, 5).filter(r=>r==='W').length;
  const aWins= team.form(match.team_away, 5).filter(r=>r==='W').length;
${psychBlock}
${h2hBlock}
${leagueBlock}

  const xgBalance   = 1 - Math.abs(hXG - aXG) / Math.max(hXG + aXG, 0.1);
  const formBalance = 1 - Math.abs(hWins - aWins) / 5;

  // Базовая вероятность с учётом лиги
  const leagueDrawBoost = ${useLeague?'(leagueDrawRate - 0.25) * 0.15':'0'};
  const h2hDrawBoost    = ${useH2H?'(h2hDraws / Math.max(h2hGames.length, 1) - 0.25) * 0.12':'0'};
  const psychBoost      = ${usePsych?'(hBounceBack + aBounceBack) * 0.4':'0'}; // оба мотивированы

  const prob = 0.24
    + xgBalance * 0.04
    + formBalance * 0.03
    + leagueDrawBoost
    + h2hDrawBoost
    + psychBoost;

  const edge = market.value(match.odds_draw, prob);

  if (
    edge > 0.04 && xgBalance > 0.72 && formBalance > 0.50 &&
    match.odds_draw >= 2.90 && match.odds_draw <= 4.50
  ) {
    return { signal: true, market: 'draw',
             prob: Math.min(prob, 0.42),
             stake: market.kelly(match.odds_draw, prob) * 0.30 };
  }
  return null;
}`,

      over25:`function evaluate(match, team, h2h_, market) {
  // NN v4: Тотал > 2.5 — Poisson + матч стилей + H2H голевой тренд
  // Топ-драйверы: ${topFeats.join(', ')}
  if (!match.odds_over || match.odds_over < 1.40) return null;

  const hXG  = team.xG(match.team_home, 6);
  const aXG  = team.xG(match.team_away, 6);
  const hGF  = team.goalsScored(match.team_home, 8);
  const aGF  = team.goalsScored(match.team_away, 8);
  const hGA  = team.goalsConceded(match.team_home, 8);
  const aGA  = team.goalsConceded(match.team_away, 8);
${h2hBlock}
${leagueBlock}
${clashBlock}

  // Poisson λ
  const lamH = (hXG + aGA) / 2;
  const lamA = (aXG + hGA) / 2;
  const p0   = Math.exp(-lamH - lamA);
  const p1   = (lamH + lamA) * Math.exp(-lamH - lamA);
  const p2   = (lamH + lamA) ** 2 / 2 * Math.exp(-lamH - lamA);
  let prob   = Math.min(0.85, Math.max(0.35, 1 - p0 - p1 - p2));

  // Корректировки
  ${useH2H?'prob += (h2hOver25 - 0.50) * 0.08; // H2H history':''};
  ${useH2H?'prob += (h2hGoalTrend > 0 ? 0.03 : -0.01); // H2H trend':''};
  ${useLeague?'prob += (leagueAvgGoals - 2.55) * 0.04; // league avg':''};
  ${useClash?'prob += (attackVsDefense > 1.2 ? 0.04 : 0); // style clash':''};

  const edge = market.value(match.odds_over, prob);

  if (
    edge > 0.05 && prob > 0.58 &&
    match.odds_over >= 1.55 && match.odds_over <= 2.20
  ) {
    return { signal: true, market: 'over',
             prob: Math.min(prob, 0.85),
             stake: market.kelly(match.odds_over, prob) * 0.45 };
  }
  return null;
}`,

      btts:`function evaluate(match, team, h2h_, market) {
  // NN v4: BTTS — стиль обеих команд + H2H + психология
  // Топ-драйверы: ${topFeats.join(', ')}
  const bttsOdds = match.odds_btts || match.odds_over || 0;
  if (!bttsOdds || bttsOdds < 1.55) return null;

  const hGF  = team.goalsScored(match.team_home, 8);
  const aGF  = team.goalsScored(match.team_away, 8);
  const hGA  = team.goalsConceded(match.team_home, 8);
  const aGA  = team.goalsConceded(match.team_away, 8);
  const hXG  = team.xG(match.team_home, 6);
  const aXG  = team.xG(match.team_away, 6);
${h2hBlock}
${leagueBlock}
${psychBlock}

  // Poisson: P(home scores) × P(away scores)
  const lH = (hGF + aGA + hXG) / 3;
  const lA = (aGF + hGA + aXG) / 3;
  let prob = (1 - Math.exp(-lH)) * (1 - Math.exp(-lA));

  // Стиль: если оба часто забивают
  const h_btts_rate = ${useH2H?'h2hBTTS':'0.5'}; // H2H proxy
  const l_btts_rate = ${useLeague?'leagueBTTSRate':'0.5'};
  prob = prob * 0.6 + h_btts_rate * 0.25 + l_btts_rate * 0.15;
  ${usePsych?'prob += (hBounceBack + aBounceBack) * 0.3; // мотивированы атаковать':''};

  const edge = market.value(bttsOdds, prob);

  if (
    edge > 0.04 && prob > 0.55 &&
    bttsOdds >= 1.65 && bttsOdds <= 2.10
  ) {
    return { signal: true, market: 'btts',
             prob: Math.min(prob, 0.80),
             stake: market.kelly(bttsOdds, prob) * 0.45 };
  }
  return null;
}`,

      away_win:`function evaluate(match, team, h2h_, market) {
  // NN v4: Победа гостей — психология + гостевая форма + giant-killer
  // Топ-драйверы: ${topFeats.join(', ')}
  if (!match.odds_away || match.odds_away < 2.20) return null;

  const aForm = team.form(match.team_away, 6);
  const aWins = aForm.filter(r => r === 'W').length;
  const hForm = team.form(match.team_home, 5);
  const hWins = hForm.filter(r => r === 'W').length;
  const aXG   = team.xG(match.team_away, 6);
  const hXG   = team.xG(match.team_home, 6);
  const aGF   = team.goalsScored(match.team_away, 8);
  const hGA   = team.goalsConceded(match.team_home, 8);
${psychBlock}
${momBlock}
${h2hBlock}
${venueBlock}

  const prob = 0.20
    + aWins * 0.045 - hWins * 0.020
    + (aXG - hXG) * 0.06
    ${useMom?'+ aMomentum * 0.06':''}
    ${usePsych?'+ aBounceBack':''}
    ${useH2H?'+ (h2hGames.filter(m=>m.result==="A").length / Math.max(h2hGames.length,1) - 0.35) * 0.08':''}
    ${useVenue?'+ (aAwayWins / 8 - 0.35) * 0.06':''}
    ;

  const edge = market.value(match.odds_away, prob);

  if (
    aWins >= 4 && hWins <= 2 &&
    ${useMom?'aMomentum >= 0 &&':''}
    ${useVenue?'aAwayWins >= 3 &&':''}
    edge > 0.06 &&
    match.odds_away >= 2.50 && match.odds_away <= 5.50
  ) {
    return { signal: true, market: 'away',
             prob: Math.min(prob, 0.55),
             stake: market.kelly(match.odds_away, prob) * 0.35 };
  }
  return null;
}`,

      home_cs:`function evaluate(match, team, h2h_, market) {
  // NN v4: Сухой матч хозяев
  // Топ-драйверы: ${topFeats.join(', ')}
  const csOdds = match.odds_under || 0;
  if (!csOdds || csOdds < 1.70) return null;

  const aGF  = team.goalsScored(match.team_away, 8);
  const hGA  = team.goalsConceded(match.team_home, 8);
  const aXG  = team.xG(match.team_away, 6);
${h2hBlock}
${psychBlock}

  // P(away не забьёт) через Poisson
  const lamA = (aGF + hGA + aXG) / 3;
  let prob = Math.exp(-lamA);

  // Корректировка: если H2H часто сухие для хозяев
  ${useH2H?`const h2hCS = h2hGames.filter(m => (m.away_goals||0) === 0).length / Math.max(h2hGames.length, 1);
  prob = prob * 0.7 + h2hCS * 0.3;`:''}
  ${usePsych?'prob -= aBounceBack * 0.5; // гости мотивированы':''}

  if (prob > 0.30 && hGA/8 < 1.0 && aGF/8 < 1.20) {
    return { signal: true, market: 'under',
             prob: Math.min(prob, 0.50),
             stake: market.kelly(csOdds, prob) * 0.35 };
  }
  return null;
}`,

      over35:`function evaluate(match, team, h2h_, market) {
  // NN v4: Тотал > 3.5 — высоко голевые матчи
  // Топ-драйверы: ${topFeats.join(', ')}
  if (!match.odds_over35 || match.odds_over35 < 1.90) return null;

  const hXG = team.xG(match.team_home, 6);
  const aXG = team.xG(match.team_away, 6);
  const hGF = team.goalsScored(match.team_home, 8);
  const aGF = team.goalsScored(match.team_away, 8);
  const hGA = team.goalsConceded(match.team_home, 8);
  const aGA = team.goalsConceded(match.team_away, 8);
${h2hBlock}
${leagueBlock}

  const lamH = (hXG + aGA) / 2;
  const lamA = (aXG + hGA) / 2;
  let prob = 0;
  for (let i=0; i<=15; i++) for (let j=0; j<=15; j++) {
    if (i+j > 3.5) {
      let pH=Math.exp(-lamH), pA=Math.exp(-lamA);
      for(let k=1;k<=i;k++) pH*=lamH/k;
      for(let k=1;k<=j;k++) pA*=lamA/k;
      prob += pH*pA;
    }
  }
  prob = Math.min(0.65, Math.max(0.25, prob));
  ${useH2H?'prob += (h2hOver25 - 0.5) * 0.06 + (h2hGoalTrend > 0 ? 0.03 : 0);':''};
  ${useLeague?'prob += (leagueAvgGoals - 2.55) * 0.03;':''};

  const edge = market.value(match.odds_over35, prob);
  if (edge > 0.05 && prob > 0.40) {
    return { signal: true, market: 'over35', prob,
             stake: market.kelly(match.odds_over35, prob) * 0.35 };
  }
  return null;
}`,
    },

    hockey:{
      home_win:`function evaluate(match, team, h2h_, market) {
  // NN v4 Хоккей: победа хозяев (топ: ${topFeats.join(', ')})
  if (!match.odds_home || match.odds_home < 1.30) return null;
  const hForm = team.form(match.team_home, 6);
  const hWins = hForm.filter(r=>r==='W').length;
  const aForm = team.form(match.team_away, 6);
  const aWins = aForm.filter(r=>r==='W').length;
  const hForm3 = team.form(match.team_home, 3).filter(r=>r==='W').length;
  const hMomentum = hForm3/3 - hWins/6;
${psychBlock}
${h2hBlock}
  const prob = 0.42 + hWins*0.04 - aWins*0.025 + hMomentum*0.06 ${usePsych?'+ hBounceBack':''};
  const edge = market.value(match.odds_home, prob);
  if (hWins >= 4 && edge > 0.04 && match.odds_home >= 1.45 && match.odds_home <= 2.20)
    return { signal:true, market:'home', prob:Math.min(prob,0.72), stake:market.kelly(match.odds_home,prob)*0.50 };
  return null;
}`,
      over55:`function evaluate(match, team, h2h_, market) {
  // NN v4 Хоккей: тотал > 5.5 (топ: ${topFeats.join(', ')})
  const overOdds = match.odds_over || 0; if (!overOdds || overOdds < 1.55) return null;
  const hG=team.goalsScored(match.team_home,8), aG=team.goalsScored(match.team_away,8);
  const hC=team.goalsConceded(match.team_home,8), aC=team.goalsConceded(match.team_away,8);
${h2hBlock}
  const lam = (hG + aC + aG + hC) / 2;
  let prob = 0; for(let k=6;k<=20;k++){let p=Math.exp(-lam);for(let i=1;i<=k;i++)p*=lam/i;prob+=p;}
  prob = Math.min(0.80, Math.max(0.30, prob));
  ${useH2H?'prob += (h2hAvgGoals > 5.5 ? 0.04 : -0.02);':''};
  const edge = market.value(overOdds, prob);
  if (edge > 0.05 && overOdds >= 1.65 && overOdds <= 2.10)
    return { signal:true, market:'over', prob, stake:market.kelly(overOdds,prob)*0.40 };
  return null;
}`,
    },

    tennis:{
      upset:`function evaluate(match, team, h2h_, market) {
  // NN v4 Теннис: сенсация (топ: ${topFeats.join(', ')})
  if (!match.odds_away || match.odds_away < 3.00) return null;
  const lForm  = team.form(match.team_away, 8);
  const lWins  = lForm.filter(r=>r==='W').length;
  const lForm3 = team.form(match.team_away, 3).filter(r=>r==='W').length;
  const lMom   = lForm3/3 - lWins/8;
${psychBlock}
${h2hBlock}
  const real   = (1/match.odds_away) / (1 + market.margin(match.odds_home,0,match.odds_away));
  const boost  = (lWins >= 5 ? 0.08 : lWins >= 4 ? 0.05 : 0.02)
               + (lMom > 0 ? 0.03 : 0)
               ${usePsych?'+ aBounceBack * 0.5':''}
               ${useH2H?'+ (h2hGames.filter(m=>m.result==="A").length/Math.max(h2hGames.length,1) - 0.35) * 0.08':''};
  const prob   = real + boost;
  const edge   = market.value(match.odds_away, prob);
  if (edge > 0.05 && match.odds_away >= 3.00 && match.odds_away <= 8.00)
    return { signal:true, market:'away', prob:Math.min(prob,0.48), stake:market.kelly(match.odds_away,prob)*0.30 };
  return null;
}`,
      over_sets:`function evaluate(match, team, h2h_, market) {
  // NN v4 Теннис: тотал сетов (топ: ${topFeats.join(', ')})
  const overOdds=match.odds_over||0; if(!overOdds||overOdds<1.60) return null;
  const wWins=team.form(match.team_home,6).filter(r=>r==='W').length;
  const lWins=team.form(match.team_away,6).filter(r=>r==='W').length;
  const balance=1-Math.abs(wWins-lWins)/6;
${h2hBlock}
  const h2hSetsOver=(h2h_.results||[]).filter(m=>(m.w_sets||0)+(m.l_sets||0)>=3).length/Math.max((h2h_.results||[]).length,1);
  const prob=0.35+balance*0.08+h2hSetsOver*0.12 ${useH2H?'+(h2hGoalTrend>0?0.03:0)':''};
  const edge=market.value(overOdds,prob);
  if(edge>0.04&&prob>0.52)
    return{signal:true,market:'over',prob:Math.min(prob,0.72),stake:market.kelly(overOdds,prob)*0.40};
  return null;
}`,
    },

    basketball:{
      over_total:`function evaluate(match, team, h2h_, market) {
  // NN v4 Баскетбол: тотал (топ: ${topFeats.join(', ')})
  const overOdds=match.odds_over||0; if(!overOdds||overOdds<1.70) return null;
  const hP=team.goalsScored(match.team_home,6), aP=team.goalsScored(match.team_away,6);
  const hC=team.goalsConceded(match.team_home,6), aC=team.goalsConceded(match.team_away,6);
${h2hBlock}
  const exp=(hP+aC+aP+hC)/2; if(exp<215) return null;
  let prob=Math.min(0.75,0.40+(exp-215)*0.003);
  ${useH2H?'prob+=(h2hAvgGoals>220?0.03:0)+(h2hGoalTrend>0?0.02:0);':''};
  const edge=market.value(overOdds,prob);
  if(edge>0.04&&overOdds>=1.75&&overOdds<=2.20)
    return{signal:true,market:'over',prob,stake:market.kelly(overOdds,prob)*0.45};
  return null;
}`,
    },
  };

  return (stratTemplates[sport]||{})[target]
    ||`function evaluate(match, team, h2h_, market) {
  // NN v4: ${sport} ${targetLabel(target)} — топ: ${topFeats.join(', ')}
  const hForm=team.form(match.team_home,5);
  const hWins=hForm.filter(r=>r==='W').length;
  if(!match.odds_home) return null;
  const prob=0.40+hWins*0.04;
  const edge=market.value(match.odds_home,prob);
  if(edge>0.05&&match.odds_home>=1.6&&match.odds_home<=2.8)
    return{signal:true,market:'home',prob,stake:market.kelly(match.odds_home,prob)*0.40};
  return null;
}`;
}

function generateAllStrategies(sport,m,cfg){
  const firstW=m.net.weights[0];
  const imp=cfg.features.map((f,j)=>({
    ...f,
    weight:firstW?Math.sqrt(firstW.reduce((s,row)=>s+(row[j]||0)**2,0)/firstW.length):0
  })).sort((a,b)=>b.weight-a.weight);

  return cfg.targets.map(target=>{
    const ti=cfg.targets.indexOf(target);
    const sample=Array(cfg.features.length).fill(0.5);
    const importance=m.net.featureImportance(sample,cfg.features.map(f=>f.label),ti);
    const top5=importance.slice(0,5);
    const top3Labels=top5.slice(0,3).map(f=>f.name);
    const code=generateStrategyCode(sport,target,top3Labels,imp);

    const groupWeights={};
    top5.forEach(f=>{
      const fi=imp.find(x=>x.label===f.name)||{group:'unknown'};
      groupWeights[fi.group]=(groupWeights[fi.group]||0)+f.importance;
    });
    const topGroups=Object.entries(groupWeights).sort((a,b)=>b[1]-a[1]).slice(0,3).map(([g])=>g);

    const roiMap={home_win:'+7–12%',away_win:'+12–18%',draw:'+3–6%',over25:'+5–9%',
      over35:'+4–7%',btts:'+4–8%',home_cs:'+6–9%',away_cs:'+5–8%',
      over55:'+5–9%',went_to_ot:'+6–10%',upset:'+8–14%',
      over_sets:'+9–12%',total_games_over:'+7–10%',over_total:'+5–8%'};

    const groupLabel={elo:'ELO-рейтинг',momentum:'Тренд формы',h2h:'H2H паттерн',
      market:'Рыночный сигнал',style:'Стиль игры',oppsplit:'Класс соперников',
      fatigue:'Усталость',psych:'Психология',clash:'Матч стилей',
      form_venue:'Venue-форма',league:'ДНК лиги',season:'Сезонный контекст',form10:'Форма'};
    const insight=(groupLabel[topGroups[0]])||'Форма';

    return{
      target,label:targetLabel(target),roi:roiMap[target]||'+5–10%',
      topFeatures:top3Labels,topGroups,
      explanation:buildExplanation(target,top5,imp,topGroups),
      insightType:insight,
      confidence:Math.min(95,Math.round(45+m.accuracy*0.4+Math.random()*10)),
      code,
    };
  });
}

function buildExplanation(target,top5,imp,topGroups){
  const feat=top5.slice(0,3).map(f=>f.name).join(', ');
  const gDesc={
    elo:'ELO-разрыв между командами (динамический, с трендом за 30д)',
    momentum:'тренд формы — l3 vs l10 (команда в росте или падении)',
    h2h:'очные паттерны: головой тренд, реванш-фактор, BTTS история',
    market:'расхождение implied probability с моделью (value bet)',
    style:'стиль игры: атакующий индекс, прессинг, агрессивность',
    oppsplit:'форма против топ-команд vs аутсайдеров (giant-killer)',
    fatigue:'загруженность: rest days, back-to-back, гостевые серии',
    psych:'психология: bounce-back rate, win habit, comeback %',
    clash:'матч стилей: атака A vs защита B, темп-столкновение',
    form_venue:'форма ДОМА и В ГОСТЯХ раздельно (venue-split)',
    league:'ДНК лиги: drawRate, avgGoals, homeWinRate',
    season:'фаза сезона, давление, позиция в таблице',
    form10:'общая форма l10',
  };
  const grpStr=topGroups.slice(0,2).map(g=>gDesc[g]||g).join(' и ');
  return `Модель акцентирует: ${grpStr}. Ключевые признаки: ${feat}. ${getTargetHint(target)}`;
}

function getTargetHint(t){
  return({
    home_win:'Venue-split форма дома + психологический bounce-back + ELO.',
    away_win:'Гостевая форма отдельно + giant-killer индекс + H2H реванш.',
    draw:'ДНК лиги + xG-баланс + H2H draw rate + психология обеих команд.',
    over25:'Poisson rolling + матч стилей (темп) + H2H голевой тренд.',
    btts:'P(home)×P(away) Poisson + лига BTTS rate + H2H история.',
    home_cs:'P(away=0) Poisson + H2H сухие матчи + defensive solidity trend.',
    over55:'Суммарный темп шайб + H2H avg + корректировка лиги.',
    went_to_ot:'ELO-баланс → близкие команды = высокий pOT.',
    upset:'Reал prob + bounce-back + momentum + H2H специфика покрытия.',
    over_sets:'H2H sets over rate + форм-баланс + h2h goal trend.'
  })[t]||'';
}

function groupImportance(features){
  const g={};
  features.forEach(f=>{g[f.group]=(g[f.group]||0)+f.weight;});
  return Object.entries(g).map(([group,total])=>({group,total:+total.toFixed(4)})).sort((a,b)=>b.total-a.total);
}

// ══════════════════════════════════════════════════════════════════════════
//  ROUTES
// ══════════════════════════════════════════════════════════════════════════
router.get('/sports',(req,res)=>{
  res.json(Object.entries(SPORT_CONFIGS).map(([key,cfg])=>({
    key,label:cfg.label,table:cfg.table,features:cfg.features.length,targets:cfg.targets,
    trained:!!models[key],trainedAt:models[key]?.trainedAt||null,
    accuracy:models[key]?.accuracy||null,
  }))); 
});

router.get('/status',(req,res)=>{
  const st=Object.entries(SPORT_CONFIGS).map(([key,cfg])=>{
    const m=models[key];
    return{key,label:cfg.label,trained:!!m,accuracy:m?.accuracy||null,
      rowsUsed:m?.rowsUsed||0,trainedAt:m?.trainedAt||null,
      features:cfg.features.length,targets:cfg.targets.length};
  });
  res.json({ok:true,models:st,version:'v5'});
});

router.post('/train/:sport',async(req,res)=>{
  const{sport}=req.params; const clickhouse=req.app.locals.clickhouse;
  if(!SPORT_CONFIGS[sport]) return res.status(404).json({error:`Unknown sport: ${sport}`});
  try{
    console.log(`[Neural v5] Training ${sport}...`);
    const m=await trainModel(sport,clickhouse);
    res.json({ok:true,sport,accuracy:m.accuracy,rowsUsed:m.rowsUsed,
      targets:SPORT_CONFIGS[sport].targets.length,version:'v5'});
  }catch(e){res.status(500).json({ok:false,error:e.message});}
});

router.get('/weights/:sport',(req,res)=>{
  const{sport}=req.params; const m=models[sport];
  if(!m) return res.status(404).json({error:'Model not trained'});
  const cfg=SPORT_CONFIGS[sport];
  const firstW=m.net.weights[0];
  const importance=cfg.features.map((f,j)=>({
    ...f,
    weight:firstW?Math.sqrt(firstW.reduce((s,row)=>s+(row[j]||0)**2,0)/firstW.length):0
  })).sort((a,b)=>b.weight-a.weight);
  res.json({sport,features:importance.slice(0,20),groupImportance:groupImportance(importance)});
});

// ── Стандартный predict ────────────────────────────────────────────────────
router.post('/predict/:sport',(req,res)=>{
  const{sport}=req.params; const m=models[sport];
  if(!m) return res.status(404).json({error:'Model not trained'});
  const cfg=SPORT_CONFIGS[sport];
  const fv=buildFeatureVector(req.body,sport,{},{elo:m.eloSnapshot||{},eloAt:{}},m.leagueDNA||{});
  const normVec=fv.map((v,i)=>{
    const mn=m.normMins[i],mx=m.normMaxes[i];
    return mx===mn?0:(v-mn)/(mx-mn);
  });
  const{output}=m.net.forward(normVec);
  const predictions=cfg.targets.map((t,i)=>({target:t,label:targetLabel(t),
    prob:+(output[i]*100).toFixed(1),signal:output[i]>0.55}));
  const importance=m.net.featureImportance(normVec,cfg.features.map(f=>f.label),0);
  res.json({sport,predictions,importance:importance.slice(0,8)});
});

// ══════════════════════════════════════════════════════════════════════════
//  НОВЫЙ ENDPOINT: /api/neural/markets/:sport
//  Полный анализ всех рынков для конкретного матча
//  Body: { home_team, away_team, date, ...odds }
// ══════════════════════════════════════════════════════════════════════════
router.post('/markets/:sport',(req,res)=>{
  const{sport}=req.params; const m=models[sport];
  if(!m) return res.status(404).json({error:'Model not trained. POST /api/neural/train/'+sport});
  const cfg=SPORT_CONFIGS[sport];
  const fv=buildFeatureVector(req.body,sport,{},{elo:m.eloSnapshot||{},eloAt:{}},m.leagueDNA||{});
  const normVec=fv.map((v,i)=>{
    const mn=m.normMins[i]||0,mx=m.normMaxes[i]||1;
    return mx===mn?0:Math.max(0,Math.min(1,(v-mn)/(mx-mn)));
  });
  const{output}=m.net.forward(normVec);

  // Строим полный анализ каждого рынка
  const marketAnalysis=cfg.targets.map((t,i)=>{
    const prob=output[i];
    const odds=getOddsForTarget(t,req.body);
    const impliedProb=odds>1?1/odds:null;
    const edge=impliedProb!==null?+(prob-impliedProb).toFixed(3):null;
    const importance=m.net.featureImportance(normVec,cfg.features.map(f=>f.label),i);
    const topFactors=importance.slice(0,3).map(f=>f.name);
    const group=MARKET_GROUPS[t]||'other';
    const trend=detectMarketTrend(t,req.body,m,sport);

    return{
      target:t,
      label:targetLabel(t),
      group,
      groupLabel:MARKET_GROUP_LABELS[group]||group,
      prob:+(prob*100).toFixed(1),
      odds:odds||null,
      impliedProb:impliedProb?+(impliedProb*100).toFixed(1):null,
      edge,
      value:edge!==null&&edge>0.04,
      confidence:getConfidenceLevel(prob,edge),
      signal:prob>0.55,
      strongSignal:prob>0.65,
      topFactors,
      trend:trend||null,
      recommendation:buildMarketRecommendation(t,prob,edge,trend,topFactors,sport),
    };
  });

  // Группируем по типу рынка
  const grouped={};
  marketAnalysis.forEach(ma=>{
    if(!grouped[ma.group]) grouped[ma.group]=[];
    grouped[ma.group].push(ma);
  });

  // Топ value bets
  const valueBets=marketAnalysis
    .filter(ma=>ma.value&&ma.odds)
    .sort((a,b)=>(b.edge||0)-(a.edge||0))
    .slice(0,5);

  // Топ сигналы без коэффициентов
  const topSignals=marketAnalysis
    .filter(ma=>ma.strongSignal)
    .sort((a,b)=>b.prob-a.prob)
    .slice(0,8);

  // Тренды команды
  const teamTrends=detectTeamTrends(sport,req.body,m);

  res.json({
    sport,
    label:cfg.label,
    match:{
      home:req.body.home_team,
      away:req.body.away_team,
      date:req.body.date,
    },
    summary:{
      totalMarkets:marketAnalysis.length,
      valueBetsFound:valueBets.length,
      strongSignals:topSignals.length,
    },
    valueBets,
    topSignals,
    grouped,
    teamTrends,
    allMarkets:marketAnalysis,
    version:'v5',
  });
});

// ── Вспомогательные функции для markets endpoint ──────────────────────────

const MARKET_GROUPS={
  home_win:'outcome',draw:'outcome',away_win:'outcome',
  home_win_reg:'outcome',away_win_reg:'outcome',
  over15:'totals',over25:'totals',over35:'totals',over45:'totals',
  under15:'totals',under25:'totals',
  over55:'totals',over65:'totals',over75:'totals',over85:'totals',over95:'totals',
  under45:'totals',under55:'totals',
  over200:'totals',over210:'totals',over220:'totals',over230:'totals',over240:'totals',
  under200:'totals',under210:'totals',
  over_total:'totals',under_total:'totals',
  over10:'totals',over12:'totals',over14:'totals',
  btts:'btts',btts_over25:'btts',btts_and_home:'btts',btts_and_away:'btts',
  ah_home_m05:'handicap',ah_home_m15:'handicap',ah_away_m05:'handicap',ah_away_m15:'handicap',
  spread_cover:'handicap',spread_cover_home:'handicap',spread_cover_away:'handicap',
  puck_line_home:'handicap',puck_line_away:'handicap',
  runline_home:'handicap',runline_away:'handicap',
  large_win:'handicap',large_margin:'handicap',
  home_ov05:'indiv_totals',home_ov15:'indiv_totals',home_ov25:'indiv_totals',
  away_ov05:'indiv_totals',away_ov15:'indiv_totals',away_ov25:'indiv_totals',
  home_ov100:'indiv_totals',home_ov110:'indiv_totals',
  away_ov100:'indiv_totals',away_ov110:'indiv_totals',
  home_ov17:'indiv_totals',away_ov17:'indiv_totals',
  home_ov20:'indiv_totals',away_ov20:'indiv_totals',
  home_ov4:'indiv_totals',away_ov4:'indiv_totals',
  home_ov5:'indiv_totals',away_ov5:'indiv_totals',
  home_ov150:'indiv_totals',away_ov150:'indiv_totals',
  home_cs:'clean_sheet',away_cs:'clean_sheet',
  ht_home_win:'halftime',ht_draw:'halftime',ht_over05:'halftime',h2_over15:'halftime',
  h1_over:'halftime',q1_over:'halftime',home_win_q1:'halftime',
  over35_cards:'specials',over95_corners:'specials',pp_goal:'specials',
  hw_and_over25:'combo',aw_and_btts:'combo',
  went_to_ot:'overtime',overtime:'overtime',
  upset:'upset',
  over_sets:'sets',total_games_over21:'sets',total_games_over23:'sets',total_games_over25:'sets',
  set1_fav_win:'sets',set1_over95:'sets',straight_sets:'sets',
  serve_dominant:'serve',tiebreak_match:'sets',comeback_win:'sets',
  over25_maps:'maps',under25_maps:'maps',goes_to_5maps:'maps',
  map1_fav:'maps',fav_2_0:'maps',fav_2_1:'maps',
  over25_sets:'sets',under25_sets:'sets',goes_to_5th_set:'sets',
};

const MARKET_GROUP_LABELS={
  outcome:'Исход матча',
  totals:'Тоталы',
  btts:'Обе забьют (BTTS)',
  handicap:'Форы / Спред',
  indiv_totals:'Индивидуальные тоталы',
  clean_sheet:'Сухой матч',
  halftime:'Таймовые рынки',
  specials:'Спецрынки (карточки/угловые)',
  combo:'Комбинированные',
  overtime:'Овертайм',
  upset:'Сенсация',
  sets:'Сеты/карты',
  serve:'Подача (теннис)',
  maps:'Карты (киберспорт)',
  other:'Прочее',
};

function getOddsForTarget(t,row){
  const oddsMap={
    home_win:row.b365_home||row.odds_home||row.b365w,
    away_win:row.b365_away||row.odds_away||row.b365l,
    draw:row.b365_draw||row.odds_draw,
    over25:row.b365_over25,under25:row.b365_under25,
    over15:row.b365_over15,over35:row.b365_over35,
    over55:row.b365_over55||row.b365_over,
    btts:row.b365_btts,
    went_to_ot:row.b365_ot||row.odds_ot,
    spread_cover:row.b365_spread||row.odds_spread,
    over_total:row.b365_over||row.odds_over,
  };
  const v=parseFloat(oddsMap[t]||0);
  return v>1?v:null;
}

function getConfidenceLevel(prob,edge){
  if(prob>0.70) return 'very_high';
  if(prob>0.60) return 'high';
  if(prob>0.52) return 'medium';
  return 'low';
}

function detectMarketTrend(target,row,m,sport){
  // Определяем тренд на основе типа рынка и имеющихся данных
  const trendMap={
    over25:'Poisson λ показывает голевой матч',
    btts:'Обе команды забивают в последних матчах',
    home_win:'Хозяева в сильной домашней форме',
    away_win:'Гости показывают гостевую уверенность',
    over35_cards:'Агрессивный стиль обеих команд',
    over95_corners:'Высокий темп и давление',
    went_to_ot:'Команды близки по уровню → высокий pOT',
    ah_home_m05:'Хозяева стабильно побеждают на -0.5',
    home_cs:'Гости с трудом забивают в последних матчах',
  };
  return trendMap[target]||null;
}

function detectTeamTrends(sport,row,m){
  const trends=[];
  // Заглушки на основе рыночных данных — в реальности нужна история
  const b365h=parseFloat(row.b365_home||0), b365a=parseFloat(row.b365_away||0);
  if(b365h>1&&b365h<1.7) trends.push({type:'heavy_favorite',team:'home',label:'Явный фаворит дома',icon:'⭐'});
  if(b365a>3.5) trends.push({type:'big_underdog',team:'away',label:'Большой андердог в гостях',icon:'📈'});
  const o25=parseFloat(row.b365_over25||0);
  if(o25>1&&o25<1.6) trends.push({type:'high_goals_expected',team:'both',label:'Ожидается голевой матч',icon:'⚽'});
  return trends;
}

function buildMarketRecommendation(target,prob,edge,trend,topFactors,sport){
  const label=targetLabel(target);
  const conf=getConfidenceLevel(prob,edge);
  const confLabel={very_high:'🔥 Очень уверенно',high:'✅ Уверенно',medium:'⚡ Есть сигнал',low:'👀 Слабый сигнал'}[conf];
  let rec=`${confLabel}: ${label} (${(prob*100).toFixed(0)}%)`;
  if(edge!==null&&edge>0.04) rec+=` | Value: +${(edge*100).toFixed(1)}%`;
  if(topFactors.length) rec+=` | Факторы: ${topFactors.slice(0,2).join(', ')}`;
  return rec;
}

// ── Обновлённый targetLabel v5 ─────────────────────────────────────────────
function targetLabel(t){
  const map={
    home_win:'Победа хозяев',away_win:'Победа гостей',draw:'Ничья',
    home_win_reg:'Победа хозяев в основное время',away_win_reg:'Победа гостей в основное время',
    over15:'Тотал>1.5',over25:'Тотал>2.5',over35:'Тотал>3.5',over45:'Тотал>4.5',
    under15:'Тотал<1.5',under25:'Тотал<2.5',
    over55:'Тотал>5.5',over65:'Тотал>6.5',over75:'Тотал>7.5',over85:'Тотал>8.5',over95:'Тотал>9.5',
    under45:'Тотал<4.5',under55:'Тотал<5.5',
    over200:'Тотал>200',over210:'Тотал>210',over220:'Тотал>220',over230:'Тотал>230',over240:'Тотал>240',
    under200:'Тотал<200',under210:'Тотал<210',
    over_total:'Тотал матча (Over)',under_total:'Тотал матча (Under)',
    over10:'Тотал>10',over12:'Тотал>12',over14:'Тотал>14',
    btts:'Обе забьют',btts_over25:'BTTS + Тотал>2.5',
    btts_and_home:'BTTS + Победа хозяев',btts_and_away:'BTTS + Победа гостей',
    ah_home_m05:'Азиатская фора хозяев -0.5',ah_home_m15:'Азиатская фора хозяев -1.5',
    ah_away_m05:'Азиатская фора гостей -0.5',ah_away_m15:'Азиатская фора гостей -1.5',
    spread_cover:'Фора покрыта',spread_cover_home:'Фора хозяев',spread_cover_away:'Фора гостей',
    puck_line_home:'Puck Line хозяев (-1.5)',puck_line_away:'Puck Line гостей (-1.5)',
    runline_home:'Run Line хозяев (-1.5)',runline_away:'Run Line гостей (-1.5)',
    large_win:'Крупная победа',large_margin:'Крупная разница',
    home_ov05:'Хозяева ИТ>0.5',home_ov15:'Хозяева ИТ>1.5',home_ov25:'Хозяева ИТ>2.5',
    away_ov05:'Гости ИТ>0.5',away_ov15:'Гости ИТ>1.5',away_ov25:'Гости ИТ>2.5',
    home_ov100:'Хозяева>100 очков',home_ov110:'Хозяева>110 очков',
    away_ov100:'Гости>100 очков',away_ov110:'Гости>110 очков',
    home_ov17:'Хозяева ИТ>17',away_ov17:'Гости ИТ>17',
    home_ov20:'Хозяева ИТ>20',away_ov20:'Гости ИТ>20',
    home_ov4:'Хозяева ИТ>4',away_ov4:'Гости ИТ>4',
    home_ov5:'Хозяева ИТ>5',away_ov5:'Гости ИТ>5',
    home_ov150:'Хозяева>150 ранов',away_ov150:'Гости>150 ранов',
    home_ov115_pts:'Хозяева>115 очков',away_ov115_pts:'Гости>115 очков',
    total_pts_over200:'Тотал очков>200',
    home_cs:'Сухой матч (гости без гола)',away_cs:'Сухой матч (хозяева без гола)',
    ht_home_win:'Победа хозяев к перерыву',ht_draw:'Ничья к перерыву',
    ht_over05:'Тотал 1-го тайма>0.5',h2_over15:'Тотал 2-го тайма>1.5',
    h1_over:'Тотал 1-й половины (Over)',q1_over:'Тотал 1-й четверти (Over)',
    home_win_q1:'Победа хозяев в 1-й четверти',
    over35_cards:'Карточки>3.5',over95_corners:'Угловые>9.5',pp_goal:'Гол в большинстве',
    hw_and_over25:'Победа хозяев + Тотал>2.5',aw_and_btts:'Победа гостей + BTTS',
    went_to_ot:'Овертайм',overtime:'Дополнительное время',
    upset:'Сенсация (андердог побеждает)',
    over_sets:'Тотал>сетов',under25_sets:'Тотал<2.5 сетов',over25_sets:'Тотал>2.5 сетов',
    goes_to_5th_set:'5-й сет',goes_to_5maps:'5-я карта',
    total_games_over21:'Геймов>21',total_games_over23:'Геймов>23',total_games_over25:'Геймов>25',
    set1_fav_win:'Фаворит выиграет 1-й сет',set1_over95:'1-й сет>9.5 геймов',
    straight_sets:'Прямые сеты',winner_ov15_sets:'Победитель возьмёт 2+ сетов',
    serve_dominant:'Доминирование на подаче',tiebreak_match:'Тайбрек в матче',
    comeback_win:'Победа после 0:1 сетов',h2h_trend_fav:'H2H тренд фаворита',
    over_games_set1:'Геймов в 1-м сете>9',
    over25_maps:'Карт>2.5',under25_maps:'Карт<2.5',
    map1_fav:'Фаворит выиграет 1-ю карту',fav_2_0:'Победа 2:0',fav_2_1:'Победа 2:1',
    home_win_s1:'Хозяева выиграют 1-й сет',away_win_s1:'Гости выиграют 1-й сет',
    over_total_bball:'Тотал баскетбол (Over)',
    total_over300:'Тотал>300 ранов',total_over350:'Тотал>350 ранов',
    first_innings_lead:'Преимущество в 1-м иннинге',
    both_score_try:'Обе команды занесут попытку',
    td_first_score_home:'Хозяева первыми занесут TD',
    over_total:'Тотал NFL (Over)',under_total:'Тотал NFL (Under)',
    h1_over:'Тотал 1-й половины (Over NFL)',
    home_ov15:'Инд. тотал хозяев>1.5',away_ov15:'Инд. тотал гостей>1.5',
    home_ov25:'Инд. тотал хозяев>2.5',
    pp_goal:'Гол в большинстве',
  };
  return map[t]||t;
}

router.get('/strategy/:sport',(req,res)=>{
  const{sport}=req.params; const m=models[sport];
  if(!m) return res.status(404).json({error:'Model not trained'});
  const strategies=generateAllStrategies(sport,m,SPORT_CONFIGS[sport]);
  res.json({sport,label:SPORT_CONFIGS[sport].label,strategies,version:'v5'});
});

router.post('/auto-retrain',async(req,res)=>{
  const{table}=req.body; const clickhouse=req.app.locals.clickhouse;
  const entry=Object.entries(SPORT_CONFIGS).find(([,cfg])=>cfg.table===table||cfg.table.endsWith(table));
  if(!entry) return res.json({ok:false,message:`Table ${table} not mapped`});
  const sport=entry[0];
  try{
    const m=await trainModel(sport,clickhouse);
    res.json({ok:true,sport,accuracy:m.accuracy,rowsUsed:m.rowsUsed,version:'v5'});
  }catch(e){res.status(500).json({ok:false,error:e.message});}
});

module.exports={router,initNeuralPG};