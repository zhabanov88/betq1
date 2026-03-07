-- =========================================
-- BETTING ADVANCED - PostgreSQL Schema
-- =========================================

-- Extensions
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
CREATE EXTENSION IF NOT EXISTS "pg_trgm";

-- =================== USERS ===================
CREATE TABLE IF NOT EXISTS users (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    username VARCHAR(50) UNIQUE NOT NULL,
    email VARCHAR(255) UNIQUE NOT NULL,
    password_hash VARCHAR(255) NOT NULL,
    role VARCHAR(20) DEFAULT 'user' CHECK (role IN ('user', 'admin', 'analyst')),
    subscription VARCHAR(20) DEFAULT 'free' CHECK (subscription IN ('free', 'pro', 'enterprise')),
    settings JSONB DEFAULT '{}',
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW(),
    last_login TIMESTAMPTZ
);

-- =================== SPORTS & COMPETITIONS ===================
CREATE TABLE IF NOT EXISTS sports (
    id SERIAL PRIMARY KEY,
    name VARCHAR(100) NOT NULL,
    slug VARCHAR(50) UNIQUE NOT NULL,
    icon VARCHAR(10),
    active BOOLEAN DEFAULT true
);

CREATE TABLE IF NOT EXISTS competitions (
    id VARCHAR(50) PRIMARY KEY,  -- e.g. "PL", "BL1", "CL"
    name VARCHAR(200) NOT NULL,
    country VARCHAR(100),
    sport_id INTEGER REFERENCES sports(id),
    tier INTEGER DEFAULT 1,
    seasons JSONB DEFAULT '[]',
    logo_url TEXT,
    active BOOLEAN DEFAULT true
);

CREATE TABLE IF NOT EXISTS teams (
    id VARCHAR(50) PRIMARY KEY,
    name VARCHAR(200) NOT NULL,
    short_name VARCHAR(50),
    country VARCHAR(100),
    sport_id INTEGER REFERENCES sports(id),
    founded INTEGER,
    venue VARCHAR(200),
    logo_url TEXT,
    metadata JSONB DEFAULT '{}'
);

-- =================== MATCHES ===================
CREATE TABLE IF NOT EXISTS matches (
    id VARCHAR(100) PRIMARY KEY,
    competition_id VARCHAR(50) REFERENCES competitions(id),
    season VARCHAR(20) NOT NULL,
    matchday INTEGER,
    home_team_id VARCHAR(50) REFERENCES teams(id),
    away_team_id VARCHAR(50) REFERENCES teams(id),
    scheduled_at TIMESTAMPTZ NOT NULL,
    status VARCHAR(30) DEFAULT 'scheduled',
    score_home INTEGER,
    score_away INTEGER,
    score_ht_home INTEGER,
    score_ht_away INTEGER,
    venue VARCHAR(200),
    referee VARCHAR(200),
    attendance INTEGER,
    weather JSONB,
    metadata JSONB DEFAULT '{}',
    created_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_matches_competition ON matches(competition_id);
CREATE INDEX IF NOT EXISTS idx_matches_home_team ON matches(home_team_id);
CREATE INDEX IF NOT EXISTS idx_matches_away_team ON matches(away_team_id);
CREATE INDEX IF NOT EXISTS idx_matches_scheduled ON matches(scheduled_at);
CREATE INDEX IF NOT EXISTS idx_matches_status ON matches(status);

-- =================== MATCH STATISTICS ===================
CREATE TABLE IF NOT EXISTS match_stats (
    match_id VARCHAR(100) PRIMARY KEY REFERENCES matches(id),
    home_shots INTEGER,
    away_shots INTEGER,
    home_shots_on_target INTEGER,
    away_shots_on_target INTEGER,
    home_possession DECIMAL(5,2),
    away_possession DECIMAL(5,2),
    home_corners INTEGER,
    away_corners INTEGER,
    home_fouls INTEGER,
    away_fouls INTEGER,
    home_yellow_cards INTEGER,
    away_yellow_cards INTEGER,
    home_red_cards INTEGER,
    away_red_cards INTEGER,
    home_xg DECIMAL(6,3),
    away_xg DECIMAL(6,3),
    home_passes INTEGER,
    away_passes INTEGER,
    home_pass_accuracy DECIMAL(5,2),
    away_pass_accuracy DECIMAL(5,2),
    raw_data JSONB DEFAULT '{}'
);

-- =================== ODDS ===================
CREATE TABLE IF NOT EXISTS odds_snapshots (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    match_id VARCHAR(100) REFERENCES matches(id),
    bookmaker VARCHAR(100) NOT NULL,
    market VARCHAR(50) NOT NULL,  -- 1x2, btts, over_under, asian_handicap, etc
    selection VARCHAR(100) NOT NULL,  -- home, draw, away, yes, no, over_2.5, etc
    odds DECIMAL(10,4) NOT NULL,
    recorded_at TIMESTAMPTZ DEFAULT NOW(),
    is_closing BOOLEAN DEFAULT false,
    source VARCHAR(50)
);

CREATE INDEX IF NOT EXISTS idx_odds_match ON odds_snapshots(match_id);
CREATE INDEX IF NOT EXISTS idx_odds_bookmaker ON odds_snapshots(bookmaker);
CREATE INDEX IF NOT EXISTS idx_odds_market ON odds_snapshots(market);
CREATE INDEX IF NOT EXISTS idx_odds_recorded ON odds_snapshots(recorded_at);

-- =================== STRATEGIES ===================
CREATE TABLE IF NOT EXISTS strategies (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    user_id UUID REFERENCES users(id) ON DELETE CASCADE,
    name VARCHAR(200) NOT NULL,
    description TEXT,
    sport_id INTEGER REFERENCES sports(id),
    competition_ids TEXT[] DEFAULT '{}',
    code TEXT NOT NULL,  -- JavaScript strategy code
    language VARCHAR(20) DEFAULT 'js',
    parameters JSONB DEFAULT '{}',
    tags TEXT[] DEFAULT '{}',
    is_public BOOLEAN DEFAULT false,
    is_ai_generated BOOLEAN DEFAULT false,
    ai_prompt TEXT,
    version INTEGER DEFAULT 1,
    parent_id UUID REFERENCES strategies(id),
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_strategies_user ON strategies(user_id);
CREATE INDEX IF NOT EXISTS idx_strategies_sport ON strategies(sport_id);
CREATE INDEX IF NOT EXISTS idx_strategies_public ON strategies(is_public);

-- =================== BACKTEST RESULTS ===================
CREATE TABLE IF NOT EXISTS backtests (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    user_id UUID REFERENCES users(id) ON DELETE CASCADE,
    strategy_id UUID REFERENCES strategies(id),
    name VARCHAR(200),
    config JSONB NOT NULL,  -- date range, competitions, initial bankroll, staking, etc
    status VARCHAR(20) DEFAULT 'pending' CHECK (status IN ('pending', 'running', 'completed', 'failed')),
    -- Summary stats
    total_bets INTEGER DEFAULT 0,
    winning_bets INTEGER DEFAULT 0,
    losing_bets INTEGER DEFAULT 0,
    void_bets INTEGER DEFAULT 0,
    win_rate DECIMAL(5,4),
    roi DECIMAL(10,4),  -- Return on Investment
    yield DECIMAL(10,4),
    profit DECIMAL(12,2),
    max_drawdown DECIMAL(10,4),
    max_drawdown_amount DECIMAL(12,2),
    sharpe_ratio DECIMAL(10,4),
    sortino_ratio DECIMAL(10,4),
    kelly_criterion DECIMAL(10,4),
    avg_odds DECIMAL(8,4),
    avg_stake DECIMAL(10,2),
    initial_bankroll DECIMAL(12,2) DEFAULT 1000,
    final_bankroll DECIMAL(12,2),
    -- Series data stored in ClickHouse, referenced here
    ch_table_ref VARCHAR(200),
    error_message TEXT,
    duration_ms INTEGER,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    completed_at TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS idx_backtests_user ON backtests(user_id);
CREATE INDEX IF NOT EXISTS idx_backtests_strategy ON backtests(strategy_id);
CREATE INDEX IF NOT EXISTS idx_backtests_status ON backtests(status);

-- =================== WALK-FORWARD OPTIMIZATION ===================
CREATE TABLE IF NOT EXISTS walkforward_tests (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    user_id UUID REFERENCES users(id) ON DELETE CASCADE,
    strategy_id UUID REFERENCES strategies(id),
    config JSONB NOT NULL,
    windows JSONB DEFAULT '[]',  -- array of {train_start, train_end, test_start, test_end, params, results}
    overall_stats JSONB DEFAULT '{}',
    status VARCHAR(20) DEFAULT 'pending',
    created_at TIMESTAMPTZ DEFAULT NOW(),
    completed_at TIMESTAMPTZ
);

-- =================== ALERTS ===================
CREATE TABLE IF NOT EXISTS alerts (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    user_id UUID REFERENCES users(id) ON DELETE CASCADE,
    name VARCHAR(200) NOT NULL,
    type VARCHAR(50) NOT NULL,  -- odds_movement, value_bet, strategy_trigger, etc
    conditions JSONB NOT NULL,
    notification_channels TEXT[] DEFAULT '{email}',
    is_active BOOLEAN DEFAULT true,
    last_triggered TIMESTAMPTZ,
    trigger_count INTEGER DEFAULT 0,
    created_at TIMESTAMPTZ DEFAULT NOW()
);

-- =================== VALUE BETS LOG ===================
CREATE TABLE IF NOT EXISTS value_bets (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    user_id UUID REFERENCES users(id),
    match_id VARCHAR(100) REFERENCES matches(id),
    market VARCHAR(50) NOT NULL,
    selection VARCHAR(100) NOT NULL,
    bookmaker VARCHAR(100) NOT NULL,
    bookmaker_odds DECIMAL(10,4) NOT NULL,
    fair_odds DECIMAL(10,4) NOT NULL,
    edge DECIMAL(8,4) NOT NULL,  -- (bookmaker_odds / fair_odds - 1)
    kelly_stake DECIMAL(8,4),
    recommended_stake DECIMAL(10,2),
    model VARCHAR(100),
    confidence DECIMAL(5,4),
    status VARCHAR(20) DEFAULT 'pending',  -- pending, won, lost, void
    profit DECIMAL(12,2),
    created_at TIMESTAMPTZ DEFAULT NOW(),
    settled_at TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS idx_value_bets_user ON value_bets(user_id);
CREATE INDEX IF NOT EXISTS idx_value_bets_match ON value_bets(match_id);
CREATE INDEX IF NOT EXISTS idx_value_bets_created ON value_bets(created_at);

-- =================== BANKROLL TRACKER ===================
CREATE TABLE IF NOT EXISTS bankroll_entries (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    user_id UUID REFERENCES users(id) ON DELETE CASCADE,
    date DATE NOT NULL,
    balance DECIMAL(12,2) NOT NULL,
    deposit DECIMAL(12,2) DEFAULT 0,
    withdrawal DECIMAL(12,2) DEFAULT 0,
    profit_loss DECIMAL(12,2) DEFAULT 0,
    bets_count INTEGER DEFAULT 0,
    notes TEXT,
    created_at TIMESTAMPTZ DEFAULT NOW()
);

-- =================== PLAYER STATISTICS ===================
CREATE TABLE IF NOT EXISTS players (
    id VARCHAR(50) PRIMARY KEY,
    name VARCHAR(200) NOT NULL,
    nationality VARCHAR(100),
    position VARCHAR(50),
    date_of_birth DATE,
    team_id VARCHAR(50) REFERENCES teams(id),
    market_value BIGINT,  -- in euros
    metadata JSONB DEFAULT '{}'
);

CREATE TABLE IF NOT EXISTS player_match_stats (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    player_id VARCHAR(50) REFERENCES players(id),
    match_id VARCHAR(100) REFERENCES matches(id),
    minutes_played INTEGER,
    goals INTEGER DEFAULT 0,
    assists INTEGER DEFAULT 0,
    yellow_cards INTEGER DEFAULT 0,
    red_cards INTEGER DEFAULT 0,
    shots INTEGER DEFAULT 0,
    shots_on_target INTEGER DEFAULT 0,
    key_passes INTEGER DEFAULT 0,
    dribbles INTEGER DEFAULT 0,
    xg DECIMAL(6,3),
    xa DECIMAL(6,3),
    raw_data JSONB DEFAULT '{}'
);

CREATE INDEX IF NOT EXISTS idx_player_stats_player ON player_match_stats(player_id);
CREATE INDEX IF NOT EXISTS idx_player_stats_match ON player_match_stats(match_id);

-- =================== AUDIT LOG ===================
CREATE TABLE IF NOT EXISTS audit_log (
    id BIGSERIAL PRIMARY KEY,
    user_id UUID,
    action VARCHAR(100) NOT NULL,
    entity_type VARCHAR(50),
    entity_id VARCHAR(100),
    changes JSONB,
    ip_address INET,
    user_agent TEXT,
    created_at TIMESTAMPTZ DEFAULT NOW()
);

-- =================== SEED DATA ===================
INSERT INTO sports (name, slug, icon) VALUES
    ('Football', 'football', '⚽'),
    ('Basketball', 'basketball', '🏀'),
    ('Tennis', 'tennis', '🎾'),
    ('Ice Hockey', 'hockey', '🏒'),
    ('Baseball', 'baseball', '⚾'),
    ('American Football', 'american-football', '🏈'),
    ('Rugby', 'rugby', '🏉'),
    ('Cricket', 'cricket', '🏏'),
    ('Handball', 'handball', '🤾'),
    ('Volleyball', 'volleyball', '🏐')
ON CONFLICT DO NOTHING;

INSERT INTO competitions (id, name, country, sport_id, tier) VALUES
    ('PL', 'Premier League', 'England', 1, 1),
    ('BL1', 'Bundesliga', 'Germany', 1, 1),
    ('SA', 'Serie A', 'Italy', 1, 1),
    ('PD', 'La Liga', 'Spain', 1, 1),
    ('FL1', 'Ligue 1', 'France', 1, 1),
    ('ELC', 'Championship', 'England', 1, 2),
    ('PPL', 'Primeira Liga', 'Portugal', 1, 1),
    ('DED', 'Eredivisie', 'Netherlands', 1, 1),
    ('BSA', 'Brasileirao', 'Brazil', 1, 1),
    ('MLS', 'Major League Soccer', 'USA', 1, 1),
    ('CL', 'UEFA Champions League', 'Europe', 1, 1),
    ('EL', 'UEFA Europa League', 'Europe', 1, 1),
    ('NBA', 'NBA', 'USA', 2, 1),
    ('ATP', 'ATP Tour', 'International', 3, 1),
    ('NHL', 'NHL', 'USA/Canada', 4, 1),
    ('MLB', 'MLB', 'USA', 5, 1),
    ('NFL', 'NFL', 'USA', 6, 1)
ON CONFLICT DO NOTHING;

-- Default admin user (password: Admin2024!)
INSERT INTO users (username, email, password_hash, role) VALUES
    ('admin', 'admin@betting-advanced.local', '$2b$12$K8gNfxV5eLt7qBn2mY9lZeY9rB3pX4wQ1vH6jK8nM2oP5sT7uA9bC', 'admin')
ON CONFLICT DO NOTHING;

-- Update trigger
CREATE OR REPLACE FUNCTION update_updated_at()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER update_users_updated_at BEFORE UPDATE ON users
    FOR EACH ROW EXECUTE FUNCTION update_updated_at();

CREATE TRIGGER update_strategies_updated_at BEFORE UPDATE ON strategies
    FOR EACH ROW EXECUTE FUNCTION update_updated_at();
