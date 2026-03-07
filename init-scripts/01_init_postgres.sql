-- BetQuant Pro — PostgreSQL init (users, strategies, alerts, journal)
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

CREATE TABLE IF NOT EXISTS users (
    id SERIAL PRIMARY KEY,
    username VARCHAR(50) UNIQUE NOT NULL,
    password_hash TEXT NOT NULL,
    email VARCHAR(100),
    role VARCHAR(20) DEFAULT 'user',
    created_at TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS strategies (
    id UUID DEFAULT uuid_generate_v4() PRIMARY KEY,
    user_id INT REFERENCES users(id),
    name VARCHAR(200) NOT NULL,
    description TEXT,
    code TEXT NOT NULL,
    sport VARCHAR(50),
    is_public BOOLEAN DEFAULT FALSE,
    tags TEXT[],
    backtest_roi NUMERIC(8,2),
    backtest_bets INT,
    created_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS journal (
    id UUID DEFAULT uuid_generate_v4() PRIMARY KEY,
    user_id INT REFERENCES users(id),
    date DATE NOT NULL,
    sport VARCHAR(50),
    match_name VARCHAR(200),
    market VARCHAR(100),
    selection VARCHAR(100),
    odds NUMERIC(8,3),
    stake NUMERIC(10,2),
    result VARCHAR(20),
    pnl NUMERIC(10,2),
    strategy_id UUID,
    bookmaker VARCHAR(50),
    notes TEXT,
    created_at TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS alerts (
    id UUID DEFAULT uuid_generate_v4() PRIMARY KEY,
    user_id INT REFERENCES users(id),
    name VARCHAR(200),
    type VARCHAR(50),
    conditions JSONB,
    channels TEXT[],
    is_active BOOLEAN DEFAULT TRUE,
    last_triggered TIMESTAMP,
    created_at TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS backtest_results (
    id UUID DEFAULT uuid_generate_v4() PRIMARY KEY,
    user_id INT REFERENCES users(id),
    strategy_id UUID,
    params JSONB,
    stats JSONB,
    created_at TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS clv_bets (
    id            SERIAL PRIMARY KEY,
    match_id      VARCHAR(64),
    match_name    VARCHAR(255) NOT NULL,
    market        VARCHAR(64)  NOT NULL DEFAULT '1X2',
    selection     VARCHAR(128),
    bet_odds      DECIMAL(6,3) NOT NULL,
    closing_odds  DECIMAL(6,3),
    clv_pct       DECIMAL(6,2),
    stake         DECIMAL(10,2) NOT NULL DEFAULT 10,
    result        VARCHAR(10),   -- win | loss | void
    pnl           DECIMAL(10,2),
    settled       BOOLEAN DEFAULT FALSE,
    bet_date      TIMESTAMPTZ DEFAULT NOW(),
    created_at    TIMESTAMPTZ DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS clv_bets_date_idx ON clv_bets(bet_date DESC);

-- Default admin user (password: admin123)
INSERT INTO users (username, password_hash, email, role) 
VALUES ('admin', '$2b$10$rQZ9K1mN2vX4yL6wH8pJ5eBfDgCsIt7oMrAkPuWnYxV3TqEj0.hei', 'admin@betquant.pro', 'admin')
ON CONFLICT DO NOTHING;

CREATE INDEX IF NOT EXISTS idx_journal_user ON journal(user_id);
CREATE INDEX IF NOT EXISTS idx_journal_date ON journal(date);
CREATE INDEX IF NOT EXISTS idx_strategies_user ON strategies(user_id);
CREATE INDEX IF NOT EXISTS idx_strategies_public ON strategies(is_public);

\echo 'PostgreSQL init complete'
