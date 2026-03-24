-- +goose Up
-- GateDrop initial schema using normalized pick_positions

CREATE EXTENSION IF NOT EXISTS pgcrypto;

CREATE TABLE seasons (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    name TEXT NOT NULL,
    starts_at TIMESTAMP NOT NULL,
    ends_at TIMESTAMP NOT NULL,
    is_active BOOLEAN NOT NULL DEFAULT FALSE
);

CREATE TABLE events (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    season_id UUID NOT NULL REFERENCES seasons(id) ON DELETE CASCADE,
    name TEXT NOT NULL,
    starts_at TIMESTAMP NOT NULL
);

CREATE TABLE classes (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    code TEXT NOT NULL UNIQUE
);

CREATE TABLE riders (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    first_name TEXT NOT NULL,
    last_name TEXT NOT NULL,
    class_id UUID REFERENCES classes(id) ON DELETE SET NULL
);

CREATE TABLE event_classes (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    event_id UUID NOT NULL REFERENCES events(id) ON DELETE CASCADE,
    class_id UUID NOT NULL REFERENCES classes(id) ON DELETE CASCADE,
    CONSTRAINT event_classes_event_id_class_id_key UNIQUE (event_id, class_id)
);

CREATE TABLE results (
    event_class_id UUID NOT NULL REFERENCES event_classes(id) ON DELETE CASCADE,
    rider_id UUID NOT NULL REFERENCES riders(id) ON DELETE CASCADE,
    position INT NOT NULL,
    PRIMARY KEY (event_class_id, rider_id),
    CONSTRAINT results_event_class_id_position_key UNIQUE (event_class_id, position),
    CONSTRAINT results_position_check CHECK (position > 0)
);

CREATE TABLE users (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    username TEXT NOT NULL UNIQUE,
    email TEXT NOT NULL UNIQUE,
    password_hash TEXT NOT NULL,
    balance INT NOT NULL DEFAULT 0,
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP NOT NULL DEFAULT NOW(),
    CONSTRAINT users_balance_check CHECK (balance >= 0)
);

CREATE TABLE picks (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    user_id UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    event_class_id UUID NOT NULL REFERENCES event_classes(id) ON DELETE CASCADE,
    wager INT NOT NULL,
    status TEXT NOT NULL DEFAULT 'submitted',
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP NOT NULL DEFAULT NOW(),
    CONSTRAINT picks_user_id_event_class_id_key UNIQUE (user_id, event_class_id),
    CONSTRAINT picks_wager_check CHECK (wager >= 0),
    CONSTRAINT picks_status_check CHECK (status IN ('draft', 'submitted', 'scored'))
);

CREATE TABLE pick_positions (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    pick_id UUID NOT NULL REFERENCES picks(id) ON DELETE CASCADE,
    rider_id UUID NOT NULL REFERENCES riders(id) ON DELETE CASCADE,
    position INT NOT NULL,
    CONSTRAINT pick_positions_pick_id_position_key UNIQUE (pick_id, position),
    CONSTRAINT pick_positions_pick_id_rider_id_key UNIQUE (pick_id, rider_id),
    CONSTRAINT pick_positions_position_check CHECK (position BETWEEN 1 AND 5)
);

CREATE INDEX idx_events_season_id ON events(season_id);
CREATE INDEX idx_riders_class_id ON riders(class_id);
CREATE INDEX idx_event_classes_event_id ON event_classes(event_id);
CREATE INDEX idx_event_classes_class_id ON event_classes(class_id);
CREATE INDEX idx_results_rider_id ON results(rider_id);
CREATE INDEX idx_picks_user_id ON picks(user_id);
CREATE INDEX idx_picks_event_class_id ON picks(event_class_id);
CREATE INDEX idx_picks_status ON picks(status);
CREATE INDEX idx_pick_positions_pick_id ON pick_positions(pick_id);
CREATE INDEX idx_pick_positions_rider_id ON pick_positions(rider_id);

-- +goose Down
DROP INDEX IF EXISTS idx_pick_positions_rider_id;
DROP INDEX IF EXISTS idx_pick_positions_pick_id;
DROP INDEX IF EXISTS idx_picks_status;
DROP INDEX IF EXISTS idx_picks_event_class_id;
DROP INDEX IF EXISTS idx_picks_user_id;
DROP INDEX IF EXISTS idx_results_rider_id;
DROP INDEX IF EXISTS idx_event_classes_class_id;
DROP INDEX IF EXISTS idx_event_classes_event_id;
DROP INDEX IF EXISTS idx_riders_class_id;
DROP INDEX IF EXISTS idx_events_season_id;

DROP TABLE IF EXISTS pick_positions;
DROP TABLE IF EXISTS picks;
DROP TABLE IF EXISTS users;
DROP TABLE IF EXISTS results;
DROP TABLE IF EXISTS event_classes;
DROP TABLE IF EXISTS riders;
DROP TABLE IF EXISTS classes;
DROP TABLE IF EXISTS events;
DROP TABLE IF EXISTS seasons;
