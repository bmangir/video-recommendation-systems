-- Searches Table
-- Tracks user search queries for analytics, personalization, and autocomplete.

DROP INDEX IF EXISTS idx_search_user;
DROP INDEX IF EXISTS idx_user_search_time;
DROP INDEX IF EXISTS idx_search_time;
DROP INDEX IF EXISTS idx_search_query_trgm;
DROP TABLE IF EXISTS searches CASCADE;

-- Enable trigram extension for fuzzy search (autocomplete)
CREATE EXTENSION IF NOT EXISTS pg_trgm;

CREATE TABLE searches (
    id SERIAL PRIMARY KEY,
    user_id INTEGER NOT NULL REFERENCES user_profiles(id) ON DELETE CASCADE,
    query TEXT,
    searched_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_search_user ON searches(user_id);
CREATE INDEX idx_user_search_time ON searches(user_id, searched_at DESC);
CREATE INDEX idx_search_time ON searches(searched_at DESC);
CREATE INDEX idx_search_query_trgm ON searches USING gin(query gin_trgm_ops);