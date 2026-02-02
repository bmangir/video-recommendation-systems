-- Searches Table
-- Tracks user search queries for analytics, personalization, and autocomplete.

DROP INDEX IF EXISTS idx_search_user;
DROP INDEX IF EXISTS idx_user_search_time;
DROP INDEX IF EXISTS idx_search_time;
DROP INDEX IF EXISTS idx_search_query_trgm;
DROP TABLE IF EXISTS activity.searches CASCADE;

-- Enable trigram extension for fuzzy search (autocomplete)
CREATE EXTENSION IF NOT EXISTS pg_trgm;

CREATE TABLE activity.searches (
    id SERIAL PRIMARY KEY,
    user_id INTEGER NOT NULL REFERENCES core.user_profiles(id) ON DELETE CASCADE,
    query TEXT,
    searched_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_search_user ON activity.searches(user_id);
CREATE INDEX idx_user_search_time ON activity.searches(user_id, searched_at DESC);
CREATE INDEX idx_search_time ON activity.searches(searched_at DESC);
CREATE INDEX idx_search_query_trgm ON activity.searches USING gin(query gin_trgm_ops);