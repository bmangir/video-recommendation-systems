DROP INDEX IF EXISTS idx_watch_user;
DROP INDEX IF EXISTS idx_watch_video;
DROP INDEX IF EXISTS idx_watch_completed;
DROP INDEX IF EXISTS idx_watch_last;
DROP INDEX IF EXISTS idx_watch_percentage;
DROP TABLE IF EXISTS watch_history CASCADE;

CREATE TABLE watch_history (
    id SERIAL PRIMARY KEY,
    user_id INTEGER NOT NULL REFERENCES user_profiles(id),
    video_id INTEGER NOT NULL REFERENCES videos(id),
    watched_seconds INTEGER NOT NULL DEFAULT 0,
    video_duration_seconds INTEGER NOT NULL,
    watch_percentage DECIMAL(5,2) GENERATED ALWAYS AS (
        ROUND((watched_seconds::DECIMAL / NULLIF(video_duration_seconds, 0)) * 100, 2)
    ) STORED,
    is_completed BOOLEAN DEFAULT false,
    watch_count INTEGER DEFAULT 1,

    -- Resume functionality
    last_position_seconds INTEGER DEFAULT 0,

    device_type VARCHAR(20),
    source VARCHAR(50),
    first_watched_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    last_watched_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(user_id, video_id)
);

CREATE INDEX idx_watch_user ON watch_history(user_id);
CREATE INDEX idx_watch_video ON watch_history(video_id);
CREATE INDEX idx_watch_completed ON watch_history(is_completed) WHERE is_completed = true;
CREATE INDEX idx_watch_last ON watch_history(user_id, last_watched_at DESC);
CREATE INDEX idx_watch_percentage ON watch_history(watch_percentage);

-- Composite index for ALS recommendations query
CREATE INDEX idx_watch_user_video_completed ON watch_history(user_id, video_id, is_completed);

-- Trigger to auto-set is_completed when watch_percentage >= 90
CREATE OR REPLACE FUNCTION update_watch_completion()
RETURNS TRIGGER AS $$
BEGIN
    IF NEW.watched_seconds::DECIMAL / NULLIF(NEW.video_duration_seconds, 0) >= 0.90 THEN
        NEW.is_completed = true;
    END IF;
    NEW.last_watched_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trigger_watch_completion
    BEFORE INSERT OR UPDATE ON watch_history
    FOR EACH ROW
    EXECUTE FUNCTION update_watch_completion();

