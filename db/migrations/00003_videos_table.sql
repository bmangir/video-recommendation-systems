DROP INDEX IF EXISTS idx_videos_category;
DROP INDEX IF EXISTS idx_videos_publisher;
DROP INDEX IF EXISTS idx_videos_uploaded;
DROP INDEX IF EXISTS idx_videos_tags;
DROP INDEX IF EXISTS idx_videos_status;
DROP TABLE IF EXISTS core.videos CASCADE;

CREATE TABLE core.videos (
    id SERIAL PRIMARY KEY,
    title VARCHAR(255) NOT NULL,
    description TEXT,
    duration_seconds INTEGER NOT NULL,
    category_id INTEGER REFERENCES core.categories(id),
    publisher_id INTEGER REFERENCES core.user_profiles(id),
    thumbnail_url TEXT,
    video_url TEXT NOT NULL,
    tags TEXT[],
    language CHAR(2) DEFAULT 'en',
    age_rating VARCHAR(10) DEFAULT 'G',
    status VARCHAR(20) DEFAULT 'active',
    is_monetized BOOLEAN DEFAULT false,
    uploaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_videos_category ON core.videos(category_id);
CREATE INDEX idx_videos_publisher ON core.videos(publisher_id);
CREATE INDEX idx_videos_uploaded ON core.videos(uploaded_at DESC);
CREATE INDEX idx_videos_tags ON core.videos USING GIN(tags);
CREATE INDEX idx_videos_status ON core.videos(status) WHERE status = 'active';

-- Trigger to update updated_at
CREATE OR REPLACE FUNCTION update_videos_updated_at()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trigger_videos_updated_at
    BEFORE UPDATE ON core.videos
    FOR EACH ROW
    EXECUTE FUNCTION update_videos_updated_at();
