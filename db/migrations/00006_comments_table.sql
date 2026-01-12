DROP INDEX IF EXISTS idx_comments_video;
DROP INDEX IF EXISTS idx_comments_user;
DROP INDEX IF EXISTS idx_comments_parent;
DROP INDEX IF EXISTS idx_comments_video_time;
DROP TABLE IF EXISTS comments CASCADE;

CREATE TABLE comments (
    id SERIAL PRIMARY KEY,
    user_id INTEGER NOT NULL REFERENCES user_profiles(id),
    video_id INTEGER NOT NULL REFERENCES videos(id),
    parent_id INTEGER REFERENCES comments(id),
    content TEXT NOT NULL,
    like_count INTEGER DEFAULT 0,
    reply_count INTEGER DEFAULT 0,
    status VARCHAR(20) DEFAULT 'active',
    is_pinned BOOLEAN DEFAULT false,
    is_creator_heart BOOLEAN DEFAULT false,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT chk_comment_status CHECK (status IN ('active', 'deleted', 'hidden', 'spam'))
);

CREATE INDEX idx_comments_video ON comments(video_id);
CREATE INDEX idx_comments_user ON comments(user_id);
CREATE INDEX idx_comments_parent ON comments(parent_id);
CREATE INDEX idx_comments_video_time ON comments(video_id, created_at DESC);

-- Partial index for active comments only
CREATE INDEX idx_comments_active ON comments(video_id, created_at DESC) WHERE status = 'active';

-- Trigger to update reply_count on parent
CREATE OR REPLACE FUNCTION update_parent_reply_count()
RETURNS TRIGGER AS $$
BEGIN
    IF NEW.parent_id IS NOT NULL THEN
        UPDATE comments
        SET reply_count = reply_count + 1
        WHERE id = NEW.parent_id;
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trigger_update_reply_count
    AFTER INSERT ON comments
    FOR EACH ROW
    EXECUTE FUNCTION update_parent_reply_count();

-- Trigger to update updated_at
CREATE OR REPLACE FUNCTION update_comments_updated_at()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trigger_comments_updated_at
    BEFORE UPDATE ON comments
    FOR EACH ROW
    EXECUTE FUNCTION update_comments_updated_at();

