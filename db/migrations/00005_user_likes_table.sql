DROP INDEX IF EXISTS idx_likes_user;
DROP INDEX IF EXISTS idx_likes_video;
DROP INDEX IF EXISTS idx_likes_type;
DROP TABLE IF EXISTS activity.user_likes CASCADE;

CREATE TABLE activity.user_likes (
    id SERIAL PRIMARY KEY,
    user_id INTEGER NOT NULL REFERENCES core.user_profiles(id),
    video_id INTEGER NOT NULL REFERENCES core.videos(id),
    like_type SMALLINT NOT NULL DEFAULT 1,  -- 1: like, -1: dislike
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(user_id, video_id),
    CONSTRAINT chk_like_type CHECK (like_type IN (1, -1))
);

CREATE INDEX idx_likes_user ON activity.user_likes(user_id);
CREATE INDEX idx_likes_video ON activity.user_likes(video_id);
CREATE INDEX idx_likes_type ON activity.user_likes(video_id, like_type);

-- Partial index for quick like counts
CREATE INDEX idx_likes_only ON activity.user_likes(video_id) WHERE like_type = 1;
CREATE INDEX idx_dislikes_only ON activity.user_likes(video_id) WHERE like_type = -1;
