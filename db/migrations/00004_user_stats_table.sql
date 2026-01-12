DROP INDEX IF EXISTS idx_user_stats_user_id;
DROP INDEX IF EXISTS idx_user_stats_status;
DROP INDEX IF EXISTS idx_user_stats_verified;
DROP TABLE IF EXISTS user_stats;

CREATE TABLE user_stats (
    user_id INTEGER NOT NULL UNIQUE REFERENCES user_profiles(id) ON DELETE CASCADE,
    is_verified BOOLEAN DEFAULT false,
    total_video_count INTEGER DEFAULT 0,
    total_views BIGINT DEFAULT 0,
    status VARCHAR(20) DEFAULT 'active',
    preferred_language CHAR(2) DEFAULT 'en',
    autoplay_enabled BOOLEAN DEFAULT true,
    notifications_enabled BOOLEAN DEFAULT true,
    theme VARCHAR(10) DEFAULT 'dark',
    default_video_quality VARCHAR(10) DEFAULT '720p',
    CONSTRAINT chk_status CHECK (status IN ('active', 'suspended', 'deleted', 'pending_verification')),
    CONSTRAINT chk_theme CHECK (theme IN ('dark', 'light', 'system')),
    CONSTRAINT chk_quality CHECK (default_video_quality IN ('auto', '360p', '480p', '720p', '1080p', '1440p', '4k'))
);

CREATE INDEX idx_user_stats_user_id ON user_stats(user_id);
CREATE INDEX idx_user_stats_status ON user_stats(status);
CREATE INDEX idx_user_stats_verified ON user_stats(is_verified) WHERE is_verified = true;
