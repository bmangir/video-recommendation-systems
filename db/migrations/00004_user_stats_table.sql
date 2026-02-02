DROP INDEX IF EXISTS idx_user_stats_user_id;
DROP INDEX IF EXISTS idx_user_stats_status;
DROP INDEX IF EXISTS idx_user_stats_verified;
DROP TABLE IF EXISTS core.user_stats CASCADE;

CREATE TABLE core.user_stats (
    user_id INTEGER NOT NULL UNIQUE REFERENCES core.user_profiles(id) ON DELETE CASCADE,
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

CREATE INDEX idx_user_stats_user_id ON core.user_stats(user_id);
CREATE INDEX idx_user_stats_status ON core.user_stats(status);
CREATE INDEX idx_user_stats_verified ON core.user_stats(is_verified) WHERE is_verified = true;

-- Function to calculate and update user stats
CREATE OR REPLACE FUNCTION calculate_user_stats(p_user_id INTEGER)
RETURNS void AS $$
DECLARE
    v_total_video_count INTEGER;
    v_total_views BIGINT;
BEGIN
    -- Get total video count for this user (as publisher)
    SELECT COUNT(*)
    INTO v_total_video_count
    FROM core.videos
    WHERE publisher_id = p_user_id AND status = 'active';

    -- Get total views across all user's videos
    SELECT COALESCE(SUM(vs.total_views), 0)
    INTO v_total_views
    FROM core.videos v
    JOIN activity.video_stats vs ON v.id = vs.video_id
    WHERE v.publisher_id = p_user_id AND v.status = 'active';

    -- Upsert user stats
    INSERT INTO core.user_stats (user_id, total_video_count, total_views)
    VALUES (p_user_id, COALESCE(v_total_video_count, 0), COALESCE(v_total_views, 0))
    ON CONFLICT (user_id) DO UPDATE SET
        total_video_count = EXCLUDED.total_video_count,
        total_views = EXCLUDED.total_views;
END;
$$ LANGUAGE plpgsql;

-- Function to calculate stats for all users
CREATE OR REPLACE FUNCTION calculate_all_user_stats()
RETURNS INTEGER AS $$
DECLARE
    v_user_id INTEGER;
    v_count INTEGER := 0;
BEGIN
    FOR v_user_id IN 
        SELECT DISTINCT id FROM core.user_profiles WHERE is_creator = true
    LOOP
        PERFORM calculate_user_stats(v_user_id);
        v_count := v_count + 1;
    END LOOP;
    
    RETURN v_count;
END;
$$ LANGUAGE plpgsql;

COMMENT ON FUNCTION calculate_user_stats IS 'Calculates and updates stats for a single user (video count, total views).';
COMMENT ON FUNCTION calculate_all_user_stats IS 'Batch function to update stats for all creator users.';
