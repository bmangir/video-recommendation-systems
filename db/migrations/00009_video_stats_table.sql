DROP INDEX IF EXISTS idx_video_stats_trending;
DROP INDEX IF EXISTS idx_video_stats_engagement;
DROP TABLE IF EXISTS activity.video_stats CASCADE;

CREATE TABLE activity.video_stats (
    video_id INTEGER PRIMARY KEY REFERENCES core.videos(id),

    -- Raw counts
    total_views INTEGER DEFAULT 0,
    total_likes INTEGER DEFAULT 0,
    total_dislikes INTEGER DEFAULT 0,
    total_comments INTEGER DEFAULT 0,
    total_shares INTEGER DEFAULT 0,

    -- Derived metrics
    unique_viewers INTEGER DEFAULT 0,
    total_watch_time_seconds BIGINT DEFAULT 0,
    avg_watch_percentage DECIMAL(5,2) DEFAULT 0,
    completion_rate DECIMAL(5,2) DEFAULT 0,

    -- Calculated scores
    engagement_score DECIMAL(10,4) DEFAULT 0,
    trending_score DECIMAL(10,4) DEFAULT 0,

    -- Metadata
    last_calculated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_video_stats_trending ON activity.video_stats(trending_score DESC);
CREATE INDEX idx_video_stats_engagement ON activity.video_stats(engagement_score DESC);
CREATE INDEX idx_video_stats_views ON activity.video_stats(total_views DESC);

-- Function to calculate and update video stats (called by batch job)
CREATE OR REPLACE FUNCTION calculate_video_stats(p_video_id INTEGER)
RETURNS void AS $$
DECLARE
    v_total_views INTEGER;
    v_total_likes INTEGER;
    v_total_dislikes INTEGER;
    v_total_comments INTEGER;
    v_unique_viewers INTEGER;
    v_total_watch_time BIGINT;
    v_avg_watch_pct DECIMAL(5,2);
    v_completion_rate DECIMAL(5,2);
    v_engagement_score DECIMAL(10,4);
    v_trending_score DECIMAL(10,4);
BEGIN
    -- Get view counts
    SELECT COUNT(*), COUNT(DISTINCT user_id), SUM(watched_seconds),
           AVG(watch_percentage),
           (COUNT(*) FILTER (WHERE is_completed = true)::DECIMAL / NULLIF(COUNT(*), 0)) * 100
    INTO v_total_views, v_unique_viewers, v_total_watch_time, v_avg_watch_pct, v_completion_rate
    FROM activity.watch_history
    WHERE video_id = p_video_id;

    -- Get like/dislike counts
    SELECT
        COUNT(*) FILTER (WHERE like_type = 1),
        COUNT(*) FILTER (WHERE like_type = -1)
    INTO v_total_likes, v_total_dislikes
    FROM activity.user_likes
    WHERE video_id = p_video_id;

    -- Get comment count
    SELECT COUNT(*) INTO v_total_comments
    FROM activity.comments
    WHERE video_id = p_video_id AND status = 'active';

    -- Calculate engagement score
    v_engagement_score := (
        COALESCE(v_total_likes, 0) * 2.0 +
        COALESCE(v_total_comments, 0) * 3.0 +
        COALESCE(v_completion_rate, 0) * 0.5
    ) / NULLIF(v_total_views, 0);

    -- Calculate trending score (simplified)
    v_trending_score := (
        COALESCE(v_total_views, 0) * 0.3 +
        COALESCE(v_total_likes, 0) * 0.4 +
        COALESCE(v_completion_rate, 0) * 0.3
    );

    -- Upsert stats
    INSERT INTO activity.video_stats (
        video_id, total_views, total_likes, total_dislikes, total_comments,
        unique_viewers, total_watch_time_seconds, avg_watch_percentage,
        completion_rate, engagement_score, trending_score, last_calculated_at
    ) VALUES (
        p_video_id, COALESCE(v_total_views, 0), COALESCE(v_total_likes, 0),
        COALESCE(v_total_dislikes, 0), COALESCE(v_total_comments, 0),
        COALESCE(v_unique_viewers, 0), COALESCE(v_total_watch_time, 0),
        COALESCE(v_avg_watch_pct, 0), COALESCE(v_completion_rate, 0),
        COALESCE(v_engagement_score, 0), COALESCE(v_trending_score, 0),
        CURRENT_TIMESTAMP
    )
    ON CONFLICT (video_id) DO UPDATE SET
        total_views = EXCLUDED.total_views,
        total_likes = EXCLUDED.total_likes,
        total_dislikes = EXCLUDED.total_dislikes,
        total_comments = EXCLUDED.total_comments,
        unique_viewers = EXCLUDED.unique_viewers,
        total_watch_time_seconds = EXCLUDED.total_watch_time_seconds,
        avg_watch_percentage = EXCLUDED.avg_watch_percentage,
        completion_rate = EXCLUDED.completion_rate,
        engagement_score = EXCLUDED.engagement_score,
        trending_score = EXCLUDED.trending_score,
        last_calculated_at = CURRENT_TIMESTAMP;
END;
$$ LANGUAGE plpgsql;

