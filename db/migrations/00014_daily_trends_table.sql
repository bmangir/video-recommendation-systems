DROP INDEX IF EXISTS idx_daily_trends_video_date;
DROP INDEX IF EXISTS idx_daily_trends_date;
DROP INDEX IF EXISTS idx_daily_trends_score;
DROP INDEX IF EXISTS idx_daily_trends_latest;
DROP TABLE IF EXISTS output.daily_trends CASCADE;

CREATE TABLE output.daily_trends (
    id SERIAL PRIMARY KEY,
    video_id INTEGER NOT NULL REFERENCES core.videos(id),
    date DATE NOT NULL DEFAULT CURRENT_DATE,
    
    -- Raw counts
    total_views INTEGER DEFAULT 0,
    total_likes INTEGER DEFAULT 0,
    total_dislikes INTEGER DEFAULT 0,
    total_comments INTEGER DEFAULT 0,
    total_completes INTEGER DEFAULT 0,
    
    -- Engagement metrics
    unique_viewers INTEGER DEFAULT 0,
    avg_watch_percentage DECIMAL(5,2) DEFAULT 0,
    completion_rate DECIMAL(5,2) DEFAULT 0,
    engagement_rate DECIMAL(5,4) DEFAULT 0,  -- (likes + comments) / views
    
    -- Velocity metrics (compared to previous day)
    views_velocity DECIMAL(5,2) DEFAULT 1.0,  -- views_today / views_yesterday
    likes_velocity DECIMAL(5,2) DEFAULT 1.0,
    
    -- Calculated scores
    daily_trend_score DECIMAL(10,4) DEFAULT 0,
    
    -- Metadata
    calculated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    -- Each video should have only one entry per day
    UNIQUE(video_id, date)
);

CREATE INDEX idx_daily_trends_video_date ON output.daily_trends(video_id, date DESC);
CREATE INDEX idx_daily_trends_date ON output.daily_trends(date DESC);
CREATE INDEX idx_daily_trends_score ON output.daily_trends(date, daily_trend_score DESC);
CREATE INDEX idx_daily_trends_trending_page ON output.daily_trends(date, daily_trend_score DESC, video_id);

COMMENT ON TABLE output.daily_trends IS 'Daily aggregated video statistics. One row per video per day.';
COMMENT ON COLUMN output.daily_trends.views_velocity IS 'Ratio of today views to yesterday views. >1 means growing.';
COMMENT ON COLUMN output.daily_trends.daily_trend_score IS 'Weighted score: 0.25*views + 0.20*likes + 0.15*completion + 0.15*engagement + 0.10*watch_pct + 0.10*velocity + 0.05*freshness';
