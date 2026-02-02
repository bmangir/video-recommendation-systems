-- User Embeddings Table
-- Stores user profile embeddings for content-based recommendations (Two-Tower Architecture)
-- User embedding is computed from: watch history, liked videos, search queries

DROP INDEX IF EXISTS idx_user_emb_updated;
DROP INDEX IF EXISTS idx_user_emb_model;
DROP TABLE IF EXISTS user_embeddings CASCADE;

CREATE TABLE user_embeddings (
    user_id INTEGER PRIMARY KEY REFERENCES user_profiles(id) ON DELETE CASCADE,

    -- Weighted combination of all signals
    user_embedding DOUBLE PRECISION[],

    -- Component embeddings
    watch_history_embedding DOUBLE PRECISION[],   -- Average of last N watched video embeddings
    liked_videos_embedding DOUBLE PRECISION[],    -- Average of liked video embeddings
    search_query_embedding DOUBLE PRECISION[],    -- BERT embedding of recent searches

    videos_watched_count INTEGER DEFAULT 0,
    videos_liked_count INTEGER DEFAULT 0,
    searches_count INTEGER DEFAULT 0,

    -- Embedding metadata
    embedding_dim INTEGER NOT NULL DEFAULT 384,   -- BERT MiniLM dimension

    -- Model information
    model_version VARCHAR(50) NOT NULL,
    model_params JSONB,                           -- {"watch_weight": 0.5, "like_weight": 0.3, ...}

    calculated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_user_emb_updated ON user_embeddings(updated_at DESC);
CREATE INDEX idx_user_emb_model ON user_embeddings(model_version);

-- Function to find recommended videos for a user using cosine similarity
CREATE OR REPLACE FUNCTION find_videos_for_user(
    p_user_id INTEGER,
    p_limit INTEGER DEFAULT 20,
    p_exclude_watched BOOLEAN DEFAULT true
)
RETURNS TABLE (
    video_id INTEGER,
    similarity_score DOUBLE PRECISION
) AS $$
DECLARE
    target_embedding DOUBLE PRECISION[];
BEGIN
    -- Get user's embedding
    SELECT user_embedding INTO target_embedding
    FROM user_embeddings
    WHERE user_id = p_user_id;

    IF target_embedding IS NULL THEN
        RAISE EXCEPTION 'No embedding found for user_id %', p_user_id;
    END IF;

    -- Find similar videos
    IF p_exclude_watched THEN
        RETURN QUERY
        SELECT
            ve.video_id,
            cosine_similarity(target_embedding, ve.combined_embedding) AS similarity_score
        FROM video_embeddings ve
        WHERE ve.is_latest = true
          AND ve.video_id NOT IN (
              SELECT wh.video_id 
              FROM watch_history wh 
              WHERE wh.user_id = p_user_id
          )
        ORDER BY similarity_score DESC
        LIMIT p_limit;
    ELSE
        RETURN QUERY
        SELECT
            ve.video_id,
            cosine_similarity(target_embedding, ve.combined_embedding) AS similarity_score
        FROM video_embeddings ve
        WHERE ve.is_latest = true
        ORDER BY similarity_score DESC
        LIMIT p_limit;
    END IF;
END;
$$ LANGUAGE plpgsql;

-- Function to get blended recommendations from all sources
CREATE OR REPLACE FUNCTION get_blended_recommendations(
    p_user_id INTEGER,
    p_limit INTEGER DEFAULT 20,
    p_content_weight DOUBLE PRECISION DEFAULT 0.4,
    p_als_weight DOUBLE PRECISION DEFAULT 0.4,
    p_trend_weight DOUBLE PRECISION DEFAULT 0.2
)
RETURNS TABLE (
    video_id INTEGER,
    final_score DOUBLE PRECISION,
    content_score DOUBLE PRECISION,
    als_score DOUBLE PRECISION,
    trend_score DOUBLE PRECISION,
    source VARCHAR(20)
) AS $$
BEGIN
    RETURN QUERY
    WITH 
    -- Content-based scores (Two-Tower)
    content_recs AS (
        SELECT 
            f.video_id,
            f.similarity_score AS score
        FROM find_videos_for_user(p_user_id, 100, true) f
    ),
    -- ALS collaborative filtering scores
    als_recs AS (
        SELECT 
            unnest(r.recommended_video_ids) AS video_id,
            unnest(r.scores)::DOUBLE PRECISION AS score
        FROM als_recommendations r
        WHERE r.user_id = p_user_id
          AND r.is_active = true
        ORDER BY r.generated_at DESC
        LIMIT 1
    ),
    -- Trending scores
    trend_recs AS (
        SELECT 
            t.video_id,
            t.daily_trend_score / 100.0 AS score  -- Normalize to 0-1
        FROM daily_trends t
        ORDER BY t.daily_trend_score DESC
        LIMIT 100
    ),
    -- Combine all sources
    all_videos AS (
        SELECT DISTINCT video_id FROM content_recs
        UNION
        SELECT DISTINCT video_id FROM als_recs
        UNION
        SELECT DISTINCT video_id FROM trend_recs
    ),
    -- Calculate blended scores
    blended AS (
        SELECT 
            av.video_id,
            COALESCE(c.score, 0) AS content_score,
            COALESCE(a.score, 0) AS als_score,
            COALESCE(t.score, 0) AS trend_score,
            (
                COALESCE(c.score, 0) * p_content_weight +
                COALESCE(a.score, 0) * p_als_weight +
                COALESCE(t.score, 0) * p_trend_weight
            ) AS final_score,
            CASE 
                WHEN c.score IS NOT NULL AND a.score IS NOT NULL THEN 'hybrid'
                WHEN c.score IS NOT NULL THEN 'content'
                WHEN a.score IS NOT NULL THEN 'collaborative'
                ELSE 'trending'
            END AS source
        FROM all_videos av
        LEFT JOIN content_recs c ON av.video_id = c.video_id
        LEFT JOIN als_recs a ON av.video_id = a.video_id
        LEFT JOIN trend_recs t ON av.video_id = t.video_id
    )
    SELECT 
        b.video_id,
        b.final_score,
        b.content_score,
        b.als_score,
        b.trend_score,
        b.source::VARCHAR(20)
    FROM blended b
    WHERE b.video_id NOT IN (
        SELECT wh.video_id FROM watch_history wh WHERE wh.user_id = p_user_id
    )
    ORDER BY b.final_score DESC
    LIMIT p_limit;
END;
$$ LANGUAGE plpgsql;

-- Trigger to update updated_at on any change
CREATE OR REPLACE FUNCTION update_user_embedding_timestamp()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trg_user_embedding_updated
    BEFORE UPDATE ON user_embeddings
    FOR EACH ROW
    EXECUTE FUNCTION update_user_embedding_timestamp();

COMMENT ON COLUMN user_embeddings.watch_history_embedding IS 'Average embedding of last N watched videos';
COMMENT ON COLUMN user_embeddings.liked_videos_embedding IS 'Average embedding of liked videos';
COMMENT ON COLUMN user_embeddings.search_query_embedding IS 'BERT embedding of concatenated recent search queries';
COMMENT ON FUNCTION find_videos_for_user IS 'Find top N similar videos for a user using cosine similarity';
COMMENT ON FUNCTION get_blended_recommendations IS 'Get recommendations blending content-based, ALS, and trending sources';

