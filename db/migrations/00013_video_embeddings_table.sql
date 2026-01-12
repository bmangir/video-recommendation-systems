DROP INDEX IF EXISTS idx_video_emb_model_version;
DROP INDEX IF EXISTS idx_video_emb_latest;
DROP INDEX IF EXISTS idx_video_emb_calculated;
DROP TABLE IF EXISTS video_embeddings CASCADE;

CREATE TABLE video_embeddings (
    id SERIAL PRIMARY KEY,
    video_id INTEGER NOT NULL REFERENCES videos(id),

    -- Embedding vectors
    content_embedding DOUBLE PRECISION[],     -- From title, tags, description (text-based)
    behavioral_embedding DOUBLE PRECISION[],  -- From ALS item factors (collaborative)
    combined_embedding DOUBLE PRECISION[],    -- Concatenated or weighted combination

    -- Embedding metadata
    embedding_dim INTEGER NOT NULL,           -- Dimension size

    -- Model information
    model_version VARCHAR(50) NOT NULL,       -- 'emb_v1.2_bert_als'
    model_type VARCHAR(30),                   -- 'tfidf_svd', 'word2vec', 'sentence_bert', 'als_factors'
    model_params JSONB,                       -- {"rank": 128, "window": 5, ...}

    -- Source features used
    source_features JSONB,                    -- {"title": true, "tags": true, "als": true}

    calculated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,

    -- Version control: keep history of embeddings
    is_latest BOOLEAN DEFAULT true,

    -- Each video should have only one latest embedding
    UNIQUE(video_id, is_latest) -- Partial unique when is_latest = true
);

-- Get latest embedding for a video
CREATE INDEX idx_video_emb_latest ON video_embeddings(video_id)
    WHERE is_latest = true;

-- Get embeddings by model version
CREATE INDEX idx_video_emb_model ON video_embeddings(model_version, calculated_at DESC);

-- Cleanup old embeddings
CREATE INDEX idx_video_emb_cleanup ON video_embeddings(is_latest, calculated_at);

-- For batch updates
CREATE INDEX idx_video_emb_calculated ON video_embeddings(calculated_at DESC);

-- Function to calculate cosine similarity between two embeddings
CREATE OR REPLACE FUNCTION cosine_similarity(vec1 DOUBLE PRECISION[], vec2 DOUBLE PRECISION[])
RETURNS DOUBLE PRECISION AS $$
DECLARE
    dot_product DOUBLE PRECISION := 0;
    norm1 DOUBLE PRECISION := 0;
    norm2 DOUBLE PRECISION := 0;
    i INTEGER;
BEGIN
    IF array_length(vec1, 1) != array_length(vec2, 1) THEN
        RAISE EXCEPTION 'Vector dimensions must match';
    END IF;

    FOR i IN 1..array_length(vec1, 1) LOOP
        dot_product := dot_product + (vec1[i] * vec2[i]);
        norm1 := norm1 + (vec1[i] * vec1[i]);
        norm2 := norm2 + (vec2[i] * vec2[i]);
    END LOOP;

    IF norm1 = 0 OR norm2 = 0 THEN
        RETURN 0;
    END IF;

    RETURN dot_product / (sqrt(norm1) * sqrt(norm2));
END;
$$ LANGUAGE plpgsql IMMUTABLE;

-- Function to find similar videos by embedding
CREATE OR REPLACE FUNCTION find_similar_videos(
    p_video_id INTEGER,
    p_limit INTEGER DEFAULT 10,
    p_embedding_type VARCHAR DEFAULT 'combined'
)
RETURNS TABLE (
    similar_video_id INTEGER,
    similarity_score DOUBLE PRECISION
) AS $$
DECLARE
    target_embedding DOUBLE PRECISION[];
BEGIN
    -- Get target video's embedding
    EXECUTE format(
        'SELECT %I FROM video_embeddings WHERE video_id = $1 AND is_latest = true',
        p_embedding_type || '_embedding'
    ) INTO target_embedding USING p_video_id;

    IF target_embedding IS NULL THEN
        RAISE EXCEPTION 'No embedding found for video_id %', p_video_id;
    END IF;

    -- Find similar videos
    RETURN QUERY
    SELECT
        ve.video_id AS similar_video_id,
        cosine_similarity(
            target_embedding,
            CASE p_embedding_type
                WHEN 'content' THEN ve.content_embedding
                WHEN 'behavioral' THEN ve.behavioral_embedding
                ELSE ve.combined_embedding
            END
        ) AS similarity_score
    FROM video_embeddings ve
    WHERE ve.video_id != p_video_id
      AND ve.is_latest = true
    ORDER BY similarity_score DESC
    LIMIT p_limit;
END;
$$ LANGUAGE plpgsql;

-- Function to deactivate old embeddings (keep only latest per video)
CREATE OR REPLACE FUNCTION cleanup_old_embeddings()
RETURNS INTEGER AS $$
DECLARE
    updated_count INTEGER;
BEGIN
    WITH latest AS (
        SELECT DISTINCT ON (video_id) id
        FROM video_embeddings
        ORDER BY video_id, calculated_at DESC
    )
    UPDATE video_embeddings
    SET is_latest = false
    WHERE id NOT IN (SELECT id FROM latest)
      AND is_latest = true;

    GET DIAGNOSTICS updated_count = ROW_COUNT;
    RETURN updated_count;
END;
$$ LANGUAGE plpgsql;

COMMENT ON TABLE video_embeddings IS 'Vector embeddings for videos, used for similarity search and content-based recommendations.';
COMMENT ON COLUMN video_embeddings.content_embedding IS 'Text-based embedding from title, description, tags (e.g., TF-IDF, Word2Vec, BERT)';
COMMENT ON COLUMN video_embeddings.behavioral_embedding IS 'Collaborative filtering embedding from ALS item factors';
COMMENT ON COLUMN video_embeddings.combined_embedding IS 'Hybrid embedding combining content and behavioral signals';
COMMENT ON FUNCTION cosine_similarity IS 'Calculates cosine similarity between two vectors. Returns value between -1 and 1.';
COMMENT ON FUNCTION find_similar_videos IS 'Finds top N similar videos by embedding similarity.';
