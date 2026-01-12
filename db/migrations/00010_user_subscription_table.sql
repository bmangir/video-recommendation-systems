DROP INDEX IF EXISTS idx_subs_subscriber;
DROP INDEX IF EXISTS idx_subs_channel;
DROP TABLE IF EXISTS user_subscriptions CASCADE;

CREATE TABLE user_subscriptions (
    id SERIAL PRIMARY KEY,
    subscriber_id INTEGER NOT NULL REFERENCES user_profiles(id),
    channel_id INTEGER NOT NULL REFERENCES user_profiles(id),
    notifications_enabled BOOLEAN DEFAULT true,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(subscriber_id, channel_id),
    CONSTRAINT chk_no_self_subscribe CHECK (subscriber_id != channel_id)
);

CREATE INDEX idx_subs_subscriber ON user_subscriptions(subscriber_id);
CREATE INDEX idx_subs_channel ON user_subscriptions(channel_id);

-- Index for notifications
CREATE INDEX idx_subs_notifications ON user_subscriptions(channel_id, notifications_enabled)
    WHERE notifications_enabled = true;

