DROP INDEX IF EXISTS idx_country;
DROP INDEX IF EXISTS idx_birthdate;
DROP TABLE IF EXISTS user_profiles;

CREATE TABLE user_profiles (
    id SERIAL PRIMARY KEY,
    username TEXT NOT NULL UNIQUE,
    birthdate DATE NOT NULL,
    country CHAR(2),
    email VARCHAR(255) NOT NULL UNIQUE,
    password TEXT NOT NULL,
    avatar_url TEXT,
    bio TEXT,
    is_creator BOOLEAN DEFAULT false,
    subscriber_count INTEGER DEFAULT 0,
    registered_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    last_active_at TIMESTAMP,
    CONSTRAINT chk_age CHECK (birthdate <= (CURRENT_DATE - INTERVAL '18 years'))
);

CREATE INDEX idx_country ON user_profiles(country);
CREATE INDEX idx_birthdate ON user_profiles(birthdate);