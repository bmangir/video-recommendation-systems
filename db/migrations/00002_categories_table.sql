DROP INDEX IF EXISTS idx_categories_slug;
DROP INDEX IF EXISTS idx_categories_parent;
DROP TABLE IF EXISTS core.categories CASCADE;

CREATE TABLE core.categories (
    id SERIAL PRIMARY KEY,
    name VARCHAR(100) NOT NULL UNIQUE,
    slug VARCHAR(100) NOT NULL UNIQUE,
    parent_id INTEGER REFERENCES core.categories(id),
    icon_url TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_categories_slug ON core.categories(slug);
CREATE INDEX idx_categories_parent ON core.categories(parent_id);