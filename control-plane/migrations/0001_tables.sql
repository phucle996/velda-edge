-- Velda Edge Initial PostgreSQL Schema

CREATE TABLE IF NOT EXISTS routes (
    id VARCHAR(64) PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    paths JSONB NOT NULL DEFAULT '[]'::jsonb,
    hosts JSONB NOT NULL DEFAULT '[]'::jsonb,
    methods JSONB NOT NULL DEFAULT '[]'::jsonb,
    headers JSONB NOT NULL DEFAULT '{}'::jsonb,
    upstream_id VARCHAR(64) NOT NULL,
    plugins JSONB NOT NULL DEFAULT '[]'::jsonb,
    enabled BOOLEAN NOT NULL DEFAULT TRUE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS upstreams (
    id VARCHAR(64) PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    algorithm VARCHAR(32) NOT NULL DEFAULT 'round_robin',
    targets JSONB NOT NULL DEFAULT '[]'::jsonb,
    health_check JSONB NOT NULL DEFAULT '{}'::jsonb,
    connection_pool JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS certificates (
    id VARCHAR(64) PRIMARY KEY,
    domain VARCHAR(255) NOT NULL,
    cert_data TEXT NOT NULL,
    key_data TEXT NOT NULL,
    is_ca BOOLEAN NOT NULL DEFAULT FALSE,
    expires_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS config_snapshots (
    version BIGSERIAL PRIMARY KEY,
    hash VARCHAR(64) NOT NULL,
    snapshot_data JSONB NOT NULL,
    published_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
