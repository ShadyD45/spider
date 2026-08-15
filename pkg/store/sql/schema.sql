CREATE TABLE IF NOT EXISTS peers (
    node_id TEXT PRIMARY KEY,
    address TEXT NOT NULL,
    region TEXT NOT NULL DEFAULT '',
    zone TEXT NOT NULL DEFAULT '',
    rack TEXT NOT NULL DEFAULT '',
    host TEXT NOT NULL DEFAULT '',
    last_heartbeat INTEGER NOT NULL,
    status TEXT NOT NULL DEFAULT 'HEALTHY'
);

CREATE TABLE IF NOT EXISTS artifacts (
    artifact_id TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    version TEXT NOT NULL,
    manifest_json BLOB NOT NULL
);

CREATE TABLE IF NOT EXISTS artifact_seeds (
    artifact_id TEXT NOT NULL,
    node_id TEXT NOT NULL,
    updated_at INTEGER NOT NULL,
    PRIMARY KEY (artifact_id, node_id)
);

CREATE TABLE IF NOT EXISTS chunk_locations (
    chunk_hash TEXT NOT NULL,
    node_id TEXT NOT NULL,
    updated_at INTEGER NOT NULL,
    PRIMARY KEY (chunk_hash, node_id)
);

CREATE INDEX IF NOT EXISTS idx_chunk_locations_hash ON chunk_locations(chunk_hash);
CREATE INDEX IF NOT EXISTS idx_peers_heartbeat ON peers(last_heartbeat);
CREATE INDEX IF NOT EXISTS idx_seeds_artifact ON artifact_seeds(artifact_id);
