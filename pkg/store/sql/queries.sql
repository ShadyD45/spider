-- Named queries. Placeholders are "?" and rewritten to $N for Postgres.
-- SQLite and Postgres both accept EXCLUDED in ON CONFLICT updates.

-- name: UpsertPeer
INSERT INTO peers (node_id, address, region, zone, rack, host, last_heartbeat, status)
VALUES (?, ?, ?, ?, ?, ?, ?, ?)
ON CONFLICT(node_id) DO UPDATE SET
  address=EXCLUDED.address, region=EXCLUDED.region, zone=EXCLUDED.zone,
  rack=EXCLUDED.rack, host=EXCLUDED.host, last_heartbeat=EXCLUDED.last_heartbeat, status=EXCLUDED.status

-- name: Heartbeat
UPDATE peers SET last_heartbeat=? WHERE node_id=?

-- name: GetPeer
SELECT node_id, address, region, zone, rack, host, last_heartbeat, status FROM peers WHERE node_id=?

-- name: ListPeers
SELECT node_id, address, region, zone, rack, host, last_heartbeat, status FROM peers WHERE last_heartbeat>=?

-- name: DeleteSeedsForNode
DELETE FROM artifact_seeds WHERE node_id=?

-- name: DeleteChunksForNode
DELETE FROM chunk_locations WHERE node_id=?

-- name: DeletePeer
DELETE FROM peers WHERE node_id=?

-- name: DeleteSeedsExpired
DELETE FROM artifact_seeds WHERE node_id IN (SELECT node_id FROM peers WHERE last_heartbeat<?)

-- name: DeleteChunksExpired
DELETE FROM chunk_locations WHERE node_id IN (SELECT node_id FROM peers WHERE last_heartbeat<?)

-- name: DeletePeersExpired
DELETE FROM peers WHERE last_heartbeat<?

-- name: PutArtifact
INSERT INTO artifacts (artifact_id, name, version, manifest_json) VALUES (?, ?, ?, ?)
ON CONFLICT(artifact_id) DO UPDATE SET name=EXCLUDED.name, version=EXCLUDED.version, manifest_json=EXCLUDED.manifest_json

-- name: GetArtifact
SELECT artifact_id, name, version, manifest_json FROM artifacts WHERE artifact_id=?

-- name: ReportSeed
INSERT INTO artifact_seeds (artifact_id, node_id, updated_at) VALUES (?, ?, ?)
ON CONFLICT(artifact_id, node_id) DO UPDATE SET updated_at=EXCLUDED.updated_at

-- name: ListSeeds
SELECT node_id FROM artifact_seeds WHERE artifact_id=?

-- name: TouchPeer
UPDATE peers SET last_heartbeat=? WHERE node_id=?

-- name: UpsertChunk
INSERT INTO chunk_locations (chunk_hash, node_id, updated_at) VALUES (?, ?, ?)
ON CONFLICT(chunk_hash, node_id) DO UPDATE SET updated_at=EXCLUDED.updated_at

-- name: LocateChunkNodes
SELECT node_id FROM chunk_locations WHERE chunk_hash=?
