package store

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"
	_ "modernc.org/sqlite"
)

func init() {
	Register("sqlite", func(opts Options) (Store, error) {
		return openSQL("sqlite", opts)
	})
	Register("postgres", func(opts Options) (Store, error) {
		return openSQL("pgx", opts)
	})
}

// SQLStore is a pooled database/sql tracker backend.
type SQLStore struct {
	db      *sql.DB
	driver  string
	queries map[string]string
}

func openSQL(driver string, opts Options) (*SQLStore, error) {
	dsn := opts.DSN
	if driver == "sqlite" {
		dsn = sqliteDSN(dsn)
	}
	db, err := sql.Open(driver, dsn)
	if err != nil {
		return nil, fmt.Errorf("sql open: %w", err)
	}
	configurePool(db, driver, opts.Pool)
	if err := db.Ping(); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("sql ping: %w", err)
	}
	if _, err := db.Exec(loadSQL("sql/schema.sql")); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("migrate: %w", err)
	}
	name := "sqlite"
	if driver == "pgx" {
		name = "postgres"
	}
	q := parseNamedQueries(loadSQL("sql/queries.sql"))
	if name == "postgres" {
		for k, v := range q {
			q[k] = placeholdersPostgres(v)
		}
	}
	return &SQLStore{db: db, driver: name, queries: q}, nil
}

func sqliteDSN(dsn string) string {
	if dsn == "" {
		dsn = "file:spider.db?cache=shared"
	}
	if dsn != ":memory:" && !strings.HasPrefix(dsn, "file:") {
		dsn = "file:" + strings.ReplaceAll(dsn, "\\", "/")
	}
	if !strings.Contains(dsn, "?") {
		dsn += "?_pragma=busy_timeout(5000)&_pragma=journal_mode(WAL)&_pragma=foreign_keys(1)"
	}
	return dsn
}

func configurePool(db *sql.DB, driver string, p Pool) {
	if driver == "sqlite" {
		if p.MaxOpenConns <= 0 {
			p.MaxOpenConns = 8
		}
		if p.MaxIdleConns <= 0 {
			p.MaxIdleConns = p.MaxOpenConns
		}
		db.SetMaxOpenConns(p.MaxOpenConns)
		db.SetMaxIdleConns(p.MaxIdleConns)
		if p.ConnMaxLifetime > 0 {
			db.SetConnMaxLifetime(p.ConnMaxLifetime)
		}
		if p.ConnMaxIdleTime > 0 {
			db.SetConnMaxIdleTime(p.ConnMaxIdleTime)
		}
		return
	}
	if p.MaxOpenConns <= 0 {
		p.MaxOpenConns = 25
	}
	if p.MaxIdleConns <= 0 {
		p.MaxIdleConns = 5
	}
	if p.ConnMaxLifetime <= 0 {
		p.ConnMaxLifetime = 5 * time.Minute
	}
	if p.ConnMaxIdleTime <= 0 {
		p.ConnMaxIdleTime = time.Minute
	}
	db.SetMaxOpenConns(p.MaxOpenConns)
	db.SetMaxIdleConns(p.MaxIdleConns)
	db.SetConnMaxLifetime(p.ConnMaxLifetime)
	db.SetConnMaxIdleTime(p.ConnMaxIdleTime)
}

func (s *SQLStore) q(name string) string {
	sql, ok := s.queries[name]
	if !ok {
		panic("unknown query " + name)
	}
	return sql
}

func (s *SQLStore) Name() string { return s.driver }

func (s *SQLStore) Ping(ctx context.Context) error { return s.db.PingContext(ctx) }

func (s *SQLStore) Close() error { return s.db.Close() }

func (s *SQLStore) UpsertPeer(ctx context.Context, peer Peer) error {
	if peer.NodeID == "" {
		return nil
	}
	if peer.LastHeartbeat.IsZero() {
		peer.LastHeartbeat = time.Now()
	}
	if peer.Status == "" {
		peer.Status = "HEALTHY"
	}
	_, err := s.db.ExecContext(ctx, s.q("UpsertPeer"),
		peer.NodeID, peer.Address, peer.Region, peer.Zone, peer.Rack, peer.Host, peer.LastHeartbeat.Unix(), peer.Status)
	if err != nil {
		return fmt.Errorf("upsert peer: %w", err)
	}
	return nil
}

func (s *SQLStore) Heartbeat(ctx context.Context, nodeID string) (bool, error) {
	res, err := s.db.ExecContext(ctx, s.q("Heartbeat"), time.Now().Unix(), nodeID)
	if err != nil {
		return false, fmt.Errorf("heartbeat: %w", err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		return false, err
	}
	return n > 0, nil
}

func (s *SQLStore) GetPeer(ctx context.Context, nodeID string) (*Peer, error) {
	row := s.db.QueryRowContext(ctx, s.q("GetPeer"), nodeID)
	p, err := scanPeer(row)
	if err != nil {
		return nil, fmt.Errorf("get peer: %w", err)
	}
	return p, nil
}

func scanPeer(row *sql.Row) (*Peer, error) {
	var p Peer
	var ts int64
	err := row.Scan(&p.NodeID, &p.Address, &p.Region, &p.Zone, &p.Rack, &p.Host, &ts, &p.Status)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	p.LastHeartbeat = time.Unix(ts, 0)
	return &p, nil
}

func (s *SQLStore) ListPeers(ctx context.Context, expiry time.Duration) ([]Peer, error) {
	cutoff := int64(0)
	if expiry > 0 {
		cutoff = time.Now().Add(-expiry).Unix()
	}
	rows, err := s.db.QueryContext(ctx, s.q("ListPeers"), cutoff)
	if err != nil {
		return nil, fmt.Errorf("list peers: %w", err)
	}
	defer rows.Close()
	var out []Peer
	for rows.Next() {
		var p Peer
		var ts int64
		if err := rows.Scan(&p.NodeID, &p.Address, &p.Region, &p.Zone, &p.Rack, &p.Host, &ts, &p.Status); err != nil {
			return nil, err
		}
		p.LastHeartbeat = time.Unix(ts, 0)
		out = append(out, p)
	}
	return out, rows.Err()
}

func (s *SQLStore) DeregisterPeer(ctx context.Context, nodeID string) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("deregister begin: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	if _, err := tx.ExecContext(ctx, s.q("DeleteSeedsForNode"), nodeID); err != nil {
		return fmt.Errorf("delete seeds: %w", err)
	}
	if _, err := tx.ExecContext(ctx, s.q("DeleteChunksForNode"), nodeID); err != nil {
		return fmt.Errorf("delete chunks: %w", err)
	}
	if _, err := tx.ExecContext(ctx, s.q("DeletePeer"), nodeID); err != nil {
		return fmt.Errorf("delete peer: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("deregister commit: %w", err)
	}
	return nil
}

func (s *SQLStore) PruneExpiredPeers(ctx context.Context, expiry time.Duration) (int, error) {
	cutoff := time.Now().Add(-expiry).Unix()
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return 0, fmt.Errorf("prune begin: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	if _, err := tx.ExecContext(ctx, s.q("DeleteSeedsExpired"), cutoff); err != nil {
		return 0, fmt.Errorf("prune seeds: %w", err)
	}
	if _, err := tx.ExecContext(ctx, s.q("DeleteChunksExpired"), cutoff); err != nil {
		return 0, fmt.Errorf("prune chunks: %w", err)
	}
	res, err := tx.ExecContext(ctx, s.q("DeletePeersExpired"), cutoff)
	if err != nil {
		return 0, fmt.Errorf("prune peers: %w", err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		return 0, err
	}
	if err := tx.Commit(); err != nil {
		return 0, fmt.Errorf("prune commit: %w", err)
	}
	return int(n), nil
}

func (s *SQLStore) PutArtifact(ctx context.Context, rec ArtifactRecord) error {
	_, err := s.db.ExecContext(ctx, s.q("PutArtifact"), rec.ArtifactID, rec.Name, rec.Version, rec.ManifestJSON)
	if err != nil {
		return fmt.Errorf("put artifact: %w", err)
	}
	return nil
}

func (s *SQLStore) GetArtifact(ctx context.Context, artifactID string) (*ArtifactRecord, error) {
	row := s.db.QueryRowContext(ctx, s.q("GetArtifact"), artifactID)
	var rec ArtifactRecord
	err := row.Scan(&rec.ArtifactID, &rec.Name, &rec.Version, &rec.ManifestJSON)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("get artifact: %w", err)
	}
	return &rec, nil
}

func (s *SQLStore) ReportSeed(ctx context.Context, artifactID, nodeID string) error {
	_, err := s.db.ExecContext(ctx, s.q("ReportSeed"), artifactID, nodeID, time.Now().Unix())
	if err != nil {
		return fmt.Errorf("report seed: %w", err)
	}
	return nil
}

func (s *SQLStore) ListSeeds(ctx context.Context, artifactID string) ([]string, error) {
	rows, err := s.db.QueryContext(ctx, s.q("ListSeeds"), artifactID)
	if err != nil {
		return nil, fmt.Errorf("list seeds: %w", err)
	}
	defer rows.Close()
	var ids []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		ids = append(ids, id)
	}
	return ids, rows.Err()
}

func (s *SQLStore) ReportChunks(ctx context.Context, nodeID string, hashes []string) (int64, error) {
	now := time.Now().Unix()
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return 0, fmt.Errorf("report chunks begin: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	if _, err := tx.ExecContext(ctx, s.q("TouchPeer"), now, nodeID); err != nil {
		return 0, fmt.Errorf("touch peer: %w", err)
	}
	stmt, err := tx.PrepareContext(ctx, s.q("UpsertChunk"))
	if err != nil {
		return 0, fmt.Errorf("prepare upsert chunk: %w", err)
	}
	defer stmt.Close()
	var n int64
	for _, h := range hashes {
		if h == "" {
			continue
		}
		if _, err := stmt.ExecContext(ctx, h, nodeID, now); err != nil {
			return 0, fmt.Errorf("upsert chunk: %w", err)
		}
		n++
	}
	if err := tx.Commit(); err != nil {
		return 0, fmt.Errorf("report chunks commit: %w", err)
	}
	return n, nil
}

func (s *SQLStore) LocateChunkNodes(ctx context.Context, hash string) ([]string, error) {
	rows, err := s.db.QueryContext(ctx, s.q("LocateChunkNodes"), hash)
	if err != nil {
		return nil, fmt.Errorf("locate chunk: %w", err)
	}
	defer rows.Close()
	var ids []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		ids = append(ids, id)
	}
	return ids, rows.Err()
}
