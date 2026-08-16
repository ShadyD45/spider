package cache

import (
	v1 "spider/api/v1"
)

// ManifestCachedHashes returns manifest chunk hashes that are valid committed cache entries.
func ManifestCachedHashes(store *ChunkStore, manifest *v1.ArtifactManifest) []string {
	if store == nil || manifest == nil {
		return nil
	}
	seen := make(map[string]struct{})
	var out []string
	for _, fileEntry := range manifest.Files {
		for _, chunkRef := range fileEntry.Chunks {
			if _, ok := seen[chunkRef.Hash]; ok {
				continue
			}
			if !store.HasValidCommittedChunk(chunkRef.Hash, chunkRef.Size) {
				continue
			}
			seen[chunkRef.Hash] = struct{}{}
			out = append(out, chunkRef.Hash)
		}
	}
	return out
}

// StartupReconcileHashes returns chunk hashes to advertise at daemon startup.
// When pinned artifact IDs are set, only manifest-owned chunks are included.
func StartupReconcileHashes(store *ChunkStore, pinnedArtifactIDs []string) ([]string, error) {
	if store == nil {
		return nil, nil
	}
	if len(pinnedArtifactIDs) == 0 {
		return store.ListChunks()
	}
	seen := make(map[string]struct{})
	var out []string
	for _, id := range pinnedArtifactIDs {
		manifest, err := store.GetManifest(id)
		if err != nil {
			continue
		}
		for _, h := range ManifestCachedHashes(store, manifest) {
			if _, ok := seen[h]; ok {
				continue
			}
			seen[h] = struct{}{}
			out = append(out, h)
		}
	}
	return out, nil
}
