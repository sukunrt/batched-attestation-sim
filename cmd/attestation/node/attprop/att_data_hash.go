package attprop

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"slices"

	"github.com/libp2p/go-libp2p/core/peer"
)

func hashAttestationData(data []byte) []byte {
	sum := sha256.Sum256(data)
	return slices.Clone(sum[:])
}

func attestationHashKey(hash []byte) string {
	return string(hash)
}

func attDigestHexFromHash(hash []byte) string {
	if len(hash) < 8 {
		return ""
	}
	return hex.EncodeToString(hash[:8])
}

func attDigestHexFor(data, hash []byte) string {
	if len(data) > 0 {
		return attDigestHex(data)
	}
	return attDigestHexFromHash(hash)
}

type attestationIdentityCache struct {
	hashToData map[string][]byte
}

func newAttestationIdentityCache() attestationIdentityCache {
	return attestationIdentityCache{hashToData: make(map[string][]byte)}
}

func (c *attestationIdentityCache) remember(data []byte) []byte {
	if c.hashToData == nil {
		c.hashToData = make(map[string][]byte)
	}
	hash := hashAttestationData(data)
	key := attestationHashKey(hash)
	if _, ok := c.hashToData[key]; !ok {
		c.hashToData[key] = slices.Clone(data)
	}
	return hash
}

func (c *attestationIdentityCache) resolve(data, hash []byte, requireData bool) ([]byte, []byte, error) {
	if len(data) > 0 {
		computed := c.remember(data)
		if len(hash) > 0 && !bytes.Equal(hash, computed) {
			return nil, nil, fmt.Errorf("attestation_data_hash mismatch")
		}
		return c.hashToData[attestationHashKey(computed)], computed, nil
	}
	if len(hash) != sha256.Size {
		return nil, nil, fmt.Errorf("missing attestation_data identity")
	}
	data, ok := c.hashToData[attestationHashKey(hash)]
	if requireData && !ok {
		return nil, nil, fmt.Errorf("unknown attestation_data_hash")
	}
	return data, slices.Clone(hash), nil
}

func peerSentFull(sent map[peer.ID]map[string]struct{}, p peer.ID, hash []byte) bool {
	_, ok := sent[p][attestationHashKey(hash)]
	return ok
}

func markPeerSentFull(sent map[peer.ID]map[string]struct{}, p peer.ID, hash []byte) {
	perPeer := sent[p]
	if perPeer == nil {
		perPeer = make(map[string]struct{})
		sent[p] = perPeer
	}
	perPeer[attestationHashKey(hash)] = struct{}{}
}
