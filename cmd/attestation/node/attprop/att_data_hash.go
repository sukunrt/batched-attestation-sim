package attprop

import (
	"crypto/sha256"
	"encoding/hex"
)

func hash(data []byte) []byte {
	sum := sha256.Sum256(data)
	return sum[:]
}

func hexPrefix(hash []byte) string {
	if len(hash) < 8 {
		return ""
	}
	return hex.EncodeToString(hash[:8])
}

func digestHex(data, hash []byte) string {
	if len(hash) > 0 {
		return hexPrefix(hash)
	}
	return attDigestHex(data)
}

type attestationDataCache struct {
	hashToData map[string][]byte
}

func newAttestationIdentityCache() attestationDataCache {
	return attestationDataCache{hashToData: make(map[string][]byte)}
}

func (c *attestationDataCache) Put(data []byte) []byte {
	if c.hashToData == nil {
		c.hashToData = make(map[string][]byte)
	}
	hash := (hash(data))
	if _, ok := c.hashToData[string(hash)]; ok {
		return hash
	}
	c.hashToData[string(hash)] = data
	return hash
}

func (c *attestationDataCache) Get(hash []byte) ([]byte, bool) {
	d, ok := c.hashToData[string(hash)]
	return d, ok
}
