package node

import (
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"log"
	"math/rand/v2"

	"github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/peer"
	pspb "github.com/libp2p/go-libp2p-pubsub/pb"
	ma "github.com/multiformats/go-multiaddr"
)

// Network resolves peer addresses by node number so the Node can run against
// either the production Shadow network or a simnet-backed test network.
type Network interface {
	PeerAddr(nodeNum int) ma.Multiaddr
}

// NodePrivateKey returns a deterministic Ed25519 key for the given node
// number. The same node number always produces the same key (and therefore
// the same peer ID), which lets nodes resolve each other by number without
// any discovery layer.
func NodePrivateKey(nodeNum int) crypto.PrivKey {
	var seed [32]byte
	binary.BigEndian.PutUint64(seed[:], uint64(nodeNum))
	r := rand.NewChaCha8(seed)
	key, _, err := crypto.GenerateEd25519Key(r)
	if err != nil {
		log.Fatalf("generate key: %v", err)
	}
	return key
}

// PeerIDFromNodeNum returns the libp2p peer ID corresponding to a node
// number, derived from NodePrivateKey.
func PeerIDFromNodeNum(nodeNum int) (peer.ID, error) {
	return peer.IDFromPrivateKey(NodePrivateKey(nodeNum))
}

// MessageIDFunc is the gossipsub message-id function used across the
// simulation. Hash of (topic, topic_len, data) truncated to 20 hex chars.
func MessageIDFunc(msg *pspb.Message) string {
	h := sha256.Sum256(fmt.Appendf(nil, "%s%d%s", *msg.Topic, len(*msg.Topic), string(msg.Data)))
	return hex.EncodeToString(h[:])[:20]
}
