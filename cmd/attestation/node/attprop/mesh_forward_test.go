package attprop

import (
	"context"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/require"
)

// TestMeshForwardBidirectional is the isolating gate for symmetric mesh
// forwarding: two mesh peers each publish their own committee position and push
// it to the other. Crucially it asserts the synctest bubble QUIESCES (synctest.Wait
// returns) after the exchange — a real-stream bidirectional forward must let the
// QUIC connection go idle, or the fake clock can never advance and every timer
// hangs. Both ends must end up validating both positions.
func TestMeshForwardBidirectional(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		t.Cleanup(cancel)

		opener, other := orderedPair()
		h := newHarness(t, ctx, 2, noFanout, nil)
		h.connectUp(t, ctx, opener, other)

		// The opener creates one bidirectional stream set; the acceptor registers
		// writers after all three streams arrive. Wait before forcing roles because
		// onPeerUp resets a peer to roleConnected.
		time.Sleep(50 * time.Millisecond)
		synctest.Wait()

		// Symmetric push mesh: each forwards data to the other.
		h.manager(opener, 0).forceRole(testPeerID(other), rolePush)
		h.manager(other, 0).forceRole(testPeerID(opener), rolePush)

		data := makeData(1)
		h.manager(opener, 0).PublishLocal(1, 1, []byte{1}, data)
		h.manager(other, 0).PublishLocal(1, 2, []byte{2}, data)
		synctest.Wait()
		h.manager(opener, 0).post(tickEvent{})
		h.manager(other, 0).post(tickEvent{})
		synctest.Wait()

		// If the bubble fails to quiesce here, this sleep never returns and the
		// test times out — the exact failure mode of the production hang.
		time.Sleep(200 * time.Millisecond)
		synctest.Wait()

		require.Equal(t, 2, h.manager(opener, 0).ValidatedCount(1),
			"opener validated both positions")
		require.Equal(t, 2, h.manager(other, 0).ValidatedCount(1),
			"other validated both positions")
	})
}
