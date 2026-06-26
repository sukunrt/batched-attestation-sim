package attprop

import (
	"context"
	"slices"
	"time"

	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/quic-go/quic-go"
)

type bandwidthTotals struct {
	sent, recv int
}

// bandwidthEvent asks the manager eventloop to sample and log host-wide QUIC
// bandwidth for one interval.
type bandwidthEvent struct {
	freq time.Duration
}

func (bandwidthEvent) isEvent() {}

// ReportBandwidth posts periodic bandwidth sample requests to the manager
// eventloop. The eventloop owns the QUIC stats read, delta state, and log call.
func (m *Manager) ReportBandwidth(ctx context.Context, freq time.Duration) {
	if freq <= 0 {
		return
	}
	ticker := time.NewTicker(freq)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			if !m.post(bandwidthEvent{freq: freq}) {
				return
			}
		case <-ctx.Done():
			return
		}
	}
}

func (m *Manager) onBandwidth(e bandwidthEvent) {
	if m.bandwidthPrevByPeer == nil {
		m.bandwidthPrevByPeer = make(map[peer.ID]bandwidthTotals)
	}
	byPeer := make(map[peer.ID]bandwidthTotals)
	pushPending := make(map[peer.ID]int)
	var qc *quic.Conn
	for _, c := range m.host.Network().Conns() {
		if ok := c.As(&qc); ok {
			s := qc.ConnectionStats()
			p := c.RemotePeer()
			t := byPeer[p]
			t.sent += int(s.BytesSent)
			t.recv += int(s.BytesReceived)
			byPeer[p] = t
		}
	}
	for p := range m.senders {
		if m.mesh.role(p) == rolePush {
			pushPending[p] = m.pendingSendCountForPeer(p)
		}
	}
	avgPushPending := averagePending(pushPending)

	var sent, recv int
	peers := make([]peer.ID, 0, len(byPeer))
	for p, t := range byPeer {
		sent += t.sent
		recv += t.recv
		peers = append(peers, p)
	}
	for p := range pushPending {
		if _, ok := byPeer[p]; !ok {
			peers = append(peers, p)
		}
	}
	ds, dr := sent-m.bandwidthPrevSent, recv-m.bandwidthPrevRecv
	m.bandwidthPrevSent, m.bandwidthPrevRecv = sent, recv
	sbps := int(float64(ds*8) / e.freq.Seconds())
	rbps := int(float64(dr*8) / e.freq.Seconds())
	if sbps >= 10 || rbps >= 10 {
		m.logger.Info("bandwidth",
			"sentbps", sbps, "receivedbps", rbps,
			"sentBytesTotal", sent, "receivedBytesTotal", recv,
			"push_pending_send_avg", avgPushPending)
	}

	slices.Sort(peers)
	for _, p := range peers {
		t := byPeer[p]
		prev := m.bandwidthPrevByPeer[p]
		pds, pdr := t.sent-prev.sent, t.recv-prev.recv
		psbps := int(float64(pds*8) / e.freq.Seconds())
		prbps := int(float64(pdr*8) / e.freq.Seconds())
		pending := pushPending[p]
		if psbps < 10 && prbps < 10 && pending == 0 {
			continue
		}
		role := attpropBandwidthRole(m.mesh.role(p))
		m.logger.Info("attprop_peer_bandwidth",
			"peer", shortPeer(p),
			"peer_id", p.String(),
			"role", role,
			"push_pending_send_size", pending,
			"sentbps", psbps, "receivedbps", prbps,
			"sentBytesTotal", t.sent, "receivedBytesTotal", t.recv)
	}
	m.bandwidthPrevByPeer = byPeer
}

func (m *Manager) pendingSendCountForPeer(p peer.ID) int {
	var total int
	for _, ss := range m.slots {
		for _, b := range ss.buckets {
			peerAvail := b.peerAvail[p]
			for pos := range b.validated {
				if peerAvail == nil || !peerAvail.Get(pos) {
					total++
				}
			}
		}
	}
	return total
}

func averagePending(counts map[peer.ID]int) float64 {
	if len(counts) == 0 {
		return 0
	}
	var total int
	for _, count := range counts {
		total += count
	}
	return float64(total) / float64(len(counts))
}

func attpropBandwidthRole(r meshRole) string {
	switch r {
	case rolePush:
		return "push"
	case roleBitmap:
		return "bitmap"
	case roleConnected:
		return "conn"
	default:
		return "unknown"
	}
}
