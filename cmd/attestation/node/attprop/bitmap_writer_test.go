package attprop

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/ethp2p/simlab/cmd/attestation/pb"
)

func TestBitmapWriterReplacesQueuedBitmapByAttestationData(t *testing.T) {
	w := testBitmapWriter(1)
	first := testBitmapMetadata(1, "data", []byte{0x01})
	latest := testBitmapMetadata(1, "data", []byte{0x03})

	replaced, dropped, ok := w.enqueueBitmap(first)
	require.True(t, ok)
	require.False(t, replaced)
	require.False(t, dropped)
	require.Len(t, w.work, 1)

	replaced, dropped, ok = w.enqueueBitmap(latest)
	require.True(t, ok)
	require.True(t, replaced)
	require.False(t, dropped)
	require.Len(t, w.work, 1)

	md := w.pop(<-w.work)
	require.Equal(t, []byte{0x03}, md.Available)
}

func TestBitmapWriterEnqueuesNewestBitmapWhenFull(t *testing.T) {
	w := testBitmapWriter(1)
	old := testBitmapMetadata(1, "old", []byte{0x01})
	latest := testBitmapMetadata(1, "latest", []byte{0x02})

	_, _, ok := w.enqueueBitmap(old)
	require.True(t, ok)
	replaced, dropped, ok := w.enqueueBitmap(latest)
	require.True(t, ok)
	require.False(t, replaced)
	require.True(t, dropped)
	require.Len(t, w.work, 1)

	md := w.pop(<-w.work)
	require.Equal(t, []byte("latest"), md.AttestationData)
	require.Equal(t, []byte{0x02}, md.Available)
	_, ok = w.pending[bitmapKey{slot: old.Slot, data: string(old.AttestationData)}]
	require.False(t, ok)
}

func testBitmapWriter(buf int) *bitmapWriter {
	return &bitmapWriter{
		work:    make(chan bitmapKey, buf),
		pending: make(map[bitmapKey]*pb.CommitteeAttestationPartsMetadata),
	}
}

func testBitmapMetadata(
	slot int32,
	data string,
	available []byte,
) *pb.CommitteeAttestationPartsMetadata {
	return &pb.CommitteeAttestationPartsMetadata{
		Slot:            slot,
		AttestationData: []byte(data),
		Available:       available,
	}
}
