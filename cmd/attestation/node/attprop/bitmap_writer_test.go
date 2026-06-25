package attprop

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/ethp2p/simlab/cmd/attestation/pb"
)

func TestBitmapWriterReplacesQueuedBitmapByAttestationData(t *testing.T) {
	w := testBitmapWriter()
	first := testBitmapMetadata(1, "data", []byte{0x01})
	latest := testBitmapMetadata(1, "data", []byte{0x03})

	w.enqueueBitmaps([]*pb.CommitteeAttestationPartsMetadata{first})
	require.Len(t, w.work, 1)

	w.enqueueBitmaps([]*pb.CommitteeAttestationPartsMetadata{latest})
	require.Len(t, w.work, 1)

	<-w.work
	env := w.getNextBitmap()
	require.Len(t, env.Metadatas, 1)
	md := env.Metadatas[0]
	require.Equal(t, []byte{0x03}, md.Available)
	require.Empty(t, w.pending)
}

func TestBitmapWriterCoalescesPendingBitmaps(t *testing.T) {
	w := testBitmapWriter()
	first := testBitmapMetadata(1, "first", []byte{0x01})
	second := testBitmapMetadata(1, "second", []byte{0x02})

	w.enqueueBitmaps([]*pb.CommitteeAttestationPartsMetadata{first, second})
	require.Len(t, w.work, 1)

	<-w.work
	env := w.getNextBitmap()
	require.Len(t, env.Metadatas, 2)
	requireBitmapAvailable(t, env, "first", []byte{0x01})
	requireBitmapAvailable(t, env, "second", []byte{0x02})
	require.Empty(t, w.pending)
	require.Nil(t, w.getNextBitmap())
}

func TestBitmapWriterClonesQueuedMetadata(t *testing.T) {
	w := testBitmapWriter()
	md := testBitmapMetadata(1, "data", []byte{0x01})

	w.enqueueBitmaps([]*pb.CommitteeAttestationPartsMetadata{md})
	md.Available[0] = 0xff

	<-w.work
	env := w.getNextBitmap()
	require.Len(t, env.Metadatas, 1)
	require.Equal(t, []byte{0x01}, env.Metadatas[0].Available)
}

func TestBitmapWriterKeepsFirstFullDataThroughCoalescing(t *testing.T) {
	w := testBitmapWriter()
	full := testBitmapMetadata(1, "data", []byte{0x01})
	hashOnly := testBitmapMetadata(1, "data", []byte{0x03})
	hashOnly.AttestationData = nil
	hashOnly.AttestationDataHash = hashAttestationData([]byte("data"))

	w.enqueueBitmaps([]*pb.CommitteeAttestationPartsMetadata{full})
	w.enqueueBitmaps([]*pb.CommitteeAttestationPartsMetadata{hashOnly})

	<-w.work
	first := w.getNextBitmap()
	require.Len(t, first.Metadatas, 1)
	require.Equal(t, []byte("data"), first.Metadatas[0].AttestationData)
	require.Equal(t, []byte{0x03}, first.Metadatas[0].Available)

	w.enqueueBitmaps([]*pb.CommitteeAttestationPartsMetadata{full})
	<-w.work
	second := w.getNextBitmap()
	require.Len(t, second.Metadatas, 1)
	require.Empty(t, second.Metadatas[0].AttestationData)
	require.Equal(t, hashAttestationData([]byte("data")), second.Metadatas[0].AttestationDataHash)
}

func testBitmapWriter() *bitmapWriter {
	return &bitmapWriter{
		work:    make(chan struct{}, 1),
		pending: make(map[bitmapKey]*pb.CommitteeAttestationPartsMetadata),
	}
}

func requireBitmapAvailable(
	t *testing.T,
	env *pb.ControlEnvelope,
	data string,
	available []byte,
) {
	t.Helper()
	for _, md := range env.Metadatas {
		if string(md.AttestationData) == data {
			require.Equal(t, available, md.Available)
			return
		}
	}
	t.Fatalf("metadata for %q not found", data)
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
