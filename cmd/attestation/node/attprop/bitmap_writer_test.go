package attprop

import (
	"testing"

	"github.com/libp2p/go-libp2p-pubsub/partialmessages/bitmap"
	"github.com/stretchr/testify/require"

	"github.com/ethp2p/simlab/cmd/attestation/pb"
)

func TestBitmapWriterCoalescesQueuedAvailabilityByAttestationData(t *testing.T) {
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
	require.Equal(t, []uint32{0, 1}, md.AvailableIds)
	require.Empty(t, md.Available)
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
	requireBitmapAvailableIDs(t, env, "first", []uint32{0})
	requireBitmapAvailableIDs(t, env, "second", []uint32{1})
	require.Empty(t, w.pending)
	require.Nil(t, w.getNextBitmap())
}

func TestBitmapWriterUsesLatestIdentityThroughCoalescing(t *testing.T) {
	w := testBitmapWriter()
	fullDataMetadata := testBitmapMetadata(1, "data", []byte{0x01})
	hashOnly := testBitmapMetadata(1, "data", []byte{0x03})
	hashOnly.AttestationData = nil
	hashOnly.AttestationDataHash = hash([]byte("data"))

	w.enqueueBitmaps([]*pb.CommitteeAttestationPartsMetadata{fullDataMetadata})
	w.enqueueBitmaps([]*pb.CommitteeAttestationPartsMetadata{hashOnly})

	<-w.work
	first := w.getNextBitmap()
	require.Len(t, first.Metadatas, 1)
	require.Empty(t, first.Metadatas[0].AttestationData)
	require.Equal(t, hash([]byte("data")), first.Metadatas[0].AttestationDataHash)
	require.Equal(t, []uint32{0, 1}, first.Metadatas[0].AvailableIds)
	require.Empty(t, first.Metadatas[0].Available)

	w.enqueueBitmaps([]*pb.CommitteeAttestationPartsMetadata{fullDataMetadata})
	<-w.work
	second := w.getNextBitmap()
	require.Nil(t, second, "same availability should not be re-advertised")
}

func TestBitmapWriterEmitsOnlyNewAvailableIDs(t *testing.T) {
	w := testBitmapWriter()

	w.enqueueBitmaps([]*pb.CommitteeAttestationPartsMetadata{testBitmapMetadata(1, "data", []byte{0x01})})
	<-w.work
	first := w.getNextBitmap()
	require.Len(t, first.Metadatas, 1)
	require.Equal(t, []uint32{0}, first.Metadatas[0].AvailableIds)
	require.Equal(t, []byte("data"), first.Metadatas[0].AttestationData)

	w.enqueueBitmaps([]*pb.CommitteeAttestationPartsMetadata{testBitmapMetadata(1, "data", []byte{0x05})})
	<-w.work
	second := w.getNextBitmap()
	require.Len(t, second.Metadatas, 1)
	require.Equal(t, []uint32{2}, second.Metadatas[0].AvailableIds)
	require.Empty(t, second.Metadatas[0].AttestationData)
	require.Equal(t, hash([]byte("data")), second.Metadatas[0].AttestationDataHash)
}

func TestBitmapWriterCoalescesQueuedAvailableIDsAdditively(t *testing.T) {
	w := testBitmapWriter()
	first := testBitmapMetadata(1, "data", nil)
	first.AvailableIds = []uint32{0}
	latest := testBitmapMetadata(1, "data", nil)
	latest.AvailableIds = []uint32{2}

	w.enqueueBitmaps([]*pb.CommitteeAttestationPartsMetadata{first})
	require.Len(t, w.work, 1)
	w.enqueueBitmaps([]*pb.CommitteeAttestationPartsMetadata{latest})
	require.Len(t, w.work, 1)

	<-w.work
	env := w.getNextBitmap()
	require.Len(t, env.Metadatas, 1)
	require.Equal(t, []uint32{0, 2}, env.Metadatas[0].AvailableIds)
	require.Empty(t, env.Metadatas[0].Available)
}

func TestBitmapWriterUsesLastBitmapAndUniqueQueuedIDs(t *testing.T) {
	w := testBitmapWriter()
	first := testBitmapMetadata(1, "data", []byte{0x01})
	first.AvailableIds = []uint32{2, 2}
	latest := testBitmapMetadata(1, "data", []byte{0x02})
	latest.AvailableIds = []uint32{2, 3}

	w.enqueueBitmaps([]*pb.CommitteeAttestationPartsMetadata{first})
	w.enqueueBitmaps([]*pb.CommitteeAttestationPartsMetadata{latest})

	<-w.work
	env := w.getNextBitmap()
	require.Len(t, env.Metadatas, 1)
	require.Equal(t, []uint32{1, 2, 3}, env.Metadatas[0].AvailableIds)
	require.Empty(t, env.Metadatas[0].Available)
}

func TestBitmapWriterHashOnlyFirstSendKeepsHash(t *testing.T) {
	w := testBitmapWriter()
	md := testBitmapMetadata(1, "data", []byte{0x01})
	md.AttestationData = nil
	md.AttestationDataHash = hash([]byte("data"))

	w.enqueueBitmaps([]*pb.CommitteeAttestationPartsMetadata{md})
	<-w.work
	env := w.getNextBitmap()
	require.Len(t, env.Metadatas, 1)
	require.Empty(t, env.Metadatas[0].AttestationData)
	require.Equal(t, hash([]byte("data")), env.Metadatas[0].AttestationDataHash)
	require.Equal(t, []uint32{0}, env.Metadatas[0].AvailableIds)
}

func TestBitmapWriterUsesBitmapWhenCheaperThanAvailableIDs(t *testing.T) {
	w := testBitmapWriter()
	full := newCommitteeBitmap(w.committeeSize)
	for pos := range 40 {
		full.Set(pos)
	}

	w.enqueueBitmaps([]*pb.CommitteeAttestationPartsMetadata{testBitmapMetadata(1, "data", []byte(full))})
	<-w.work
	env := w.getNextBitmap()
	require.Len(t, env.Metadatas, 1)
	require.Empty(t, env.Metadatas[0].AvailableIds)
	require.Equal(t, []byte(full), env.Metadatas[0].Available)
}

func testBitmapWriter() *bitmapWriter {
	return &bitmapWriter{
		work:          make(chan struct{}, 1),
		committeeSize: 512,
		pending:       make(map[bitmapKey][]*pb.CommitteeAttestationPartsMetadata),
		sentFull:      make(map[string]struct{}),
		sentAvailable: make(map[bitmapKey]bitmap.Bitmap),
	}
}

func requireBitmapAvailableIDs(
	t *testing.T,
	env *pb.ControlEnvelope,
	data string,
	available []uint32,
) {
	t.Helper()
	for _, md := range env.Metadatas {
		if string(md.AttestationData) == data {
			require.Equal(t, available, md.AvailableIds)
			require.Empty(t, md.Available)
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
		Slot:                slot,
		AttestationData:     []byte(data),
		AttestationDataHash: hash([]byte(data)),
		Available:           available,
	}
}
