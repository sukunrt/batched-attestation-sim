package main

import (
	"encoding/binary"
	"fmt"
	"runtime"
	"testing"

	blst "github.com/supranational/blst/bindings/go"
)

var blsDST = []byte("BLS_SIG_BLS12381G2_XMD:SHA-256_SSWU_RO_POP_")
var blsBenchmarkVerified bool
var blsBenchmarkAggregateSignature *blst.P2Affine
var blsBenchmarkAggregatePublicKey *blst.P1Affine

func BenchmarkBLSSameDataFastAggregateVerify(b *testing.B) {
	blst.SetMaxProcs(max(runtime.GOMAXPROCS(0)-1, 1))

	for _, batchSize := range []int{1, 2, 5, 10, 50, 100, 300, 500, 1000} {
		b.Run(fmt.Sprintf("batch_%d", batchSize), func(b *testing.B) {
			msg, pubKeys, signature := makeSameDataSignatureBatch(b, batchSize)

			if !signature.FastAggregateVerify(true, pubKeys, msg[:], blsDST) {
				b.Fatal("could not verify aggregate signature")
			}

			b.ReportAllocs()
			b.ResetTimer()

			var verified bool
			for b.Loop() {
				verified = signature.FastAggregateVerify(true, pubKeys, msg[:], blsDST)
				if !verified {
					b.Fatal("could not verify aggregate signature")
				}
			}
			blsBenchmarkVerified = verified

			b.ReportMetric(float64(batchSize), "signatures/op")
			b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N*batchSize), "ns/signature")
		})
	}
}

func BenchmarkBLSSameDataAggregateSignatures(b *testing.B) {
	for _, batchSize := range []int{1, 2, 5, 10, 50, 100, 300, 500, 1000} {
		b.Run(fmt.Sprintf("batch_%d", batchSize), func(b *testing.B) {
			_, _, signatures := makeSameDataSignatureInputs(b, batchSize)

			b.ReportAllocs()
			b.ResetTimer()

			var aggregateSignature *blst.P2Affine
			for b.Loop() {
				aggregate := new(blst.P2Aggregate)
				if !aggregate.Aggregate(signatures, false) {
					b.Fatal("could not aggregate signatures")
				}
				aggregateSignature = aggregate.ToAffine()
			}
			blsBenchmarkAggregateSignature = aggregateSignature

			b.ReportMetric(float64(batchSize), "signatures/op")
			b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N*batchSize), "ns/signature")
		})
	}
}

func BenchmarkBLSSameDataAggregatePublicKeys(b *testing.B) {
	for _, batchSize := range []int{1, 2, 5, 10, 50, 100, 300, 500, 1000} {
		b.Run(fmt.Sprintf("batch_%d", batchSize), func(b *testing.B) {
			_, pubKeys, _ := makeSameDataSignatureInputs(b, batchSize)

			b.ReportAllocs()
			b.ResetTimer()

			var aggregatePublicKey *blst.P1Affine
			for b.Loop() {
				aggregate := new(blst.P1Aggregate)
				if !aggregate.Aggregate(pubKeys, false) {
					b.Fatal("could not aggregate public keys")
				}
				aggregatePublicKey = aggregate.ToAffine()
			}
			blsBenchmarkAggregatePublicKey = aggregatePublicKey

			b.ReportMetric(float64(batchSize), "public_keys/op")
			b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N*batchSize), "ns/public_key")
		})
	}
}

func makeSameDataSignatureBatch(b testing.TB, batchSize int) ([32]byte, []*blst.P1Affine, *blst.P2Affine) {
	b.Helper()

	msg, pubKeys, signatures := makeSameDataSignatureInputs(b, batchSize)

	aggregate := new(blst.P2Aggregate)
	if !aggregate.Aggregate(signatures, false) {
		b.Fatal("could not aggregate signatures")
	}

	return msg, pubKeys, aggregate.ToAffine()
}

func makeSameDataSignatureInputs(b testing.TB, batchSize int) ([32]byte, []*blst.P1Affine, []*blst.P2Affine) {
	b.Helper()

	var msg [32]byte
	copy(msg[:], "batched-attestation-sim bls bench")

	pubKeys := make([]*blst.P1Affine, batchSize)
	signatures := make([]*blst.P2Affine, batchSize)

	for i := range batchSize {
		sk := deterministicBLSSecretKey(uint64(i + 1))
		if sk == nil {
			b.Fatalf("secret key %d is nil", i)
		}
		pubKeys[i] = new(blst.P1Affine).From(sk)
		signatures[i] = new(blst.P2Affine).Sign(sk, msg[:], blsDST)
	}

	return msg, pubKeys, signatures
}

func deterministicBLSSecretKey(n uint64) *blst.SecretKey {
	var ikm [32]byte
	binary.BigEndian.PutUint64(ikm[len(ikm)-8:], n)
	return blst.KeyGen(ikm[:])
}
