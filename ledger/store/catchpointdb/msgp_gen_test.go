//go:build !skip_msgp_testing
// +build !skip_msgp_testing

package catchpointdb

// Code generated by github.com/algorand/msgp DO NOT EDIT.

import (
	"testing"

	"github.com/algorand/msgp/msgp"

	"github.com/algorand/go-algorand/protocol"
	"github.com/algorand/go-algorand/test/partitiontest"
)

func TestMarshalUnmarshalCatchpointFirstStageInfo(t *testing.T) {
	partitiontest.PartitionTest(t)
	v := CatchpointFirstStageInfo{}
	bts := v.MarshalMsg(nil)
	left, err := v.UnmarshalMsg(bts)
	if err != nil {
		t.Fatal(err)
	}
	if len(left) > 0 {
		t.Errorf("%d bytes left over after UnmarshalMsg(): %q", len(left), left)
	}

	left, err = msgp.Skip(bts)
	if err != nil {
		t.Fatal(err)
	}
	if len(left) > 0 {
		t.Errorf("%d bytes left over after Skip(): %q", len(left), left)
	}
}

func TestRandomizedEncodingCatchpointFirstStageInfo(t *testing.T) {
	protocol.RunEncodingTest(t, &CatchpointFirstStageInfo{})
}

func BenchmarkMarshalMsgCatchpointFirstStageInfo(b *testing.B) {
	v := CatchpointFirstStageInfo{}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		v.MarshalMsg(nil)
	}
}

func BenchmarkAppendMsgCatchpointFirstStageInfo(b *testing.B) {
	v := CatchpointFirstStageInfo{}
	bts := make([]byte, 0, v.Msgsize())
	bts = v.MarshalMsg(bts[0:0])
	b.SetBytes(int64(len(bts)))
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		bts = v.MarshalMsg(bts[0:0])
	}
}

func BenchmarkUnmarshalCatchpointFirstStageInfo(b *testing.B) {
	v := CatchpointFirstStageInfo{}
	bts := v.MarshalMsg(nil)
	b.ReportAllocs()
	b.SetBytes(int64(len(bts)))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := v.UnmarshalMsg(bts)
		if err != nil {
			b.Fatal(err)
		}
	}
}
