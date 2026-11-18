package rtptime

import (
	"testing"
	"time"

	"github.com/pion/rtp"
	"github.com/stretchr/testify/require"
)

type dummyTrack struct {
	clockRate    int
	ptsEqualsDTS bool
}

func (t *dummyTrack) ClockRate() int {
	return t.clockRate
}

func (t *dummyTrack) PTSEqualsDTS(*rtp.Packet) bool {
	return t.ptsEqualsDTS
}

func TestGlobalDecoder(t *testing.T) {
	g := &GlobalDecoder{}
	g.Initialize()

	t1 := &dummyTrack{clockRate: 90000}
	t2 := &dummyTrack{clockRate: 48000, ptsEqualsDTS: true}

	timeNow = func() time.Time {
		return time.Date(2008, 0o5, 20, 22, 15, 20, 0, time.UTC)
	}

	_, ok := g.Decode(t1, &rtp.Packet{})
	require.Equal(t, false, ok)

	t1.ptsEqualsDTS = true
	pts, ok := g.Decode(t1, &rtp.Packet{Header: rtp.Header{Timestamp: 22500}})
	require.Equal(t, true, ok)
	require.Equal(t, int64(0), pts)

	timeNow = func() time.Time {
		return time.Date(2008, 0o5, 20, 22, 15, 21, 0, time.UTC)
	}

	pts, ok = g.Decode(t1, &rtp.Packet{Header: rtp.Header{Timestamp: 22500 + 90000}})
	require.Equal(t, true, ok)
	require.Equal(t, int64(90000), pts)

	timeNow = func() time.Time {
		return time.Date(2008, 0o5, 20, 22, 15, 25, 0, time.UTC)
	}

	pts, ok = g.Decode(t2, &rtp.Packet{Header: rtp.Header{Timestamp: 33100}})
	require.Equal(t, true, ok)
	require.Equal(t, int64(240000), pts)

	pts, ok = g.Decode(t2, &rtp.Packet{Header: rtp.Header{Timestamp: 33100 + 48000}})
	require.Equal(t, true, ok)
	require.Equal(t, int64(288000), pts)
}

func TestGlobalDecoderInvalidClockRate(t *testing.T) {
	g := &GlobalDecoder{}
	g.Initialize()

	tr := &dummyTrack{clockRate: 0, ptsEqualsDTS: true}

	_, ok := g.Decode(tr, &rtp.Packet{Header: rtp.Header{Timestamp: 90000}})
	require.Equal(t, false, ok)
}

func TestGlobalDecoderSwitchSource(t *testing.T) {
	g := &GlobalDecoder{}
	g.Initialize()

	t1 := &dummyTrack{clockRate: 90000, ptsEqualsDTS: true}
	t2 := &dummyTrack{clockRate: 48000, ptsEqualsDTS: true}

	timeNow = func() time.Time {
		return time.Date(2008, 0o5, 20, 22, 15, 20, 0, time.UTC)
	}

	pts, ok := g.Decode(t1, &rtp.Packet{Header: rtp.Header{Timestamp: 1000}})
	require.Equal(t, true, ok)
	require.Equal(t, int64(0), pts)

	pts, ok = g.Decode(t2, &rtp.Packet{Header: rtp.Header{Timestamp: 2000}})
	require.Equal(t, true, ok)
	require.Equal(t, int64(0), pts)

	timeNow = func() time.Time {
		return time.Date(2008, 0o5, 20, 22, 15, 22, 0, time.UTC)
	}

	pts, ok = g.Decode(t1, &rtp.Packet{Header: rtp.Header{Timestamp: 1000 + 180000}})
	require.Equal(t, true, ok)
	require.Equal(t, int64(180000), pts)

	// the source of t1 changes, and restarts with a new timestamp base.
	// its timeline must continue from the timeline of the leading track
	// instead of jumping back.
	g.SwitchSource(t1, &rtp.Packet{Header: rtp.Header{Timestamp: 50}})

	pts, ok = g.Decode(t1, &rtp.Packet{Header: rtp.Header{Timestamp: 50}})
	require.Equal(t, true, ok)
	require.Equal(t, int64(180000), pts)

	pts, ok = g.Decode(t1, &rtp.Packet{Header: rtp.Header{Timestamp: 50 + 90000}})
	require.Equal(t, true, ok)
	require.Equal(t, int64(270000), pts)

	// other tracks are not affected
	pts, ok = g.Decode(t2, &rtp.Packet{Header: rtp.Header{Timestamp: 2000 + 48000}})
	require.Equal(t, true, ok)
	require.Equal(t, int64(48000), pts)
}

func TestGlobalDecoderSwitchSourceInvalidClockRate(t *testing.T) {
	g := &GlobalDecoder{}
	g.Initialize()

	tr := &dummyTrack{clockRate: 0, ptsEqualsDTS: true}
	g.SwitchSource(tr, &rtp.Packet{Header: rtp.Header{Timestamp: 90000}})

	_, ok := g.Decode(tr, &rtp.Packet{Header: rtp.Header{Timestamp: 90000}})
	require.Equal(t, false, ok)
}
