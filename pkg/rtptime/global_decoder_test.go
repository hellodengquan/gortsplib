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

func TestDecodeTimeline(t *testing.T) {
	g := &GlobalDecoder{}
	g.Initialize()

	video := &dummyTrack{clockRate: 90000, ptsEqualsDTS: true}
	audio := &dummyTrack{clockRate: 48000, ptsEqualsDTS: true}

	// not available before an anchor is set
	_, ok := g.DecodeTimeline(video, &rtp.Packet{Header: rtp.Header{Timestamp: 1000}})
	require.False(t, ok)

	// PLAY response: Range npt=10s- and RTP-Info rtptime=5000 (video),
	// rtptime=7000 (audio). Both tracks share the same program origin.
	g.SetTimelineAnchor(video, 5000, 10*90000)
	g.SetTimelineAnchor(audio, 7000, 10*48000)

	// the first packet of the new position starts exactly at the anchor
	pts, ok := g.DecodeTimeline(video, &rtp.Packet{Header: rtp.Header{Timestamp: 5000}})
	require.True(t, ok)
	require.Equal(t, int64(900000), pts)

	pts, ok = g.DecodeTimeline(audio, &rtp.Packet{Header: rtp.Header{Timestamp: 7000}})
	require.True(t, ok)
	require.Equal(t, int64(480000), pts)

	// subsequent packets are placed relative to the anchor
	pts, ok = g.DecodeTimeline(video, &rtp.Packet{Header: rtp.Header{Timestamp: 5000 + 90000}})
	require.True(t, ok)
	require.Equal(t, int64(990000), pts)

	pts, ok = g.DecodeTimeline(audio, &rtp.Packet{Header: rtp.Header{Timestamp: 7000 + 48000}})
	require.True(t, ok)
	require.Equal(t, int64(528000), pts)

	// audio and video values are comparable: one second of program time is
	// 90000 video units and 48000 audio units
	videoPTS, _ := g.DecodeTimeline(video, &rtp.Packet{Header: rtp.Header{Timestamp: 5000 + 2*90000}})
	audioPTS, _ := g.DecodeTimeline(audio, &rtp.Packet{Header: rtp.Header{Timestamp: 7000 + 2*48000}})
	require.Equal(t, int64(1080000), videoPTS)
	require.Equal(t, int64(576000), audioPTS)
	require.Equal(t, videoPTS/90000, audioPTS/48000)
}

func TestDecodeTimelineSeek(t *testing.T) {
	g := &GlobalDecoder{}
	g.Initialize()

	tr := &dummyTrack{clockRate: 90000, ptsEqualsDTS: true}

	// first playback starts at NPT 0
	g.SetTimelineAnchor(tr, 1000, 0)

	pts, ok := g.DecodeTimeline(tr, &rtp.Packet{Header: rtp.Header{Timestamp: 1000}})
	require.True(t, ok)
	require.Equal(t, int64(0), pts)

	pts, _ = g.DecodeTimeline(tr, &rtp.Packet{Header: rtp.Header{Timestamp: 1000 + 90000}})
	require.Equal(t, int64(90000), pts)

	// seek to NPT 20s with a discontinuous RTP timestamp (e.g. 600000)
	g.SetTimelineAnchor(tr, 600000, 20*90000)

	// the first packet at the new position starts exactly at the new anchor,
	// the timestamp discontinuity does not leak into the timeline PTS
	pts, ok = g.DecodeTimeline(tr, &rtp.Packet{Header: rtp.Header{Timestamp: 600000}})
	require.True(t, ok)
	require.Equal(t, int64(1800000), pts)

	// reception continues from the new anchor
	pts, _ = g.DecodeTimeline(tr, &rtp.Packet{Header: rtp.Header{Timestamp: 600000 + 45000}})
	require.Equal(t, int64(1845000), pts)
}

func TestDecodeTimelineAnchorBeforeFirstPacket(t *testing.T) {
	g := &GlobalDecoder{}
	g.Initialize()

	tr := &dummyTrack{clockRate: 90000, ptsEqualsDTS: true}

	// anchor set before any packet (and before any Decode call)
	g.SetTimelineAnchor(tr, 333, 5*90000)

	pts, ok := g.DecodeTimeline(tr, &rtp.Packet{Header: rtp.Header{Timestamp: 333}})
	require.True(t, ok)
	require.Equal(t, int64(450000), pts)

	// the ordinary Decode path still works independently
	decPTS, ok := g.Decode(tr, &rtp.Packet{Header: rtp.Header{Timestamp: 333}})
	require.True(t, ok)
	require.Equal(t, int64(0), decPTS)
}
