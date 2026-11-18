// Package rtptime contains a time decoder.
package rtptime

import (
	"sync"
	"time"

	"github.com/pion/rtp"
)

var timeNow = time.Now

// avoid an int64 overflow and preserve resolution by splitting division into two parts:
// first add the integer part, then the decimal part.
func multiplyAndDivide(v, m, d int64) int64 {
	secs := v / d
	dec := v % d
	return (secs*m + dec*m/d)
}

type globalDecoderTrackData struct {
	overall int64
	prev    uint32
}

func (d *globalDecoderTrackData) decode(ts uint32) int64 {
	d.overall += int64(int32(ts - d.prev))
	d.prev = ts
	return d.overall
}

// GlobalDecoderTrack is a track (RTSP format or WebRTC track) of GlobalDecoder.
type GlobalDecoderTrack interface {
	ClockRate() int
	PTSEqualsDTS(*rtp.Packet) bool
}

// GlobalDecoder is a RTP timestamp decoder.
type GlobalDecoder struct {
	mutex             sync.Mutex
	leadingTrack      GlobalDecoderTrack
	startSystem       time.Time
	startPTS          int64
	startPTSClockRate int64
	tracks            map[GlobalDecoderTrack]*globalDecoderTrackData
}

// Initialize initializes a GlobalDecoder.
func (d *GlobalDecoder) Initialize() {
	d.tracks = make(map[GlobalDecoderTrack]*globalDecoderTrackData)
}

// Decode decodes a timestamp.
func (d *GlobalDecoder) Decode(
	track GlobalDecoderTrack,
	pkt *rtp.Packet,
) (int64, bool) {
	if track.ClockRate() == 0 {
		return 0, false
	}

	d.mutex.Lock()
	defer d.mutex.Unlock()

	df, ok := d.tracks[track]

	// never seen before track
	if !ok {
		if !track.PTSEqualsDTS(pkt) {
			return 0, false
		}

		now := timeNow()

		if d.leadingTrack == nil {
			d.leadingTrack = track
			d.startSystem = now
			d.startPTS = 0
			d.startPTSClockRate = int64(track.ClockRate())
		}

		// start from the PTS of the leading track
		startPTS := multiplyAndDivide(d.startPTS, int64(track.ClockRate()), d.startPTSClockRate)
		startPTS += multiplyAndDivide(int64(now.Sub(d.startSystem)), int64(track.ClockRate()), int64(time.Second))

		d.tracks[track] = &globalDecoderTrackData{
			overall: startPTS,
			prev:    pkt.Timestamp,
		}

		return startPTS, true
	}

	pts := df.decode(pkt.Timestamp)

	// update startSystem / startPTS
	if d.leadingTrack == track && track.PTSEqualsDTS(pkt) {
		now := timeNow()
		d.startSystem = now
		d.startPTS = pts
	}

	return pts, true
}

// SwitchSource notifies the decoder that the RTP source of a track changed,
// for instance because the camera has been rebooted and it restarted sending
// packets with a new SSRC and a new timestamp base.
//
// The timeline is kept continuous: packets of the new source are decoded
// starting from a PTS that follows the timeline of the leading track at the
// time of the switch, instead of producing a jump caused by the new timestamp
// base. Other tracks are not affected.
//
// pkt is the first packet of the new source.
func (d *GlobalDecoder) SwitchSource(
	track GlobalDecoderTrack,
	pkt *rtp.Packet,
) {
	if track.ClockRate() == 0 {
		return
	}

	d.mutex.Lock()
	defer d.mutex.Unlock()

	now := timeNow()

	if d.leadingTrack == nil {
		d.leadingTrack = track
		d.startSystem = now
		d.startPTS = 0
		d.startPTSClockRate = int64(track.ClockRate())
	}

	// continue the timeline from the PTS the leading track has at this
	// moment, as if the new source started at the switch time.
	startPTS := multiplyAndDivide(d.startPTS, int64(track.ClockRate()), d.startPTSClockRate)
	startPTS += multiplyAndDivide(int64(now.Sub(d.startSystem)), int64(track.ClockRate()), int64(time.Second))

	d.tracks[track] = &globalDecoderTrackData{
		overall: startPTS,
		prev:    pkt.Timestamp,
	}

	if d.leadingTrack == track {
		d.startSystem = now
		d.startPTS = startPTS
	}
}
