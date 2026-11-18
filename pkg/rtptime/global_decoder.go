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

	// program timeline anchor (all values are in track clock units)
	anchorSet      bool
	anchorRTPTime  uint32 // RTP timestamp corresponding to anchorTimeline
	anchorTimeline int64  // PTS on the program timeline
}

func (d *globalDecoderTrackData) decode(ts uint32) int64 {
	d.overall += int64(int32(ts - d.prev))
	d.prev = ts
	return d.overall
}

// decodeTimeline decodes a timestamp into a PTS on the program timeline.
// All values are in the track clock units.
func (d *globalDecoderTrackData) decodeTimeline(ts uint32) int64 {
	return d.anchorTimeline + int64(int32(ts-d.anchorRTPTime))
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

// SetTimelineAnchor maps an RTP timestamp to a position on the program timeline.
// The anchor is expressed in the clock units of the given track: rtpTime is the
// RTP timestamp (for instance from the "rtptime" parameter of a RTP-Info header)
// corresponding to timelinePTS, the position on the program timeline (for
// instance the start of a Range header converted into track clock units).
//
// Anchors set on different tracks all refer to the same program origin, so the
// PTS values returned by DecodeTimeline can be compared directly between
// tracks (e.g. audio and video of the same playback).
func (d *GlobalDecoder) SetTimelineAnchor(
	track GlobalDecoderTrack,
	rtpTime uint32,
	timelinePTS int64,
) {
	d.mutex.Lock()
	defer d.mutex.Unlock()

	df, ok := d.tracks[track]
	if !ok {
		df = &globalDecoderTrackData{
			prev: rtpTime,
		}
		d.tracks[track] = df
	}

	df.anchorSet = true
	df.anchorRTPTime = rtpTime
	df.anchorTimeline = timelinePTS
}

// DecodeTimeline decodes a timestamp into a PTS on the program timeline.
// The returned value is in the clock units of the track. It is available only
// if a timeline anchor has been set for the track with SetTimelineAnchor.
func (d *GlobalDecoder) DecodeTimeline(
	track GlobalDecoderTrack,
	pkt *rtp.Packet,
) (int64, bool) {
	if track.ClockRate() == 0 {
		return 0, false
	}

	d.mutex.Lock()
	defer d.mutex.Unlock()

	df, ok := d.tracks[track]
	if !ok || !df.anchorSet {
		return 0, false
	}

	return df.decodeTimeline(pkt.Timestamp), true
}
