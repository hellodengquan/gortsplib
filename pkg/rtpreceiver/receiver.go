// Package rtpreceiver contains a utility to receive RTP packets.
package rtpreceiver

import (
	"fmt"
	"sync"
	"time"

	"github.com/pion/rtcp"
	"github.com/pion/rtp"

	"github.com/bluenviron/gortsplib/v5/pkg/ntp"
)

const (
	// period after which, in absence of packets from the current RTP source,
	// the replaced sources are accepted again, so that the receiver can follow
	// the stream back if the current source disappears. During this period,
	// packets of replaced sources are discarded as late packets, so that they
	// cannot make the receiver switch back and forth between sources.
	retiredSourcePeriod = 2 * time.Second

	// maximum number of replaced RTP sources that are kept in memory.
	maxRetiredSources = 8

	// maximum number of sources whose RTCP sender reports are kept in memory.
	maxSenderReports = 8
)

type retiredSource struct {
	ssrc uint32
}

// Receiver is a utility to receive RTP packets.
// It is in charge of:
// - removing duplicate packets (when transport is unreliable)
// - reordering packets (when transport is unrealiable)
// - counting received and lost packets
// - generating RTCP receiver reports
// - switching to a new RTP source when the source changes
type Receiver struct {
	// Track clock rate.
	ClockRate int

	// Local SSRC
	LocalSSRC uint32

	// Whether the transport is unrealiable.
	// When this is enabled, duplicate packets are removed, and packets are reordered.
	UnrealiableTransport bool

	// Size of the buffer for reordering packets.
	// It defaults to 64.
	BufferSize int

	// Period of RTCP receiver reports.
	Period time.Duration

	// time.Now function.
	TimeNow func() time.Time

	// Called when a RTCP receiver report is ready to be written.
	WritePacketRTCP func(rtcp.Packet)

	// Called when the source (SSRC) of the stream changes, for instance
	// because the camera has been rebooted and restarted sending packets with
	// a new SSRC and a new timestamp base.
	// pkt is the first packet of the new source.
	// It is called from inside ProcessPacket2, after the receiver has
	// switched to the new source and before the packet is returned.
	OnSourceChange func(pkt *rtp.Packet)

	mutex sync.RWMutex

	// data from RTP packets
	firstRTPPacketReceived     bool
	timeInitialized            bool
	buffer                     []*rtp.Packet
	absPos                     uint16
	negativeCount              int
	sequenceNumberCycles       uint16
	lastSequenceNumber         uint16
	remoteSSRC                 uint32
	lastRTP                    uint32
	lastSystem                 time.Time
	lost                       uint64
	lostSinceReport            uint64
	lostInCurrentSource        uint64
	received                   uint64
	receivedAndLostSinceReport uint64
	jitter                     float64
	retiredSources             []retiredSource
	currentSourceAt            time.Time

	// data from RTCP sender reports, per source
	senderReports []senderReportData

	terminate chan struct{}
	done      chan struct{}
}

// senderReportData contains the data of the last RTCP sender report of a
// given source.
// Reports are kept per source, so that the absolute time of a source can be
// computed only from its own reports, and reports that arrive before the
// first RTP packet of a source are not lost.
type senderReportData struct {
	ssrc       uint32
	ntpTime    uint64
	rtpTime    uint32
	systemTime time.Time
}

// Initialize initializes Receiver.
func (rr *Receiver) Initialize() error {
	if rr.BufferSize == 0 {
		rr.BufferSize = 64
	}

	if rr.Period == 0 {
		return fmt.Errorf("invalid Period")
	}

	if rr.TimeNow == nil {
		rr.TimeNow = time.Now
	}

	if rr.UnrealiableTransport {
		rr.buffer = make([]*rtp.Packet, rr.BufferSize)
	}

	rr.terminate = make(chan struct{})
	rr.done = make(chan struct{})

	go rr.run()

	return nil
}

// Close closes the Receiver.
func (rr *Receiver) Close() {
	close(rr.terminate)
	<-rr.done
}

func (rr *Receiver) run() {
	defer close(rr.done)

	t := time.NewTicker(rr.Period)
	defer t.Stop()

	for {
		select {
		case <-t.C:
			report := rr.report()
			if report != nil {
				rr.WritePacketRTCP(report)
			}

		case <-rr.terminate:
			return
		}
	}
}

func (rr *Receiver) report() rtcp.Packet {
	rr.mutex.Lock()
	defer rr.mutex.Unlock()

	if !rr.firstRTPPacketReceived || rr.ClockRate == 0 {
		return nil
	}

	system := rr.TimeNow()

	var fractionLost uint8
	if rr.receivedAndLostSinceReport != 0 {
		// equivalent to taking the integer part after multiplying the
		// loss fraction by 256
		fractionLost = uint8((min(rr.lostSinceReport, 0xFFFFFF) * 256) / rr.receivedAndLostSinceReport)
	}

	report := &rtcp.ReceiverReport{
		SSRC: rr.LocalSSRC,
		Reports: []rtcp.ReceptionReport{
			{
				SSRC:               rr.remoteSSRC,
				LastSequenceNumber: uint32(rr.sequenceNumberCycles)<<16 | uint32(rr.lastSequenceNumber),
				FractionLost:       fractionLost,
				// cumulative number of packets lost for the current source
				TotalLost: uint32(min(rr.lostInCurrentSource, 0xFFFFFF)), // allow up to 24 bits
				Jitter:    uint32(rr.jitter),
			},
		},
	}

	rr.lostSinceReport = 0
	rr.receivedAndLostSinceReport = 0

	if sr := rr.senderReportUnsafe(rr.remoteSSRC); sr != nil {
		// middle 32 bits out of 64 in the NTP of last sender report
		report.Reports[0].LastSenderReport = uint32(sr.ntpTime >> 16)

		// delay, expressed in units of 1/65536 seconds, between
		// receiving the last SR packet from source SSRC_n and sending this
		// reception report block
		report.Reports[0].Delay = uint32(system.Sub(sr.systemTime).Seconds() * 65536)
	}

	return report
}

// ProcessPacket processes an inbound RTP packet.
// It returns reordered packets and number of lost packets.
//
// Deprecated: replaced by ProcessPacket2.
func (rr *Receiver) ProcessPacket(
	pkt *rtp.Packet,
	system time.Time,
	ptsEqualsDTS bool,
) ([]*rtp.Packet, uint64, error) {
	pkts, lost := rr.ProcessPacket2(pkt, system, ptsEqualsDTS)
	return pkts, lost, nil
}

// ProcessPacket2 processes an inbound RTP packet.
// It returns reordered packets and number of lost packets.
func (rr *Receiver) ProcessPacket2(
	pkt *rtp.Packet,
	system time.Time,
	ptsEqualsDTS bool,
) ([]*rtp.Packet, uint64) {
	pkts, lost, switched := rr.processPacket(pkt, system, ptsEqualsDTS)

	if switched && rr.OnSourceChange != nil {
		rr.OnSourceChange(pkt)
	}

	return pkts, lost
}

func (rr *Receiver) processPacket(
	pkt *rtp.Packet,
	system time.Time,
	ptsEqualsDTS bool,
) ([]*rtp.Packet, uint64, bool) {
	rr.mutex.Lock()
	defer rr.mutex.Unlock()

	// the source of the stream changed (for instance because the camera has
	// been rebooted): switch to the new source and restart the stream
	// position from its first packet, so that the discontinuity is not
	// counted as network packet loss.
	if rr.firstRTPPacketReceived && pkt.SSRC != rr.remoteSSRC {
		// packets of a source that has just been replaced are late packets of
		// the previous source. discard them, otherwise they would make the
		// receiver switch back and forth between sources.
		if rr.isLatePacketOfRetiredSource(pkt.SSRC, system) {
			return nil, 0, false
		}

		rr.switchSource(pkt, system, ptsEqualsDTS)

		// the first packet of the new source is delivered as the first packet
		// of the new stream position.
		return []*rtp.Packet{pkt}, 0, true
	}

	// first packet
	if !rr.firstRTPPacketReceived {
		rr.firstRTPPacketReceived = true
		rr.received = 1
		rr.receivedAndLostSinceReport = 1
		rr.lastSequenceNumber = pkt.SequenceNumber
		rr.remoteSSRC = pkt.SSRC
		rr.currentSourceAt = system

		if ptsEqualsDTS {
			rr.timeInitialized = true
			rr.lastRTP = pkt.Timestamp
			rr.lastSystem = system
		}

		return []*rtp.Packet{pkt}, 0, false
	}

	rr.currentSourceAt = system

	var pkts []*rtp.Packet
	var lost uint64

	if rr.UnrealiableTransport {
		pkts, lost = rr.reorder(pkt)
	} else {
		pkts = []*rtp.Packet{pkt}
		lost = uint64(pkt.SequenceNumber - rr.lastSequenceNumber - 1)
	}

	rr.lost += lost
	rr.lostInCurrentSource += lost
	rr.lostSinceReport += lost
	rr.received += uint64(len(pkts))
	rr.receivedAndLostSinceReport += uint64(len(pkts)) + lost

	for _, pkt := range pkts {
		// overflow
		diff := int32(pkt.SequenceNumber) - int32(rr.lastSequenceNumber)
		if diff < -0x0FFF {
			rr.sequenceNumberCycles++
		}

		rr.lastSequenceNumber = pkt.SequenceNumber

		if ptsEqualsDTS {
			if rr.timeInitialized && rr.ClockRate != 0 {
				// update jitter
				// https://tools.ietf.org/html/rfc3550#page-39
				D := system.Sub(rr.lastSystem).Seconds()*float64(rr.ClockRate) -
					(float64(pkt.Timestamp) - float64(rr.lastRTP))
				if D < 0 {
					D = -D
				}
				rr.jitter += (D - rr.jitter) / 16
			}

			rr.timeInitialized = true
			rr.lastRTP = pkt.Timestamp
			rr.lastSystem = system
		}
	}

	return pkts, lost, false
}

func (rr *Receiver) reorder(pkt *rtp.Packet) ([]*rtp.Packet, uint64) {
	relPos := int16(pkt.SequenceNumber - rr.lastSequenceNumber - 1)

	// packet is a duplicate or has been sent
	// before the first packet processed by Reorderer.
	// discard.
	if relPos < 0 {
		rr.negativeCount++

		// stream has been resetted, therefore reset reorderer too
		if rr.negativeCount > len(rr.buffer) {
			rr.negativeCount = 0

			// clear buffer
			for i := uint16(0); i < uint16(len(rr.buffer)); i++ {
				p := (rr.absPos + i) & (uint16(len(rr.buffer)) - 1)
				rr.buffer[p] = nil
			}

			// reset position.
			return []*rtp.Packet{pkt}, 0
		}

		return nil, 0
	}

	rr.negativeCount = 0

	// there's a missing packet and buffer is full.
	// return entire buffer and clear it.
	if relPos >= int16(len(rr.buffer)) {
		n := 1
		for i := uint16(0); i < uint16(len(rr.buffer)); i++ {
			p := (rr.absPos + i) & (uint16(len(rr.buffer)) - 1)
			if rr.buffer[p] != nil {
				n++
			}
		}

		ret := make([]*rtp.Packet, n)
		pos := 0

		for i := uint16(0); i < uint16(len(rr.buffer)); i++ {
			p := (rr.absPos + i) & (uint16(len(rr.buffer)) - 1)
			if rr.buffer[p] != nil {
				ret[pos], rr.buffer[p] = rr.buffer[p], nil
				pos++
			}
		}

		ret[pos] = pkt

		return ret, uint64(int(relPos) - n + 1)
	}

	// there's a missing packet
	if relPos != 0 {
		p := (rr.absPos + uint16(relPos)) & (uint16(len(rr.buffer)) - 1)

		// current packet is a duplicate. discard
		if rr.buffer[p] != nil {
			return nil, 0
		}

		// put current packet in buffer
		rr.buffer[p] = pkt
		return nil, 0
	}

	// all packets have been received correctly.
	// return them.

	n := uint16(1)
	for {
		p := (rr.absPos + n) & (uint16(len(rr.buffer)) - 1)
		if rr.buffer[p] == nil {
			break
		}
		n++
	}

	ret := make([]*rtp.Packet, n)
	ret[0] = pkt
	rr.absPos++
	rr.absPos &= (uint16(len(rr.buffer)) - 1)

	for i := uint16(1); i < n; i++ {
		ret[i], rr.buffer[rr.absPos] = rr.buffer[rr.absPos], nil
		rr.absPos++
		rr.absPos &= (uint16(len(rr.buffer)) - 1)
	}

	return ret, 0
}

// switchSource changes the source of the stream.
// The position of the stream is restarted from the packet of the new source,
// so that the discontinuity between the two sources is not counted as lost
// packets, and packets of the new source are delivered according to its own
// sequence numbers.
func (rr *Receiver) switchSource(
	pkt *rtp.Packet,
	system time.Time,
	ptsEqualsDTS bool,
) {
	rr.retireSource(rr.remoteSSRC)

	// the new source is not retired anymore
	for i := range rr.retiredSources {
		if rr.retiredSources[i].ssrc == pkt.SSRC {
			rr.retiredSources = append(rr.retiredSources[:i], rr.retiredSources[i+1:]...)
			break
		}
	}

	rr.remoteSSRC = pkt.SSRC
	rr.currentSourceAt = system
	rr.lastSequenceNumber = pkt.SequenceNumber
	rr.sequenceNumberCycles = 0
	rr.negativeCount = 0
	rr.absPos = 0

	// packets queued for reordering belong to the previous source
	if rr.buffer != nil {
		clear(rr.buffer)
	}

	// jitter is measured per source
	rr.jitter = 0
	rr.lostInCurrentSource = 0
	rr.lostSinceReport = 0
	rr.receivedAndLostSinceReport = 0

	rr.received++
	rr.receivedAndLostSinceReport++

	if ptsEqualsDTS {
		rr.timeInitialized = true
		rr.lastRTP = pkt.Timestamp
		rr.lastSystem = system
	} else {
		rr.timeInitialized = false
	}
}

func (rr *Receiver) retireSource(ssrc uint32) {
	if ssrc == 0 {
		return
	}

	for _, s := range rr.retiredSources {
		if s.ssrc == ssrc {
			return
		}
	}

	if len(rr.retiredSources) >= maxRetiredSources {
		rr.retiredSources = rr.retiredSources[1:]
	}

	rr.retiredSources = append(rr.retiredSources, retiredSource{ssrc: ssrc})
}

// isLatePacketOfRetiredSource reports whether a packet belongs to a source
// that has been replaced by the current one.
// Such packets are late packets of the previous source and are discarded, so
// that they cannot make the receiver switch back and forth between sources.
// When the current source stops sending packets for retiredSourcePeriod, the
// previous sources are accepted again, so that the receiver can follow the
// stream back if the current source disappears.
func (rr *Receiver) isLatePacketOfRetiredSource(ssrc uint32, now time.Time) bool {
	// the current source is not sending anymore: everything is accepted.
	if now.Sub(rr.currentSourceAt) >= retiredSourcePeriod {
		rr.retiredSources = nil
		return false
	}

	for _, s := range rr.retiredSources {
		if s.ssrc == ssrc {
			return true
		}
	}
	return false
}

// ProcessSenderReport processes an inbound RTCP sender report.
func (rr *Receiver) ProcessSenderReport(sr *rtcp.SenderReport, system time.Time) {
	rr.mutex.Lock()
	defer rr.mutex.Unlock()

	// reports are stored per source, so that the absolute time of the current
	// source is computed only from its own reports, and reports that arrive
	// before the first packet of a source are retained.
	for i := range rr.senderReports {
		if rr.senderReports[i].ssrc == sr.SSRC {
			rr.senderReports[i].ntpTime = sr.NTPTime
			rr.senderReports[i].rtpTime = sr.RTPTime
			rr.senderReports[i].systemTime = system
			return
		}
	}

	if len(rr.senderReports) >= maxSenderReports {
		rr.senderReports = rr.senderReports[1:]
	}

	rr.senderReports = append(rr.senderReports, senderReportData{
		ssrc:       sr.SSRC,
		ntpTime:    sr.NTPTime,
		rtpTime:    sr.RTPTime,
		systemTime: system,
	})
}

func (rr *Receiver) senderReportUnsafe(ssrc uint32) *senderReportData {
	for i := range rr.senderReports {
		if rr.senderReports[i].ssrc == ssrc {
			return &rr.senderReports[i]
		}
	}
	return nil
}

func (rr *Receiver) packetNTPUnsafe(ts uint32) (time.Time, bool) {
	if rr.ClockRate == 0 {
		return time.Time{}, false
	}

	sr := rr.senderReportUnsafe(rr.remoteSSRC)
	if sr == nil {
		return time.Time{}, false
	}

	timeDiff := int32(ts - sr.rtpTime)
	timeDiffGo := (time.Duration(timeDiff) * time.Second) / time.Duration(rr.ClockRate)

	return ntp.Decode(sr.ntpTime).Add(timeDiffGo), true
}

// PacketNTP returns the NTP (absolute timestamp) of the packet.
func (rr *Receiver) PacketNTP(ts uint32) (time.Time, bool) {
	rr.mutex.RLock()
	defer rr.mutex.RUnlock()

	return rr.packetNTPUnsafe(ts)
}

// RemoteSSRC returns the SSRC of the current RTP source.
func (rr *Receiver) RemoteSSRC() (uint32, bool) {
	rr.mutex.RLock()
	defer rr.mutex.RUnlock()

	return rr.remoteSSRC, rr.firstRTPPacketReceived
}

// Stats are statistics.
type Stats struct {
	// inbound RTP packets correctly received and processed.
	Received uint64
	// lost inbound RTP packets.
	Lost uint64
	// Jitter of inbound RTP packets.
	Jitter float64
	// Last sequence number of inbound RTP packets.
	LastSequenceNumber uint16
	// Last RTP time of inbound RTP packets.
	LastRTP uint32
	// Last NTP time of inbound RTP packets.
	LastNTP time.Time

	// Deprecated: use Received.
	TotalReceived uint64
	// Deprecated: use Lost.
	TotalLost uint64
	// Deprecated: will be removed in next version.
	RemoteSSRC uint32
}

// Stats returns statistics.
func (rr *Receiver) Stats() *Stats {
	rr.mutex.RLock()
	defer rr.mutex.RUnlock()

	if !rr.firstRTPPacketReceived {
		return nil
	}

	ntp, _ := rr.packetNTPUnsafe(rr.lastRTP)

	return &Stats{
		Received:           rr.received,
		Lost:               rr.lost,
		Jitter:             rr.jitter,
		LastSequenceNumber: rr.lastSequenceNumber,
		LastRTP:            rr.lastRTP,
		LastNTP:            ntp,
		// deprecated
		TotalReceived: rr.received,
		TotalLost:     rr.lost,
		RemoteSSRC:    rr.remoteSSRC,
	}
}
