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

// maximum interval between checks for expired reordering waits.
const maxPacketWaitCheckPeriod = 100 * time.Millisecond

// Receiver is a utility to receive RTP packets.
// It is in charge of:
// - removing duplicate packets (when transport is unreliable)
// - reordering packets (when transport is unreliable)
// - counting received and lost packets
// - generating RTCP receiver reports
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

	// Maximum time to wait for a missing packet before the packets already
	// stored in the reordering buffer are delivered anyway.
	// This only has effect when UnrealiableTransport is true.
	// When zero, the feature is disabled and packets are released only when
	// the buffer is full, as in a receiver without this option.
	MaxPacketWait time.Duration

	// Period of RTCP receiver reports.
	Period time.Duration

	// time.Now function.
	TimeNow func() time.Time

	// Called when a RTCP receiver report is ready to be written.
	WritePacketRTCP func(rtcp.Packet)

	// Used to determine whether the PTS of a packet is equal to its DTS.
	// It is used when packets are delivered by the MaxPacketWait timer,
	// i.e. outside of a ProcessPacket2() call.
	// It defaults to a function returning true.
	PTSEqualsDTS func(*rtp.Packet) bool

	// Called when packets are delivered by the MaxPacketWait timer, i.e.
	// without any new packet being received.
	// It receives the delivered packets, in sequence number order, and the
	// number of packets that were declared lost in order to release them.
	// It is not called for packets returned by ProcessPacket2().
	// When nil, timer-based delivery is disabled even if MaxPacketWait is set.
	OnPackets func(pkts []*rtp.Packet, lost uint64)

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
	received                   uint64
	receivedAndLostSinceReport uint64
	jitter                     float64

	// time at which the current reordering wait started.
	// It is zero when the reordering buffer is empty.
	// It is used only when MaxPacketWait is set.
	waitStart time.Time

	// data from RTCP sender reports
	firstSenderReportReceived  bool
	lastSenderReportTimeNTP    uint64
	lastSenderReportTimeRTP    uint32
	lastSenderReportTimeSystem time.Time

	// used to wake up the background goroutine when a wait starts.
	wake chan struct{}

	terminate chan struct{}
	done      chan struct{}
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

	if rr.PTSEqualsDTS == nil {
		rr.PTSEqualsDTS = func(*rtp.Packet) bool { return true }
	}

	if rr.UnrealiableTransport {
		rr.buffer = make([]*rtp.Packet, rr.BufferSize)
	}

	if rr.MaxPacketWait > 0 && rr.OnPackets != nil {
		rr.wake = make(chan struct{}, 1)
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

	reportTicker := time.NewTicker(rr.Period)
	defer reportTicker.Stop()

	// timer-based reordering is disabled, the goroutine only has to
	// generate RTCP receiver reports.
	if rr.wake == nil {
		for {
			select {
			case <-reportTicker.C:
				report := rr.report()
				if report != nil && rr.WritePacketRTCP != nil {
					rr.WritePacketRTCP(report)
				}

			case <-rr.terminate:
				return
			}
		}
	}

	checkPeriod := rr.MaxPacketWait
	if checkPeriod > maxPacketWaitCheckPeriod {
		checkPeriod = maxPacketWaitCheckPeriod
	}

	waitTimer := time.NewTimer(checkPeriod)
	timerPending := false
	if !waitTimer.Stop() {
		<-waitTimer.C
	}

	for {
		// the timer is armed only while the reordering buffer is not empty.
		if timerPending {
			select {
			case <-waitTimer.C:
				rr.flushExpired()
				timerPending = false

			case <-rr.wake:
				if !waitTimer.Stop() {
					select {
					case <-waitTimer.C:
					default:
					}
				}
				timerPending = false

			case <-reportTicker.C:
				report := rr.report()
				if report != nil && rr.WritePacketRTCP != nil {
					rr.WritePacketRTCP(report)
				}

			case <-rr.terminate:
				if !waitTimer.Stop() {
					select {
					case <-waitTimer.C:
					default:
					}
				}
				return
			}
		} else {
			select {
			case <-rr.wake:
			case <-reportTicker.C:
				report := rr.report()
				if report != nil && rr.WritePacketRTCP != nil {
					rr.WritePacketRTCP(report)
				}

			case <-rr.terminate:
				return
			}
		}

		// re-arm the timer if a wait is still pending.
		if rr.isWaitPending() {
			waitTimer.Reset(checkPeriod)
			timerPending = true
		}
	}
}

func (rr *Receiver) isWaitPending() bool {
	rr.mutex.RLock()
	defer rr.mutex.RUnlock()
	return !rr.waitStart.IsZero()
}

// flushExpired releases the content of the reordering buffer when the
// maximum wait for a missing packet has elapsed.
func (rr *Receiver) flushExpired() {
	rr.mutex.Lock()

	if rr.waitStart.IsZero() || rr.TimeNow().Sub(rr.waitStart) < rr.MaxPacketWait {
		rr.mutex.Unlock()
		return
	}

	pkts, lost := rr.flushBufferLocked()
	system := rr.TimeNow()
	rr.accountLocked(pkts, lost, system, rr.PTSEqualsDTS)

	rr.mutex.Unlock()

	if len(pkts) != 0 || lost != 0 {
		rr.OnPackets(pkts, lost)
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
				TotalLost:          uint32(min(rr.lost, 0xFFFFFF)), // allow up to 24 bits
				Jitter:             uint32(rr.jitter),
			},
		},
	}

	if rr.firstSenderReportReceived {
		// middle 32 bits out of 64 in the NTP of last sender report
		report.Reports[0].LastSenderReport = uint32(rr.lastSenderReportTimeNTP >> 16)

		// delay, expressed in units of 1/65536 seconds, between
		// receiving the last SR packet from source SSRC_n and sending this
		// reception report block
		report.Reports[0].Delay = uint32(system.Sub(rr.lastSenderReportTimeSystem).Seconds() * 65536)
	}

	rr.lostSinceReport = 0
	rr.receivedAndLostSinceReport = 0

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
	rr.mutex.Lock()
	defer rr.mutex.Unlock()

	// first packet
	if !rr.firstRTPPacketReceived {
		rr.firstRTPPacketReceived = true
		rr.received = 1
		rr.receivedAndLostSinceReport = 1
		rr.lastSequenceNumber = pkt.SequenceNumber
		rr.remoteSSRC = pkt.SSRC

		if ptsEqualsDTS {
			rr.timeInitialized = true
			rr.lastRTP = pkt.Timestamp
			rr.lastSystem = system
		}

		return []*rtp.Packet{pkt}, 0
	}

	var pkts []*rtp.Packet
	var lost uint64

	if rr.UnrealiableTransport {
		pkts, lost = rr.reorder(pkt, system)
	} else {
		pkts = []*rtp.Packet{pkt}
		lost = uint64(pkt.SequenceNumber - rr.lastSequenceNumber - 1)
	}

	rr.accountLocked(pkts, lost, system, func(*rtp.Packet) bool { return ptsEqualsDTS })

	return pkts, lost
}

// accountLocked updates statistics with a batch of delivered packets and
// the number of packets declared lost to release them.
func (rr *Receiver) accountLocked(
	pkts []*rtp.Packet,
	lost uint64,
	system time.Time,
	ptsEqualsDTS func(*rtp.Packet) bool,
) {
	rr.lost += lost
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

		if ptsEqualsDTS(pkt) {
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
}

func (rr *Receiver) reorder(pkt *rtp.Packet, system time.Time) ([]*rtp.Packet, uint64) {
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

			// the buffer has been cleared, any pending wait is obsolete.
			rr.waitStart = time.Time{}

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

		// the buffer is empty again, any pending wait is obsolete.
		rr.waitStart = time.Time{}

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
		rr.signalWaitLocked(system)

		// if the wait has already expired, release the buffer immediately,
		// without waiting for the background timer.
		if rr.MaxPacketWait > 0 && !rr.waitStart.IsZero() &&
			system.Sub(rr.waitStart) >= rr.MaxPacketWait {
			return rr.flushBufferLocked()
		}

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

	// the wait ends when the buffer is empty again.
	if rr.bufferEmptyLocked() {
		rr.waitStart = time.Time{}
	}

	return ret, 0
}

// signalWaitLocked starts the reordering wait timer when a packet is stored
// in the buffer for the first time since the buffer was empty.
func (rr *Receiver) signalWaitLocked(system time.Time) {
	if rr.MaxPacketWait == 0 || rr.OnPackets == nil || !rr.waitStart.IsZero() {
		return
	}

	rr.waitStart = system
	if rr.wake != nil {
		select {
		case rr.wake <- struct{}{}:
		default:
		}
	}
}

// bufferEmptyLocked returns whether the reordering buffer does not contain
// any packet.
func (rr *Receiver) bufferEmptyLocked() bool {
	for i := uint16(0); i < uint16(len(rr.buffer)); i++ {
		if rr.buffer[i] != nil {
			return false
		}
	}
	return true
}

// flushBufferLocked returns all the packets stored in the reordering buffer,
// in sequence number order, and the number of missing packets that were
// skipped to release them. The buffer is cleared and the wait is ended.
func (rr *Receiver) flushBufferLocked() ([]*rtp.Packet, uint64) {
	n := 0
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

	// number of sequence numbers from the expected one to the sequence
	// number of the last delivered packet, minus the packets that are
	// actually delivered.
	last := ret[n-1].SequenceNumber
	lost := uint64(int16(last-rr.lastSequenceNumber)) - uint64(n)

	// absPos is unchanged: the buffer wraps on itself and is now empty.
	rr.waitStart = time.Time{}

	return ret, lost
}

// ProcessSenderReport processes an inbound RTCP sender report.
func (rr *Receiver) ProcessSenderReport(sr *rtcp.SenderReport, system time.Time) {
	rr.mutex.Lock()
	defer rr.mutex.Unlock()

	rr.firstSenderReportReceived = true
	rr.lastSenderReportTimeNTP = sr.NTPTime
	rr.lastSenderReportTimeRTP = sr.RTPTime
	rr.lastSenderReportTimeSystem = system
}

func (rr *Receiver) packetNTPUnsafe(ts uint32) (time.Time, bool) {
	if !rr.firstSenderReportReceived || rr.ClockRate == 0 {
		return time.Time{}, false
	}

	timeDiff := int32(ts - rr.lastSenderReportTimeRTP)
	timeDiffGo := (time.Duration(timeDiff) * time.Second) / time.Duration(rr.ClockRate)

	return ntp.Decode(rr.lastSenderReportTimeNTP).Add(timeDiffGo), true
}

// PacketNTP returns the NTP (absolute timestamp) of the packet.
func (rr *Receiver) PacketNTP(ts uint32) (time.Time, bool) {
	rr.mutex.RLock()
	defer rr.mutex.RUnlock()

	return rr.packetNTPUnsafe(ts)
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
