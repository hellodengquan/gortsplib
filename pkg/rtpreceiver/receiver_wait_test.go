package rtpreceiver_test

import (
	"testing"
	"time"

	"github.com/pion/rtcp"
	"github.com/pion/rtp"
	"github.com/stretchr/testify/require"

	"github.com/bluenviron/gortsplib/v5/pkg/rtpreceiver"
)

// a timer-based delivery collected by the OnPackets callback.
type timerDelivery struct {
	pkts []*rtp.Packet
	lost uint64
}

func newTimerReceiver(t *testing.T, maxWait time.Duration) (*rtpreceiver.Receiver, chan timerDelivery) {
	t.Helper()

	delivered := make(chan timerDelivery, 16)

	rr := &rtpreceiver.Receiver{
		ClockRate:            90000,
		LocalSSRC:            0x65f83afb,
		UnrealiableTransport: true,
		MaxPacketWait:        maxWait,
		Period:               500 * time.Millisecond,
		OnPackets: func(pkts []*rtp.Packet, lost uint64) {
			delivered <- timerDelivery{pkts: pkts, lost: lost}
		},
	}
	require.NoError(t, rr.Initialize())
	t.Cleanup(rr.Close)

	return rr, delivered
}

func seqPacket(seq uint16) *rtp.Packet {
	return &rtp.Packet{
		Header: rtp.Header{
			SequenceNumber: seq,
		},
	}
}

func TestMaxPacketWaitFlush(t *testing.T) {
	rr, delivered := newTimerReceiver(t, 200*time.Millisecond)

	// first packet is delivered immediately.
	out, lost := rr.ProcessPacket2(seqPacket(100), time.Now(), true)
	require.Equal(t, []*rtp.Packet{seqPacket(100)}, out)
	require.Equal(t, uint64(0), lost)

	// packet 101 is missing, packets 102 and 103 are buffered.
	out, lost = rr.ProcessPacket2(seqPacket(102), time.Now(), true)
	require.Nil(t, out)
	require.Equal(t, uint64(0), lost)

	out, lost = rr.ProcessPacket2(seqPacket(103), time.Now(), true)
	require.Nil(t, out)
	require.Equal(t, uint64(0), lost)

	// nothing is delivered before the deadline.
	select {
	case d := <-delivered:
		t.Fatalf("unexpected early delivery: %v", d)
	case <-time.After(100 * time.Millisecond):
	}

	// after the deadline, buffered packets are delivered in order and the
	// missing packet is counted as lost, even without new packets arriving.
	select {
	case d := <-delivered:
		require.Equal(t, []*rtp.Packet{seqPacket(102), seqPacket(103)}, d.pkts)
		require.Equal(t, uint64(1), d.lost)
	case <-time.After(500 * time.Millisecond):
		t.Fatal("timed out waiting for timer-based delivery")
	}

	stats := rr.Stats()
	require.Equal(t, uint64(3), stats.Received)
	require.Equal(t, uint64(1), stats.Lost)
	require.Equal(t, uint16(103), stats.LastSequenceNumber)
}

func TestMaxPacketWaitLatePacketDiscarded(t *testing.T) {
	rr, delivered := newTimerReceiver(t, 200*time.Millisecond)

	rr.ProcessPacket2(seqPacket(100), time.Now(), true)
	rr.ProcessPacket2(seqPacket(102), time.Now(), true)

	d := <-delivered
	require.Equal(t, []*rtp.Packet{seqPacket(102)}, d.pkts)
	require.Equal(t, uint64(1), d.lost)

	// the missing packet arrives late: it must not be delivered and must not
	// be counted as lost again.
	out, lost := rr.ProcessPacket2(seqPacket(101), time.Now(), true)
	require.Nil(t, out)
	require.Equal(t, uint64(0), lost)

	// a contiguous new packet is still delivered normally.
	out, lost = rr.ProcessPacket2(seqPacket(103), time.Now(), true)
	require.Equal(t, []*rtp.Packet{seqPacket(103)}, out)
	require.Equal(t, uint64(0), lost)

	select {
	case d := <-delivered:
		t.Fatalf("unexpected timer delivery: %v", d)
	case <-time.After(300 * time.Millisecond):
	}

	stats := rr.Stats()
	require.Equal(t, uint64(3), stats.Received) // 100, 102, 103
	require.Equal(t, uint64(1), stats.Lost)     // only 101
	require.Equal(t, uint16(103), stats.LastSequenceNumber)
}

func TestMaxPacketWaitFilledBeforeDeadline(t *testing.T) {
	delivered := make(chan timerDelivery, 16)

	rr := &rtpreceiver.Receiver{
		ClockRate:            90000,
		LocalSSRC:            0x65f83afb,
		UnrealiableTransport: true,
		BufferSize:           4,
		MaxPacketWait:        10 * time.Second,
		Period:               500 * time.Millisecond,
		OnPackets: func(pkts []*rtp.Packet, lost uint64) {
			delivered <- timerDelivery{pkts: pkts, lost: lost}
		},
	}
	require.NoError(t, rr.Initialize())
	defer rr.Close()

	rr.ProcessPacket2(seqPacket(100), time.Now(), true)

	// packet 101 is missing; sending 105 (relPos 4) fills the buffer, which
	// is therefore released synchronously without waiting for the timer,
	// preserving the original reordering behavior.
	out, lost := rr.ProcessPacket2(seqPacket(105), time.Now(), true)
	require.Equal(t, []*rtp.Packet{seqPacket(105)}, out)
	require.Equal(t, uint64(4), lost)

	select {
	case d := <-delivered:
		t.Fatalf("unexpected timer delivery: %v", d)
	case <-time.After(300 * time.Millisecond):
	}
}

func TestMaxPacketWaitGapClosedBeforeDeadline(t *testing.T) {
	rr, delivered := newTimerReceiver(t, 10*time.Second)

	rr.ProcessPacket2(seqPacket(100), time.Now(), true)
	rr.ProcessPacket2(seqPacket(102), time.Now(), true)

	// the missing packet arrives before the deadline: the batch is delivered
	// in order through the regular path and no loss is reported.
	out, lost := rr.ProcessPacket2(seqPacket(101), time.Now(), true)
	require.Equal(t, []*rtp.Packet{seqPacket(101), seqPacket(102)}, out)
	require.Equal(t, uint64(0), lost)

	select {
	case d := <-delivered:
		t.Fatalf("unexpected timer delivery: %v", d)
	case <-time.After(300 * time.Millisecond):
	}

	stats := rr.Stats()
	require.Equal(t, uint64(3), stats.Received)
	require.Equal(t, uint64(0), stats.Lost)
	require.Equal(t, uint16(102), stats.LastSequenceNumber)
}

func TestMaxPacketWaitSequenceWraparound(t *testing.T) {
	rr, delivered := newTimerReceiver(t, 200*time.Millisecond)

	// start just before the sequence number wrap.
	rr.ProcessPacket2(seqPacket(0xffff), time.Now(), true)

	// 0x0000 is missing, 0x0001 and 0x0002 are buffered.
	rr.ProcessPacket2(seqPacket(0x0001), time.Now(), true)
	rr.ProcessPacket2(seqPacket(0x0002), time.Now(), true)

	d := <-delivered
	require.Equal(t, []*rtp.Packet{seqPacket(0x0001), seqPacket(0x0002)}, d.pkts)
	require.Equal(t, uint64(1), d.lost)

	// late 0x0000 is discarded.
	out, lost := rr.ProcessPacket2(seqPacket(0x0000), time.Now(), true)
	require.Nil(t, out)
	require.Equal(t, uint64(0), lost)

	// traffic continues after the wrap.
	out, lost = rr.ProcessPacket2(seqPacket(0x0003), time.Now(), true)
	require.Equal(t, []*rtp.Packet{seqPacket(0x0003)}, out)
	require.Equal(t, uint64(0), lost)

	stats := rr.Stats()
	require.Equal(t, uint64(4), stats.Received)
	require.Equal(t, uint64(1), stats.Lost)
	require.Equal(t, uint16(0x0003), stats.LastSequenceNumber)
}

func TestMaxPacketWaitMultipleGaps(t *testing.T) {
	rr, delivered := newTimerReceiver(t, 200*time.Millisecond)

	rr.ProcessPacket2(seqPacket(100), time.Now(), true)

	// missing 101, 103; buffered 102 and 104.
	rr.ProcessPacket2(seqPacket(102), time.Now(), true)
	rr.ProcessPacket2(seqPacket(104), time.Now(), true)

	d := <-delivered
	require.Equal(t, []*rtp.Packet{seqPacket(102), seqPacket(104)}, d.pkts)
	require.Equal(t, uint64(2), d.lost)

	stats := rr.Stats()
	require.Equal(t, uint64(3), stats.Received)
	require.Equal(t, uint64(2), stats.Lost)
	require.Equal(t, uint16(104), stats.LastSequenceNumber)
}

func TestMaxPacketWaitDisabled(t *testing.T) {
	// MaxPacketWait is zero: behavior must be identical to a receiver
	// without the feature, even if OnPackets is set.
	delivered := make(chan timerDelivery, 16)

	rr := &rtpreceiver.Receiver{
		ClockRate:            90000,
		LocalSSRC:            0x65f83afb,
		UnrealiableTransport: true,
		Period:               500 * time.Millisecond,
		OnPackets: func(pkts []*rtp.Packet, lost uint64) {
			delivered <- timerDelivery{pkts: pkts, lost: lost}
		},
	}
	require.NoError(t, rr.Initialize())
	defer rr.Close()

	rr.ProcessPacket2(seqPacket(100), time.Now(), true)
	rr.ProcessPacket2(seqPacket(102), time.Now(), true)

	// the packet remains buffered and no timer delivery ever happens.
	select {
	case d := <-delivered:
		t.Fatalf("unexpected timer delivery: %v", d)
	case <-time.After(400 * time.Millisecond):
	}

	stats := rr.Stats()
	require.Equal(t, uint64(1), stats.Received)
	require.Equal(t, uint64(0), stats.Lost)
}

func TestMaxPacketWaitRTCPReport(t *testing.T) {
	reportRecv := make(chan rtcp.Packet, 4)

	rr := &rtpreceiver.Receiver{
		ClockRate:            90000,
		LocalSSRC:            0x65f83afb,
		UnrealiableTransport: true,
		MaxPacketWait:        100 * time.Millisecond,
		Period:               200 * time.Millisecond,
		OnPackets:            func([]*rtp.Packet, uint64) {},
		WritePacketRTCP: func(pkt rtcp.Packet) {
			reportRecv <- pkt
		},
	}
	require.NoError(t, rr.Initialize())
	defer rr.Close()

	// 100 delivered; 101 is missing; 102 and 103 buffered and then released
	// by the timer, which counts 101 as lost.
	rr.ProcessPacket2(seqPacket(100), time.Now(), true)
	rr.ProcessPacket2(seqPacket(102), time.Now(), true)
	rr.ProcessPacket2(seqPacket(103), time.Now(), true)

	// the RTCP report generated after the flush must reflect the timer-based
	// delivery: highest sequence number 103 and one lost packet.
	select {
	case pkt := <-reportRecv:
		report := pkt.(*rtcp.ReceiverReport)
		require.Len(t, report.Reports, 1)
		require.Equal(t, uint32(103), report.Reports[0].LastSequenceNumber)
		require.Equal(t, uint32(1), report.Reports[0].TotalLost)
		require.NotEqual(t, uint8(0), report.Reports[0].FractionLost)
	case <-time.After(1 * time.Second):
		t.Fatal("timed out waiting for RTCP report")
	}
}

func TestMaxPacketWaitRTCPReportWraparound(t *testing.T) {
	reportRecv := make(chan rtcp.Packet, 4)

	rr := &rtpreceiver.Receiver{
		ClockRate:            90000,
		LocalSSRC:            0x65f83afb,
		UnrealiableTransport: true,
		MaxPacketWait:        100 * time.Millisecond,
		Period:               200 * time.Millisecond,
		OnPackets:            func([]*rtp.Packet, uint64) {},
		WritePacketRTCP: func(pkt rtcp.Packet) {
			reportRecv <- pkt
		},
	}
	require.NoError(t, rr.Initialize())
	defer rr.Close()

	// wrap across the 16-bit sequence number boundary; 0x0000 is missing and
	// is counted as lost when the timer releases 0x0001.
	rr.ProcessPacket2(seqPacket(0xffff), time.Now(), true)
	rr.ProcessPacket2(seqPacket(0x0001), time.Now(), true)

	select {
	case pkt := <-reportRecv:
		report := pkt.(*rtcp.ReceiverReport)
		// extended highest sequence number keeps the cycle count.
		require.Equal(t, uint32(1<<16|0x0001), report.Reports[0].LastSequenceNumber)
		require.Equal(t, uint32(1), report.Reports[0].TotalLost)
	case <-time.After(1 * time.Second):
		t.Fatal("timed out waiting for RTCP report")
	}
}
