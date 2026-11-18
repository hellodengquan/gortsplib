package gortsplib

import (
	"time"

	"github.com/pion/rtcp"
	"github.com/pion/rtp"

	"github.com/bluenviron/gortsplib/v5/pkg/format"
	"github.com/bluenviron/gortsplib/v5/pkg/rtpsender"
)

type serverMulticastWriterFormat struct {
	senderReportPeriod       time.Duration
	timeNow                  func() time.Time
	disableRTCPSenderReports bool
	smm                      *serverMulticastWriterMedia
	format                   format.Format

	rtpSender *rtpsender.Sender

	// it suspends the delivery of this media to the multicast group when its
	// write queue overflows.
	deliveryGate mediaDeliveryGate
}

func (smf *serverMulticastWriterFormat) initialize() {
	smf.deliveryGate.initialize(smf.format)

	smf.rtpSender = &rtpsender.Sender{
		ClockRate: smf.format.ClockRate(),
		Period:    smf.senderReportPeriod,
		TimeNow:   smf.timeNow,
		WritePacketRTCP: func(pkt rtcp.Packet) {
			if !smf.disableRTCPSenderReports {
				smf.smm.writePacketRTCP(pkt) //nolint:errcheck
			}
		},
	}
	smf.rtpSender.Initialize()
}

func (smf *serverMulticastWriterFormat) close() {
	smf.rtpSender.Close()
}

// writePacketRTPEncoded writes an encoded RTP packet to the multicast group.
// It reports whether the packet entered the send queue.
// Queue overflows never interrupt the writing of the stream.
func (smf *serverMulticastWriterFormat) writePacketRTPEncoded(
	pkt *rtp.Packet,
	ntp time.Time,
	ptsEqualsDTS bool,
	payload []byte,
) bool {
	// when the queue overflowed, the media is suspended until an
	// independently decodable picture is available.
	if !smf.deliveryGate.isWritable(pkt) {
		return false
	}

	ok := smf.smm.writer.Push(func() error {
		smf.smm.rtpl.write(payload, smf.smm.rtpAddr) //nolint:errcheck

		// do not report write errors for two reasons:
		// - errors make the async processor stop silently, and there's no error reporting mechanism
		// - we're writing to several interfaces at once, and the error might be located on a single one
		return nil
	})
	if !ok {
		smf.deliveryGate.onQueueFull()
		return false
	}

	// statistics and RTCP sender reports must only account for
	// packets that entered the send queue.
	smf.rtpSender.ProcessPacket(pkt, ntp, ptsEqualsDTS)

	return true
}
