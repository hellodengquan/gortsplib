package gortsplib

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/pion/rtcp"
	"github.com/pion/rtp"
	"github.com/stretchr/testify/require"

	"github.com/bluenviron/gortsplib/v5/internal/asyncprocessor"
	"github.com/bluenviron/gortsplib/v5/pkg/description"
	"github.com/bluenviron/gortsplib/v5/pkg/format"
	"github.com/bluenviron/gortsplib/v5/pkg/rtpsender"
)

var (
	testVideoMedia = &description.Media{
		Type:    description.MediaTypeVideo,
		Formats: []format.Format{&format.H264{PayloadTyp: 96, PacketizationMode: 1}},
	}
	testAudioMedia = &description.Media{
		Type:    description.MediaTypeAudio,
		Formats: []format.Format{&format.G711{PayloadTyp: 0, MULaw: true, SampleRate: 8000, ChannelCount: 1}},
	}

	testKeyFramePacket = &rtp.Packet{
		Header:  rtp.Header{Version: 2, PayloadType: 96, SequenceNumber: 10, Timestamp: 1000},
		Payload: []byte{0x65, 0x01, 0x02}, // IDR
	}
	testNonKeyFramePacket = &rtp.Packet{
		Header:  rtp.Header{Version: 2, PayloadType: 96, SequenceNumber: 11, Timestamp: 2000},
		Payload: []byte{0x41, 0x01, 0x02}, // non-IDR
	}
	testAudioPacket = &rtp.Packet{
		Header:  rtp.Header{Version: 2, PayloadType: 0, SequenceNumber: 20, Timestamp: 3000},
		Payload: []byte{0x01, 0x02},
	}
)

// newTestProcessor returns a processor that is not started yet.
// With a buffer size of 1, the first pushed packet fills the queue and every
// following one overflows it, in a deterministic way.
func newTestProcessor(bufferSize int) *asyncprocessor.Processor {
	p := &asyncprocessor.Processor{
		BufferSize: bufferSize,
		OnError:    func(_ context.Context, _ error) {},
	}
	p.Initialize()
	return p
}

// newTestSession builds a session in playing state, associated with a single
// media of the given stream.
func newTestSession(
	t *testing.T,
	writer *asyncprocessor.Processor,
	medi *description.Media,
) *ServerSession {
	t.Helper()

	ss := &ServerSession{
		s: &Server{
			MaxPacketSize: 1472,
			timeNow:       time.Now,
		},
		setuppedTransport:   &SessionTransport{Protocol: ProtocolTCP},
		state:               ServerSessionStatePlay,
		setuppedMedias:      make(map[*description.Media]*serverSessionMedia),
		udpCheckStreamTimer: emptyTimer(),
	}
	ss.writer = writer

	sm := &serverSessionMedia{
		ss:      ss,
		media:   medi,
		formats: make(map[uint8]*serverSessionFormat),
	}
	ss.setuppedMedias[medi] = sm

	for _, forma := range medi.Formats {
		ssf := &serverSessionFormat{
			ssm:                   sm,
			format:                forma,
			localSSRC:             1234,
			writePacketRTPInQueue: func([]byte) error { return nil },
		}
		ssf.deliveryGate.initialize(forma)

		ssf.rtpSender = &rtpsender.Sender{
			ClockRate:       forma.ClockRate(),
			Period:          time.Hour,
			TimeNow:         time.Now,
			WritePacketRTCP: func(rtcp.Packet) {},
		}
		ssf.rtpSender.Initialize()

		sm.formats[forma.PayloadType()] = ssf
	}

	return ss
}

func (ss *ServerSession) testFormat(medi *description.Media, payloadType uint8) *serverSessionFormat {
	return ss.setuppedMedias[medi].formats[payloadType]
}

func newTestMulticastWriterFormat(t *testing.T, proc *asyncprocessor.Processor, forma format.Format) *serverMulticastWriterFormat {
	t.Helper()

	rtpl := &serverUDPListener{
		writeTimeout: time.Second,
		listenPacket: net.ListenPacket,
		address:      "127.0.0.1:0",
	}
	require.NoError(t, rtpl.initialize())
	t.Cleanup(rtpl.close)

	smm := &serverMulticastWriterMedia{
		media:   &description.Media{},
		writer:  proc,
		rtpl:    rtpl,
		rtpAddr: &net.UDPAddr{IP: net.ParseIP("127.0.0.1"), Port: 65534},
		formats: make(map[uint8]*serverMulticastWriterFormat),
	}

	smf := &serverMulticastWriterFormat{
		senderReportPeriod:       time.Hour,
		disableRTCPSenderReports: true,
		smm:                      smm,
		format:                   forma,
	}
	smf.initialize()

	return smf
}

// TestServerSessionFormatQueueOverflow checks that, when the send queue of a
// session overflows, the media is suspended and non-key frames are skipped
// without being accounted in statistics.
func TestServerSessionFormatQueueOverflow(t *testing.T) {
	proc := newTestProcessor(1)
	defer proc.Close()

	ss := newTestSession(t, proc, testVideoMedia)
	ssf := ss.testFormat(testVideoMedia, 96)
	defer ssf.rtpSender.Close()

	// the packet enters the queue.
	sent, err := ssf.writePacketRTPEncoded(testNonKeyFramePacket, time.Now(), true, []byte{0x01})
	require.NoError(t, err)
	require.True(t, sent)

	// the queue is full: the packet is refused and the media is suspended.
	sent, err = ssf.writePacketRTPEncoded(testNonKeyFramePacket, time.Now(), true, []byte{0x02})
	require.Error(t, err)
	require.False(t, sent)

	// while suspended, non-key frames are skipped without reporting errors.
	sent, err = ssf.writePacketRTPEncoded(testNonKeyFramePacket, time.Now(), true, []byte{0x03})
	require.NoError(t, err)
	require.False(t, sent)

	sent, err = ssf.writePacketRTPEncoded(testNonKeyFramePacket, time.Now(), true, []byte{0x04})
	require.NoError(t, err)
	require.False(t, sent)

	// statistics and sender reports only account for packets that entered the queue.
	require.Equal(t, uint64(1), ssf.ssm.bytesSent.Load())
	require.Equal(t, uint64(1), ssf.rtpSender.Stats().Sent)
}

// TestServerSessionFormatRecovery checks that, after a suspension, the delivery
// restarts exactly from the first packet of an independently decodable picture.
func TestServerSessionFormatRecovery(t *testing.T) {
	proc := newTestProcessor(16)
	defer proc.Close()
	proc.Start()

	ss := newTestSession(t, proc, testVideoMedia)
	ssf := ss.testFormat(testVideoMedia, 96)
	defer ssf.rtpSender.Close()

	// simulate a suspension caused by a previous overflow.
	ssf.deliveryGate.onQueueFull()

	sent, err := ssf.writePacketRTPEncoded(testNonKeyFramePacket, time.Now(), true, []byte{0x01})
	require.NoError(t, err)
	require.False(t, sent)

	sent, err = ssf.writePacketRTPEncoded(testNonKeyFramePacket, time.Now(), true, []byte{0x02})
	require.NoError(t, err)
	require.False(t, sent)

	require.Equal(t, uint64(0), ssf.ssm.bytesSent.Load())

	// the delivery restarts from the key frame.
	sent, err = ssf.writePacketRTPEncoded(testKeyFramePacket, time.Now(), true, []byte{0x03})
	require.NoError(t, err)
	require.True(t, sent)

	// subsequent packets are delivered again.
	sent, err = ssf.writePacketRTPEncoded(testNonKeyFramePacket, time.Now(), true, []byte{0x04})
	require.NoError(t, err)
	require.True(t, sent)

	require.Equal(t, uint64(2), ssf.ssm.bytesSent.Load())
	require.Equal(t, uint64(2), ssf.rtpSender.Stats().Sent)
}

// TestServerSessionFormatNoKeyFrameSupport checks that encodings whose
// pictures can't be analyzed keep the best-effort behavior and are never
// suspended.
func TestServerSessionFormatNoKeyFrameSupport(t *testing.T) {
	proc := newTestProcessor(1)
	defer proc.Close()

	ss := newTestSession(t, proc, testAudioMedia)
	ssf := ss.testFormat(testAudioMedia, 0)
	defer ssf.rtpSender.Close()

	sent, err := ssf.writePacketRTPEncoded(testAudioPacket, time.Now(), true, []byte{0x01})
	require.NoError(t, err)
	require.True(t, sent)

	// the queue is full, but the media is not suspended.
	sent, err = ssf.writePacketRTPEncoded(testAudioPacket, time.Now(), true, []byte{0x02})
	require.Error(t, err)
	require.False(t, sent)

	sent, err = ssf.writePacketRTPEncoded(testAudioPacket, time.Now(), true, []byte{0x03})
	require.Error(t, err)
	require.False(t, sent)
}

// newTestStream returns an initialized stream that doesn't need network
// resources, since receivers are attached manually.
func newTestStream(t *testing.T, medias ...*description.Media) *ServerStream {
	t.Helper()

	stream := &ServerStream{
		Server: &Server{
			MaxPacketSize: 1472,
			timeNow:       time.Now,
			sessions:      make(map[string]*ServerSession),
		},
		Desc: &description.Session{Medias: medias},
	}
	require.NoError(t, stream.Initialize())
	t.Cleanup(stream.Close)

	return stream
}

// TestServerStreamDeliveryIsolation checks that the recovery of a reader
// doesn't affect the other readers, nor the other medias of the same reader.
func TestServerStreamDeliveryIsolation(t *testing.T) {
	stream := newTestStream(t, testVideoMedia, testAudioMedia)

	// a reader with a healthy connection
	healthyProc := newTestProcessor(16)
	defer healthyProc.Close()
	healthyProc.Start()
	healthy := newTestSession(t, healthyProc, testVideoMedia)

	// a reader whose video queue overflows after holding two packets
	slowProc := newTestProcessor(2)
	defer slowProc.Close()
	slow := newTestSession(t, slowProc, testVideoMedia)

	// a reader that only receives audio
	audioProc := newTestProcessor(16)
	defer audioProc.Close()
	audioProc.Start()
	audio := newTestSession(t, audioProc, testAudioMedia)

	stream.activeUnicastReaders[healthy] = struct{}{}
	stream.activeUnicastReaders[slow] = struct{}{}
	stream.activeUnicastReaders[audio] = struct{}{}

	sf := stream.medias[testVideoMedia].formats[96]

	// the first key frame is delivered to all video readers.
	require.NoError(t, sf.writePacketRTP(testKeyFramePacket, time.Now()))

	// this packet fills the queue of the slow reader.
	require.NoError(t, sf.writePacketRTP(testNonKeyFramePacket, time.Now()))

	// every video reader received both packets.
	require.Equal(t, uint64(2), healthy.testFormat(testVideoMedia, 96).rtpSender.Stats().Sent)
	require.Equal(t, uint64(2), slow.testFormat(testVideoMedia, 96).rtpSender.Stats().Sent)

	// this packet overflows the queue of the slow reader,
	// which gets suspended. The healthy reader is not affected.
	require.NoError(t, sf.writePacketRTP(testNonKeyFramePacket, time.Now()))

	require.Equal(t, uint64(3), healthy.testFormat(testVideoMedia, 96).rtpSender.Stats().Sent)
	require.Equal(t, uint64(2), slow.testFormat(testVideoMedia, 96).rtpSender.Stats().Sent)

	// the audio media keeps being delivered to the audio reader.
	sfAudio := stream.medias[testAudioMedia].formats[0]
	require.NoError(t, sfAudio.writePacketRTP(testAudioPacket, time.Now()))
	require.Equal(t, uint64(1), audio.testFormat(testAudioMedia, 0).rtpSender.Stats().Sent)

	// after the overflow, non-key frames reach the healthy reader only,
	// and the stream counters account for the single delivery.
	require.NoError(t, sf.writePacketRTP(testNonKeyFramePacket, time.Now()))

	require.Equal(t, uint64(4), healthy.testFormat(testVideoMedia, 96).rtpSender.Stats().Sent)
	require.Equal(t, uint64(2), slow.testFormat(testVideoMedia, 96).rtpSender.Stats().Sent)

	// stream counters are the sum of the deliveries to each receiver:
	// four to the healthy reader and two to the suspended one.
	require.Equal(t, uint64(6),
		stream.Stats().Medias[testVideoMedia].Formats[testVideoMedia.Formats[0]].OutboundRTPPackets)

	// the slow reader drains its queue and resumes at the key frame,
	// while the others are unaffected.
	slowProc.Start()

	sent := false
	require.Eventually(t, func() bool {
		var err error
		sent, err = slow.testFormat(testVideoMedia, 96).writePacketRTPEncoded(
			testKeyFramePacket, time.Now(), true, []byte{0x10})
		return err == nil && sent
	}, 2*time.Second, time.Millisecond)

	require.NoError(t, sf.writePacketRTP(testKeyFramePacket, time.Now()))

	require.Eventually(t, func() bool {
		return slow.testFormat(testVideoMedia, 96).rtpSender.Stats().Sent == 4
	}, 2*time.Second, time.Millisecond)
	require.Equal(t, uint64(5), healthy.testFormat(testVideoMedia, 96).rtpSender.Stats().Sent)

	// the RTP-Info of a new reader always refers to the next packet of the
	// stream, that is delivered without limitations.
	require.NotNil(t, sf.rtpInfoEntry(time.Now()))
}

// TestServerMulticastWriterFormatQueueOverflow checks that a queue overflow
// suspends the media without interrupting the stream, and without reporting
// errors or counting refused packets.
func TestServerMulticastWriterFormatQueueOverflow(t *testing.T) {
	proc := newTestProcessor(1)
	defer proc.Close()

	smf := newTestMulticastWriterFormat(t, proc, testVideoMedia.Formats[0])
	defer smf.close()

	require.True(t, smf.writePacketRTPEncoded(testNonKeyFramePacket, time.Now(), true, []byte{0x01}))

	// the queue is full: the packet is refused, but no error is reported,
	// since overflows must not interrupt the stream.
	require.False(t, smf.writePacketRTPEncoded(testNonKeyFramePacket, time.Now(), true, []byte{0x02}))

	// while suspended, non-key frames are skipped.
	require.False(t, smf.writePacketRTPEncoded(testNonKeyFramePacket, time.Now(), true, []byte{0x03}))

	// statistics and sender reports only account for packets that entered the queue.
	require.Equal(t, uint64(1), smf.rtpSender.Stats().Sent)
}

// TestServerMulticastWriterFormatRecovery checks that, after a suspension, the
// multicast delivery restarts from the first packet of an independently
// decodable picture.
func TestServerMulticastWriterFormatRecovery(t *testing.T) {
	proc := newTestProcessor(16)
	defer proc.Close()
	proc.Start()

	smf := newTestMulticastWriterFormat(t, proc, testVideoMedia.Formats[0])
	defer smf.close()

	// simulate a suspension caused by a previous overflow.
	smf.deliveryGate.onQueueFull()

	require.False(t, smf.writePacketRTPEncoded(testNonKeyFramePacket, time.Now(), true, []byte{0x01}))

	// no packet has been sent yet: no sender report has been generated either.
	require.Nil(t, smf.rtpSender.Stats())

	require.True(t, smf.writePacketRTPEncoded(testKeyFramePacket, time.Now(), true, []byte{0x02}))
	require.True(t, smf.writePacketRTPEncoded(testNonKeyFramePacket, time.Now(), true, []byte{0x03}))

	require.Equal(t, uint64(2), smf.rtpSender.Stats().Sent)
}

// TestServerStreamFormatMediaSentCounters checks that the counters of a stream
// only account for packets that entered the send queues of the receivers.
func TestServerStreamFormatMediaSentCounters(t *testing.T) {
	stream := newTestStream(t, testVideoMedia)

	sf := stream.medias[testVideoMedia].formats[96]

	// without receivers the packet is not delivered and not counted,
	// while the stream position is still tracked for RTP-Info.
	err := sf.writePacketRTP(testKeyFramePacket, time.Now())
	require.NoError(t, err)
	require.NotNil(t, sf.rtpInfoEntry(time.Now()))

	stats := stream.medias[testVideoMedia].stats()
	require.Equal(t, uint64(0), stats.OutboundBytes)
	require.Equal(t, uint64(0), stats.Formats[testVideoMedia.Formats[0]].OutboundRTPPackets)
}
