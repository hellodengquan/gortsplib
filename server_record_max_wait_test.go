package gortsplib

import (
	"bufio"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/pion/rtp"
	"github.com/stretchr/testify/require"

	"github.com/bluenviron/gortsplib/v5/pkg/base"
	"github.com/bluenviron/gortsplib/v5/pkg/conn"
	"github.com/bluenviron/gortsplib/v5/pkg/description"
	"github.com/bluenviron/gortsplib/v5/pkg/format"
	"github.com/bluenviron/gortsplib/v5/pkg/headers"
)

// TestServerRecordMaxPacketWait verifies that, on the server side receiving a
// pushed UDP stream, a missing packet does not stall delivery forever: the
// buffered packets are released after MaxPacketWait, the gap is counted as
// lost, and late packets are discarded.
func TestServerRecordMaxPacketWait(t *testing.T) {
	var (
		mu        sync.Mutex
		recvSeqs  []uint16
		totalLost uint64
	)
	lossRecv := make(chan struct{}, 16)
	flushRecv := make(chan struct{})

	s := &Server{
		MaxPacketWait: 300 * time.Millisecond,
		Handler: &testServerHandler{
			onAnnounce: func(_ *ServerHandlerOnAnnounceCtx) (*base.Response, error) {
				return &base.Response{StatusCode: base.StatusOK}, nil
			},
			onSetup: func(_ *ServerHandlerOnSetupCtx) (*base.Response, *ServerStream, error) {
				return &base.Response{StatusCode: base.StatusOK}, nil, nil
			},
			onRecord: func(ctx *ServerHandlerOnRecordCtx) (*base.Response, error) {
				ctx.Session.OnPacketRTPAny(func(_ *description.Media, _ format.Format, pkt *rtp.Packet) {
					mu.Lock()
					recvSeqs = append(recvSeqs, pkt.SequenceNumber)
					if pkt.SequenceNumber == 536 {
						close(flushRecv)
					}
					mu.Unlock()
				})
				return &base.Response{StatusCode: base.StatusOK}, nil
			},
			onGetParameter: func(_ *ServerHandlerOnGetParameterCtx) (*base.Response, error) {
				return &base.Response{StatusCode: base.StatusOK}, nil
			},
			onPacketsLost: func(ctx *ServerHandlerOnPacketsLostCtx) {
				mu.Lock()
				totalLost += ctx.Lost
				mu.Unlock()
				lossRecv <- struct{}{}
			},
		},
		UDPRTPAddress:  "127.0.0.1:8000",
		UDPRTCPAddress: "127.0.0.1:8001",
		RTSPAddress:    "localhost:8554",
	}

	require.NoError(t, s.Start())
	defer s.Close()

	nconn, err := net.Dial("tcp", "localhost:8554")
	require.NoError(t, err)
	defer nconn.Close()
	rtspConn := conn.NewConn(bufio.NewReader(nconn), nconn)

	medias := []*description.Media{{
		Type: description.MediaTypeApplication,
		Formats: []format.Format{&format.Generic{
			PayloadTyp: 97,
			RTPMa:      "private/90000",
		}},
	}}

	doAnnounce(t, rtspConn, "rtsp://localhost:8554/teststream", medias)

	l1, err := net.ListenPacket("udp", "127.0.0.1:35466")
	require.NoError(t, err)
	defer l1.Close()

	inTH := &headers.Transport{
		Delivery:    new(headers.TransportDeliveryUnicast),
		Mode:        new(headers.TransportModeRecord),
		Protocol:    headers.TransportProtocolUDP,
		ClientPorts: &[2]int{35466, 35467},
	}

	res, th := doSetup(t, rtspConn,
		"rtsp://localhost:8554/teststream/"+medias[0].Control, inTH, "")
	session := readSession(t, res)
	doRecord(t, rtspConn, "rtsp://localhost:8554/teststream", session)

	writeRTP := func(seq uint16) {
		_, err = l1.WriteTo(mustMarshalPacketRTP(&rtp.Packet{
			Header: rtp.Header{
				Version:        2,
				Marker:         true,
				PayloadType:    97,
				SequenceNumber: seq,
				SSRC:           753621,
			},
			Payload: []byte{1, 2, 3, 4},
		}), &net.UDPAddr{
			IP:   net.ParseIP("127.0.0.1"),
			Port: th.ServerPorts[0],
		})
		require.NoError(t, err)
	}

	// 534 is delivered immediately; 535 is missing and 536 is buffered.
	writeRTP(534)
	writeRTP(536)

	// without any further packet, the timer must release 536 and report 535
	// as lost well before the buffer would fill.
	select {
	case <-flushRecv:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for timer-based delivery")
	}

	select {
	case <-lossRecv:
	case <-time.After(1 * time.Second):
		t.Fatal("timed out waiting for loss notification")
	}

	// the missing packet arrives late: it must be discarded.
	writeRTP(535)

	// traffic keeps flowing: 537 is contiguous after 536 and delivered now.
	writeRTP(537)

	require.Eventually(t, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(recvSeqs) == 3
	}, 2*time.Second, 50*time.Millisecond)

	mu.Lock()
	require.Equal(t, []uint16{534, 536, 537}, recvSeqs)
	require.Equal(t, uint64(1), totalLost) // only 535, never counted twice
	mu.Unlock()
}
