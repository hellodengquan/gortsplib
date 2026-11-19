package gortsplib

import (
	"bufio"
	"net"
	"strings"
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

// TestClientPlayMaxPacketWait verifies that, on the play client side over UDP,
// a missing packet does not stall delivery forever: buffered packets are
// released after MaxPacketWait through the regular OnPacketRTP callback, the
// gap is reported through OnPacketsLost, and late packets are discarded.
func TestClientPlayMaxPacketWait(t *testing.T) {
	var (
		mu       sync.Mutex
		recvSeqs []uint16
	)
	lossRecv := make(chan uint64, 16)
	flushRecv := make(chan struct{})

	l, err := net.Listen("tcp", "localhost:8554")
	require.NoError(t, err)
	defer l.Close()

	serverDone := make(chan struct{})
	defer func() { <-serverDone }()

	go func() {
		defer close(serverDone)

		nconn, err2 := l.Accept()
		require.NoError(t, err2)
		defer nconn.Close()
		rtspConn := conn.NewConn(bufio.NewReader(nconn), nconn)

		req, err2 := rtspConn.ReadRequest()
		require.NoError(t, err2)
		require.Equal(t, base.Options, req.Method)

		require.NoError(t, rtspConn.WriteResponse(&base.Response{
			StatusCode: base.StatusOK,
			Header: base.Header{
				"Public": base.HeaderValue{strings.Join([]string{
					string(base.Describe),
					string(base.Setup),
					string(base.Play),
				}, ", ")},
			},
		}))

		req, err2 = rtspConn.ReadRequest()
		require.NoError(t, err2)
		require.Equal(t, base.Describe, req.Method)

		medias := []*description.Media{{
			Type: description.MediaTypeApplication,
			Formats: []format.Format{&format.Generic{
				PayloadTyp: 97,
				RTPMa:      "private/90000",
			}},
		}}

		require.NoError(t, rtspConn.WriteResponse(&base.Response{
			StatusCode: base.StatusOK,
			Header: base.Header{
				"Content-Type": base.HeaderValue{"application/sdp"},
				"Content-Base": base.HeaderValue{"rtsp://localhost:8554/stream/"},
			},
			Body: mediasToSDP(medias),
		}))

		req, err2 = rtspConn.ReadRequest()
		require.NoError(t, err2)
		require.Equal(t, base.Setup, req.Method)

		var inTH headers.Transport
		require.NoError(t, inTH.Unmarshal(req.Header["Transport"]))

		l1, err2 := net.ListenPacket("udp", "127.0.0.1:34556")
		require.NoError(t, err2)
		defer l1.Close()

		l2, err2 := net.ListenPacket("udp", "127.0.0.1:34557")
		require.NoError(t, err2)
		defer l2.Close()

		require.NoError(t, rtspConn.WriteResponse(&base.Response{
			StatusCode: base.StatusOK,
			Header: base.Header{
				"Transport": (&headers.Transport{
					Delivery:    new(headers.TransportDeliveryUnicast),
					Protocol:    headers.TransportProtocolUDP,
					ClientPorts: inTH.ClientPorts,
					ServerPorts: &[2]int{34556, 34557},
				}).Marshal(),
			},
		}))

		req, err2 = rtspConn.ReadRequest()
		require.NoError(t, err2)
		require.Equal(t, base.Play, req.Method)

		require.NoError(t, rtspConn.WriteResponse(&base.Response{
			StatusCode: base.StatusOK,
		}))

		writeRTP := func(seq uint16) {
			_, err2 = l1.WriteTo(mustMarshalPacketRTP(&rtp.Packet{
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
				Port: inTH.ClientPorts[0],
			})
			require.NoError(t, err2)
		}

		// 100 delivered immediately, 101 missing, 102 buffered.
		writeRTP(100)
		writeRTP(102)

		// the timer must release 102 without any further packet.
		select {
		case <-flushRecv:
		case <-time.After(2 * time.Second):
			t.Error("timed out waiting for timer-based delivery")
			return
		}

		// the missing packet arrives late and must be discarded.
		writeRTP(101)

		// traffic resumes in order.
		writeRTP(103)
	}()

	udpProto := ProtocolUDP
	c := &Client{
		Protocol:              &udpProto,
		MaxPacketWait:         300 * time.Millisecond,
		OnPacketsLost:         func(lost uint64) { lossRecv <- lost },
		ReadTimeout:           2 * time.Second,
		InitialUDPReadTimeout: 2 * time.Second,
	}

	u, err := base.ParseURL("rtsp://localhost:8554/stream")
	require.NoError(t, err)

	c.Scheme = u.Scheme
	c.Host = u.Host

	err = c.Start()
	require.NoError(t, err)
	defer c.Close()

	desc, _, err := c.Describe(u)
	require.NoError(t, err)

	_, err = c.Setup(desc.BaseURL, desc.Medias[0], 0, 0)
	require.NoError(t, err)

	forma := desc.Medias[0].Formats[0]
	c.OnPacketRTP(desc.Medias[0], forma, func(pkt *rtp.Packet) {
		mu.Lock()
		recvSeqs = append(recvSeqs, pkt.SequenceNumber)
		if pkt.SequenceNumber == 102 {
			select {
			case <-flushRecv:
			default:
				close(flushRecv)
			}
		}
		mu.Unlock()
	})

	_, err = c.Play(nil)
	require.NoError(t, err)

	select {
	case lost := <-lossRecv:
		require.Equal(t, uint64(1), lost)
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for loss notification")
	}

	require.Eventually(t, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(recvSeqs) == 3
	}, 2*time.Second, 50*time.Millisecond)

	mu.Lock()
	require.Equal(t, []uint16{100, 102, 103}, recvSeqs)
	mu.Unlock()
}
