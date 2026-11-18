package gortsplib

import (
	"bufio"
	"net"
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

// rtpInfoTestServer runs a minimal RTSP server that serves a single H264
// media. The behavior of the second PLAY response and of the packets sent
// after it is controlled by the parameters. It returns a function that waits
// for the server to terminate.
func rtpInfoTestServer(
	t *testing.T,
	transport string,
	secondPlayHeader base.Header,
	packetsAfterSecondPlay func(write func(*rtp.Packet)),
) func() {
	t.Helper()

	l, err := net.Listen("tcp", "localhost:8554")
	require.NoError(t, err)

	serverDone := make(chan struct{})

	go func() {
		defer close(serverDone)

		nconn, err2 := l.Accept()
		require.NoError(t, err2)
		defer nconn.Close()
		connection := conn.NewConn(bufio.NewReader(nconn), nconn)

		req, err2 := connection.ReadRequest()
		require.NoError(t, err2)
		require.Equal(t, base.Options, req.Method)

		err2 = connection.WriteResponse(&base.Response{
			StatusCode: base.StatusOK,
			Header: base.Header{
				"Public": base.HeaderValue{"DESCRIBE, SETUP, PLAY, PAUSE, TEARDOWN"},
			},
		})
		require.NoError(t, err2)

		req, err2 = connection.ReadRequest()
		require.NoError(t, err2)
		require.Equal(t, base.Describe, req.Method)

		medias := []*description.Media{testH264Media}

		err2 = connection.WriteResponse(&base.Response{
			StatusCode: base.StatusOK,
			Header: base.Header{
				"Content-Type": base.HeaderValue{"application/sdp"},
				"Content-Base": base.HeaderValue{"rtsp://localhost:8554/teststream/"},
			},
			Body: mediasToSDP(medias),
		})
		require.NoError(t, err2)

		req, err2 = connection.ReadRequest()
		require.NoError(t, err2)
		require.Equal(t, base.Setup, req.Method)

		var inTH headers.Transport
		err2 = inTH.Unmarshal(req.Header["Transport"])
		require.NoError(t, err2)

		th := headers.Transport{
			Delivery: new(headers.TransportDeliveryUnicast),
		}

		var udpConn net.PacketConn
		if transport == "udp" {
			th.Protocol = headers.TransportProtocolUDP
			th.ServerPorts = &[2]int{34556, 34557}
			th.ClientPorts = inTH.ClientPorts

			udpConn, err2 = net.ListenPacket("udp", "localhost:34556")
			require.NoError(t, err2)
			defer udpConn.Close()
		} else {
			th.Protocol = headers.TransportProtocolTCP
			th.InterleavedIDs = inTH.InterleavedIDs
		}

		err2 = connection.WriteResponse(&base.Response{
			StatusCode: base.StatusOK,
			Header: base.Header{
				"Transport": th.Marshal(),
			},
		})
		require.NoError(t, err2)

		writePacket := func(pkt *rtp.Packet) {
			raw := mustMarshalPacketRTP(pkt)
			if transport == "udp" {
				_, err2 = udpConn.WriteTo(raw, &net.UDPAddr{
					IP:   net.ParseIP("127.0.0.1"),
					Port: inTH.ClientPorts[0],
				})
				require.NoError(t, err2)
			} else {
				err2 = connection.WriteInterleavedFrame(&base.InterleavedFrame{
					Channel: 0,
					Payload: raw,
				}, make([]byte, 1024))
				require.NoError(t, err2)
			}
		}

		// first PLAY: no RTP-Info, keeps the legacy behavior
		req, err2 = connection.ReadRequest()
		require.NoError(t, err2)
		require.Equal(t, base.Play, req.Method)

		err2 = connection.WriteResponse(&base.Response{
			StatusCode: base.StatusOK,
		})
		require.NoError(t, err2)

		// packets of the first position: seq 100..104
		for sn := uint16(100); sn <= 104; sn++ {
			writePacket(&rtp.Packet{
				Header: rtp.Header{
					Version:        2,
					PayloadType:    96,
					SSRC:           0x38F27A2F,
					SequenceNumber: sn,
					Timestamp:      90000 * uint32(sn-100),
				},
				Payload: []byte{5, 2, 3, 4},
			})
		}

		req, err2 = connection.ReadRequest()
		require.NoError(t, err2)
		require.Equal(t, base.Pause, req.Method)

		err2 = connection.WriteResponse(&base.Response{
			StatusCode: base.StatusOK,
		})
		require.NoError(t, err2)

		// second PLAY: the server declares a new, non-contiguous starting point
		req, err2 = connection.ReadRequest()
		require.NoError(t, err2)
		require.Equal(t, base.Play, req.Method)

		err2 = connection.WriteResponse(&base.Response{
			StatusCode: base.StatusOK,
			Header:     secondPlayHeader,
		})
		require.NoError(t, err2)

		packetsAfterSecondPlay(writePacket)

		req, err2 = connection.ReadRequest()
		require.NoError(t, err2)
		require.Equal(t, base.Teardown, req.Method)

		err2 = connection.WriteResponse(&base.Response{
			StatusCode: base.StatusOK,
		})
		require.NoError(t, err2)
	}()

	return func() {
		<-serverDone
		l.Close()
	}
}

func newRTPInfoRestartHeader() base.Header {
	const (
		newSeq     = uint16(500)
		newRTPTime = uint32(600000)
		newNPT     = 10 * time.Second
	)

	return base.Header{
		"Range": headers.Range{
			Value: &headers.RangeNPT{Start: newNPT},
		}.Marshal(),
		"RTP-Info": headers.RTPInfo{
			{
				URL:            "rtsp://localhost:8554/teststream/trackID=0",
				SequenceNumber: ptrUint16(newSeq),
				Timestamp:      ptrUint32(newRTPTime),
			},
		}.Marshal(),
	}
}

// TestClientPlayRTPInfoRestart verifies that, after a pause / new PLAY cycle,
// the client restarts reception from the starting point declared by the server
// in the PLAY response (Range + RTP-Info):
//   - the first packet of the new position is delivered,
//   - the sequence discontinuity does not produce fake lost packets,
//   - late packets of the previous position are isolated,
//   - packets can be located on the program timeline through PacketPTS2.
func TestClientPlayRTPInfoRestart(t *testing.T) {
	for _, transport := range []string{"tcp", "udp"} {
		t.Run(transport, func(t *testing.T) {
			const (
				newSeq     = uint16(500)
				newRTPTime = uint32(600000)
			)

			secondPlayPackets := make(chan *rtp.Packet, 16)

			waitServer := rtpInfoTestServer(t, transport, newRTPInfoRestartHeader(),
				func(write func(*rtp.Packet)) {
					// a late packet of the previous position: it must be isolated
					write(&rtp.Packet{
						Header: rtp.Header{
							Version:        2,
							PayloadType:    96,
							SSRC:           0x38F27A2F,
							SequenceNumber: 105,
							Timestamp:      90000 * 5,
						},
						Payload: []byte{5, 2, 3, 4},
					})

					// packets of the new position: seq 500..503
					for i := uint32(0); i < 4; i++ {
						pkt := &rtp.Packet{
							Header: rtp.Header{
								Version:        2,
								PayloadType:    96,
								SSRC:           0x38F27A2F,
								SequenceNumber: newSeq + uint16(i),
								Timestamp:      newRTPTime + 90000*i,
							},
							Payload: []byte{5, 2, 3, 4},
						}
						write(pkt)
						secondPlayPackets <- pkt
					}
				})
			defer waitServer()

			var lostCount uint64
			var receivedSeqs []uint16
			packetRecv := make(chan struct{}, 32)

			c := Client{
				Protocol: protocolPtr(transportProtocol(transport)),
				OnPacketsLost: func(lost uint64) {
					lostCount += lost
				},
			}

			err := readAll(&c, "rtsp://localhost:8554/teststream",
				func(_ *description.Media, _ format.Format, pkt *rtp.Packet) {
					receivedSeqs = append(receivedSeqs, pkt.SequenceNumber)
					packetRecv <- struct{}{}
				})
			require.NoError(t, err)
			defer c.Close()

			// wait for the five packets of the first position
			for i := 0; i < 5; i++ {
				select {
				case <-packetRecv:
				case <-time.After(2 * time.Second):
					t.Fatal("timeout waiting for first position packets")
				}
			}

			_, err = c.Pause()
			require.NoError(t, err)

			_, err = c.Play(nil)
			require.NoError(t, err)

			// wait for the four packets of the new position
			gotNewPosition := 0
			deadline := time.After(2 * time.Second)
			for gotNewPosition < 4 {
				select {
				case <-packetRecv:
					gotNewPosition++
				case <-deadline:
					t.Fatal("timeout waiting for new position packets")
				}
			}

			// every packet of the new position is delivered, starting from the
			// declared sequence number; the late packet (105) has been isolated
			require.Equal(t, []uint16{100, 101, 102, 103, 104, 500, 501, 502, 503},
				receivedSeqs)

			// the seek itself did not produce fake lost packets
			require.Equal(t, uint64(0), lostCount)

			// the packets can be located on the program timeline
			medi := c.lastDescribeDesc.Medias[0]
			firstPkt := <-secondPlayPackets
			pts, ok := c.PacketPTS2(medi, firstPkt)
			require.True(t, ok)
			// NPT 10s on a 90kHz clock
			require.Equal(t, int64(900000), pts)
		})
	}
}

// TestClientPlayRTPInfoPartial verifies that partial server information keeps
// working: a RTP-Info without rtptime still restarts reception, while the
// timeline PTS simply remains unavailable.
func TestClientPlayRTPInfoPartial(t *testing.T) {
	for _, transport := range []string{"tcp", "udp"} {
		t.Run(transport, func(t *testing.T) {
			waitServer := rtpInfoTestServer(t, transport, base.Header{
				"RTP-Info": headers.RTPInfo{
					{
						URL:            "rtsp://localhost:8554/teststream/trackID=0",
						SequenceNumber: ptrUint16(800),
					},
				}.Marshal(),
			}, func(write func(*rtp.Packet)) {
				write(&rtp.Packet{
					Header: rtp.Header{
						Version:        2,
						PayloadType:    96,
						SSRC:           0x38F27A2F,
						SequenceNumber: 800,
						Timestamp:      90000 * 800,
					},
					Payload: []byte{5, 2, 3, 4},
				})
			})
			defer waitServer()

			var lostCount uint64
			packetRecv := make(chan struct{}, 16)

			c := Client{
				Protocol: protocolPtr(transportProtocol(transport)),
				OnPacketsLost: func(lost uint64) {
					lostCount += lost
				},
			}

			err := readAll(&c, "rtsp://localhost:8554/teststream",
				func(_ *description.Media, _ format.Format, _ *rtp.Packet) {
					packetRecv <- struct{}{}
				})
			require.NoError(t, err)
			defer c.Close()

			select {
			case <-packetRecv:
			case <-time.After(2 * time.Second):
				t.Fatal("timeout waiting for first packet")
			}

			_, err = c.Pause()
			require.NoError(t, err)

			_, err = c.Play(nil)
			require.NoError(t, err)

			select {
			case <-packetRecv:
			case <-time.After(2 * time.Second):
				t.Fatal("timeout waiting for packet after restart")
			}

			require.Equal(t, uint64(0), lostCount)

			// timeline PTS is not available since rtptime was not provided
			medi := c.lastDescribeDesc.Medias[0]
			_, ok := c.PacketPTS2(medi, &rtp.Packet{Header: rtp.Header{PayloadType: 96}})
			require.False(t, ok)
		})
	}
}

func transportProtocol(transport string) Protocol {
	if transport == "udp" {
		return ProtocolUDP
	}
	return ProtocolTCP
}

func ptrUint16(v uint16) *uint16 {
	return &v
}

func ptrUint32(v uint32) *uint32 {
	return &v
}

func protocolPtr(p Protocol) *Protocol {
	return &p
}
