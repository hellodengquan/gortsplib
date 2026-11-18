package gortsplib

import (
	"sync/atomic"

	"github.com/bluenviron/mediacommon/v2/pkg/codecs/h264"
	"github.com/bluenviron/mediacommon/v2/pkg/codecs/h265"
	"github.com/pion/rtp"
	"github.com/pion/rtp/codecs"

	"github.com/bluenviron/gortsplib/v5/pkg/format"
)

// mediaDeliveryGate suspends the delivery of a media to a single receiver
// after its send queue overflowed, until a picture that can be decoded
// independently from the previous ones is available.
// It is used independently for every receiver and every media, so that the
// recovery of one receiver doesn't affect the others, nor the other medias.
type mediaDeliveryGate struct {
	format           format.Format
	supportsKeyFrame bool

	waitingKeyFrame atomic.Bool
}

func (g *mediaDeliveryGate) initialize(forma format.Format) {
	g.format = forma
	g.supportsKeyFrame = keyFrameRecoverySupported(forma)
}

// isWritable reports whether a packet can be delivered.
func (g *mediaDeliveryGate) isWritable(pkt *rtp.Packet) bool {
	// encodings that can't be analyzed are always delivered.
	if !g.supportsKeyFrame {
		return true
	}

	// fast path: the queue never overflowed.
	if !g.waitingKeyFrame.Load() {
		return true
	}

	// resume from the first packet of an independently decodable picture.
	if !isKeyFramePacket(g.format, pkt) {
		return false
	}

	// only one packet can restart the delivery.
	return g.waitingKeyFrame.CompareAndSwap(true, false)
}

// onQueueFull suspends the delivery until the next independently decodable picture.
func (g *mediaDeliveryGate) onQueueFull() {
	if !g.supportsKeyFrame {
		return
	}

	g.waitingKeyFrame.Store(true)
}

// keyFrameRecoverySupported reports whether the encoding of a format allows
// to identify the first packet of a picture that can be decoded independently
// from the previous ones.
// Encodings that don't provide such an information (audio, MJPEG, ...) are
// always delivered in a best-effort way, without waiting for a recovery point.
func keyFrameRecoverySupported(forma format.Format) bool {
	switch forma.(type) {
	case *format.H264, *format.H265, *format.VP8, *format.VP9:
		return true
	}
	return false
}

// isKeyFramePacket reports whether an RTP packet is the first packet of a picture
// that can be decoded independently from the previous ones.
func isKeyFramePacket(forma format.Format, pkt *rtp.Packet) bool {
	switch forma.(type) {
	case *format.H264:
		return h264PacketStartsKeyFrame(pkt.Payload)

	case *format.H265:
		return h265PacketStartsKeyFrame(pkt.Payload)

	case *format.VP8:
		return vp8PacketStartsKeyFrame(pkt.Payload)

	case *format.VP9:
		return vp9PacketStartsKeyFrame(pkt.Payload)
	}

	return false
}

// h264PacketStartsKeyFrame implements RFC6184.
func h264PacketStartsKeyFrame(payload []byte) bool {
	if len(payload) == 0 {
		return false
	}

	typ := h264.NALUType(payload[0] & 0x1F)

	switch typ {
	case h264.NALUTypeIDR:
		return true

	case h264.NALUTypeSTAPA, h264.NALUTypeSTAPB:
		// STAP-B packets contain a 2-byte decoding order number after the header.
		if typ == h264.NALUTypeSTAPB {
			if len(payload) < 3 {
				return false
			}
			payload = payload[3:]
		} else {
			payload = payload[1:]
		}

		for len(payload) >= 2 {
			size := int(payload[0])<<8 | int(payload[1])
			payload = payload[2:]

			if size == 0 || size > len(payload) {
				return false
			}

			if h264.NALUType(payload[0]&0x1F) == h264.NALUTypeIDR {
				return true
			}

			payload = payload[size:]
		}

		return false

	case h264.NALUTypeFUA:
		// the start bit marks the first fragment of a NALU, that is the first
		// packet of the picture when the NALU is an IDR one.
		return len(payload) >= 2 &&
			(payload[1]&0x80) != 0 &&
			h264.NALUType(payload[1]&0x1F) == h264.NALUTypeIDR

	case h264.NALUTypeFUB:
		// FU-B packets contain a 2-byte decoding order number before the fragment header.
		return len(payload) >= 4 &&
			(payload[3]&0x80) != 0 &&
			h264.NALUType(payload[3]&0x1F) == h264.NALUTypeIDR

	case h264.NALUTypeMTAP16, h264.NALUTypeMTAP24:
		// multi-time aggregation packets contain a 2-byte decoding order number
		// and, for each NALU, a timestamp offset whose size depends on the type.
		if len(payload) < 3 {
			return false
		}

		tsOffsetSize := 2
		if typ == h264.NALUTypeMTAP24 {
			tsOffsetSize = 3
		}

		payload = payload[3:]

		for len(payload) >= 2 {
			size := int(payload[0])<<8 | int(payload[1])
			payload = payload[2:]

			// skip the decoding order number and the timestamp offset.
			headerSize := 2 + tsOffsetSize
			if size <= headerSize || size > len(payload) {
				return false
			}

			if h264.NALUType(payload[headerSize]&0x1F) == h264.NALUTypeIDR {
				return true
			}

			payload = payload[size:]
		}

		return false
	}

	return false
}

func h265NALUTypeIsRandomAccess(typ h265.NALUType) bool {
	// BLA, IDR, CRA and reserved IRAP pictures can be decoded independently
	// from the previous ones.
	return typ >= h265.NALUType_BLA_W_LP && typ <= h265.NALUType_RSV_IRAP_VCL23
}

// h265PacketStartsKeyFrame implements RFC7798.
func h265PacketStartsKeyFrame(payload []byte) bool {
	if len(payload) == 0 {
		return false
	}

	typ := h265.NALUType((payload[0] >> 1) & 0x3F)

	switch typ {
	case h265.NALUType_AggregationUnit:
		// skip the 2-byte payload header
		if len(payload) < 4 {
			return false
		}
		payload = payload[2:]

		for len(payload) >= 2 {
			size := int(payload[0])<<8 | int(payload[1])
			payload = payload[2:]

			if size == 0 || size > len(payload) {
				return false
			}

			if h265NALUTypeIsRandomAccess(h265.NALUType((payload[0] >> 1) & 0x3F)) {
				return true
			}

			payload = payload[size:]
		}

		return false

	case h265.NALUType_FragmentationUnit:
		// skip the 2-byte payload header.
		// the start bit marks the first fragment of a NALU, that is the first
		// packet of the picture when the NALU is a random access one.
		return len(payload) >= 3 &&
			(payload[2]&0x80) != 0 &&
			h265NALUTypeIsRandomAccess(h265.NALUType(payload[2]&0x3F))
	}

	return h265NALUTypeIsRandomAccess(typ)
}

// vp8PacketStartsKeyFrame implements RFC7741.
func vp8PacketStartsKeyFrame(payload []byte) bool {
	var vpkt codecs.VP8Packet
	_, err := vpkt.Unmarshal(payload)
	if err != nil {
		return false
	}

	// only the first packet of the first partition contains the frame header.
	if vpkt.S != 1 || vpkt.PID != 0 || len(vpkt.Payload) == 0 {
		return false
	}

	// the least significant bit of the frame tag is the interframe predicted flag.
	return (vpkt.Payload[0] & 0x01) == 0
}

// vp9PacketStartsKeyFrame implements RFC9628.
func vp9PacketStartsKeyFrame(payload []byte) bool {
	var vpkt codecs.VP9Packet
	_, err := vpkt.Unmarshal(payload)
	if err != nil {
		return false
	}

	// B marks the first packet of a frame, P marks an inter-picture predicted frame.
	return vpkt.B && !vpkt.P
}
