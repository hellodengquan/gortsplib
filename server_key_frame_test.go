package gortsplib

import (
	"testing"

	"github.com/pion/rtp"
	"github.com/stretchr/testify/require"

	"github.com/bluenviron/gortsplib/v5/pkg/format"
)

func TestKeyFrameRecoverySupported(t *testing.T) {
	for _, ca := range []struct {
		name     string
		forma    format.Format
		supports bool
	}{
		{
			"h264",
			&format.H264{PayloadTyp: 96, PacketizationMode: 1},
			true,
		},
		{
			"h265",
			&format.H265{PayloadTyp: 96},
			true,
		},
		{
			"vp8",
			&format.VP8{PayloadTyp: 96},
			true,
		},
		{
			"vp9",
			&format.VP9{PayloadTyp: 96},
			true,
		},
		{
			"g711",
			&format.G711{PayloadTyp: 0, MULaw: true, SampleRate: 8000, ChannelCount: 1},
			false,
		},
		{
			"mpeg4audio",
			&format.MPEG4Audio{PayloadTyp: 96},
			false,
		},
	} {
		t.Run(ca.name, func(t *testing.T) {
			require.Equal(t, ca.supports, keyFrameRecoverySupported(ca.forma))
		})
	}
}

func TestH264PacketStartsKeyFrame(t *testing.T) {
	for _, ca := range []struct {
		name    string
		payload []byte
		key     bool
	}{
		{
			"empty",
			[]byte{},
			false,
		},
		{
			"single IDR",
			[]byte{0x65, 0x01, 0x02},
			true,
		},
		{
			"single non-IDR",
			[]byte{0x41, 0x01, 0x02},
			false,
		},
		{
			"single SPS",
			[]byte{0x67, 0x01, 0x02},
			false,
		},
		{
			"STAP-A with IDR",
			[]byte{0x78, 0x00, 0x02, 0x65, 0x11},
			true,
		},
		{
			"STAP-A without IDR",
			[]byte{0x78, 0x00, 0x02, 0x67, 0x11},
			false,
		},
		{
			"STAP-A truncated",
			[]byte{0x78, 0x00, 0x05, 0x65},
			false,
		},
		{
			"FU-A start of IDR",
			[]byte{0x7C, 0x85, 0x01},
			true,
		},
		{
			"FU-A continuation of IDR",
			[]byte{0x7C, 0x05, 0x01},
			false,
		},
		{
			"FU-A start of non-IDR",
			[]byte{0x7C, 0x81, 0x01},
			false,
		},
		{
			"FU-B start of IDR",
			[]byte{0x7D, 0x00, 0x01, 0x85},
			true,
		},
	} {
		t.Run(ca.name, func(t *testing.T) {
			require.Equal(t, ca.key, h264PacketStartsKeyFrame(ca.payload))
		})
	}
}

func TestH265PacketStartsKeyFrame(t *testing.T) {
	for _, ca := range []struct {
		name    string
		payload []byte
		key     bool
	}{
		{
			"empty",
			[]byte{},
			false,
		},
		{
			"IDR_W_RADL",
			[]byte{0x26, 0x01, 0x00},
			true,
		},
		{
			"IDR_N_LP",
			[]byte{0x28, 0x01, 0x00},
			true,
		},
		{
			"CRA",
			[]byte{0x2A, 0x01, 0x00},
			true,
		},
		{
			"VPS",
			[]byte{0x40, 0x01, 0x00},
			false,
		},
		{
			"TRAIL_R",
			[]byte{0x02, 0x01, 0x00},
			false,
		},
		{
			"AP with IDR",
			[]byte{0x60, 0x01, 0x00, 0x02, 0x26, 0x01},
			true,
		},
		{
			"AP without IDR",
			[]byte{0x60, 0x01, 0x00, 0x02, 0x02, 0x01},
			false,
		},
		{
			"AP truncated",
			[]byte{0x60, 0x01, 0x00, 0x05, 0x26},
			false,
		},
		{
			"FU start of IDR",
			[]byte{0x62, 0x01, 0x93, 0x01},
			true,
		},
		{
			"FU continuation of IDR",
			[]byte{0x62, 0x01, 0x13, 0x01},
			false,
		},
		{
			"FU start of non-IDR",
			[]byte{0x62, 0x01, 0x82, 0x01},
			false,
		},
	} {
		t.Run(ca.name, func(t *testing.T) {
			require.Equal(t, ca.key, h265PacketStartsKeyFrame(ca.payload))
		})
	}
}

func TestVP8PacketStartsKeyFrame(t *testing.T) {
	for _, ca := range []struct {
		name    string
		payload []byte
		key     bool
	}{
		{
			"empty",
			[]byte{},
			false,
		},
		{
			"key frame",
			[]byte{0x10, 0x00, 0x00},
			true,
		},
		{
			"inter frame",
			[]byte{0x10, 0x01, 0x00},
			false,
		},
		{
			"continuation of key frame",
			[]byte{0x00, 0x00},
			false,
		},
		{
			"non-first partition",
			[]byte{0x11, 0x00},
			false,
		},
	} {
		t.Run(ca.name, func(t *testing.T) {
			require.Equal(t, ca.key, vp8PacketStartsKeyFrame(ca.payload))
		})
	}
}

func TestVP9PacketStartsKeyFrame(t *testing.T) {
	for _, ca := range []struct {
		name    string
		payload []byte
		key     bool
	}{
		{
			"empty",
			[]byte{},
			false,
		},
		{
			"key frame",
			[]byte{0x08, 0x00},
			true,
		},
		{
			"inter frame",
			[]byte{0x48, 0x00},
			false,
		},
		{
			"continuation of key frame",
			[]byte{0x00, 0x00},
			false,
		},
	} {
		t.Run(ca.name, func(t *testing.T) {
			require.Equal(t, ca.key, vp9PacketStartsKeyFrame(ca.payload))
		})
	}
}

func TestIsKeyFramePacket(t *testing.T) {
	h264Forma := &format.H264{PayloadTyp: 96, PacketizationMode: 1}
	require.True(t, isKeyFramePacket(h264Forma, &rtp.Packet{Payload: []byte{0x65}}))
	require.False(t, isKeyFramePacket(h264Forma, &rtp.Packet{Payload: []byte{0x41}}))

	// formats without a key frame concept are never reported as key frames,
	// but they are never suspended either.
	g711Forma := &format.G711{PayloadTyp: 0, MULaw: true, SampleRate: 8000, ChannelCount: 1}
	require.False(t, isKeyFramePacket(g711Forma, &rtp.Packet{Payload: []byte{0x01}}))
}

func TestMediaDeliveryGate(t *testing.T) {
	h264Forma := &format.H264{PayloadTyp: 96, PacketizationMode: 1}

	nonKey := &rtp.Packet{Payload: []byte{0x41, 0x01}}
	key := &rtp.Packet{Payload: []byte{0x65, 0x01}}
	keyContinuation := &rtp.Packet{Payload: []byte{0x7C, 0x05, 0x01}}
	keyFragment := &rtp.Packet{Payload: []byte{0x7C, 0x85, 0x01}}

	t.Run("no limitation when the queue never overflows", func(t *testing.T) {
		var g mediaDeliveryGate
		g.initialize(h264Forma)

		require.True(t, g.isWritable(nonKey))
		require.True(t, g.isWritable(nonKey))
		require.True(t, g.isWritable(key))
	})

	t.Run("suspension and recovery", func(t *testing.T) {
		var g mediaDeliveryGate
		g.initialize(h264Forma)

		require.True(t, g.isWritable(nonKey))

		g.onQueueFull()

		// non-key frames are discarded.
		require.False(t, g.isWritable(nonKey))
		require.False(t, g.isWritable(nonKey))

		// the recovery starts from the first packet of an IDR picture.
		require.True(t, g.isWritable(keyFragment))

		// packets of the same picture and of the following ones are delivered.
		require.True(t, g.isWritable(keyContinuation))
		require.True(t, g.isWritable(nonKey))
	})

	t.Run("single NALU key frame", func(t *testing.T) {
		var g mediaDeliveryGate
		g.initialize(h264Forma)

		g.onQueueFull()

		require.False(t, g.isWritable(nonKey))
		require.True(t, g.isWritable(key))
	})

	t.Run("format without key frames", func(t *testing.T) {
		var g mediaDeliveryGate
		g.initialize(&format.G711{PayloadTyp: 0, MULaw: true, SampleRate: 8000, ChannelCount: 1})

		g.onQueueFull()

		// delivery is never suspended.
		require.True(t, g.isWritable(&rtp.Packet{Payload: []byte{0x01}}))
		require.True(t, g.isWritable(&rtp.Packet{Payload: []byte{0x02}}))
	})
}
