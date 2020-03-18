package udp

import (
	"github.com/google/gopacket"
	"github.com/google/gopacket/layers"
	"net"
)

// imported from https://github.com/dimalinux/spoofsourceip

type UdpFrameOptions struct {
	SourceIP, DestIP     net.IP
	SourcePort, DestPort uint16
	SourceMac, DestMac   net.HardwareAddr
	IsIPv6               bool
	PayloadBytes         []byte
}

type serializableNetworkLayer interface {
	gopacket.NetworkLayer
	SerializeTo(b gopacket.SerializeBuffer, opts gopacket.SerializeOptions) error
}

// createSerializedUDPFrame creates an Ethernet frame encapsulating our UDP
// packet for injection to the local network
func CreateSerializedUDPFrame(opts UdpFrameOptions) ([]byte, error) {

	buf := gopacket.NewSerializeBuffer()
	serializeOpts := gopacket.SerializeOptions{
		FixLengths:       true,
		ComputeChecksums: true,
	}
	ethernetType := layers.EthernetTypeIPv4
	if opts.IsIPv6 {
		ethernetType = layers.EthernetTypeIPv6
	}
	eth := &layers.Ethernet{
		SrcMAC:       opts.SourceMac,
		DstMAC:       opts.DestMac,
		EthernetType: ethernetType,
	}
	var ip serializableNetworkLayer
	if !opts.IsIPv6 {
		ip = &layers.IPv4{
			SrcIP:    opts.SourceIP,
			DstIP:    opts.DestIP,
			Protocol: layers.IPProtocolUDP,
			Version:  4,
			TTL:      32,
		}
	} else {
		ip = &layers.IPv6{
			SrcIP:      opts.SourceIP,
			DstIP:      opts.DestIP,
			NextHeader: layers.IPProtocolUDP,
			Version:    6,
			HopLimit:   32,
		}
		ip.LayerType()
	}

	udp := &layers.UDP{
		SrcPort: layers.UDPPort(opts.SourcePort),
		DstPort: layers.UDPPort(opts.DestPort),
		// we configured "Length" and "Checksum" to be set for us
	}
	err := udp.SetNetworkLayerForChecksum(ip)
	if err != nil {
		return nil, err
	}
	err = gopacket.SerializeLayers(buf, serializeOpts, eth, ip, udp, gopacket.Payload(opts.PayloadBytes))
	if err != nil {
		return nil, err
	}

	return buf.Bytes(), nil
}
