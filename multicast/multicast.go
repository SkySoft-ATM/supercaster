package multicast

import (
	"fmt"
	"github.com/skysoft-atm/gorillaz"
	"github.com/skysoft-atm/gorillaz/stream"
	"github.com/skysoft-atm/supercaster/network"
	"go.uber.org/zap"
	"golang.org/x/net/ipv4"
	"net"
)

func ReceiveData(source network.UdpSource, handler network.Handler) error {
	host, port, err := network.GetHostAndPort(source.HostPort)
	if err != nil {
		return err
	}
	group := net.ParseIP(host)
	c, err := net.ListenPacket("udp", fmt.Sprintf("0.0.0.0:%d", port))
	if err != nil {
		return fmt.Errorf("unable to listen: %w", err)
	}
	defer func() {
		_ = c.Close()
	}()
	p := ipv4.NewPacketConn(c)
	if err := p.JoinGroup(source.NetInterface, &net.UDPAddr{IP: group}); err != nil {
		return fmt.Errorf("unable to join multicast address: %w", err)
	}
	if loop, err := p.MulticastLoopback(); err == nil {
		if !loop {
			if err := p.SetMulticastLoopback(true); err != nil {
				return fmt.Errorf("unable to set multicast loopback: %w", err)
			}
		}
	}
	b := make([]byte, source.MaxDatagramSize)
	for {
		n, _, src, err := p.ReadFrom(b)
		if err != nil {
			gorillaz.Log.Fatal("Unable to read.", zap.Error(err))
		}
		handler(n, src.String(), source.HostPort, b)
	}
}

func UdpToStream(g *gorillaz.Gaz, source network.UdpSource, streamName string) error {
	sp, err := g.NewStreamProvider(streamName, "bytes")
	if err != nil {
		return err
	}
	return ReceiveData(source, func(nbBytes int, source, dest string, data []byte) {
		gorillaz.Log.Debug(fmt.Sprintf("Received %d bytes from %s", nbBytes, source))
		err = sp.SubmitNonBlocking(&stream.Event{
			Key:   []byte(source + ">" + dest),
			Value: data[:nbBytes],
		})
		if err != nil {
			gorillaz.Log.Error("error while submitting on stream", zap.Error(err))
		}
	})

}
