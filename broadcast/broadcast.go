package broadcast

import (
	"fmt"
	"github.com/skysoft-atm/gorillaz"
	"github.com/skysoft-atm/gorillaz/mux"
	"github.com/skysoft-atm/gorillaz/stream"
	"github.com/skysoft-atm/supercaster/network"
	"go.uber.org/zap"
	"golang.org/x/net/ipv4"
	"net"
)

func ReceiveData(source network.UdpSource, handler network.Handler) error {
	c, err := net.ListenPacket("udp", source.HostPort)
	if err != nil {
		return fmt.Errorf("unable to listen: %w", err)
	}
	defer func() {
		_ = c.Close()
	}()
	p := ipv4.NewPacketConn(c)
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
		gorillaz.Log.Debug(fmt.Sprintf("Received %d bytes from %s.", nbBytes, source))
		err = sp.SubmitNonBlocking(&stream.Event{
			Key:   []byte(source + ">" + dest),
			Value: data[:nbBytes],
		})
		if err != nil {
			gorillaz.Log.Error("error while submitting on stream", zap.Error(err))
		}
	})
}

func UdpToBroadcaster(source network.UdpSource, broadcaster *mux.Broadcaster) error {
	return ReceiveData(source, func(nbBytes int, source, dest string, data []byte) {
		gorillaz.Log.Debug(fmt.Sprintf("Received %d bytes from %s.", nbBytes, source))
		err := broadcaster.SubmitNonBlocking(&stream.Event{
			Key:   []byte(source + ">" + dest),
			Value: data[:nbBytes],
		})
		if err != nil {
			gorillaz.Log.Warn("error while submitting on broadcaster", zap.Error(err))
		}
	})
}
