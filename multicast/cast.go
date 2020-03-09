package multicast

import (
	"fmt"
	"github.com/skysoft-atm/gorillaz"
	"github.com/skysoft-atm/gorillaz/stream"
	"go.uber.org/zap"
	"golang.org/x/net/ipv4"
	"net"
	"strings"
	"time"
)

const ConfigNetworkInterface = "network.interface"

type Handler func(nbBytes int, source string, data []byte)

type UdpSource struct {
	NetInterface      *net.Interface
	MulticastHostPort string
	MaxDatagramSize   int
}

func ReceiveData(source UdpSource, handler Handler) error {
	hostPort := strings.Split(source.MulticastHostPort, ":")
	group := net.ParseIP(hostPort[0])
	c, err := net.ListenPacket("udp", fmt.Sprintf("0.0.0.0:%s", hostPort[1]))
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
		handler(n, src.String(), b)
	}
}

func UdpToStream(g *gorillaz.Gaz, source UdpSource, streamName string) error {
	sp, err := g.NewStreamProvider(streamName, "bytes")
	if err != nil {
		return err
	}
	return ReceiveData(source, func(nbBytes int, source string, data []byte) {
		err = sp.SubmitNonBlocking(&stream.Event{
			Value: data,
		})
		if err != nil {
			gorillaz.Log.Error("error while submitting on stream", zap.Error(err))
		}
	})

}

func PublishTestData(multicastAddr string) error {
	addr, err := net.ResolveUDPAddr("udp", multicastAddr)
	if err != nil {
		return err
	}
	c, err := net.DialUDP("udp", nil, addr)
	if err != nil {
		return err
	}
	for i := 0; i < 10; i++ {
		time.Sleep(2 * time.Second)
		gorillaz.Log.Info(fmt.Sprintf("Sending testdata%d", i))
		_, err := c.Write([]byte(fmt.Sprintf("testdata%d", i)))
		if err != nil {
			gorillaz.Log.Error("Error while writing to UDP ", zap.Error(err))
		}
	}
	return nil
}

func GetNetworkInterface(gaz *gorillaz.Gaz) *net.Interface {
	interfaces, e := net.Interfaces()
	if e != nil {
		panic(e)
	}
	gorillaz.Log.Info("Available interfaces:")
	for _, i := range interfaces {
		gorillaz.Log.Info(i.Name)
	}
	var netInterface *net.Interface
	interfaceName := gaz.Viper.GetString(ConfigNetworkInterface)
	if interfaceName != "" {
		for _, i := range interfaces {
			if i.Name == strings.TrimSpace(interfaceName) {
				selected := i
				netInterface = &selected
			}
		}
	}
	if netInterface == nil {
		gorillaz.Log.Info("Listening on all network interfaces")
	} else {
		gorillaz.Sugar.Infof("Listening on network interface %s", netInterface.Name)
	}
	return netInterface
}

func GrpcToMulticast(grpcEndpoints []string, streamName string, multicastHostPort string, gaz *gorillaz.Gaz) error {
	gorillaz.Log.Info("Starting publication", zap.String("stream name", streamName), zap.Strings("endpoint", grpcEndpoints),
		zap.String("multicast address", multicastHostPort))
	for {
		consumer, err := gaz.ConsumeStream(grpcEndpoints, streamName)
		if err != nil {
			return fmt.Errorf("unable to consume stream %s: %w", streamName, err)
		}
		err = StreamToMulticast(consumer, multicastHostPort)
		if err != nil {
			return err
		}
	}
}

func ServiceStreamToMulticast(service string, streamName string, multicastHostPort string, gaz *gorillaz.Gaz) error {
	gorillaz.Log.Info("Starting publication", zap.String("stream name", streamName), zap.String("service", service),
		zap.String("multicast address", multicastHostPort))

	for {
		consumer, err := gaz.DiscoverAndConsumeServiceStream(service, streamName)
		if err != nil {
			return fmt.Errorf("unable to consume stream %s/%s: %w", service, streamName, err)
		}
		err = StreamToMulticast(consumer, multicastHostPort)
		if err != nil {
			return err
		}
	}
}

func StreamToMulticast(stream gorillaz.StreamConsumer, multicastHostPort string) error {
	addr, err := net.ResolveUDPAddr("udp", multicastHostPort)
	if err != nil {
		return fmt.Errorf("unable to resolve address: %w", err)
	}
	c, err := net.DialUDP("udp", nil, addr)
	if err != nil {
		return fmt.Errorf("unable to dial UDP: %w", err)
	}
	defer func() {
		_ = c.Close()
	}()

	for msg := range stream.EvtChan() {
		_, err := c.Write(msg.Value)
		if err != nil {
			gorillaz.Log.Error("Error while writing to UDP ", zap.Error(err))
		}
	}
	return nil
}
