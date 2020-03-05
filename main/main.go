package main

import (
	"flag"
	"fmt"
	"github.com/skysoft-atm/gorillaz"
	"github.com/skysoft-atm/gorillaz/stream"
	"go.uber.org/zap"
	"golang.org/x/net/ipv4"
	"net"
	"strings"
	"time"
)

const configMulticastAddress = "multicast.address"
const configGrpcStreamName = "grpc.stream.name"
const configGrpcStreamAddress = "grpc.stream.address"
const configDirection = "direction"
const configDirectionM2G = "m2g"
const configDirectionG2M = "g2m"
const configDirectionMM = "mm"
const configNetworkInterface = "network.interface"
const configMaxDatagramSize = "multicast.maxDatagramSize"

func init() {
	flag.String(configMulticastAddress, "", "Multicast address and port separated by ':'.")
	flag.String(configGrpcStreamName, "", "gRPC stream name.")
	flag.String(configGrpcStreamAddress, "", "gRPC stream address and port separated by ':'.")
	flag.String(configDirection, "", "'m2g' for multicast-to-grpc, 'grpcToMulticast' for grpc-to-multicast.")
	flag.String(configNetworkInterface, "", "Network interface to use.")
	flag.String(configMaxDatagramSize, "", "Max datagram size.")
}

func main() {
	gaz := gorillaz.New()
	gaz.SetLive(true)
	gaz.Run()

	multicastHostport := gaz.Viper.GetString(configMulticastAddress)
	streamName := gaz.Viper.GetString(configGrpcStreamName)
	direction := gaz.Viper.GetString(configDirection)
	netInterface := getNetworkInterface(gaz)
	maxDatagramSize := gaz.Viper.GetInt(configMaxDatagramSize)
	streamHostport := gaz.Viper.GetString(configGrpcStreamAddress)

	switch direction {
	case configDirectionM2G:
		go multicastToGrpc(netInterface, multicastHostport, maxDatagramSize, streamName, gaz)
	case configDirectionG2M:
		grpcEndpoints := make([]string, 0)
		grpcEndpoints[0] = streamHostport
		go grpcToMulticast(grpcEndpoints, streamName, multicastHostport, gaz)
	case configDirectionMM:
		go receiveTestDataFromMulticast(netInterface, multicastHostport, maxDatagramSize)
		go publishTestDataToMulticast(multicastHostport)
	default:
		panic(fmt.Errorf("invalid direction: %s", direction))
	}

	gaz.SetReady(true)
	select {}
}

func receiveTestDataFromMulticast(netInterface *net.Interface, multicastHostPort string, maxDatagramSize int) {
	hostPort := strings.Split(multicastHostPort, ":")
	group := net.ParseIP(hostPort[0])
	c, err := net.ListenPacket("udp", fmt.Sprintf("0.0.0.0:%s", hostPort[1]))
	if err != nil {
		gorillaz.Log.Fatal("Unable to listen.", zap.Error(err))
	}
	defer func() {
		if c.Close() != nil {
			// nothing
		}
	}()
	p := ipv4.NewPacketConn(c)
	if err := p.JoinGroup(netInterface, &net.UDPAddr{IP: group}); err != nil {
		gorillaz.Log.Fatal("Unable to join multicast address.", zap.Error(err))
	}
	if loop, err := p.MulticastLoopback(); err == nil {
		if !loop {
			if err := p.SetMulticastLoopback(true); err != nil {
				gorillaz.Log.Fatal("Unable to set multicast loopback.", zap.Error(err))
			}
		}
	}
	b := make([]byte, maxDatagramSize)
	for {
		n, _, src, err := p.ReadFrom(b)
		if err != nil {
			gorillaz.Log.Fatal("Unable to read.", zap.Error(err))
		}
		gorillaz.Log.Info(fmt.Sprintf("Received %d bytes from %s: %s", n, src, string(b[:n])))
	}
}

func publishTestDataToMulticast(multicastAddr string) {
	addr, err := net.ResolveUDPAddr("udp", multicastAddr)
	if err != nil {
		panic(err)
	}
	c, err := net.DialUDP("udp", nil, addr)
	if err != nil {
		panic(err)
	}
	for i := 0; i < 10; i++ {
		time.Sleep(2 * time.Second)
		gorillaz.Log.Info(fmt.Sprintf("Sending testdata%d", i))
		_, err := c.Write([]byte(fmt.Sprintf("testdata%d", i)))
		if err != nil {
			gorillaz.Log.Error("Error while writing to UDP ", zap.Error(err))
		}
	}
}

func getNetworkInterface(gaz *gorillaz.Gaz) *net.Interface {
	interfaces, e := net.Interfaces()
	if e != nil {
		panic(e)
	}
	gorillaz.Log.Info("Available interfaces:")
	for _, i := range interfaces {
		gorillaz.Log.Info(i.Name)
	}
	var netInterface *net.Interface
	interfaceName := gaz.Viper.GetString(configNetworkInterface)
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

func multicastToGrpc(netInterface *net.Interface, multicastHostPort string, maxDatagramSize int, streamName string, gaz *gorillaz.Gaz) {
	provider, err := gaz.NewStreamProvider(streamName, "unknown")
	if err != nil {
		panic(err)
	}
	hostPort := strings.Split(multicastHostPort, ":")
	group := net.ParseIP(hostPort[0])
	c, err := net.ListenPacket("udp", fmt.Sprintf("0.0.0.0:%s", hostPort[1]))
	if err != nil {
		gorillaz.Log.Fatal("Unable to listen.", zap.Error(err))
	}
	defer func() {
		if c.Close() != nil {
			// nothing
		}
	}()
	p := ipv4.NewPacketConn(c)
	if err := p.JoinGroup(netInterface, &net.UDPAddr{IP: group}); err != nil {
		gorillaz.Log.Fatal("Unable to join multicast address.", zap.Error(err))
	}
	if loop, err := p.MulticastLoopback(); err == nil {
		if !loop {
			if err := p.SetMulticastLoopback(true); err != nil {
				gorillaz.Log.Fatal("Unable to set multicast loopback.", zap.Error(err))
			}
		}
	}
	b := make([]byte, maxDatagramSize)
	for {
		n, _, src, err := p.ReadFrom(b)
		if err != nil {
			gorillaz.Log.Fatal("Unable to read.", zap.Error(err))
		}
		gorillaz.Log.Info(fmt.Sprintf("Received %d bytes from %s.", n, src))
		provider.Submit(&stream.Event{Value: b[:n]})
		gorillaz.Log.Debug("Published on stream")
	}
}

func grpcToMulticast(grpcEndpoints []string, streamName string, multicastHostport string, gaz *gorillaz.Gaz) {
	gorillaz.Log.Info("Starting publication", zap.String("stream name", streamName), zap.Strings("endpoint", grpcEndpoints),
		zap.String("multicast address", multicastHostport))
	addr, err := net.ResolveUDPAddr("udp", multicastHostport)
	if err != nil {
		panic(err)
	}
	c, err := net.DialUDP("udp", nil, addr)
	if err != nil {
		panic(err)
	}

	consumer, err := gaz.ConsumeStream(grpcEndpoints, streamName)
	if err != nil {
		panic(err)
	}
	for {
		select {
		case msg := <-consumer.EvtChan():
			_, err := c.Write(msg.Value)
			if err != nil {
				gorillaz.Log.Error("Error while writing to UDP ", zap.Error(err))
			}
		}
	}
}
