package network

import (
	"context"
	"fmt"
	"github.com/lestrrat-go/backoff"
	"github.com/skysoft-atm/gorillaz"
	"go.uber.org/zap"
	"net"
	"strings"
	"time"
)

var backoffPolicy = backoff.NewExponential(
	backoff.WithInterval(500*time.Millisecond),
	backoff.WithMaxInterval(5*time.Second),
	backoff.WithJitterFactor(0.05),
	backoff.WithMaxRetries(0),
)

const ConfigNetworkInterface = "network.interface"

type Handler func(nbBytes int, source string, data []byte)

type UdpSource struct {
	NetInterface    *net.Interface
	HostPort        string
	MaxDatagramSize int
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

func PublishTestData(hostPort string) error {
	addr, err := net.ResolveUDPAddr("udp", hostPort)
	if err != nil {
		return err
	}
	c, err := net.DialUDP("udp", nil, addr)
	if err != nil {
		return err
	}
	for i := 0; i < 50; i++ {
		time.Sleep(2 * time.Second)
		gorillaz.Log.Info(fmt.Sprintf("Sending testdata%d", i))
		_, err := c.Write([]byte(fmt.Sprintf("testdata%d", i)))
		if err != nil {
			gorillaz.Log.Error("Error while writing to UDP ", zap.Error(err))
		}
	}
	return nil
}

func GrpcToUdp(grpcEndpoints []string, streamName string, multicastHostPort string, gaz *gorillaz.Gaz) error {
	gorillaz.Log.Info("Starting publication", zap.String("stream name", streamName), zap.Strings("endpoint", grpcEndpoints),
		zap.String("multicast address", multicastHostPort))
	bo, cancel := backoffPolicy.Start(context.Background())
	defer cancel()
	for backoff.Continue(bo) {
		consumer, err := gaz.ConsumeStream(grpcEndpoints, streamName)
		if err != nil {
			return fmt.Errorf("unable to consume stream %s: %w", streamName, err)
		}
		err = StreamToUdp(consumer, multicastHostPort)
		if err != nil {
			return err
		}
	}
	return nil
}

func ServiceStreamToUdp(service string, streamName string, multicastHostPort string, gaz *gorillaz.Gaz) error {
	gorillaz.Log.Info("Starting publication", zap.String("stream name", streamName), zap.String("service", service),
		zap.String("multicast address", multicastHostPort))
	bo, cancel := backoffPolicy.Start(context.Background())
	defer cancel()
	for backoff.Continue(bo) {
		consumer, err := gaz.DiscoverAndConsumeServiceStream(service, streamName)
		if err != nil {
			return fmt.Errorf("unable to consume stream %s/%s: %w", service, streamName, err)
		}
		err = StreamToUdp(consumer, multicastHostPort)
		if err != nil {
			return err
		}
	}
	return nil
}

func StreamToUdp(stream gorillaz.StreamConsumer, hostPort string) error {
	addr, err := net.ResolveUDPAddr("udp", hostPort)
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
