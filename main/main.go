package main

import (
	"flag"
	"fmt"
	"github.com/skysoft-atm/gorillaz"
	"github.com/skysoft-atm/gorillaz/stream"
	"github.com/skysoft-atm/supercaster/multicast"
)

const configMulticastAddress = "multicast.address"
const configGrpcStreamName = "grpc.stream.name"
const configGrpcStreamAddress = "grpc.stream.address"
const configDirection = "direction"
const configDirectionM2G = "m2g"
const configDirectionG2M = "g2m"
const configDirectionMCSend = "mcsend"
const configDirectionMCRecv = "mcrecv"
const configMaxDatagramSize = "multicast.maxDatagramSize"

func init() {
	flag.String(configMulticastAddress, "", "Multicast address and port separated by ':'.")
	flag.String(configGrpcStreamName, "", "gRPC stream name.")
	flag.String(configGrpcStreamAddress, "", "gRPC stream address and port separated by ':'.")
	flag.String(configDirection, "", "'m2g' for multicast-to-grpc, 'g2m' for grpc-to-multicast, 'mcsend' for multicast sender only, 'mcrecv' for multicast receiver only.")
	flag.String(multicast.ConfigNetworkInterface, "", "Network interface to use.")
	flag.String(configMaxDatagramSize, "", "Max datagram size.")
}

func main() {
	gaz := gorillaz.New()
	gaz.Run()

	multicastHostport := gaz.Viper.GetString(configMulticastAddress)
	streamName := gaz.Viper.GetString(configGrpcStreamName)
	direction := gaz.Viper.GetString(configDirection)
	maxDatagramSize := gaz.Viper.GetInt(configMaxDatagramSize)
	streamHostport := gaz.Viper.GetString(configGrpcStreamAddress)

	switch direction {
	case configDirectionM2G:
		provider, err := gaz.NewStreamProvider(streamName, "unknown")
		if err != nil {
			panic(err)
		}
		netInterface := multicast.GetNetworkInterface(gaz)
		source := multicast.UdpSource{
			NetInterface:      netInterface,
			MulticastHostPort: multicastHostport,
			MaxDatagramSize:   maxDatagramSize,
		}
		go panicIf(multicast.ReceiveData(source, func(n int, src string, b []byte) {
			gorillaz.Log.Info(fmt.Sprintf("Received %d bytes from %s.", n, src))
			provider.Submit(&stream.Event{Value: b[:n]})
			gorillaz.Log.Debug("Published on stream")
		}))
	case configDirectionG2M:
		grpcEndpoints := make([]string, 0)
		grpcEndpoints = append(grpcEndpoints, streamHostport)
		go panicIf(multicast.GrpcToMulticast(grpcEndpoints, streamName, multicastHostport, gaz))
	case configDirectionMCRecv:
		netInterface := multicast.GetNetworkInterface(gaz)
		source := multicast.UdpSource{
			NetInterface:      netInterface,
			MulticastHostPort: multicastHostport,
			MaxDatagramSize:   maxDatagramSize,
		}
		go panicIf(multicast.ReceiveData(source, func(n int, src string, b []byte) {
			gorillaz.Log.Info(fmt.Sprintf("Received %d bytes from %s: %s", n, src, string(b[:n])))
		}))
	case configDirectionMCSend:
		go panicIf(multicast.PublishTestData(multicastHostport))
	default:
		panic(fmt.Errorf("invalid direction: %s", direction))
	}

	gaz.SetReady(true)
	select {}
}

func panicIf(err error) {
	if err != nil {
		panic(err)
	}
}
