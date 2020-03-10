package main

import (
	"flag"
	"fmt"
	"github.com/skysoft-atm/gorillaz"
	"github.com/skysoft-atm/supercaster/broadcast"
	"github.com/skysoft-atm/supercaster/multicast"
	"github.com/skysoft-atm/supercaster/network"
)

const configUdpHostPort = "udp.hostport"
const configGrpcStreamName = "grpc.stream.name"
const configGrpcStreamHostPort = "grpc.stream.hostport"
const configGrpcStreamService = "grpc.stream.service"
const configDirection = "direction"
const configDirectionM2G = "m2g"
const configDirectionB2G = "b2g"
const configDirectionG2U = "g2u"
const configDirectionTestUdpSend = "testudpsend"
const configDirectionMCRecv = "mcrecv"
const configDirectionBCRecv = "bcrecv"
const configMaxDatagramSize = "udp.maxDatagramSize"

func init() {
	flag.String(configUdpHostPort, "", "Multicast address and port separated by ':'.")
	flag.String(configGrpcStreamName, "", "gRPC stream name.")
	flag.String(configGrpcStreamHostPort, "", "gRPC stream address and port separated by ':'.")
	flag.String(configGrpcStreamService, "", "gRPC stream service name.")
	flag.String(configDirection, "", "'m2g' for multicast-to-grpc, 'b2g' for broadcast-to-grpc, 'g2u' for grpc-to-udp, 'testudpsend' for UDP sender only, 'mcrecv' for multicast receiver, 'bcrecv' for broadcast receiver.")
	flag.String(network.ConfigNetworkInterface, "", "Network interface to use.")
	flag.String(configMaxDatagramSize, "", "Max datagram size.")
}

func main() {
	gaz := gorillaz.New()
	gaz.Run()

	udpHostport := gaz.Viper.GetString(configUdpHostPort)
	if udpHostport == "" {
		panic(fmt.Errorf("invalid UDP host and port: %s", udpHostport))
	}
	streamName := gaz.Viper.GetString(configGrpcStreamName)
	direction := gaz.Viper.GetString(configDirection)
	maxDatagramSize := gaz.Viper.GetInt(configMaxDatagramSize)

	switch direction {
	case configDirectionM2G:
		go panicIf(multicast.UdpToStream(gaz, udpSource(gaz, udpHostport, maxDatagramSize), streamName))
	case configDirectionB2G:
		go panicIf(broadcast.UdpToStream(gaz, udpSource(gaz, udpHostport, maxDatagramSize), streamName))
	case configDirectionG2U:
		streamService := gaz.Viper.GetString(configGrpcStreamService)
		streamHostport := gaz.Viper.GetString(configGrpcStreamHostPort)
		if streamService != "" {
			go panicIf(network.ServiceStreamToUdp(streamService, streamName, udpHostport, gaz))
		} else if streamHostport != "" {
			grpcEndpoints := make([]string, 0)
			grpcEndpoints = append(grpcEndpoints, streamHostport)
			go panicIf(network.GrpcToUdp(grpcEndpoints, streamName, udpHostport, gaz))
		} else {
			panic(fmt.Errorf("at least one of these parameters is required: %s, %s", configGrpcStreamService, configGrpcStreamHostPort))
		}
	case configDirectionMCRecv:
		go panicIf(multicast.ReceiveData(udpSource(gaz, udpHostport, maxDatagramSize), func(n int, src string, b []byte) {
			gorillaz.Log.Info(fmt.Sprintf("Received %d bytes from %s: %s", n, src, string(b[:n])))
		}))
	case configDirectionBCRecv:
		go panicIf(broadcast.ReceiveData(udpSource(gaz, udpHostport, maxDatagramSize), func(n int, src string, b []byte) {
			gorillaz.Log.Info(fmt.Sprintf("Received %d bytes from %s: %s", n, src, string(b[:n])))
		}))
	case configDirectionTestUdpSend:
		go panicIf(network.PublishTestData(udpHostport))
	default:
		panic(fmt.Errorf("invalid direction: %s", direction))
	}

	gaz.SetReady(true)
	select {}
}

func udpSource(gaz *gorillaz.Gaz, udpHostport string, maxDatagramSize int) network.UdpSource {
	source := network.UdpSource{
		NetInterface:    network.GetNetworkInterface(gaz),
		HostPort:        udpHostport,
		MaxDatagramSize: maxDatagramSize,
	}
	return source
}

func panicIf(err error) {
	if err != nil {
		panic(err)
	}
}
