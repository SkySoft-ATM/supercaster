package network

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"github.com/google/gopacket/layers"
	"github.com/google/gopacket/pcap"
	"github.com/lestrrat-go/backoff"
	"github.com/skysoft-atm/gorillaz"
	"github.com/skysoft-atm/gorillaz/stream"
	"github.com/skysoft-atm/supercaster/udp"
	"go.uber.org/zap"
	"net"
	"strconv"
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

var multicastHwBase, _ = net.ParseMAC("01:00:5e:00:00:00")

type Handler func(nbBytes int, source string, data []byte)

type UdpSource struct {
	NetInterface    *net.Interface
	HostPort        string
	MaxDatagramSize int
}

func GetNetworkInterface(interfaceName string) *net.Interface {
	interfaces, e := net.Interfaces()
	if e != nil {
		panic(e)
	}
	gorillaz.Log.Info("Available interfaces:")

	for _, i := range interfaces {

		addresses := make([]string, 0)
		addr, err := i.Addrs()
		if err == nil {
			for _, a := range addr {
				ip := a.String()
				if ip != "" {
					ip := removeMask(ip)
					addresses = append(addresses, ip)
				}
			}
			gorillaz.Sugar.Infof("%s - %s", i.Name, strings.Join(addresses, ","))
		} else {
			gorillaz.Log.Info(i.Name)
		}

	}
	var netInterface *net.Interface
	if interfaceName != "" {
		for _, i := range interfaces {
			if i.Name == strings.TrimSpace(interfaceName) {
				selected := i
				netInterface = &selected
			}
		}
	}
	if netInterface == nil {
		gorillaz.Log.Info("Network interface not found")
	} else {
		gorillaz.Sugar.Infof("Selected network interface %s", netInterface.Name)
	}
	return netInterface
}

func removeMask(ip string) string {
	maskIndex := strings.LastIndex(ip, "/")
	if maskIndex != -1 {
		return ip[:maskIndex]
	}
	return ip
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
	i := 0
	t := time.NewTicker(2 * time.Second)
	defer t.Stop()
	for range t.C {
		gorillaz.Log.Info(fmt.Sprintf("Sending testdata%d", i))
		_, err := c.Write([]byte(fmt.Sprintf("testdata%d", i)))
		if err != nil {
			gorillaz.Log.Error("Error while writing to UDP ", zap.Error(err))
		}
		i++
	}
	return nil
}

func GrpcToUdp(grpcEndpoints []string, streamName string, hostPort string, gaz *gorillaz.Gaz) error {
	gorillaz.Log.Info("Starting publication", zap.String("stream name", streamName), zap.Strings("endpoint", grpcEndpoints),
		zap.String("multicast address", hostPort))
	bo, cancel := backoffPolicy.Start(context.Background())
	defer cancel()
	for backoff.Continue(bo) {
		consumer, err := gaz.ConsumeStream(grpcEndpoints, streamName)
		if err != nil {
			return fmt.Errorf("unable to consume stream %s: %w", streamName, err)
		}
		err = StreamToUdp(consumer, hostPort)
		if err != nil {
			return err
		}
	}
	return nil
}

func ServiceStreamToUdp(service string, streamName string, hostPort string, gaz *gorillaz.Gaz) error {
	gorillaz.Log.Info("Starting publication", zap.String("stream name", streamName), zap.String("service", service),
		zap.String("multicast address", hostPort))
	bo, cancel := backoffPolicy.Start(context.Background())
	defer cancel()
	for backoff.Continue(bo) {
		consumer, err := gaz.DiscoverAndConsumeServiceStream(service, streamName)
		if err != nil {
			return fmt.Errorf("unable to consume stream %s/%s: %w", service, streamName, err)
		}
		err = StreamToUdp(consumer, hostPort)
		consumer.Stop()
		if err != nil {
			return err
		}
	}
	return nil
}

type EventStream interface {
	EvtChan() chan *stream.Event
}

func StreamToUdp(stream EventStream, hostPort string) error {
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

type streamDiscovery interface {
	DiscoverAndConsumeServiceStream(service, stream string, opts ...gorillaz.ConsumerConfigOpt) (gorillaz.StreamConsumer, error)
}

func ServiceStreamToUdpSpoofSourceAddr(service, streamName, interfaceName, hostPort string, sd streamDiscovery, pubType UdpPubType) error {
	gorillaz.Log.Info("Starting publication", zap.String("stream name", streamName), zap.String("service", service),
		zap.String("address", hostPort))
	bo, cancel := backoffPolicy.Start(context.Background())
	defer cancel()

	for backoff.Continue(bo) {
		consumer, err := sd.DiscoverAndConsumeServiceStream(service, streamName)
		if err != nil {
			return fmt.Errorf("unable to consume stream %s/%s: %w", service, streamName, err)
		}
		err = StreamToUdpSpoofSourceAddr(consumer, UdpPub{
			HostPort:      hostPort,
			InterfaceName: interfaceName,
			Type:          pubType,
		})
		consumer.Stop()
		if err != nil {
			return err
		}
	}
	return nil
}

type UdpPubType int

const (
	Broadcast = iota
	Multicast
)

type UdpPub struct {
	InterfaceName string
	HostPort      string
	Type          UdpPubType
}

func StreamToUdpSpoofSourceAddr(stream EventStream, udpPub UdpPub) error {
	netIf := GetNetworkInterface(udpPub.InterfaceName)
	hwAddr := netIf.HardwareAddr
	device, err := GetPcapNetworkDevice(netIf)
	if err != nil {
		return fmt.Errorf("network device not found: %w", err)
	}
	handle, err := pcap.OpenLive(device, 1024, false, pcap.BlockForever)
	if err != nil {
		return fmt.Errorf("error on OpenLive: %w", err)
	}
	defer handle.Close()

	dstMac := layers.EthernetBroadcast
	destIp := net.ParseIP(getIp(udpPub.HostPort))
	if udpPub.Type == Multicast {
		if destIp == nil {
			return fmt.Errorf("multicast destination IP not configured")
		}
		dstMac, err = MulticastIpToMac(destIp)
		if err != nil {
			return err
		}
	}
	if udpPub.Type == Broadcast && destIp == nil {
		destIp, err = getBroadcastAddress(netIf)
		if err != nil {
			return err
		}
	}
	dstPort, err := getPort(udpPub.HostPort)
	if err != nil {
		return err
	}

	for e := range stream.EvtChan() {
		srcPort, err := getPort(string(e.Key))
		if err != nil {
			gorillaz.Log.Warn("could not extract port", zap.Error(err))
			continue
		}
		frameBytes, err := udp.CreateSerializedUDPFrame(udp.UdpFrameOptions{
			SourceIP:     net.ParseIP(getIp(string(e.Key))),
			DestIP:       destIp,
			SourcePort:   srcPort,
			DestPort:     dstPort,
			SourceMac:    hwAddr,
			DestMac:      dstMac,
			IsIPv6:       false,
			PayloadBytes: e.Value,
		})
		if err != nil {
			gorillaz.Log.Fatal("error on createSerializedUDPFrame", zap.Error(err))
		}

		if err := handle.WritePacketData(frameBytes); err != nil {
			gorillaz.Log.Fatal("error on WritePacketData", zap.Error(err))
		}
		fmt.Println("Sent", string(e.Value))
	}

	return nil
}

//returns the last address of an IPNet (i.e. broadcast address)
func LastAddr(n *net.IPNet) (net.IP, error) {
	if n.IP.To4() == nil {
		return net.IP{}, errors.New("does not support IPv6 addresses")
	}
	ip := make(net.IP, len(n.IP.To4()))
	binary.BigEndian.PutUint32(ip, binary.BigEndian.Uint32(n.IP.To4())|^binary.BigEndian.Uint32(net.IP(n.Mask).To4()))
	return ip, nil
}

func getBroadcastAddress(netIf *net.Interface) (net.IP, error) {
	addrs, err := netIf.Addrs()
	if err != nil {
		return nil, fmt.Errorf("could not find addresses on network interface %s : %w", netIf.Name, err)
	}
	for _, a := range addrs {
		ip, ipnet, err := net.ParseCIDR(a.String())
		if err != nil {
			return nil, fmt.Errorf("could not parse address %v : %w", a, err)
		}
		if ip.To4() != nil {
			b, err := LastAddr(ipnet)
			if err != nil {
				return nil, fmt.Errorf("could not find last address for %v : %w", ipnet.String(), err)
			}
			return b, nil
		}
	}
	return nil, fmt.Errorf("empty addresses on network interface %s : %w", netIf.Name, err)
}

func getIp(hostPort string) string {
	sp := strings.Split(hostPort, ":")
	if len(sp) > 0 {
		return sp[0]
	}
	return ""
}

func getPort(hostPort string) (uint16, error) {
	sp := strings.Split(hostPort, ":")
	if len(sp) > 1 {
		p, err := strconv.Atoi(sp[1])
		if err != nil {
			return 0, err
		}
		return uint16(p), nil
	}
	return 0, fmt.Errorf("unable to extract port from %s", hostPort)
}

// finds the pcap network device matching the requested network interface
// on windows the network devices have weird names, so we match the interface and device based on their assigned IP addresses
// see https://forum.golangbridge.org/t/soved-gopacket-pcap-and-windows-device-names/15856/2
// https://superuser.com/questions/902577/windows-7-network-adapter-device-name-using-winpcap
func GetPcapNetworkDevice(netInterface *net.Interface) (string, error) {
	ifAddresses, err := getInterfaceIps(netInterface)
	if err != nil {
		return "", err
	}

	ifs, err := pcap.FindAllDevs()
	if err != nil {
		return "", err
	}
	for _, i := range ifs {
		addresses := getDeviceIps(i)
		if containsSameElements(ifAddresses, addresses) {
			return i.Name, nil
		}
	}
	return "", fmt.Errorf("pcap network device not found for net interface %s", netInterface.Name)
}

func getInterfaceIps(netInterface *net.Interface) ([]string, error) {
	addrs, err := netInterface.Addrs()
	if err != nil {
		return nil, err
	}
	ifAddresses := make([]string, len(addrs))
	for i, a := range addrs {
		ifAddresses[i] = removeMask(a.String())
	}
	return ifAddresses, nil
}

func getDeviceIps(i pcap.Interface) []string {
	addresses := make([]string, 0, len(i.Addresses))
	for _, a := range i.Addresses {
		addr := a.IP.String()
		if addr != "" {
			addresses = append(addresses, addr)
		}
	}
	return addresses
}

func containsSameElements(a []string, b []string) bool {
	aMap := make(map[string]struct{})
	bMap := make(map[string]struct{})
	for _, i := range a {
		aMap[i] = struct{}{}
	}
	for _, i := range b {
		bMap[i] = struct{}{}
	}
	if len(aMap) != len(bMap) {
		return false
	}
	for k := range aMap {
		_, ok := bMap[k]
		if !ok {
			return false
		}
	}
	return true
}

// Calculates the mac multicast address for a given ip multicast address
// see http://www.dqnetworks.ie/toolsinfo.d/multicastaddressing.html#convertertool
func MulticastIpToMac(ip net.IP) (net.HardwareAddr, error) {
	if !ip.IsMulticast() {
		return nil, fmt.Errorf("IP %s is not a multicast address", ip.String())
	}
	res := make([]byte, 0, 6)
	base := []byte(multicastHwBase)
	res = append(res, base[:3]...)
	res = append(res, []byte(ip)[len(ip)-3]&0x7F) // sets the first bit to 0
	res = append(res, []byte(ip)[len(ip)-2:]...)
	return res, nil
}
