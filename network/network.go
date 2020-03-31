package network

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"github.com/lestrrat-go/backoff"
	"github.com/skysoft-atm/gorillaz"
	"github.com/skysoft-atm/gorillaz/mux"
	"github.com/skysoft-atm/gorillaz/stream"
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

type Handler func(nbBytes int, source, dest string, data []byte)

type UdpSource struct {
	NetInterface    *net.Interface
	HostPort        string
	MaxDatagramSize int
}

func GetNetworkInterface(interfaceName string) *net.Interface {
	gorillaz.Log.Info("Available interfaces:")
	addresses, err := GetNetworkInterfaceAddresses()
	if err != nil {
		panic(err)
	}

	for k, v := range addresses {
		gorillaz.Sugar.Infof("%s - %s", k, strings.Join(v, ","))
	}

	interfaces, e := net.Interfaces()
	if e != nil {
		panic(e)
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

// return the list of IP addresses by network interface
func GetNetworkInterfaceAddresses() (map[string][]string, error) {
	interfaces, e := net.Interfaces()
	if e != nil {
		return nil, e
	}
	res := make(map[string][]string)

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
			res[strings.TrimSpace(i.Name)] = addresses
		} else {
			gorillaz.Log.Warn("error while getting addresses from interface", zap.String("interface", i.Name), zap.Error(err))
		}
	}
	return res, nil
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
		err = StreamToUdp(context.Background(), consumer, hostPort)
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
		err = StreamToUdp(context.Background(), consumer, hostPort)
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

func StreamToUdp(ctx context.Context, stream EventStream, hostPort string) error {
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

	for {
		select {
		case msg := <-stream.EvtChan():
			_, err := c.Write(msg.Value)
			if err != nil {
				gorillaz.Log.Error("Error while writing to UDP ", zap.Error(err))
			}
		case <-ctx.Done():
			gorillaz.Log.Debug("Stopping udp publication", zap.String("destination", hostPort))
			return nil
		}
	}
}

func BroadcasterToUdp(ctx context.Context, broadcaster *mux.Broadcaster, hostPort string) error {
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
	cli := make(chan interface{})
	broadcaster.Register(cli)
	defer broadcaster.Unregister(cli)

	for {
		select {
		case msg := <-cli:
			_, err := c.Write(msg.(*stream.Event).Value)
			if err != nil {
				gorillaz.Log.Error("Error while writing to UDP ", zap.Error(err))
			}
		case <-ctx.Done():
			gorillaz.Log.Debug("Stopping udp publication", zap.String("destination", hostPort))
			return nil
		}
	}
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

//returns the last address of an IPNet (i.e. broadcast address)
func LastAddr(n *net.IPNet) (net.IP, error) {
	if n.IP.To4() == nil {
		return net.IP{}, errors.New("does not support IPv6 addresses")
	}
	ip := make(net.IP, len(n.IP.To4()))
	binary.BigEndian.PutUint32(ip, binary.BigEndian.Uint32(n.IP.To4())|^binary.BigEndian.Uint32(net.IP(n.Mask).To4()))
	return ip, nil
}

func GetHostAndPort(hostPort string) (string, uint16, error) {
	sp := strings.Split(hostPort, ":")
	host := ""
	if len(sp) > 0 {
		host = sp[0]
	}
	if len(sp) > 1 {
		p, err := strconv.Atoi(sp[1])
		if err != nil {
			return "", 0, err
		}
		return host, uint16(p), nil
	} else {
		return "", 0, fmt.Errorf("unable to extract port from %s", hostPort)
	}
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
