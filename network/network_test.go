package network

import (
	"fmt"
	"net"
	"testing"
)

const expectedMac = "01:00:5e:00:00:17"

func TestIpMulticastToMac(t *testing.T) {
	//multiple IP addresses can map to the same mac address
	mac, err := MulticastIpToMac(net.ParseIP("224.0.0.23"))
	assertNil(t, err)
	checkExpected(t, mac, expectedMac)
	mac, err = MulticastIpToMac(net.ParseIP("224.128.0.23"))
	assertNil(t, err)
	checkExpected(t, mac, expectedMac)
	mac, err = MulticastIpToMac(net.ParseIP("225.0.0.23"))
	assertNil(t, err)
	checkExpected(t, mac, expectedMac)
	mac, err = MulticastIpToMac(net.ParseIP("238.128.0.23"))
	assertNil(t, err)
	checkExpected(t, mac, expectedMac)
	mac, err = MulticastIpToMac(net.ParseIP("224.0.0.59"))
	assertNil(t, err)
	checkExpected(t, mac, "01:00:5e:00:00:3b")
	mac, err = MulticastIpToMac(net.ParseIP("238.128.0.59"))
	assertNil(t, err)
	checkExpected(t, mac, "01:00:5e:00:00:3b")

	_, err = MulticastIpToMac(net.ParseIP("192.168.0.5"))
	if err == nil {
		t.Error("Expected an error on a non multicast IP")
	}

}

func checkExpected(t *testing.T, mac net.HardwareAddr, expected string) {
	if mac.String() != expected {
		t.Error(fmt.Sprintf("Expected %s got %s", expected, mac.String()))
	}
}

func assertNil(t *testing.T, err error) {
	if err != nil {
		t.Error(err)
	}
}
