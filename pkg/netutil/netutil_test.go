package netutil

import "testing"

func TestIsLoopbackListen(t *testing.T) {
	if IsLoopbackListen(":50051") || IsLoopbackListen("0.0.0.0:50051") {
		t.Fatal("wildcard bind is not loopback")
	}
	if !IsLoopbackListen("127.0.0.1:50051") {
		t.Fatal("127.0.0.1 should be loopback")
	}
}

func TestIsPrivateOrLoopbackAddr(t *testing.T) {
	if !IsPrivateOrLoopbackAddr("10.0.0.1:1") || !IsPrivateOrLoopbackAddr("192.168.1.2:1") {
		t.Fatal("RFC1918 should be private")
	}
	if IsPrivateOrLoopbackAddr("8.8.8.8:53") {
		t.Fatal("public IP should not be private")
	}
}
