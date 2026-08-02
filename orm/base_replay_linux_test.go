//go:build linux

package orm

import (
	"context"
	"fmt"
	"net"
	"os"
	"testing"

	"golang.org/x/sys/unix"
)

func TestInheritedReplayDatabaseDialerConsumesOnlyPinnedDescriptors(t *testing.T) {
	fds, err := unix.Socketpair(unix.AF_UNIX, unix.SOCK_STREAM, 0)
	if err != nil {
		t.Fatal(err)
	}
	serverFile := os.NewFile(uintptr(fds[1]), "replay-server")
	defer serverFile.Close()
	dial, err := inheritedReplayDatabaseDialer(fmt.Sprintf("%d-%d", fds[0], fds[0]))
	if err != nil {
		t.Fatal(err)
	}
	connection, err := dial(context.Background(), "tcp", "1.1.1.1:5432")
	if err != nil {
		t.Fatal(err)
	}
	defer connection.Close()
	server, err := net.FileConn(serverFile)
	if err != nil {
		t.Fatal(err)
	}
	defer server.Close()
	if _, err = connection.Write([]byte("pinned")); err != nil {
		t.Fatal(err)
	}
	buffer := make([]byte, 6)
	if _, err = server.Read(buffer); err != nil || string(buffer) != "pinned" {
		t.Fatalf("read pinned descriptor: %q %v", buffer, err)
	}
	if _, err = dial(context.Background(), "tcp", "127.0.0.1:5432"); err == nil {
		t.Fatal("exhausted replay descriptor range was reused")
	}
}

func TestInheritedReplayDatabaseDialerRejectsInvalidRange(t *testing.T) {
	for _, value := range []string{"", "2-3", "5-4", "a-b", "3-999"} {
		if _, err := inheritedReplayDatabaseDialer(value); err == nil {
			t.Fatalf("invalid descriptor range accepted: %q", value)
		}
	}
}
