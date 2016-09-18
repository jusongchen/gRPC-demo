// TDatahe svr command runs a syncUp Server that promote changes and handle changes to/from multiple peers.
package main

import (
	"flag"
	"fmt"
	"log"
	"net"
	_ "net/http/pprof"
	"os"
	"os/signal"
	"sync"
	"syscall"

	"google.golang.org/grpc"

	"github.com/jusongchen/gRPC-demo/cli"
	pb "github.com/jusongchen/gRPC-demo/replica"
	"github.com/jusongchen/gRPC-demo/svr"
)

var (
	// peers = flag.String("peers", "localhost:36061,localhost:36062", "comma-separated peer addresses")
	serverPort = flag.Int("p", 36060, "the port to bind to.")
	joinTo     = flag.String("join", "",
		`       A address to use when a new node is joining an existing cluster. 
		For the first node in a cluster, -join should NOT be specified.`)
	consolePort = flag.Int("ConsolePort", 8080, "the port for the console to bind to.")
)

func main() {
	flag.Parse()

	go handleSignals()

	client := cli.NewClient(*joinTo, *serverPort, *consolePort)

	go func() {
		//launch the server goroutine
		lis, err := net.Listen("tcp", fmt.Sprintf(":%d", *serverPort)) // RPC port
		if err != nil {
			log.Fatalf("\nfailed to listen: %v", err)
		}

		g := grpc.NewServer()

		// s := &svr.Server{c: &client}
		s := svr.NewServer(client)

		pb.RegisterSyncUpServer(g, s)
		g.Serve(lis)
	}()

	log.Fatal(client.Run())
}

func handleSignals() {
	var wg sync.WaitGroup
	wg.Add(1)

	signalCh := make(chan os.Signal, 1)
	signal.Notify(signalCh, os.Interrupt, os.Kill)
	signal.Notify(signalCh, syscall.SIGTERM, syscall.SIGTRAP)
	// signal.Notify(signalCh, os.Interrupt)
	go func() {
		<-signalCh

		fmt.Printf("\n processing signal")
		wg.Done()
	}()
	wg.Wait()
}
