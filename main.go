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
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"golang.org/x/net/context"

	"google.golang.org/grpc"

	"github.com/jusongchen/gRPC-demo/cli"
	pb "github.com/jusongchen/gRPC-demo/clusterpb"
	"github.com/jusongchen/gRPC-demo/console"
	"github.com/jusongchen/gRPC-demo/svr"
	"github.com/pkg/errors"
)

var (
	// peers = flag.String("peers", "localhost:36061,localhost:36062", "comma-separated peer addresses")
	serverPort = flag.Int("p", 36060, "the port to bind to.")
	joinTo     = flag.String("join", "",
		`       A address to use when a new node is joining an existing cluster. 
		For the first node in a cluster, -join should NOT be specified.`)
	consolePort = flag.Int("ConsolePort", 8080, "the port for the console to bind to.")
)

func handleSignals(c *cli.Client) {
	var wg sync.WaitGroup
	wg.Add(1)

	signalCh := make(chan os.Signal, 1)
	signal.Notify(signalCh, os.Interrupt, os.Kill)
	signal.Notify(signalCh, syscall.SIGTERM, syscall.SIGTRAP)
	// signal.Notify(signalCh, os.Interrupt)
	go func() {
		<-signalCh
		fmt.Printf("\nNotifying peers this node is about to quit ...")

		req := pb.NodeChgRequest{
			Operation: pb.NodeChgRequest_DROP,
			Node:      &c.Node,
		}

		ctx, _ := context.WithTimeout(context.Background(), 100*time.Millisecond)
		if err := c.NodeChange(ctx, &req); err != nil {
			log.Fatal(errors.Wrapf(err, "handleSignals:%q NodeChange", *c))
		}

		fmt.Printf("\nPeers notified.")
		wg.Done()
	}()
	wg.Wait()
	fmt.Printf("\nOK to exit now.")
	os.Exit(1)
}

func main() {
	flag.Parse()

	client := cli.NewClient(int32(*serverPort), int32(*consolePort))
	// var server *svr.Server
	server := svr.NewServer(client)

	go func() {
		//launch the server goroutine
		lis, err := net.Listen("tcp", fmt.Sprintf(":%d", *serverPort)) // RPC port
		if err != nil {
			log.Fatalf("\nfailed to listen: %v", err)
		}
		g := grpc.NewServer()
		pb.RegisterSyncUpServer(g, server)

		log.Printf("Starting server, RPC port %d, console port %d ...", *serverPort, *consolePort)
		g.Serve(lis)
	}()

	var joinToNode *pb.Node
	//first node has jointTo as empty string
	if *joinTo != "" {
		RPCPort, err := strconv.Atoi(strings.Split(*joinTo, ":")[1])
		if err != nil {
			log.Fatal(errors.Wrapf(err, "joinTo not a valid address:%s", *joinTo))
		}
		joinToNode = &pb.Node{
			Hostname: strings.Split(*joinTo, ":")[0],
			RPCPort:  int32(RPCPort),
		}
	}
	//need to wait for server to start up first
	time.Sleep(time.Millisecond * 100)
	go handleSignals(client)

	if err := client.ConnToPeers(joinToNode); err != nil {
		log.Fatal(err)
	}

	log.Fatal(console.Start(client, server))
}
