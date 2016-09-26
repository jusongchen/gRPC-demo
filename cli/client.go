// Package cli handles requests and promote changes to  peers.
package cli

import (
	"fmt"
	"log"
	"os"
	"strconv"
	"strings"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/grpclog"

	pb "github.com/jusongchen/gRPC-demo/replica"
	"github.com/pkg/errors"
	"golang.org/x/net/context"
)

//Peer not exported
type Peer struct {
	Node             pb.Node
	Addr             string
	RpcClient        pb.SyncUpClient
	in               chan *pb.ChatMsg
	LastRpcErr       error
	NumMsgSent       int64
	LastMsgSentAt    time.Time
	NumMsgSentLast   int64
	LastSentDuration time.Duration
}

//Client not exported
type Client struct {
	Node pb.Node
	//set to true after connected to any peers
	Connected bool
	Peers     []Peer
}

// Server  not exported
// type Server struct {
// 	c *Client
// }

// type InstanceStat struct {
// 	*Client
// 	nMsgReceived    int
// 	lastMsgReceived time.Time
// }

type nodeChgResult struct {
	res *pb.NodeChgResponse
	err error
}

//NodeChange handles node change request
func (c *Client) NodeChange(ctx context.Context, req *pb.NodeChgRequest, opts ...grpc.CallOption) error {

	ch := make(chan *nodeChgResult, len(c.Peers))

	for _, b := range c.Peers {
		go func(peer pb.SyncUpClient) {
			res, err := peer.NodeChange(ctx, req)
			ch <- &nodeChgResult{res, err}
		}(b.RpcClient)
	}

	// chgResp := pb.NodeChgResponse{}

	var err error
	for i := range c.Peers {
		// res []*pb.NodeChgResponse
		r := <-ch
		if r.err != nil {
			log.Printf("From client %v: %s", c.Peers[i].Node, r.err.Error())
			err = r.err
		}
	}
	return err
	// return &pb.NodeChgResponse{Success:true}, nil
}

func (c *Client) NodeQuery(ctx context.Context, req *pb.NodeQryRequest, opts ...grpc.CallOption) (*pb.NodeQryResponse, error) {
	//[TODO] simplified version, only query the first peer
	res, err := c.Peers[0].RpcClient.NodeQuery(ctx, req)
	if err != nil {
		return &pb.NodeQryResponse{}, err
	}

	return &pb.NodeQryResponse{Nodes: res.Nodes}, nil
}

//Ping not exported
func (c *Client) Ping(ctx context.Context, req *pb.PingRequest, opts ...grpc.CallOption) (*pb.PingResponse, error) {
	return nil, nil
}

func (c *Client) connToPeers(hostname string, RPCPort int32) error {
	//first node
	if hostname == "" {
		return nil
	}
	//add first peer, we do not know the console port yet. Once we get all peer list, console port will be updated
	n := pb.Node{
		Hostname:    hostname,
		RPCPort:     RPCPort,
		ConsolePort: 0,
	}

	if err := c.AddPeer(n); err != nil {
		return errors.Wrapf(err, "JoinCluster addPeer %v fail", n)
	}

	ctx := context.Background()

	//making a Node Join request
	// addr := fmt.Sprintf("%s:%d", c.Node.Hostname, c.Node.RPCPort)
	req := &pb.NodeChgRequest{Operation: pb.NodeChgRequest_JOIN, Node: &c.Node}

	err := c.NodeChange(ctx, req)
	if err != nil {
		err = errors.Wrap(err, "rpc NodeChange fail")
		return err
	}

	return c.QueryAndAddPeers()

}

func (c *Client) QueryAndAddPeers() error {

	ctx := context.Background()

	//making a Node Query Request to get a list of nodes from the peer this node joined to
	// fmt.Sprintf("%s:%d", c.Node.Hostname, c.Node.RPCPort)
	req := &pb.NodeQryRequest{ScrNode: &c.Node}
	res, err := c.NodeQuery(ctx, req)
	if err != nil {
		errors.Wrap(err, "gRPC NodeQuery fail")
		return err
	}

	//for each addresses, add them as peer
	for _, n := range res.Nodes {
		// rpcAddr := fmt.Sprintf("%s:%d", c.Node.Hostname, c.Node.RPCPort)
		// no need to skip, add node will check if the node exists or not
		// if n.Hostname == c.Node.Hostname && n.RPCPort == c.Node.RPCPort {
		// 	continue
		// }
		errAdd := c.AddPeer(*n)
		if errAdd != nil {
			err = errAdd
		}

	}
	return err
}

// func (c *Client) AddPeerByAddr(addr string) error {

// 	port, _ := strconv.Atoi(strings.Split(addr, ":")[1])

// 	n := pb.Node{
// 		Hostname:    strings.Split(addr, ":")[0],
// 		RPCPort:     int32(port),
// 		ConsolePort: 0,
// 	}
// 	return c.AddPeer(&n)
// }

func (c *Client) AddPeer(n pb.Node) error {
	//check if it is this server, skip
	// rpcAddr := fmt.Sprintf("%s:%d", c.Node.Hostname, c.Node.RPCPort)
	if c.Node.Hostname == n.Hostname && c.Node.RPCPort == n.RPCPort {
		return nil
	}
	//check if client has already registered, change Console port
	for i, v := range c.Peers {
		if v.Node.Hostname == n.Hostname && v.Node.RPCPort == n.RPCPort {
			c.Peers[i].Node.ConsolePort = v.Node.ConsolePort
			return nil
		}
	}
	clientAddr := fmt.Sprintf("%s:%d", n.Hostname, n.RPCPort)
	conn, err := grpc.Dial(clientAddr, grpc.WithInsecure())
	if err != nil {
		return errors.Wrapf(err, "grpc.Dail to %s fail", clientAddr)
	}
	// log.Printf("Connected to peer:%s\n", clientAddr)
	// c.inCluster = true

	peer := Peer{
		Node: n,
		// Hostname:    n.Hostname,
		// RPCPort:     n.RPCPort,
		// ConsolePort: n.ConsolePort,
		RpcClient: pb.NewSyncUpClient(conn),
		in:        make(chan *pb.ChatMsg),
	}
	peer.Addr = fmt.Sprintf("%s:%d", peer.Node.Hostname, peer.Node.RPCPort)
	c.Peers = append(c.Peers, peer)
	c.Connected = true
	// log.Printf("Server %s: peer update:", c.Node.RPCPort)
	for i := range c.Peers {
		log.Printf("Connect to peer %v", c.Peers[i].Node)
	}
	return nil
}

// NewClient creates a NewClient instance
func NewClient(joinTo string, serverPort, consolePort int32) *Client {

	hostname, _ := os.Hostname()

	c := Client{
		Node: pb.Node{
			RPCPort:     serverPort,
			Hostname:    hostname,
			ConsolePort: consolePort,
		},
	}
	//launch the client go rountine
	go func() {

		if len(joinTo) == 0 { //first node, no peer to connect
			return
		}

		RPCPort, err := strconv.Atoi(strings.Split(joinTo, ":")[1])
		if err != nil {
			log.Fatal(errors.Wrapf(err, "joinTo not a valid address:%s", joinTo))
		}
		hostname := strings.Split(joinTo, ":")[0]
		if err := c.connToPeers(hostname, int32(RPCPort)); err != nil {
			log.Fatal(err)
		}
	}()
	return &c
}

//PromoteDataChange makes  RPC calls in parallel to the peers and get change status.
func (c *Client) PromoteDataChange(records []*pb.ChatMsg) error {

	if !c.Connected {
		return fmt.Errorf("Not connected to any peers.")
	}
	ctx := context.Background()

	ch := make(chan *pb.DataChgSummary, len(c.Peers))
	//replicate changes to peer channels
	for i := range c.Peers {
		c.Peers[i].in = make(chan *pb.ChatMsg)
	}

	go func() {
		rowCnt := 0
		for _, r := range records {
			rowCnt++
			// fmt.Printf("replicate row:%v\n", r)
			for i := range c.Peers {
				c.Peers[i].in <- r
			}
		}
		for i := range c.Peers {
			close(c.Peers[i].in)
		}
	}()
	// for r := range chRows {
	// 	for i := range c.Peers {
	// 		c.Peers[i].in <- r
	// 	}
	// }

	for _, b := range c.Peers {

		//peer connection not established yet
		if b.RpcClient == nil {
			continue
		}

		go func(client pb.SyncUpClient, in <-chan *pb.ChatMsg) {
			//	DataChange(ctx context.Context, opts ...grpc.CallOption) (SyncUp_DataChangeClient, error)
			startTime := time.Now()

			stream, err := client.DataChange(ctx)
			if err != nil {
				grpclog.Fatalf("%v.DataChange(_) = _, %v", client, err)
			}

			for row := range in {
				if err := stream.Send(row); err != nil {
					grpclog.Fatalf("%v.Send(%v) = %v", stream, *row, err)
				}
			}

			chgSummary, err := stream.CloseAndRecv()
			if err != nil {
				grpclog.Fatalf("%v.CloseAndRecv() got error %v, want %v", stream, err, nil)
			}
			// grpclog.Printf("Change summary: %v", chgSummary)
			endTime := time.Now()

			chgSummary.ElapsedTime = uint64(endTime.Sub(startTime))
			ch <- chgSummary
		}(b.RpcClient, b.in)
	}

	for i := range c.Peers {
		p := &c.Peers[i]
		r := <-ch
		lastStatus := fmt.Sprintf("%v:%d rows sync'ed to server %s in %d seconds (process rate:%f rows/second).\n", time.Now(),
			r.RecordCount, p.Addr, r.ElapsedTime, float64(r.RecordCount)/float64(r.ElapsedTime))
		log.Print(lastStatus)

		p.LastMsgSentAt = time.Now()
		p.NumMsgSentLast = r.RecordCount
		p.LastSentDuration = time.Duration(r.ElapsedTime)
		p.NumMsgSent += r.RecordCount

	}
	return nil
}
