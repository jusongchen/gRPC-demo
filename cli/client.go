// Package cli handles requests and promote changes to  peers.
package cli

import (
	"fmt"
	"log"
	"os"
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
	Node          pb.Node
	LastMsgSentAt time.Time
	Peers         []Peer
}

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

func (c *Client) nodeQuery(ctx context.Context, req *pb.NodeQryRequest, opts ...grpc.CallOption) (*pb.NodeQryResponse, error) {

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

func (c *Client) ConnToPeers(joinTo *pb.Node) error {
	//first node
	if joinTo == nil {
		return nil
	}

	//add first peer, we do not know the console port yet. Once we get all peer list, console port will be updated
	if err := c.AddPeer(joinTo); err != nil {
		return errors.Wrapf(err, "JoinCluster addPeer %v fail", joinTo)
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

	return c.queryAndAddPeers(joinTo)
}

func (c *Client) queryAndAddPeers(joinTo *pb.Node) error {

	ctx := context.Background()

	//making a Node Query Request to get a list of nodes from the peer this node joined to
	// fmt.Sprintf("%s:%d", c.Node.Hostname, c.Node.RPCPort)
	req := &pb.NodeQryRequest{ScrNode: joinTo}
	res, err := c.nodeQuery(ctx, req)
	if err != nil {
		errors.Wrapf(err, "QueryAndAddPeers:%v", c.Node)
		return err
	}

	//for each addresses, add them as peer
	for _, n := range res.Nodes {
		// log.Printf("%v:Get peers:%v\n", c.Node, n)
		errAdd := c.AddPeer(n)
		if errAdd != nil {
			err = errAdd
		}

	}
	return err
}

func (c *Client) AddPeer(n *pb.Node) error {

	//check if client has already registered, change Console port
	for i := range c.Peers {
		node := &c.Peers[i].Node
		if node.Hostname == n.Hostname && node.RPCPort == n.RPCPort {
			node.ConsolePort = n.ConsolePort
			return nil
		}
	}

	//do not add this node
	if n.Hostname == c.Node.Hostname && n.RPCPort == c.Node.RPCPort {
		return nil
	}

	clientAddr := fmt.Sprintf("%s:%d", n.Hostname, n.RPCPort)
	conn, err := grpc.Dial(clientAddr, grpc.WithInsecure())
	if err != nil {
		return errors.Wrapf(err, "grpc.Dail to %s fail", clientAddr)
	}
	// log.Printf("Connected to peer:%s\n", clientAddr)
	// c.inCluster = true

	peer := Peer{
		Node:      *n,
		RpcClient: pb.NewSyncUpClient(conn),
		in:        make(chan *pb.ChatMsg),
	}
	peer.Addr = fmt.Sprintf("%s:%d", peer.Node.Hostname, peer.Node.RPCPort)
	c.Peers = append(c.Peers, peer)
	log.Printf("Server %v: peer update:", c.Node)
	for i := range c.Peers {
		log.Printf("Connect to peer %v", c.Peers[i].Node)
	}
	return nil
}

// NewClient creates a NewClient instance
func NewClient(serverPort, consolePort int32) *Client {

	hostname, _ := os.Hostname()

	c := Client{
		Node: pb.Node{
			RPCPort:     serverPort,
			Hostname:    hostname,
			ConsolePort: consolePort,
		},
	}

	return &c
}

//PromoteDataChange makes  RPC calls in parallel to the peers and get change status.
func (c *Client) PromoteDataChange(records []*pb.ChatMsg) error {

	if len(c.Peers) == 0 {
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
				grpclog.Fatalf("%c.Peers[i].DataChange(_) = _, %v", client, err)
			}

			for row := range in {
				if err := stream.Send(row); err != nil {
					grpclog.Fatalf("%c.Peers[i].Send(%v) = %v", stream, *row, err)
				}
			}

			chgSummary, err := stream.CloseAndRecv()
			if err != nil {
				grpclog.Fatalf("%c.Peers[i].CloseAndRecv() got error %v, want %v", stream, err, nil)
			}
			// grpclog.Printf("Change summary: %v", chgSummary)
			endTime := time.Now()

			chgSummary.ElapsedTime = uint64(endTime.Sub(startTime))
			ch <- chgSummary
		}(b.RpcClient, b.in)
	}

	lastMsgSentAt := time.Now()
	for i := range c.Peers {
		p := &c.Peers[i]
		r := <-ch
		lastStatus := fmt.Sprintf("%v:%d rows sync'ed to server %s in %d seconds (process rate:%f rows/second).\n", time.Now(),
			r.RecordCount, p.Addr, r.ElapsedTime, float64(r.RecordCount)/float64(r.ElapsedTime))
		log.Print(lastStatus)

		p.LastMsgSentAt = lastMsgSentAt
		p.NumMsgSentLast = r.RecordCount
		p.LastSentDuration = time.Duration(r.ElapsedTime)
		p.NumMsgSent += r.RecordCount

	}
	c.LastMsgSentAt = lastMsgSentAt
	return nil
}
