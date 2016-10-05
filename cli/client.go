// Package cli handles requests and promote changes to  peers.
package cli

import (
	"fmt"
	"log"
	"os"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/grpclog"

	pb "github.com/jusongchen/gRPC-demo/clusterpb"
	"github.com/pkg/errors"
	"golang.org/x/net/context"
	"golang.org/x/sync/errgroup"
)

type peerStatus int32

const (
	//PEER_ACTIVE       peerStatus = 0
	PEER_ACTIVE peerStatus = 0
	// PEER_QUIT         peerStatus = 1
	PEER_QUIT peerStatus = 1
	// PEER_DISCONNECTED peerStatus = 2
	PEER_DISCONNECTED peerStatus = 2
)

var peerStatus_name = map[int32]string{
	0: "ACTIVE",
	1: "QUIT",
	2: "DISCONNECTED",
}

//Peer not exported
type Peer struct {
	Node             pb.Node
	Addr             string
	RpcClient        pb.SyncUpClient
	ClientConn       *grpc.ClientConn
	Status           peerStatus
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

func (p *Peer) GetStatus() string {
	return peerStatus_name[int32(p.Status)]
}

func (p *Peer) IsActive() bool {
	return p.Status == PEER_ACTIVE
}

//NodeChange handles node change request
func (c *Client) NodeChange(ctx context.Context, req *pb.NodeChgRequest, opts ...grpc.CallOption) error {

	// ch := make(chan *nodeChgResult, len(c.Peers))
	g, ctx := errgroup.WithContext(ctx)

	for i := range c.Peers {
		rpcClient := c.Peers[i].RpcClient
		if rpcClient == nil {
			continue //NOT a ACTIVE peer
		}
		g.Go(func() error {
			res, err := rpcClient.NodeChange(ctx, req)
			if err == nil || res == nil {
				return err
			}
			return errors.Wrap(err, res.ErrMsg)
		})
	}
	return g.Wait()
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
	ctx, _ := context.WithTimeout(context.Background(), 100*time.Millisecond)

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

func (c *Client) findPeerToModify(n *pb.Node) *Peer {

	for i := range c.Peers {
		p := &c.Peers[i]
		node := p.Node
		if node.Hostname == n.Hostname && node.RPCPort == n.RPCPort {
			return p
		}
	}
	return nil
}

func (c *Client) AddPeer(n *pb.Node) error {

	//check if client has already registered, change Console port
	peer2Modify := c.findPeerToModify(n)
	if peer2Modify != nil {
		peer2Modify.Node.ConsolePort = n.ConsolePort
		if peer2Modify.Status == PEER_ACTIVE { //nothing more to do if this is alread active
			return nil
		}
	}

	//do not add this own server
	if n.Hostname == c.Node.Hostname && n.RPCPort == c.Node.RPCPort {
		return nil
	}

	clientAddr := fmt.Sprintf("%s:%d", n.Hostname, n.RPCPort)
	conn, err := grpc.Dial(clientAddr, grpc.WithInsecure())
	if err != nil {
		return errors.Wrapf(err, "grpc.Dail to %s fail", clientAddr)
	}

	rpcClient := pb.NewSyncUpClient(conn)

	//peer alread registered
	if peer2Modify != nil {
		peer2Modify.Status = PEER_ACTIVE
		peer2Modify.ClientConn = conn
		peer2Modify.RpcClient = rpcClient
		return nil
	}

	//not peer
	peer := Peer{
		Node:       *n,
		ClientConn: conn,
		Status:     PEER_ACTIVE,
		RpcClient:  rpcClient,
		in:         make(chan *pb.ChatMsg),
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
