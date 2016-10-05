// Package cli handles requests and promote changes to  peers.
package cli

import (
	"fmt"
	"log"
	"os"
	"strconv"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/grpclog"

	//	"github.com/jusongchen/gRPC-demo/cli"

	pb "github.com/jusongchen/gRPC-demo/clusterpb"
	"github.com/pkg/errors"
	"golang.org/x/net/context"
	"golang.org/x/sync/errgroup"
)

type peerStatus int32

const (
	//PEER_UNKOWN peerStatus = 0
	PEER_UNKOWN peerStatus = 0

	//PEER_ACTIVE       peerStatus = 1
	PEER_ACTIVE peerStatus = 1

	//PEER_DISCONNECTED         peerStatus = 1
	PEER_DISCONNECTED peerStatus = 2
)

var peerStatusName = map[int32]string{
	0: "INIT",
	1: "ACTIVE",
	2: "DISCONNECTED",
}

//Peer type
type Peer struct {
	Node       pb.Node
	RPCClient  pb.SyncUpClient
	ClientConn *grpc.ClientConn
	Status     peerStatus
	// in               chan *pb.ChatMsg
	LastRPCErr       error
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

//GetStatus referred by console template
func (p *Peer) GetStatus() string {
	return peerStatusName[int32(p.Status)]
}

//Addr referred by console template
func (p *Peer) Addr() string {
	// peer.Addr = fmt.Sprintf("%s:%d", peer.Node.Hostname, peer.Node.RPCPort)
	return p.Node.Hostname + ":" + strconv.Itoa(int(p.Node.RPCPort))
}

//IsActive used by console template
func (p *Peer) IsActive() bool {
	return p.Status == PEER_ACTIVE
}

func (c *Client) activePeers() []*Peer {
	p := []*Peer{}
	for i := range c.Peers {
		if c.Peers[i].Status == PEER_ACTIVE {
			p = append(p, &c.Peers[i])
		}
	}
	return p
}

//NodeChange handles node change request
func (c *Client) NodeChange(ctx context.Context, req *pb.NodeChgRequest, opts ...grpc.CallOption) error {

	// ch := make(chan *nodeChgResult, len(c.Peers))
	g, ctx := errgroup.WithContext(ctx)

	for _, p := range c.activePeers() {
		rpcClient := p.RPCClient
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

	//[TODO] simplified version, only query the first active peer
	p := c.activePeers()
	if p == nil {
		return &pb.NodeQryResponse{}, errors.Errorf("nodeQuery:%v no active peers", c.Node)
	}

	res, err := p[0].RPCClient.NodeQuery(ctx, req)
	if err != nil {
		return &pb.NodeQryResponse{}, err
	}

	return &pb.NodeQryResponse{Nodes: res.Nodes}, nil
}

//Ping not exported
func (c *Client) Ping(ctx context.Context, req *pb.PingRequest, opts ...grpc.CallOption) (*pb.PingResponse, error) {
	return nil, nil
}

//ConnToPeers make connections to peers
func (c *Client) ConnToPeers(joinTo *pb.Node) error {
	//first node
	if joinTo == nil {
		return nil
	}

	//add first peer, we do not know the console port yet. Once we get all peer list, console port will be updated
	if err := c.upSertPeer(joinTo); err != nil {
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
		errAdd := c.upSertPeer(n)
		if errAdd != nil {
			err = errAdd
		}

	}
	return err
}

//FindPeerByNode search and find matched peer
func (c *Client) FindPeerByNode(n *pb.Node) *Peer {
	for i := range c.Peers {
		p := &c.Peers[i]
		node := p.Node
		if node.Hostname == n.Hostname && node.RPCPort == n.RPCPort {
			return p
		}
	}
	return nil
}

func (c *Client) reportPeerStatus() {
	log.Println("Peers update:")
	for i := range c.Peers {
		log.Printf("Peer %v status %s", c.Peers[i].Node, c.Peers[i].GetStatus())
	}
}

//
//UpdatePeer update peers
func (c *Client) UpdatePeer(req *pb.NodeChgRequest) error {

	switch req.Operation {
	case pb.NodeChgRequest_JOIN, pb.NodeChgRequest_ADD:
		return c.upSertPeer(req.Node)
	case pb.NodeChgRequest_DROP:
		return c.DisconnectPeer(req.Node)
	default:
		return errors.Errorf("Unkown req operation %v", req.Operation)
	}
}

//DisconnectPeer disconnects the peer
func (c *Client) DisconnectPeer(n *pb.Node) error {
	defer c.reportPeerStatus()

	//check if client has already registered, change Console port
	p := c.FindPeerByNode(n)

	if p == nil {
		return errors.Errorf("DisconnectPeer:cannot find peer %v", n)
	}
	err := p.ClientConn.Close()
	p.RPCClient = nil
	p.ClientConn = nil
	p.Status = PEER_DISCONNECTED
	return err
}

//
//UpdatePeers update peers
func (c *Client) upSertPeer(n *pb.Node) error {
	defer c.reportPeerStatus()

	//check if client has already registered, change Console port
	peer2Modify := c.FindPeerByNode(n)
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
		peer2Modify.RPCClient = rpcClient
		return nil
	}

	//new peer
	peer := Peer{
		Node:       *n,
		ClientConn: conn,
		Status:     PEER_ACTIVE,
		RPCClient:  rpcClient,
		// in:         make(chan *pb.ChatMsg),
	}

	c.Peers = append(c.Peers, peer)
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

	activePeers := c.activePeers()
	if len(activePeers) == 0 {
		return fmt.Errorf("Not connected to any peers.")
	}
	ctx := context.Background()

	ch := make(chan *pb.DataChgSummary, len(activePeers))

	for _, p := range activePeers {

		//peer connection not established yet
		if p.RPCClient == nil {
			continue
		}

		go func(client pb.SyncUpClient, records []*pb.ChatMsg) {
			//	DataChange(ctx context.Context, opts ...grpc.CallOption) (SyncUp_DataChangeClient, error)
			startTime := time.Now()

			stream, err := client.DataChange(ctx)
			if err != nil {
				grpclog.Fatal(err)
			}

			for _, row := range records {
				if err := stream.Send(row); err != nil {
					grpclog.Fatal(err)
				}
			}

			chgSummary, err := stream.CloseAndRecv()
			if err != nil {
				grpclog.Fatal(err)
			}
			// grpclog.Printf("Change summary: %v", chgSummary)
			endTime := time.Now()

			chgSummary.ElapsedTime = uint64(endTime.Sub(startTime))
			ch <- chgSummary
		}(p.RPCClient, records)
	}

	lastMsgSentAt := time.Now()
	for _, p := range c.activePeers() {
		r := <-ch
		lastStatus := fmt.Sprintf("%v:%d rows sync'ed to server %v in %d seconds (process rate:%f rows/second).\n", time.Now(),
			r.RecordCount, p.Node, r.ElapsedTime, float64(r.RecordCount)/float64(r.ElapsedTime))
		log.Print(lastStatus)

		p.LastMsgSentAt = lastMsgSentAt
		p.NumMsgSentLast = r.RecordCount
		p.LastSentDuration = time.Duration(r.ElapsedTime)
		p.NumMsgSent += r.RecordCount

	}
	c.LastMsgSentAt = lastMsgSentAt
	return nil
}
