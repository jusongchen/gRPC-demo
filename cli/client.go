// Package cli handles requests and promote changes to  peers.
package cli

import (
	"fmt"
	"log"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/grpclog"

	pb "github.com/jusongchen/gRPC-demo/replica"
	"github.com/pkg/errors"
	"golang.org/x/net/context"
)

//Peer not exported
type Peer struct {
	Addr string
	// lastAliveTime time.Time
	RpcClient  pb.SyncUpClient
	in         chan *pb.Record
	lastRpcErr error
}

//Client not exported
type Client struct {
	OwnAddr string

	//set to true after connected to any peers
	Connected bool
	Peers     []Peer
}

// Server  not exported
type Server struct {
	c *Client
}

type nodeChgResult struct {
	res *pb.NodeChgResponse
	err error
}

// func (c *Client) NodeChange(ctx context.Context, req *pb.NodeChgRequest, opts ...grpc.CallOption) (*pb.NodeChgResponse, error) {
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
			log.Printf("From client%s:%s", c.Peers[i].Addr, r.err.Error())
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
	return &pb.NodeQryResponse{NodeAddr: res.NodeAddr}, nil
}

//Ping not exported
func (c *Client) Ping(ctx context.Context, req *pb.PingRequest, opts ...grpc.CallOption) (*pb.PingResponse, error) {
	return nil, nil
}

func (c *Client) connToPeers(AddrToJoin string) error {
	//first node
	if AddrToJoin == "" {
		return nil
	}
	//add first peer
	if err := c.AddPeer(AddrToJoin); err != nil {
		return errors.Wrapf(err, "JoinCluster addPeer %s fail", AddrToJoin)
	}

	ctx := context.Background()

	//making a Node Join request
	req := &pb.NodeChgRequest{Operation: pb.NodeChgRequest_JOIN, NodeAddr: c.OwnAddr}

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
	req := &pb.NodeQryRequest{NodeAddr: c.OwnAddr}
	res, err := c.NodeQuery(ctx, req)
	if err != nil {
		errors.Wrap(err, "gRPC NodeQuery fail")
		return err
	}

	//for each addresses, add them as peer
	for _, addr := range res.NodeAddr {
		if addr == c.OwnAddr {
			continue
		}
		err_ := c.AddPeer(addr)
		if err_ != nil {
			err = err_
		}

	}
	return err
}

func (c *Client) AddPeer(clientAddr string) error {
	//check if it is this server, skip
	if clientAddr == c.OwnAddr {
		return nil
	}
	//check if client has already registered
	for _, v := range c.Peers {
		if v.Addr == clientAddr {
			return nil
		}
	}

	conn, err := grpc.Dial(clientAddr, grpc.WithInsecure())
	if err != nil {
		return errors.Wrapf(err, "grpc.Dail to %s fail", clientAddr)
	}
	// log.Printf("Connected to peer:%s\n", clientAddr)
	// c.inCluster = true

	peer := Peer{Addr: clientAddr, RpcClient: pb.NewSyncUpClient(conn), in: make(chan *pb.Record)}

	c.Peers = append(c.Peers, peer)
	c.Connected = true
	log.Printf("Server %s: peer update:", c.OwnAddr)
	for i := range c.Peers {
		log.Printf("Connect to peer %s", c.Peers[i].Addr)
	}
	return nil
}

func (c *Client) MonitorAndPromoteChg() error {
	// ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second) // HL
	// defer cancel()

	changeChannel := make(chan chRowSet)
	go func() {
		err := c.getChgData(changeChannel)
		if err != nil {
			log.Fatal(err)
		}
	}()

	for {
		select {
		//there are changes coming in
		case dataCh := <-changeChannel:

			// fmt.Printf("\n%s generate data changes request:\n", c.OownAddr)

			ctx := context.Background()
			err := c.DataChange(ctx, dataCh)
			if err != nil {
				return errors.Wrap(err, "client.DataChange()")
			}
		}
	}
	close(changeChannel)
	return nil
}

type chRowSet chan *pb.Record

func (c *Client) getChgData(ch chan<- chRowSet) error {
	// db.GetChanges()

	for batch := 0; ; batch++ {
		//do nothing if not connected to any peer , Or
		if !c.Connected {
			log.Printf("Not connected to any peers.\n")
			time.Sleep(time.Second)
			continue
		}

		dataCh := make(chRowSet)
		//pass out new data channel

		ch <- dataCh

		//populate data
		// dataCh <- &chRowSet

		//all data populated
		close(dataCh)

	}
	return nil
}

// Change issues Change RPCs in parallel to the peers and get change status.
func (c *Client) DataChange(ctx context.Context, chRows <-chan *pb.Record, opts ...grpc.CallOption) error {
	if !c.Connected {
		return fmt.Errorf("Not connected to any peers.")
	}

	ch := make(chan *pb.DataChgSummary, len(c.Peers))

	//replicate changes to peer channels
	for i := range c.Peers {
		c.Peers[i].in = make(chan *pb.Record)
	}

	go func() {
		rowCnt := 0
		for r := range chRows {
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

		go func(client pb.SyncUpClient, in <-chan *pb.Record) {
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

			chgSummary.ElapsedTime = int32(endTime.Sub(startTime).Seconds())
			ch <- chgSummary
		}(b.RpcClient, b.in)
	}

	for _, p := range c.Peers {
		r := <-ch
		log.Printf("\n")
		log.Printf("%d rows sync'ed to server %s in %d seconds (process rate:%f rows/second).\n",
			r.RecordCount, p.Addr, r.ElapsedTime, float64(r.RecordCount)/float64(r.ElapsedTime))
	}
	return nil
}