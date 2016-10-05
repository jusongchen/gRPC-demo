//Package svr handles gRPC requests from peers
package svr

import (
	"fmt"
	"io"
	"log"
	// _ "net/http/pprof"
	"time"

	"github.com/jusongchen/gRPC-demo/cli"
	pb "github.com/jusongchen/gRPC-demo/clusterpb"
	"github.com/pkg/errors"
	"golang.org/x/net/context"
)

// Server  not exported
type Server struct {
	c                 *cli.Client
	NumMsgReceived    int64
	LastMsgReceivedAt time.Time
}

//NodeChange not exported
func (s *Server) NodeChange(ctx context.Context, req *pb.NodeChgRequest) (*pb.NodeChgResponse, error) {
	// fmt.Printf("\nServer get Node change request:%#v\n", req)

	switch req.Operation {
	case pb.NodeChgRequest_JOIN:
		//notify all clients to add Node
		s.c.NodeChange(ctx, &pb.NodeChgRequest{Operation: pb.NodeChgRequest_ADD, Node: req.Node})

		//add this new node as peer
		if err := s.c.UpdatePeer(req); err != nil {
			return &pb.NodeChgResponse{ErrMsg: err.Error()}, errors.Wrap(err, "Server:NodeChgJoin:AddPeer fail")
		}
		return &pb.NodeChgResponse{}, nil

	case pb.NodeChgRequest_ADD:

		//add this new node as peer
		if err := s.c.UpdatePeer(req); err != nil {
			resp := pb.NodeChgResponse{ErrMsg: fmt.Sprintf("Server.NodeChange:AddPeer %v failed", *req.Node)}
			return &resp, errors.Wrap(err, "Server:NodeChgJoin:AddPeer fail")
		}
		return &pb.NodeChgResponse{}, nil

	case pb.NodeChgRequest_DROP:
		var err error
		//identify the peer and mark its status as quit
		err = s.c.UpdatePeer(req)
		if err == nil {
			return &pb.NodeChgResponse{}, nil
		}
		return &pb.NodeChgResponse{ErrMsg: err.Error()}, err

	default:
		log.Fatalf("Server NodeChange:unknown Operation %v", req.Operation)
	}

	return &pb.NodeChgResponse{}, nil
}

//NodeQuery to implement pb.SyncUpServer
func (s *Server) NodeQuery(ctx context.Context, req *pb.NodeQryRequest) (*pb.NodeQryResponse, error) {
	// fmt.Printf("\nServer get Node query request:%#v\n", req)
	// rpcAddr := fmt.Sprintf("%s:%d", s.c.Hostname, s.c.RPCPort)

	//return this server's own address as well
	nodes := append([]*pb.Node{}, &s.c.Node)

	// nodes = append(nodes, &n)
	for i := range s.c.Peers {
		nodes = append(nodes, &s.c.Peers[i].Node)
	}
	// for i := range nodes {
	// 	log.Printf("Server %v return peers %#v\n", s.c.Node, *nodes[i])
	// }

	return &pb.NodeQryResponse{Nodes: nodes}, nil
}

//Ping impletes pb.SyncUpServer
func (s *Server) Ping(ctx context.Context, req *pb.PingRequest) (*pb.PingResponse, error) {

	return nil, nil
}

//DataChange implements pb.SyncUpServer
func (s *Server) DataChange(stream pb.SyncUp_DataChangeServer) error {

	var rowCount int64
	defer func() {
		s.NumMsgReceived += rowCount
		s.LastMsgReceivedAt = time.Now()
	}()
	startTime := time.Now()
	for {
		record, err := stream.Recv()

		if err == io.EOF {
			endTime := time.Now()
			log.Printf("Server %v received %d records and sync'ed them to DB.", s.c.Node, rowCount)
			return stream.SendAndClose(&pb.DataChgSummary{
				RecordCount: rowCount,
				ElapsedTime: uint64(endTime.Sub(startTime)),
			})
		}
		if err != nil {
			return err
		}
		// fmt.Printf("Server %s received record %v\n", s.c.OwnAddr, record)
		if err := s.insert2DB(record); err != nil {
			return errors.Wrap(err, "Server DataChange() fail")
		}
		rowCount++
	}
}

func (s *Server) insert2DB(r *pb.ChatMsg) error {

	//For now, just write to stdout
	fmt.Printf("received:%v", r)
	return nil
}

//NewServer create an Server instance
func NewServer(client *cli.Client) *Server {
	return &Server{c: client}
}
