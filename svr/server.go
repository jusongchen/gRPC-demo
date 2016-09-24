// TDatahe svr command runs a syncUp Server that promote changes and handle changes to/from multiple peers.
package svr

import (
	"fmt"
	"io"
	"log"
	_ "net/http/pprof"
	"time"

	"github.com/jusongchen/gRPC-demo/cli"
	pb "github.com/jusongchen/gRPC-demo/replica"
	"github.com/pkg/errors"
	"golang.org/x/net/context"
)

// Server  not exported
type Server struct {
	c *cli.Client
}

//NodeChange not exported
func (s *Server) NodeChange(ctx context.Context, req *pb.NodeChgRequest) (*pb.NodeChgResponse, error) {
	// fmt.Printf("\nServer get Node change request:%#v\n", req)

	switch req.Operation {
	case pb.NodeChgRequest_JOIN:
		//notify all clients to add Node
		s.c.NodeChange(ctx, &pb.NodeChgRequest{Operation: pb.NodeChgRequest_ADD, NodeAddr: req.NodeAddr})

		//add this new node as peer
		if err := s.c.AddPeer(req.NodeAddr); err != nil {
			return &pb.NodeChgResponse{Fail: true}, errors.Wrap(err, "Server:NodeChgJoin:AddPeer fail")
		}
		return &pb.NodeChgResponse{Fail: false}, nil

	case pb.NodeChgRequest_ADD:
		//notify all clients to add Node

		// log.Printf("get NodeChgRequest:AddNode:%s", req.NodeAddr)
		//add this new node as peer
		if err := s.c.AddPeer(req.NodeAddr); err != nil {
			return &pb.NodeChgResponse{Fail: true}, errors.Wrapf(err, "Server:NodeChgAdd:AddPeer %s fail", req.NodeAddr)
		}
		return &pb.NodeChgResponse{Fail: false}, nil

	default:
		log.Fatal("Server NodeChange:unknown Operation %v", req.Operation)
	}

	return &pb.NodeChgResponse{}, nil
}

func (s *Server) NodeQuery(ctx context.Context, req *pb.NodeQryRequest) (*pb.NodeQryResponse, error) {
	// fmt.Printf("\nServer get Node query request:%#v\n", req)

	//return server's own address as well
	addrs := []string{s.c.OwnAddr}
	for _, p := range s.c.Peers {
		addrs = append(addrs, p.Addr)
	}

	return &pb.NodeQryResponse{NodeAddr: addrs}, nil
}

func (s *Server) Ping(ctx context.Context, req *pb.PingRequest) (*pb.PingResponse, error) {

	return nil, nil
}

func (s *Server) DataChange(stream pb.SyncUp_DataChangeServer) error {

	var rowCount int64

	startTime := time.Now()
	for {
		record, err := stream.Recv()

		if err == io.EOF {
			endTime := time.Now()
			log.Printf("Server %s received %d records and sync'ed them to DB.", s.c.OwnAddr, rowCount)
			return stream.SendAndClose(&pb.DataChgSummary{
				RecordCount: rowCount,
				ElapsedTime: int32(endTime.Sub(startTime).Seconds()),
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
