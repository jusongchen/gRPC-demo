package main

import (
	"fmt"
	"io"
	"log"
	"time"

	pb "github.com/jusongchen/gRPC-demo/replica"
	"github.com/pkg/errors"

	"golang.org/x/net/context"
	"google.golang.org/grpc"
	"google.golang.org/grpc/grpclog"
)

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

			// fmt.Printf("\n%s generate data changes request:\n", c.ownAddr)

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
		if !c.connected {
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
	if !c.connected {
		return fmt.Errorf("Not connected to any peers.")
	}

	ch := make(chan *pb.DataChgSummary, len(c.peers))

	//replicate changes to peer channels
	for i := range c.peers {
		c.peers[i].in = make(chan *pb.Record)
	}

	go func() {
		rowCnt := 0
		for r := range chRows {
			rowCnt++
			// fmt.Printf("replicate row:%v\n", r)
			for i := range c.peers {
				c.peers[i].in <- r
			}
		}
		for i := range c.peers {
			close(c.peers[i].in)
		}
	}()
	// for r := range chRows {
	// 	for i := range c.peers {
	// 		c.peers[i].in <- r
	// 	}
	// }

	for _, b := range c.peers {

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

	for _, p := range c.peers {
		r := <-ch
		log.Printf("\n")
		log.Printf("%d rows sync'ed to server %s in %d seconds (process rate:%f rows/second).\n",
			r.RecordCount, p.Addr, r.ElapsedTime, float64(r.RecordCount)/float64(r.ElapsedTime))
	}
	return nil
}

func (s *Server) DataChange(stream pb.SyncUp_DataChangeServer) error {

	var rowCount int64

	startTime := time.Now()
	for {
		record, err := stream.Recv()

		if err == io.EOF {
			endTime := time.Now()
			log.Printf("Server %s received %d records and sync'ed them to DB.", s.c.ownAddr, rowCount)
			return stream.SendAndClose(&pb.DataChgSummary{
				RecordCount: rowCount,
				ElapsedTime: int32(endTime.Sub(startTime).Seconds()),
			})
		}
		if err != nil {
			return err
		}
		// fmt.Printf("Server %s received record %v\n", s.c.ownAddr, record)
		if err := s.insert2DB(record); err != nil {
			return errors.Wrap(err, "Server DataChange() fail")
		}
		rowCount++
	}
}

func (s *Server) insert2DB(r *pb.Record) error {

	return nil
}
