package rpc

import (
	"context"
	"disk-db/Distributed/rpc/pb"
)

type RPCserver struct {
	pb.UnimplementedHelloServer
}

func (s *RPCserver) SayHello(ctx context.Context, in *pb.HelloRequest) (*pb.HelloResponse, error) {
	return &pb.HelloResponse{Message: "Hello" + in.GetName()}, nil
}
