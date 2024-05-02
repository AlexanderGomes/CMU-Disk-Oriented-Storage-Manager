package rpc

import (
	"context"
	queryengine "disk-db/DB/query-engine"
	"disk-db/DB/storage"
	m "disk-db/Distributed/manager"
	"disk-db/Distributed/rpc/pb"
	"fmt"
	"log"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

type RPCServer struct {
	pb.UnimplementedQueryServiceServer
	QueryEngine *queryengine.QueryEngine
	Manager     *m.Manager
}

func (s *RPCServer) ExecuteQuery(ctx context.Context, queryReq *pb.QueryRequest) (*pb.QueryResponse, error) {
	query, err := s.QueryEngine.ExecuteQuery(queryReq.Sql)
	if err != nil {
		log.Printf("Error executing query: %v", err)
		return nil, status.Errorf(codes.InvalidArgument, "Error parsing query: %v", err)
	}

	pbRows := ConvertStorageRowToPB(query.Result)
	res := pb.QueryResponse{Result: pbRows}

	for i, node := range s.Manager.Copies {
		if i == len(s.Manager.Copies)-1 {
			break
		}
		fmt.Println(node.ClientCon, node.FileName)
		creds := insecure.NewCredentials()
		conn, err := grpc.Dial(node.RPCcon, grpc.WithTransportCredentials(creds))
		if err != nil {
			log.Fatalf("Failed to connect to server: %v", err)
		}
		client := pb.NewQueryServiceClient(conn)
		resp, err := client.ExecuteQuery(context.Background(), queryReq)
		defer conn.Close()

		if err != nil {
			log.Printf("ExecuteQuery failed: %v", err)
			continue
		}

		for _, row := range resp.Result {
			log.Printf("Row: %v", row)
		}

	}

	return &res, nil
}

func ConvertStorageRowToPB(storageRows []storage.Row) []*pb.Row {
	var pbRows []*pb.Row

	for _, storageRow := range storageRows {
		pbRow := &pb.Row{
			Values: make(map[string]string),
		}
		for key, value := range storageRow.Values {
			pbRow.Values[key] = value
		}
		pbRows = append(pbRows, pbRow)
	}

	return pbRows
}
