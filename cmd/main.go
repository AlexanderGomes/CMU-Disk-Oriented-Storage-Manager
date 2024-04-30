package main

import (
	"context"
	m "disk-db/manager"
	"disk-db/pb"
	queryengine "disk-db/query-engine"
	"disk-db/storage"
	"disk-db/tcp"
	"encoding/json"
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"google.golang.org/grpc"
)

const (
	HeaderSize = 8
	k          = 2
)

type RPCserver struct {
	pb.UnimplementedHelloServer
}

func (s *RPCserver) SayHello(ctx context.Context, in *pb.HelloRequest) (*pb.HelloResponse, error) {
	return &pb.HelloResponse{Message: "Hello" + in.GetName()}, nil
}

func main() {
	node := GetCommandLineInputs()
	s := tcp.NewServer(node.HeartCon, node)

	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		s.Start()
	}()

	go func() {
		defer wg.Done()
		StartRpcServer(node.RPCcon)
	}()

	time.Sleep(10 * time.Second)
	s.Manager.StartElection(s.Node)
	wg.Wait()
}

func Test(m *m.Manager) {
	time.Sleep(10 * time.Second)
	fmt.Println(m.Copies, "COPIES")
	fmt.Println(m.Leader, "LEADER")
}

func StartRpcServer(port string) {
	println("Running RPC Server")
	lis, _ := net.Listen("tcp", port)

	s := grpc.NewServer()
	pb.RegisterHelloServer(s, &RPCserver{})

	if err := s.Serve(lis); err != nil {
		log.Fatalf("FAILED TO SERVER %v", err)
	}
}

func GetCommandLineInputs() *m.Node {
	args := os.Args[1:]
	var nodeJSON string

	for i := 0; i < len(args); i++ {
		if args[i] == "--node" && i+1 < len(args) {
			nodeJSON = args[i+1]
			break
		}
	}

	var node m.Node
	err := json.Unmarshal([]byte(nodeJSON), &node)
	if err != nil {
		fmt.Printf("Error unmarshaling node JSON: %v\n", err)
		return nil
	}

	return &node
}

func ExecuteQuery(sql string, DB *storage.BufferPoolManager) {
	parsedSQL, err := queryengine.Parser(sql)
	if err != nil {
		fmt.Println(err)
	}

	queryPlan, err := queryengine.GenerateQueryPlan(parsedSQL)
	if err != nil {
		fmt.Println(err)
	}

	queryengine.ExecuteQueryPlan(queryPlan, parsedSQL, DB)
}

func gracefulShutdown(s chan os.Signal, DB *storage.BufferPoolManager) {
	signal.Notify(s, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-s
		fmt.Println(`
		____  _  _  ____    ___  _  _  ____ 
	   / ___)/ )( \(  __)  / __)/ )( \(  __)
	  ( (___ ) \/ ( ) _)  ( (__(  )  / ) _) 
	   \____)\____/(____)  \___)(__)(__(____)
	  `)
		fmt.Println("Gracefully shutting down")

		DB.FlushAll()
		os.Exit(0)
	}()
}
