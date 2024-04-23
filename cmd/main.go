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
	"syscall"

	"google.golang.org/grpc"
)

const (
	HeaderSize = 8
	k          = 2
)

type Server struct {
	pb.UnimplementedHelloServer
}

func (s *Server) SayHello(ctx context.Context, in *pb.HelloRequest) (*pb.HelloResponse, error) {
	return &pb.HelloResponse{Message: "Hello" + in.GetName()}, nil
}

func main() {
	_, rpcPort, _, heartPort := GetCommandLineInputs()
	s := tcp.NewServer(heartPort)
	go s.Start()
	StartRpcServer(rpcPort)
}

func StartRpcServer(port string) {
	println("Running RPC Server")
	lis, _ := net.Listen("tcp", port)

	s := grpc.NewServer()
	pb.RegisterHelloServer(s, &Server{})

	if err := s.Serve(lis); err != nil {
		log.Fatalf("FAILED TO SERVER %v", err)
	}
}

func GetCommandLineInputs() (string, string, *m.Manager, string) {
	args := os.Args[1:]
	var fileName string
	var rpcPort string
	var heartPort string
	var managerJSON string

	for i := 0; i < len(args); i++ {
		switch args[i] {
		case "--filename":
			if i+1 < len(args) {
				fileName = args[i+1]
			}
		case "--rpcPort":
			if i+1 < len(args) {
				rpcPort = args[i+1]
			}
		case "--heartPort":
			if i+1 < len(args) {
				heartPort = args[i+1]
			}
		case "--manager":
			if i+1 < len(args) {
				managerJSON = args[i+1]
			}
		}
	}

	manager := &m.Manager{}
	if managerJSON != "" {
		err := json.Unmarshal([]byte(managerJSON), &manager)
		if err != nil {
			fmt.Println("Error parsing manager JSON:", err)
		}
	}

	return fileName, rpcPort, manager, heartPort
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
