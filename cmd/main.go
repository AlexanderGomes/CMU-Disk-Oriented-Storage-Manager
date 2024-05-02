package main

import (
	"disk-db/DB/cmd"
	query "disk-db/DB/query-engine"
	"disk-db/DB/storage"
	m "disk-db/Distributed/manager"
	"disk-db/Distributed/rpc"
	"disk-db/Distributed/rpc/pb"
	"disk-db/Distributed/tcp"
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

func main() {
	node := GetCommandLineInputs()
	node.PID = os.Getpid()
	fmt.Println(node.PID)

	queryEngine, err := cmd.InitDatabase(k, node.FileName, HeaderSize)
	if err != nil {
		fmt.Println("ERROR DATABASE: ", err)
	}

	s := tcp.NewServer(node.HeartCon, node)

	var wg sync.WaitGroup
	wg.Add(2)

	go func() {
		defer wg.Done()
		s.Start()
	}()

	go func() {
		defer wg.Done()
		for s.Manager == nil {
			time.Sleep(1 * time.Second)
			fmt.Println("NILL MANAGER")
		}

		fmt.Println("READY MANAGER")
		go StartRpcServer(node.RPCcon, queryEngine, s.Manager)
	}()

	s.LeaderHeartbeat()
	s.IsLeaderAlive()

	wg.Wait()
}

func StartRpcServer(port string, qe *query.QueryEngine, manager *m.Manager) {
	println("Running RPC Server")
	lis, _ := net.Listen("tcp", port)

	s := grpc.NewServer()

	pb.RegisterQueryServiceServer(s, &rpc.RPCServer{QueryEngine: qe, Manager: manager})

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
