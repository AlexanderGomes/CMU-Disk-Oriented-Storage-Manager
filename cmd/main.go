package main

import (
	"disk-db/pb"
	"disk-db/query-engine"
	"disk-db/storage"
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"strconv"
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


func main() {
	fileName, port := GetCommandLineInputs()
	DB, _ := storage.InitDatabase(k, fileName, HeaderSize)
	fmt.Println(DB)

	StartRpcServer(port)
}

func StartRpcServer(port int) {
	println("Running RPC Server")
	lis, _ := net.Listen("tcp", fmt.Sprintf("localhost:%d", port))
	s := grpc.NewServer()
	pb.RegisterHelloServer(s, &Server{})

	if err := s.Serve(lis); err != nil {
		log.Fatalf("FAILED TO SERVER %v", err)
	}
}

func GetCommandLineInputs() (string, int) {
	args := os.Args[1:]
	var fileName string
	var port int

	for i := 0; i < len(args); i++ {
		switch args[i] {
		case "--filename":
			if i+1 < len(args) {
				fileName = args[i+1]
			}
		case "--port":
			if i+1 < len(args) {
				port, _ = strconv.Atoi(args[i+1])
			}
		}
	}

	return fileName, port
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
