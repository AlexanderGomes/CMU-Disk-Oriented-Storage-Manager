package main

import (
	"context"
	m "disk-db/Distributed/manager"
	"disk-db/Distributed/rpc/pb"
	"disk-db/Distributed/tcp"
	"encoding/json"
	"fmt"
	"log"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

const (
	nodesQty        = 5
	messageDelaySec = 3
	clientCon       = ":4068"
)

func main() {
	manager := &m.Manager{}
	channel := make(chan []byte)
	program := "/Users/alexsandergomes/Documents/DBMs_Visualizer/CMD"

	m.InitNodes(program, nodesQty, channel, clientCon)
	UpdateState(channel, manager)

	s := tcp.NewServer(clientCon, nil)
	s.Manager = manager

	go s.Start()

	sqlChan := make(chan string)
	go QueryProcess(sqlChan, s)
	go FakeQuery(sqlChan)

	select {}
}

func FakeQuery(sqlchan chan string) {
	ticker := time.NewTicker(2 * time.Second)
	for range ticker.C {
		sql := `  CREATE TABLE escola (
			ID INT PRIMARY KEY,
				 Name VARCHAR(255),
				 Age INT,
				 Company VARCHAR(255)
			  )
		`
		sqlchan <- sql
	}
}

func QueryProcess(sqlChan chan string, s *tcp.Server) {
	for sql := range sqlChan {
		creds := insecure.NewCredentials()
		conn, err := grpc.Dial(s.Manager.Leader.RPCcon, grpc.WithTransportCredentials(creds))
		if err != nil {
			log.Fatalf("Failed to connect to server: %v", err)
		}
		client := pb.NewQueryServiceClient(conn)

		queryReq := &pb.QueryRequest{
			Sql: sql,
		}

		resp, err := client.ExecuteQuery(context.Background(), queryReq)
		conn.Close()

		if err != nil {
			log.Fatalf("ExecuteQuery failed: %v", err)
			continue
		}

		for _, row := range resp.Result {
			log.Printf("Row: %v", row)
		}
	}
}

func UpdateState(channel chan []byte, manager *m.Manager) {
	nodes := []*m.Node{}

	for nodesReceived := 0; nodesReceived < nodesQty; nodesReceived++ {
		data := <-channel
		var node m.Node
		err := json.Unmarshal(data, &node)
		if err != nil {
			fmt.Print("unmarshal error: ", err)
			continue
		}

		nodes = append(nodes, &node)

		if node.IsLeader {
			manager.Leader = &node
		} else {
			manager.Copies = append(manager.Copies, &node)
		}
	}

	time.Sleep(messageDelaySec * time.Second)

	for _, node := range nodes {
		message := m.Message{
			Type:    "MANAGER UPDATE",
			Content: manager,
		}
		err := m.SendMessage(node.HeartCon, message)
		if err != nil {
			fmt.Println("error sending message: ", err)
		}
	}
}
