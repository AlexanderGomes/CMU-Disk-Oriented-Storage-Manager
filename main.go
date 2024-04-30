package main

import (
	m "disk-db/Distributed/manager"
	"encoding/json"
	"fmt"
	"time"
)

const (
	nodesQty        = 5
	messageDelaySec = 3
)

func main() {
	manager := &m.Manager{}
	channel := make(chan []byte)
	program := "/Users/alexsandergomes/Documents/DBMs_Visualizer/CMD"

	m.InitNodes(program, nodesQty, channel)
	UpdateState(channel, manager)
	select {}
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
