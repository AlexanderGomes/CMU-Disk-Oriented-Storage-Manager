package tcp

import (
	m "disk-db/Distributed/manager"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"net"
	"time"
)

const (
	beatInterval = 3
)

type Server struct {
	address     string
	ln          net.Listener
	quitch      chan struct{}
	Manager     *m.Manager
	Node        *m.Node
	LastBeat    time.Time
	StopRoutine chan bool
}

type HeartBeat struct {
	TimeStamp time.Time
	HeartCon  string
}

func NewServer(address string, node *m.Node) *Server {
	return &Server{
		address:     address,
		quitch:      make(chan struct{}),
		Node:        node,
		StopRoutine: make(chan bool),
	}
}

func (s *Server) Start() error {
	ln, err := net.Listen("tcp", s.address)
	if err != nil {
		return err
	}
	defer ln.Close()

	fmt.Println("TCP STARTED")
	s.ln = ln
	go s.acceptLoop()

	<-s.quitch

	return nil
}

func (s *Server) acceptLoop() {
	for {
		conn, err := s.ln.Accept()
		if err != nil {
			fmt.Println("accept error: ", err)
			continue
		}
		go s.readLoop(conn)
	}
}

func (s *Server) readLoop(conn net.Conn) {
	defer conn.Close()
	buf := make([]byte, 2048)
	for {
		n, err := conn.Read(buf)
		if err != nil {
			if err == io.EOF {
				fmt.Println("Client disconnected")
				return
			}
			fmt.Println("read error: ", err)
			return
		}

		var msg m.Message
		err = json.Unmarshal(buf[:n], &msg)
		if err != nil {
			fmt.Println(err)
		}

		switch msg.Type {
		case "HEARTBEAT":
			s.HeartBeat(msg)
		case "MANAGER UPDATE":
			s.SetManager(msg)
		case "PREPARE":
			s.Promise(msg)
		case "PROMISE":
			s.ElectLeader(msg)
		}
	}
}

func (s *Server) LeaderHeartbeat() {
	if s.Node.IsLeader {
		go func() {
			ticker := time.Tick(beatInterval * time.Second)
			for range ticker {
				message := m.Message{
					Type: "HEARTBEAT",
					Content: HeartBeat{
						HeartCon:  s.Node.HeartCon,
						TimeStamp: time.Now(),
					},
				}

				for _, node := range s.Manager.Copies {
					m.SendMessage(node.HeartCon, message)
				}
			}
		}()
	}
}

func (s *Server) IsLeaderAlive() {
	go func() {
		if !s.Node.IsLeader {
			ticker := time.NewTicker(beatInterval * time.Second)
			defer ticker.Stop()

			for {
				select {
				case <-ticker.C:
					elapsedTime := int(time.Since(s.LastBeat) / time.Second)
					aboveTolerance := elapsedTime > beatInterval*3
					fmt.Println("ELAPSED TIME", elapsedTime)

					if aboveTolerance {
						fmt.Println("ELECTION STARTED")
						s.Manager.StartElection(s.Node)
					} else {
						fmt.Println("EVERYTHING IS FINE")
					}
				case <-s.StopRoutine:
					if s.Node.IsLeader {
						fmt.Println("QUITTEDDDDDD")
						return
					}
				}
			}
		}
	}()

	// Example usage to stop the goroutine after some time
	// time.Sleep(10 * time.Minute)
	// close(stop)
}

var activeNodes []string

func (s *Server) HeartBeat(msg m.Message) {
	heartMap := msg.Content.(map[string]interface{})
	heartCon := getStringValue(heartMap["HeartCon"])
	timeStamp, _ := parseTimeStamp(heartMap["TimeStamp"])
	isLeader := s.Node.IsLeader

	if isLeader {
		activeNodes = append(activeNodes, heartCon)
		fmt.Println(activeNodes, "ACTIVE NODES")
		if len(activeNodes) == len(s.Manager.Copies) {
			activeNodes = []string{}
		}

		return
	}

	message := m.Message{
		Type: "HEARTBEAT",
		Content: HeartBeat{
			HeartCon:  s.Node.HeartCon,
			TimeStamp: time.Now(),
		},
	}

	m.SendMessage(heartCon, message)

	s.LastBeat = timeStamp
}

var LastProposalID int = math.MaxInt64

func (s *Server) Promise(msg m.Message) {
	prepareMap := msg.Content.(map[string]interface{})
	proposalID := getIntValue(prepareMap["ProposalID"])
	heartCon := getStringValue(prepareMap["HeartCon"])

	if proposalID < LastProposalID {
		LastProposalID = proposalID
	} else {
		fmt.Println("election was started by a different node")
		return
	}

	res := m.Message{
		Type: "PROMISE",
		Content: m.Promise{
			ProposalID:   proposalID,
			LastAccepted: s.Node.ProposedId,
			Acceptor:     s.Node,
		},
	}

	m.SendMessage(heartCon, res)
}

func (s *Server) ElectLeader(msg m.Message) {
	promisseMap, ok := msg.Content.(map[string]interface{})
	if !ok {
		fmt.Println("promisseMap error")
		return
	}
	node, _ := extractNode(promisseMap["Acceptor"])

	s.Manager.Acceptors = append(s.Manager.Acceptors, node)

	quorumQty := len(s.Manager.Copies)/2 + 1
	acceptorsQty := len(s.Manager.Acceptors)

	if quorumQty == acceptorsQty {
		s.Manager.RemoveNodeFromCopies(s.Node.RPCcon)
		s.Manager.Leader = s.Node
		s.Node.IsLeader = true

		msg := m.Message{
			Type:    "MANAGER UPDATE",
			Content: s.Manager,
		}

		for _, node := range s.Manager.Copies {
			m.SendMessage(node.HeartCon, msg)
		}

		LastProposalID = math.MaxInt64
		s.Manager.Acceptors = []*m.Node{}

		s.LeaderHeartbeat()
		s.StopRoutine <- true
	}
}

func (s *Server) SetManager(msg m.Message) error {
	manager := &m.Manager{}

	contentMap, ok := msg.Content.(map[string]interface{})
	if !ok {
		return errors.New("msg.Content is not a map[string]interface{}")
	}

	leader, err := extractNode(contentMap["Leader"])
	if err != nil {
		return err
	}

	copies, err := extractNodesSlice(contentMap["Copies"])
	if err != nil {
		return err
	}

	manager.Leader = leader
	manager.Copies = copies

	s.Manager = manager
	return nil
}

func extractNode(data interface{}) (*m.Node, error) {
	nodeMap, ok := data.(map[string]interface{})
	if !ok {
		return nil, errors.New("leader data is not a map[string]interface{}")
	}

	return &m.Node{
		FileName:   getStringValue(nodeMap["FileName"]),
		HeartCon:   getStringValue(nodeMap["HeartCon"]),
		IsLeader:   getBoolValue(nodeMap["IsLeader"]),
		PID:        getIntValue(nodeMap["PID"]),
		ProposedId: getIntValue(nodeMap["ProposedId"]),
		RPCcon:     getStringValue(nodeMap["RPCcon"]),
	}, nil
}

func extractNodesSlice(data interface{}) ([]*m.Node, error) {
	nodesSlice, ok := data.([]interface{})
	if !ok {
		return nil, errors.New("leader data is not a map[string]interface{}")
	}

	var nodes []*m.Node
	for _, nodeData := range nodesSlice {
		node, err := extractNode(nodeData)
		if err != nil {
			return nil, err
		}
		nodes = append(nodes, node)
	}
	return nodes, nil
}

func getStringValue(value interface{}) string {
	if str, ok := value.(string); ok {
		return str
	}
	return ""
}

func getBoolValue(value interface{}) bool {
	if b, ok := value.(bool); ok {
		return b
	}
	return false
}

func getIntValue(value interface{}) int {
	if f, ok := value.(float64); ok {
		return int(f)
	}
	return 0
}

func parseTimeStamp(timeStr interface{}) (time.Time, error) {
	timeStamp, err := time.Parse(time.RFC3339, timeStr.(string))
	if err != nil {
		return time.Time{}, err
	}
	return timeStamp, nil
}
