package manager

import (
	"encoding/json"
	"fmt"
	"io"
	"net"
	"os"
	"os/exec"
	"strconv"
	"sync"
	"time"
)

type Message struct {
	Type    string
	Content interface{}
}

type Node struct {
	RPCcon     string
	HeartCon   string
	PID        int
	FileName   string
	isLeader   bool
	ProposedId int
}

type Manager struct {
	Leader *Node
	Copies []*Node
	Paxos  *Paxos
}

type Prepare struct {
	proposalID int
}

type Promise struct {
	ProposalID   int
	LastAccepted int
	Acceptor     *Node
}

type Propose struct {
	Type       string
	ProposalID int
	Value      *Node
}

type Paxos struct {
	Proposer   *Node
	Acceptors  []*Node
	Learners   []*Node
	msgChannel chan Message
}

type Acceptance struct {
}

func (m *Manager) StartElection(proposer *Node) {
	m.Paxos = &Paxos{
		Proposer:   proposer,
		msgChannel: make(chan Message),
	}

	quorum := len(m.Copies)/2 + 1
	fmt.Println(len(m.Copies), "COPIES SIZE")
	proposalID := int(time.Now().UnixNano())
	for i := 0; i < quorum; i++ {
		acceptor := m.Copies[i]
		message := Message{
			Type:    "PREPARE",
			Content: Prepare{proposalID: proposalID},
		}

		go sendMessage(acceptor.HeartCon, message)
	}

	go func() {
		accepted := 0

	outerLoop:
		for messages := range m.Paxos.msgChannel {
			switch messages.Type {
			case "PROMISE":
				content := messages.Content.(Promise)
				m.Paxos.Acceptors = append(m.Paxos.Acceptors, content.Acceptor)
			case "ACCEPTED":
				accepted++
				if accepted == quorum {
					m.Leader = proposer
					break outerLoop
				}
			}

			if len(m.Paxos.Acceptors) == quorum {
				for _, node := range m.Paxos.Acceptors {
					message := Message{
						Type: "ACCEPT",
						Content: Propose{
							Type:       "LEADER ELECTION",
							ProposalID: proposalID,
							Value:      proposer,
						},
					}
					sendMessage(node.HeartCon, message)
				}
			}
		}
	}()

}

func (m *Manager) CreateNode(rpcCon string, heartCon string, file string, isLeader bool) *Node {
	node := &Node{
		RPCcon:   rpcCon,
		HeartCon: heartCon,
		FileName: file,
		isLeader: isLeader,
	}

	if isLeader {
		m.Leader = node
		return node
	}

	m.Copies = append(m.Copies, node)
	return node
}

func (m *Manager) InitNodes(program string, wg *sync.WaitGroup) {
	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func(num int) {
			defer wg.Done()

			managerJSON, err := json.Marshal(m)
			if err != nil {
				fmt.Println("Error:", err)
				return
			}

			filename := "DB-" + strconv.Itoa(num)
			rpcPort := ":" + strconv.Itoa(8000+num)
			heartCon := ":" + strconv.Itoa(9000+num)

			isLeader := num == 1
			node := m.CreateNode(rpcPort, heartCon, filename, isLeader)

			nodeJSON, err := json.Marshal(node)
			if err != nil {
				fmt.Println("Error:", err)
				return
			}

			cmd := exec.Command("go", "run", "main.go", "--filename", filename, "--rpcPort", rpcPort, "--manager", string(managerJSON), "--heartPort", heartCon, "--node", string(nodeJSON))
			cmd.Dir = program

			stdout, err := cmd.StdoutPipe()
			if err != nil {
				fmt.Printf("Error getting stdout pipe for process %d: %v\n", num, err)
				return
			}

			stderr, err := cmd.StderrPipe()
			if err != nil {
				fmt.Printf("Error getting stderr pipe for process %d: %v\n", num, err)
				return
			}

			go func() {
				if _, err := io.Copy(os.Stdout, stdout); err != nil {
					fmt.Printf("Error copying stdout for process %d: %v\n", num, err)
					return
				}
			}()

			go func() {
				if _, err := io.Copy(os.Stderr, stderr); err != nil {
					fmt.Printf("Error copying stderr for process %d: %v\n", num, err)
					return
				}
			}()

			if err := cmd.Start(); err != nil {
				fmt.Printf("Error starting process %d: %v\n", num, err)
				return
			}

			node.PID = cmd.Process.Pid

			err = cmd.Wait()
			if err != nil {
				fmt.Println(err.Error())
				return
			}
		}(i)
	}
}

func sendMessage(address string, message Message) error {
	conn, err := net.Dial("tcp", address)
	if err != nil {
		return err
	}
	defer conn.Close()

	jsonData, err := json.Marshal(message)
	if err != nil {
		return err
	}

	_, err = conn.Write([]byte(jsonData))
	if err != nil {
		return err
	}

	fmt.Println("Message sent to", address)
	return nil
}
