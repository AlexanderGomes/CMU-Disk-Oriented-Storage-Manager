package manager

import (
	"encoding/json"
	"fmt"
	"io"
	"net"
	"os"
	"os/exec"
	"strconv"
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
	IsLeader   bool
	ProposedId int
}

type Manager struct {
	Leader    *Node
	Copies    []*Node
	Acceptors []*Node
}

type Prepare struct {
	ProposalID int
	HeartCon   string
}

type Promise struct {
	ProposalID   int
	LastAccepted int
	Acceptor     *Node
}

func (m *Manager) StartElection(proposer *Node) {
	quorum := len(m.Copies)/2 + 1
	proposalID := int(time.Now().UnixNano())
	for i := 0; i < quorum; i++ {
		acceptor := m.Copies[i]
		message := Message{
			Type:    "PREPARE",
			Content: Prepare{ProposalID: proposalID, HeartCon: proposer.HeartCon},
		}
		go SendMessage(acceptor.HeartCon, message)
	}
}

func CreateNode(rpcCon string, heartCon string, file string, isLeader bool) *Node {
	return &Node{
		RPCcon:   rpcCon,
		HeartCon: heartCon,
		FileName: file,
		IsLeader: isLeader,
	}
}

func InitNodes(program string, nodesQty int, channel chan []byte) {
	for i := 0; i < nodesQty; i++ {
		go func(num int, channel chan []byte) {
			filename := "DB-" + strconv.Itoa(num)
			rpcPort := ":" + strconv.Itoa(11000+num)
			heartCon := ":" + strconv.Itoa(10000+num)

			isLeader := num == 1
			node := CreateNode(rpcPort, heartCon, filename, isLeader)

			nodeJSON, err := json.Marshal(node)
			if err != nil {
				fmt.Printf("Error marshaling node struct for process %d: %v\n", num, err)
				return
			}

			cmd := exec.Command("go", "run", "main.go", "--node", string(nodeJSON))
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

			channel <- nodeJSON

			err = cmd.Wait()
			if err != nil {
				fmt.Println(err.Error())
				return
			}
		}(i, channel)
	}
}

func SendMessage(address string, message Message) error {
	conn, err := net.Dial("tcp", address)
	if err != nil {
		return err
	}
	defer conn.Close()

	jsonData, err := json.Marshal(message)
	if err != nil {
		return err
	}

	_, err = conn.Write(jsonData)
	if err != nil {
		return err
	}

	fmt.Println("Message sent to", address)
	return nil
}

func (m *Manager) RemoveNodeFromCopies(rpcCon string) {
	index := -1
	for i, node := range m.Copies {
		if node.RPCcon == rpcCon {
			index = i
			break
		}
	}

	if index != -1 {
		m.Copies = append(m.Copies[:index], m.Copies[index+1:]...)
	}
}
