package manager

import (
	"encoding/json"
	"fmt"
	"net"
	"os/exec"
	"strconv"
	"sync"
	"time"
)

type Node struct {
	TCPcon   int
	HeartCon int
	PID      int
	FileName string
	isLeader bool
}

type Manager struct {
	Leader *Node
	copies []*Node
}

func (m *Manager) HeartBeat(port int) {
	go func() {
		ticker := time.Tick(4 * time.Second)
		for range ticker {
			if m.Leader.TCPcon != port {
				conn, err := net.Dial("tcp", fmt.Sprintf("localhost:%d", m.Leader.HeartCon))
				if err != nil {
					fmt.Println(err)
				}

				heartbeatMsg := []byte("heartbeat")

				_, errr := conn.Write(heartbeatMsg)
				if errr != nil {
					fmt.Println(errr)
				}
			}

		}
	}()
}

func (m *Manager) CreateNode(tcp int, heartCon int, pid int, file string, isLeader bool) {
	node := &Node{
		TCPcon:   tcp,
		HeartCon: heartCon,
		PID:      pid,
		FileName: file,
		isLeader: isLeader,
	}

	if isLeader {
		m.Leader = node
		return
	}

	m.copies = append(m.copies, node)
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
			portNum := 8000 + num
			cmd := exec.Command("go", "run", "main.go", "--filename", filename, "--port", strconv.Itoa(portNum), "--manager", string(managerJSON))
			cmd.Dir = program

			err = cmd.Start()
			if err != nil {
				fmt.Printf("Error starting process %d: %v\n", num, err)
				return
			}

			isLeader := false

			if num == 0 {
				isLeader = true
			}

			heartCon := 3000 + num

			m.CreateNode(portNum, heartCon, cmd.Process.Pid, filename, isLeader)

			fmt.Println(m.Leader, "LEADER")
			fmt.Println(m.copies, "copies")
			err = cmd.Wait()
			if err != nil {
				fmt.Println(err.Error())
				return
			}
		}(i)
	}
}
