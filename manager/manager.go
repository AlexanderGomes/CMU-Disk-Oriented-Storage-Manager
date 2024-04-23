package manager

import (
	"encoding/json"
	"fmt"
	"os/exec"
	"strconv"
	"sync"
)

type Node struct {
	RPCcon   string
	HeartCon string
	PID      int
	FileName string
	isLeader bool
}

type Manager struct {
	Leader *Node
	Copies []*Node
}

func (m *Manager) CreateNode(rpcCon string, heartCon string, pid int, file string, isLeader bool) {
	node := &Node{
		RPCcon:   rpcCon,
		HeartCon: heartCon,
		PID:      pid,
		FileName: file,
		isLeader: isLeader,
	}

	if isLeader {
		m.Leader = node
		return
	}

	m.Copies = append(m.Copies, node)
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
			heartCon := ":" + strconv.Itoa(8000+num)
			cmd := exec.Command("go", "run", "main.go", "--filename", filename, "--rpcPort", rpcPort, "--manager", string(managerJSON), "--heartPort", heartCon)
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

			m.CreateNode(rpcPort, heartCon, cmd.Process.Pid, filename, isLeader)

			err = cmd.Wait()
			if err != nil {
				fmt.Println(err.Error())
				return
			}
		}(i)
	}
}
