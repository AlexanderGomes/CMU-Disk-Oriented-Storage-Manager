package main

import (
	"fmt"
	"os/exec"
	"strconv"
	"sync"
)

func main() {
	program := "/Users/alexsandergomes/Documents/DBMs_Visualizer/CMD"
	var wg sync.WaitGroup

	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func(num int) {
			defer wg.Done()

			filename := "DB-" + strconv.Itoa(num)
			portNum := 5000 + num
			cmd := exec.Command("go", "run", "main.go", "--filename", filename, "--port", strconv.Itoa(portNum))
			cmd.Dir = program

			err := cmd.Start()
			if err != nil {
				fmt.Printf("Error starting process %d: %v\n", num, err)
				return
			}


			fmt.Printf("Process %d started\n", num)

			err = cmd.Wait()
			if err != nil {
				fmt.Printf("Error waiting for process %d: %v\n", num, err)
				return
			}
			fmt.Printf("Process %d finished\n", num)
		}(i)
	}

	wg.Wait()
}
