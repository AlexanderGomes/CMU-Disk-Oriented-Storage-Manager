package main

import (
	m "disk-db/manager"
	"sync"
)

func main() {
	m := &m.Manager{}
	program := "/Users/alexsandergomes/Documents/DBMs_Visualizer/CMD"
	var wg sync.WaitGroup
	m.InitNodes(program, &wg)

	wg.Wait()
}
