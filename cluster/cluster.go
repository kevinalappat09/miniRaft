package main

import (
	"fmt"
	"log"
	"net/rpc"
	"os"
	"os/exec"
	"time"
)

var ports = []string{":8080", ":8081", ":8082"}

func main() {
	processes := []*exec.Cmd{}
	for _, port := range ports {
		cmd := exec.Command("go", "run", "../server.go", "../raft.go", port)
		cmd.Stdout = os.Stdout
		cmd.Stderr = os.Stderr

		fmt.Printf("Starting server on %s...\n", port)

		if err := cmd.Start(); err != nil {
			log.Fatalf("Failed to start server %s: %v", port, err)
		}

		processes = append(processes, cmd)
	}

	fmt.Println("Waiting for servers to be ready...")
	for _, port := range ports {
		waitForRPC(port)
		fmt.Printf("Server %s is ready.\n", port)
	}

	fmt.Println("Sending start signal to all servers...")
	for _, port := range ports {
		client, err := rpc.DialHTTP("tcp", port)
		if err != nil {
			log.Fatalf("RPC dial failed (%s): %v", port, err)
		}
		client.Call("Server.Start", struct{}{}, &struct{}{})
		fmt.Printf("Sent Start to %s\n", port)
	}

	// Block forever so cluster doesn't exit
	select {}
}

func waitForRPC(port string) {
	for {
		client, err := rpc.DialHTTP("tcp", port)
		if err == nil {
			client.Close()
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
}
