package main

import (
	"fmt"
	"net/rpc"
	"os"
	"strconv"
)

type ChangeValueArgs struct {
	Value int
}

type ChangeValueReply struct {
	Success bool
}

type GetValueArgs struct{}

type GetValueReply struct {
	Value int
}

func main() {
	peer := os.Args[1]
	valueArgs := os.Args[2]
	command := os.Args[3]
	value, _ := strconv.Atoi(valueArgs)
	client, err := rpc.DialHTTP("tcp", "localhost:"+peer)
	if err != nil {
		fmt.Printf("RPC dial to %s failed: %v\n", peer, err)
		return
	}
	defer client.Close()
	if command == "SET" {
		reply := &ChangeValueReply{}
		args := &ChangeValueArgs{
			Value: value,
		}
		err = client.Call("Server.ChangeValue", args, reply)
		if err != nil || !reply.Success {
			fmt.Printf("Call Failed")
		}
		if reply.Success {
			fmt.Printf("[server %s] Responded with success", peer)
		}
	} else {
		args := &GetValueArgs{}
		reply := &GetValueReply{}

		err = client.Call("Server.GetValue", args, reply)
		if err != nil {
			fmt.Printf("call Failed %v", err)
		}

		fmt.Printf("[server %s] Responded with value : %d\n", peer, reply.Value)

	}
}
