package main

import (
	"fmt"
	"net"
	"strings"
)

func handleConnection(connection net.Conn, aof *Aof) {
	defer connection.Close()
	for {
		resp := NewResp(connection)
		input, err := resp.Read()
		if err != nil {
			fmt.Println(err)
			return
		}
		fmt.Println("Received command:", input)
		if input.typ != "array" {
			fmt.Println("Not an array")
			continue
		}
		if len(input.array) == 0 {
			fmt.Println("Empty array")
			continue
		}
		command := strings.ToUpper(input.array[0].bulk)
		args := input.array[1:]

		writer := NewWrite(connection)

		handler, ok := handlers[command]
		if !ok {
			fmt.Println("Invalid command")
			writer.Write(Value{typ: "str", str: "Invalid Command"})
			return
		}
		if command == "SET" || command == "HSET" {
			aof.Write(input)
		}
		result := handler(args)
		writer.Write(result)

	}
}

func main() {
	fmt.Println("Starting server...\n ")
	listener, err := net.Listen("tcp", ":6379")
	if err != nil {
		fmt.Println("Listener error: ", err)
		return
	}

	fmt.Println("Listening...\n ")
	aof, err := NewAof("backup.aof")
	if err != nil {
		fmt.Println(err)
		return
	}
	defer aof.Close()

	aof.Read(func(value Value) {
		command := strings.ToUpper(value.array[0].bulk)
		args := value.array[1:]
		fmt.Println("Replaying Command:", command, args)

		handler, ok := handlers[command]
		if !ok {
			fmt.Println("Invalid command")
			return
		}
		handler(args)
	})

	for {
		connection, err := listener.Accept()
		if err != nil {
			fmt.Println("Connection error: ", err)
			continue
		}
		go handleConnection(connection, aof)
	}
}
