package main

import (
	"fmt"
	"sync"
)

var handlers = map[string]func([]Value) Value{
	"PING":    ping,
	"SET":     set,
	"GET":     get,
	"HSET":    hset,
	"HGET":    hget,
	"HGETALL": hgetall,
}
var SETs = map[string]string{}
var SETsMu = sync.RWMutex{}
var HSETs = map[string]map[string]string{}
var HSETsMu = sync.RWMutex{}

func ping(args []Value) Value {
	if len(args) == 0 {
		return Value{typ: "string", str: "PONG"}
	}
	fmt.Println("PING WORKING!!")
	return Value{typ: "string", str: args[0].bulk}
}
func set(args []Value) Value {
	if len(args) != 2 {
		return Value{typ: "error", str: "Invalid number of args for SET command\nTry 2"}
	}
	key := args[0].bulk
	value := args[1].bulk

	SETsMu.Lock()
	SETs[key] = value
	SETsMu.Unlock()

	return Value{typ: "string", str: "OK"}
}
func get(args []Value) Value {
	if len(args) != 1 {
		return Value{typ: "error", str: "Invalid number of args for GET command\nTry 1"}
	}
	key := args[0].bulk
	SETsMu.RLock()
	value, ok := SETs[key]
	SETsMu.RUnlock()
	if !ok {
		return Value{typ: "null"}
	}
	return Value{typ: "bulk", bulk: value}
}

func hset(args []Value) Value {
	if len(args) != 3 {
		return Value{typ: "error", str: "Invalid number of args for HSET command\nTry 3"}
	}

	hash := args[0].bulk
	key := args[1].bulk
	value := args[2].bulk

	HSETsMu.Lock()
	if _, ok := HSETs[hash]; !ok {
		HSETs[hash] = map[string]string{}
	}
	HSETs[hash][key] = value
	HSETsMu.Unlock()

	return Value{typ: "string", str: "OK"}
}
func hget(args []Value) Value {
	if len(args) != 2 {
		return Value{typ: "error", str: "Invalid number of args for HGET command\nTry 2"}
	}
	hash := args[0].bulk
	key := args[1].bulk
	HSETsMu.RLock()
	value, ok := HSETs[hash][key]
	HSETsMu.RUnlock()
	if !ok {
		return Value{typ: "null"}
	}
	return Value{typ: "string", str: value}
}
func hgetall(args []Value) Value {
	if len(args) != 1 {
		return Value{typ: "error", str: "Invalid number of args for HGETALL command\nTry 1"}
	}
	hash := args[0].bulk
	HSETsMu.RLock()
	values, ok := HSETs[hash]
	HSETsMu.RUnlock()

	if !ok {
		return Value{typ: "null"}
	}
	fmt.Println("HGETALL retrieved:", values)
	res := []Value{}
	for key, val := range values {
		res = append(res, Value{typ: "bulk", bulk: key})
		res = append(res, Value{typ: "bulk", bulk: val})
	}
	return Value{typ: "array", array: res}
}
