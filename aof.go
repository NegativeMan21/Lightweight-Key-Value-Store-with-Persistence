package main

import (
	"bufio"
	"fmt"
	"io"
	"os"
	"sync"
	"time"
)

type Aof struct {
	file *os.File
	rd   *bufio.Reader
	mu   sync.Mutex
}

func NewAof(filepath string) (*Aof, error) {
	f, err := os.OpenFile(filepath, os.O_CREATE|os.O_RDWR, 0666)
	if err != nil {
		return nil, err
	}

	aof := &Aof{
		file: f,
		rd:   bufio.NewReader(f),
	}

	go func() {
		for {
			aof.mu.Lock()
			aof.file.Sync()
			aof.mu.Unlock()
			time.Sleep(time.Second)
		}
	}()

	return aof, nil
}
func (aof *Aof) Close() error {
	aof.mu.Lock()
	defer aof.mu.Unlock()
	return aof.file.Close()
}

func (aof *Aof) Write(value Value) error {
	aof.mu.Lock()
	defer aof.mu.Unlock()
	_, err := aof.file.Write(value.Marshall())
	if err != nil {
		return err
	}
	return nil
}

func (aof *Aof) Read(callback func(value Value)) error {
	aof.mu.Lock()
	defer aof.mu.Unlock()
	resp := NewResp(aof.file)

	for {
		value, err := resp.Read()
		if err == nil {
			fmt.Println("err==nil")
			callback(value)
			continue
		}
		if err == io.EOF {
			fmt.Println("err==io.EOF")
			break
		}
		return err
	}
	return nil
}
