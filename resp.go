package main

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"strconv"
)

const (
	STRING  = '+'
	ERROR   = '-'
	INTEGER = ':'
	BULK    = '$'
	ARRAY   = '*'
)

type Value struct {
	typ   string
	str   string
	num   int
	bulk  string
	array []Value
}

// buffer struct
type Resp struct {
	reader *bufio.Reader
}

// Creating buffer out of reader received by connection
func NewResp(rd io.Reader) *Resp {
	return &Resp{reader: bufio.NewReader(rd)}
}

// Read
func (r *Resp) readLine() (line []byte, n int, err error) {
	for {
		i, err := r.reader.ReadByte()
		if err != nil {
			return nil, 0, err
		}
		n += 1
		line = append(line, i)
		if len(line) >= 2 && line[len(line)-2] == '\r' {
			break
		}
	}
	fmt.Println("readLine: val = ", line, "n = ", n)
	return line, n, nil
}
func (r *Resp) readInteger() (val int, n int, err error) {
	line, n, err := r.readLine()
	if err != nil {
		return 0, 0, err
	}
	x, err := strconv.Atoi(string(bytes.TrimSpace(line)))
	if err != nil {
		return 0, n, err
	}
	fmt.Println("readInteger: val = ", x, "n = ", n)
	return x, n, nil
}

// Desrialisation -> opposite of serialisation
// -> conversion from byte-stream to some object
// Reads the signType and then reads the line
func (r *Resp) Read() (Value, error) {
	signType, err := r.reader.ReadByte()
	if err != nil {
		return Value{}, err
	}
	switch signType {
	case ARRAY:
		return r.readArray()

	case BULK:
		return r.readBulk()
	default:
		fmt.Println("Unknown type: ", string(signType))
		return Value{}, nil
	}
}
func (r *Resp) readArray() (Value, error) {
	v := Value{}
	v.typ = "array"

	length, _, err := r.readInteger()
	if err != nil {
		return v, err
	}
	v.array = make([]Value, length)
	for i := 0; i < length; i++ {
		val, err := r.Read()
		if err != nil {
			return v, err
		}
		v.array[i] = val
	}
	return v, nil
}
func (r *Resp) readBulk() (Value, error) {
	v := Value{}
	v.typ = "bulk"
	length, _, err := r.readInteger()
	if err != nil {
		return v, err
	}
	bulk := make([]byte, length)
	r.reader.Read(bulk)
	v.bulk = string(bulk)
	r.readLine() //reads till CRLF
	return v, nil
}

// Value serialiser// marshall method
// till now we were reading from byte now we will change to byte-stream
func (v Value) Marshall() []byte {
	switch v.typ {
	case "array":
		return v.marshallArray()
	case "bulk":
		return v.marshallBulk()
	case "string":
		return v.marshallString()
	case "null":
		return v.marshallNull()
	case "error":
		return v.marshallError()
	default:
		return []byte{}
	}
}
func (v Value) marshallString() []byte {
	var bytes []byte
	bytes = append(bytes, STRING)
	bytes = append(bytes, v.str...)
	bytes = append(bytes, '\r', '\n')
	return bytes
}
func (v Value) marshallBulk() []byte {
	var bytes []byte
	bytes = append(bytes, BULK)
	bytes = append(bytes, strconv.Itoa(len(v.bulk))...)
	bytes = append(bytes, '\r', '\n')
	bytes = append(bytes, v.bulk...)
	bytes = append(bytes, '\r', '\n')
	return bytes
}
func (v Value) marshallArray() []byte {
	var bytes []byte
	bytes = append(bytes, ARRAY)
	bytes = append(bytes, strconv.Itoa(len(v.array))...)
	bytes = append(bytes, '\r', '\n')
	for i := 0; i < len(v.array); i++ {
		bytes = append(bytes, v.array[i].Marshall()...)
	}
	return bytes
}

func (v Value) marshallError() []byte {
	var bytes []byte
	bytes = append(bytes, ERROR)
	bytes = append(bytes, v.str...)
	bytes = append(bytes, '\r', '\n')
	return bytes
}
func (v Value) marshallNull() []byte {
	return []byte("$-1\r\n")
}

type Writer struct {
	writer io.Writer
}

func NewWrite(w io.Writer) *Writer {
	return &Writer{writer: w}
}
func (w *Writer) Write(v Value) error {
	var bytes = v.Marshall()
	_, err := w.writer.Write(bytes)
	if err != nil {
		return err
	}
	return nil
}
