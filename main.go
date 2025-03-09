package main

import (
	"bufio"
	"fmt"
	"io"
	"log"
	"net"

	"github.com/theaniketnegi/goredis/parser"
	"github.com/theaniketnegi/goredis/store"
)

func connectionHandler(conn net.Conn, store *store.InMemoryStore) {
	defer conn.Close()

	reader := bufio.NewReader(conn)

	for {
		command, args, err := parser.ParseRESP(reader)
		if err != nil {
			if err == io.EOF {
				fmt.Println("Connection closed.")
				return
			}
			log.Fatal(err)
			return
		}

		fmt.Printf("COMMAND: %q, ARGS: %v\n", command, args)
		switch command {
		case "PING":
			if len(args) == 1 {
				conn.Write(fmt.Appendf(nil, "$%d\r\n%s\r\n", len(args[0]), args[0]))
				continue
			}

			if len(args) > 1 {
				conn.Write([]byte("-ERR wrong number of arguments for 'echo' command\r\n"))
				continue
			}

			conn.Write([]byte("+PONG\r\n"))

		case "ECHO":
			if len(args) != 1 {
				conn.Write([]byte("-ERR wrong number of arguments for 'echo' command\r\n"))
				continue
			}
			conn.Write(fmt.Appendf(nil, "$%d\r\n%s\r\n", len(args[0]), args[0]))
		case "GET":
			if len(args) != 1 {
				conn.Write([]byte("-ERR wrong number of arguments for 'get' command\r\n"))
				continue
			}
			val, ok := store.Get(args[0])

			if !ok {
				conn.Write([]byte("_\r\n"))
				continue
			}
			conn.Write(fmt.Appendf(nil, "$%d\r\n%s\r\n", len(val), val))
		case "SET":
			if len(args) != 2 {
				conn.Write([]byte("-ERR wrong number of arguments for 'set' command\r\n"))
				continue
			}
			store.Set(args[0], args[1])
			conn.Write([]byte("+OK\r\n"))
		default:
			conn.Write([]byte("+PONG\r\n"))
		}
	}
}

func main() {
	listener, err := net.Listen("tcp", ":6380")

	memStore := store.NewInMemoryStore()

	if err != nil {
		panic(err)
	}

	defer listener.Close()

	for {
		conn, err := listener.Accept()
		if err != nil {
			panic(err)
		}

		go connectionHandler(conn, memStore)
	}
}
