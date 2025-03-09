package main

import (
	"bufio"
	"fmt"
	"io"
	"log"
	"net"
	"strconv"
	"strings"
	"time"

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
			conn.Write(fmt.Appendf(nil, "$%d\r\n%s\r\n", len(val.Value), val.Value))
		case "SET":
			if len(args) < 2 {
				conn.Write([]byte("-ERR wrong number of arguments for 'set' command\r\n"))
				continue
			}

			var expiry *time.Time = nil

			expiryCmd := ""
			var nx bool = false
			var xx bool = false
			var ttl bool = false
			var get bool = false

			var errFlag = 0

			for i := 2; i < len(args); {
				cmd := strings.ToUpper(args[i])
				switch cmd {
				case "EX":
					if (expiryCmd != "" && expiryCmd != cmd) || ttl {
						conn.Write([]byte("-ERR Syntax error\r\n"))
						errFlag = 1
						goto errHandler
					}

					if i+1 >= len(args) {
						conn.Write([]byte("-ERR Syntax error\r\n"))
						errFlag = 1
						goto errHandler
					}

					seconds, err := strconv.Atoi(args[i+1])

					if err != nil || seconds <= 0 {
						conn.Write([]byte("-ERR invalid expire time in 'set' command\r\n"))
						errFlag = 1
						goto errHandler
					}

					expire := time.Now().Add(time.Duration(seconds) * time.Second)
					expiry = &expire

					expiryCmd = cmd
					i += 2
				case "PX":
					if (expiryCmd != "" && expiryCmd != cmd) || ttl {
						conn.Write([]byte("-ERR Syntax error\r\n"))
						errFlag = 1
						goto errHandler
					}

					if i+1 >= len(args) {
						conn.Write([]byte("-ERR Syntax error\r\n"))
						errFlag = 1
						goto errHandler
					}

					milliseconds, err := strconv.Atoi(args[i+1])

					if err != nil || milliseconds <= 0 {
						conn.Write([]byte("-ERR invalid expire time in 'set' command\r\n"))
						errFlag = 1
						goto errHandler
					}

					expire := time.Now().Add(time.Duration(milliseconds) * time.Millisecond)
					expiry = &expire
					expiryCmd = cmd
					i += 2
				case "KEEPTTL":
					if expiryCmd != "" && expiryCmd != cmd {
						conn.Write([]byte("-ERR Syntax error\r\n"))
						errFlag = 1
						goto errHandler
					}
					expiryCmd = cmd
					ttl = true
					i++
				case "NX":
					if xx {
						conn.Write([]byte("-ERR Syntax error\r\n"))
						errFlag = 1
						goto errHandler
					}
					nx = true
					i++
				case "XX":
					if nx {
						conn.Write([]byte("-ERR Syntax error\r\n"))
						errFlag = 1
						goto errHandler
					}
					xx = true
					i++
				case "GET":
					get = true
					i++
				default:
					conn.Write([]byte("-ERR Syntax error\r\n"))
					errFlag = 1
					goto errHandler
				}
			}

		errHandler:
			if errFlag == 1 {
				continue
			}

			conn.Write([]byte(store.Set(args[0], args[1], expiry, nx, xx, ttl, get)))
		case "TTL":
			if len(args) != 1 {
				conn.Write([]byte("-ERR wrong number of arguments for 'ttl' command\r\n"))
				continue
			}

			storeVal, ok := store.Get(args[0])
			if !ok {
				conn.Write([]byte(":-2\r\n"))
				continue
			}

			if storeVal.Expiry == nil {
				conn.Write([]byte(":-1\r\n"))
				continue
			}

			conn.Write(fmt.Appendf(nil, ":%d\r\n", int64(time.Until(*storeVal.Expiry).Seconds())))
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
