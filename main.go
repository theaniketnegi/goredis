package main

import (
	"bufio"
	"flag"
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

var dir = flag.String("dir", "/tmp/redis-data", "Directory to store godb file")
var dbFilename = flag.String("dbfilename", "dump.godb", "godb file")

func connectionHandler(conn net.Conn, store *store.InMemoryStore, persistence *store.Persistence) {
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
		case "DEL":
			if len(args) == 0 {
				conn.Write([]byte("-ERR wrong number of arguments for 'del' command\r\n"))
				continue
			}

			deletedKeys := store.NumKeyExists(args, true)

			conn.Write(fmt.Appendf(nil, ":%d\r\n", deletedKeys))
		case "EXISTS":
			if len(args) == 0 {
				conn.Write([]byte("-ERR wrong number of arguments for 'del' command\r\n"))
				continue
			}

			conn.Write(fmt.Appendf(nil, ":%d\r\n", store.NumKeyExists(args, false)))
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
		case "CONFIG":
			if len(args) == 0 {
				conn.Write([]byte("-ERR wrong number of arguments for 'config' command\r\n"))
				continue
			}
			if len(args) == 1 && args[0] == "GET" {
				conn.Write([]byte("-ERR wrong number of arguments for 'config|get' command\r\n"))
				continue
			}

			if args[0] == "GET" {
				if args[1] == "dir" {
					conn.Write(fmt.Appendf(nil, "*2\r\n$3\r\ndir\r\n$%d\r\n%s\r\n", len(*dir), *dir))
				} else if args[1] == "dbfilename" {
					conn.Write(fmt.Appendf(nil, "*2\r\n$10\r\ndbfilename\r\n$%d\r\n%s\r\n", len(*dbFilename), *dbFilename))
				} else {
					conn.Write([]byte("*0\r\n"))
				}
			} else {
				conn.Write([]byte("*0\r\n"))
			}
		case "KEYS":
			if len(args) != 1 {
				conn.Write([]byte("-ERR wrong number of arguments for 'keys' command\r\n"))
				return
			}
			pattern := args[0]
			matchedKeys := store.GetKeys(parsePattern(pattern))

			var resp strings.Builder

			resp.WriteString(fmt.Sprintf("*%d\r\n", len(matchedKeys)))

			for _, key := range matchedKeys {
				resp.WriteString(fmt.Sprintf("$%d\r\n%s\r\n", len(key), key))
			}
			conn.Write([]byte(resp.String()))
		case "SAVE":
			if len(args) != 0 {
				conn.Write([]byte("-ERR wrong number of arguments for 'save' command\r\n"))
				continue
			}

			err := persistence.Save()
			if err != nil {
				conn.Write([]byte("-ERR " + err.Error() + "\r\n"))
				continue
			}
			conn.Write([]byte("+OK\r\n"))
		case "BGSAVE":
			if len(args) != 0 {
				conn.Write([]byte("-ERR wrong number of arguments for 'bgsave' command\r\n"))
				continue
			}

			err := persistence.BGSave()

			if err != nil {
				conn.Write([]byte("-ERR " + err.Error() + "\r\n"))
				continue
			}

			conn.Write([]byte("+OK\r\n"))
		case "LASTSAVE":
			if len(args) != 0 {
				conn.Write([]byte("-ERR wrong number of arguments for 'lastsave' command\r\n"))
				continue
			}

			conn.Write(fmt.Appendf(nil, ":%d\r\n", persistence.LastSave()))
		case "INCR":
			if len(args) != 1 {
				conn.Write([]byte("-ERR wrong number of arguments for 'incr' command\r\n"))
				continue
			}

			val, err := store.Increment(args[0], 1)
			if err != nil {
				conn.Write([]byte("-ERR " + err.Error() + "\r\n"))
				continue
			}
			conn.Write(fmt.Appendf(nil, ":%d\r\n", val))
		case "INCRBY":
			if len(args) != 2 {
				conn.Write([]byte("-ERR wrong number of arguments for 'incrby' command\r\n"))
				continue
			}
			by, err := strconv.Atoi(args[1])
			if err != nil {
				conn.Write([]byte("-ERR value is not an integer or out of range\r\n"))
				continue
			}
			val, err := store.Increment(args[0], int64(by))
			if err != nil {
				conn.Write([]byte("-ERR " + err.Error() + "\r\n"))
				continue
			}
			conn.Write(fmt.Appendf(nil, ":%d\r\n", val))
		case "DECR":
			if len(args) != 1 {
				conn.Write([]byte("-ERR wrong number of arguments for 'decr' command\r\n"))
				continue
			}
			val, err := store.Increment(args[0], -1)
			if err != nil {
				conn.Write([]byte("-ERR " + err.Error() + "\r\n"))
				continue
			}
			conn.Write(fmt.Appendf(nil, ":%d\r\n", val))
		case "DECRBY":
			if len(args) != 2 {
				conn.Write([]byte("-ERR wrong number of arguments for 'decrby' command\r\n"))
				continue
			}
			by, err := strconv.Atoi(args[1])
			if err != nil {
				conn.Write([]byte("-ERR value is not an integer or out of range\r\n"))
				continue
			}
			val, err := store.Increment(args[0], -int64(by))
			if err != nil {
				conn.Write([]byte("-ERR " + err.Error() + "\r\n"))
				continue
			}
			conn.Write(fmt.Appendf(nil, ":%d\r\n", val))
		default:
			conn.Write([]byte("+PONG\r\n"))
		}
	}
}

func main() {
	flag.Parse()
	listener, err := net.Listen("tcp", ":6380")

	if err != nil {
		panic(err)
	}

	memStore := store.NewInMemoryStore()
	persistence, err := store.NewPersistence(memStore, fmt.Sprintf("%s/%s", *dir, *dbFilename))

	if err != nil {
		panic(err)
	}

	defer listener.Close()

	for {
		conn, err := listener.Accept()
		if err != nil {
			panic(err)
		}

		go connectionHandler(conn, memStore, persistence)
	}
}
