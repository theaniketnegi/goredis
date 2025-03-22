package main

import (
	"bufio"
	"errors"
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
	"golang.org/x/text/cases"
	"golang.org/x/text/language"
)

var dir = flag.String("dir", "/tmp/redis-data", "Directory to store godb file")
var dbFilename = flag.String("dbfilename", "dump.godb", "godb file")
var port = flag.Uint("port", 6380, "define port")
var replicaOf = flag.String("replicaof", "", "Replicate to another server (<master_host master_port>)")
var info map[string]map[string]any = map[string]map[string]any{
	"replication": {
		"role":               "master",
		"master_replid":      "8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb",
		"master_repl_offset": "0",
	},
}
var slaves []*net.Conn

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
		if info["replication"]["role"] == "slave" && !readOnlyCommands(command) {
			conn.Write([]byte("-READONLY You can't write against a read only replica.\r\n"))
			continue
		}

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
			for _, slave := range slaves {
				_, err := (*slave).Write(convertCmdToResp(command, args))
				if err != nil {
					fmt.Print(err)
				}
			}
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
		case "INFO":
			c := cases.Title(language.English)
			if len(args) == 0 {
				var resp strings.Builder
				i := 0
				for k, v := range info {
					resp.WriteString(fmt.Sprintf("# %s\r\n", c.String(k)))
					for ck, cv := range v {
						resp.WriteString(fmt.Sprintf("%s:%s\r\n", ck, cv))
					}
					if i != len(info)-1 {
						resp.WriteString("\r\n")
					}
					i++
				}
				conn.Write(fmt.Appendf(nil, "$%d\r\n%s\r\n", len(resp.String()), resp.String()))
				continue
			}

			var resp strings.Builder
			for idx, arg := range args {
				if _, ok := info[strings.ToLower(arg)]; !ok {
					continue
				}

				resp.WriteString(fmt.Sprintf("# %s\r\n", c.String(strings.ToLower(arg))))

				for ck, cv := range info[strings.ToLower(arg)] {
					resp.WriteString(fmt.Sprintf("%s:%s\r\n", ck, cv))
				}

				if idx != len(args)-1 {
					resp.WriteString("\r\n")
				}
			}
			conn.Write(fmt.Appendf(nil, "$%d\r\n%s\r\n", len(resp.String()), resp.String()))
		case "REPLCONF":
			if len(args) > 0 {
				if strings.ToLower(args[0]) == "listening-port" {
					if len(args) < 1 {
						conn.Write([]byte("-ERR Syntax error\r\n"))
						continue
					}
					conn.Write([]byte("+OK\r\n"))
					continue
				}

				if strings.ToLower(args[0]) == "capa" {
					if len(args) < 1 {
						conn.Write([]byte("-ERR Syntax error\r\n"))
						continue
					}
					conn.Write([]byte("+OK\r\n"))
					continue
				}
				conn.Write([]byte("-ERR Syntax error\r\n"))
				continue
			}
			conn.Write([]byte("+OK\r\n"))

		case "PSYNC":
			if len(args) != 2 {
				conn.Write([]byte("-ERR wrong number of arguments for 'psync' command\r\n"))
				continue
			}
			slaves = append(slaves, &conn)
			fileContents, err := persistence.LoadFileContents()
			if err != nil {
				conn.Write(fmt.Appendf(nil, "-ERR %s\r\n", err.Error()))
				continue
			}

			conn.Write(fmt.Appendf(nil, "$%d\r\n%s", len(fileContents), fileContents))
		default:
			conn.Write([]byte("+PONG\r\n"))
		}
	}
}

func handleSlaveCommands(conn net.Conn, store *store.InMemoryStore, _ *store.Persistence) {
	reader := bufio.NewReader(conn)

	for {
		command, args, err := parser.ParseRESP(reader)
		fmt.Printf("[FROM MASTER] COMMAND: %q, ARGS: %v\n", command, args)

		if err != nil {
			if err == io.EOF {
				fmt.Println("Connection closed.")
				return
			}
			log.Fatal(err)
			return
		}

		switch command {
		case "SET":
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
						errFlag = 1
						goto errHandler
					}

					if i+1 >= len(args) {
						errFlag = 1
						goto errHandler
					}

					seconds, err := strconv.Atoi(args[i+1])

					if err != nil || seconds <= 0 {
						errFlag = 1
						goto errHandler
					}

					expire := time.Now().Add(time.Duration(seconds) * time.Second)
					expiry = &expire

					expiryCmd = cmd
					i += 2
				case "PX":
					if (expiryCmd != "" && expiryCmd != cmd) || ttl {
						errFlag = 1
						goto errHandler
					}

					if i+1 >= len(args) {
						errFlag = 1
						goto errHandler
					}

					milliseconds, err := strconv.Atoi(args[i+1])

					if err != nil || milliseconds <= 0 {
						errFlag = 1
						goto errHandler
					}

					expire := time.Now().Add(time.Duration(milliseconds) * time.Millisecond)
					expiry = &expire
					expiryCmd = cmd
					i += 2
				case "KEEPTTL":
					if expiryCmd != "" && expiryCmd != cmd {
						errFlag = 1
						goto errHandler
					}
					expiryCmd = cmd
					ttl = true
					i++
				case "NX":
					if xx {
						errFlag = 1
						goto errHandler
					}
					nx = true
					i++
				case "XX":
					if nx {
						errFlag = 1
						goto errHandler
					}
					xx = true
					i++
				case "GET":
					get = true
					i++
				default:
					errFlag = 1
					goto errHandler
				}
			}

		errHandler:
			if errFlag == 1 {
				continue
			}
			store.Set(args[0], args[1], expiry, nx, xx, ttl, get)
		}
	}
}

func sendHandshake(masterHost string, masterPort string, persistence *store.Persistence) *net.Conn {
	conn, err := net.Dial("tcp", net.JoinHostPort(masterHost, masterPort))

	if err != nil {
		panic(err)
	}

	reader := bufio.NewReader(conn)
	conn.Write([]byte("*1\r\n$4\r\nping\r\n"))
	reader.ReadString('\n')
	conn.Write(fmt.Appendf(nil, "*3\r\n$8\r\nREPLCONF\r\n$14\r\nlistening-port\r\n$4\r\n%d\r\n", *port))
	reader.ReadString('\n')
	conn.Write(fmt.Appendf(nil, "*3\r\n$8\r\nREPLCONF\r\n$4\r\ncapa\r\n$6\r\npsync2\r\n"))
	reader.ReadString('\n')
	conn.Write([]byte("*3\r\n$5\r\nPSYNC\r\n$1\r\n?\r\n$2\r\n-1\r\n"))

	_, content, _ := parser.ParseRDBMessage(reader)
	persistence.LoadDataFromFile(content)

	return &conn
}

func main() {
	flag.Parse()
	listener, err := net.Listen("tcp", fmt.Sprintf(":%d", *port))

	if err != nil {
		panic(err)
	}

	defer listener.Close()

	memStore := store.NewInMemoryStore()
	persistence, err := store.NewPersistence(memStore, fmt.Sprintf("%s/%s", *dir, *dbFilename))

	if err != nil {
		panic(err)
	}

	if *replicaOf != "" {
		go func() {
			hostPortArr := strings.Split(*replicaOf, " ")
			if len(hostPortArr) != 2 {
				panic(errors.New("invalid host/port passed"))
			}

			if err := validateHost(hostPortArr[0]); err != nil {
				panic(err)
			}

			if err := validatePort(hostPortArr[1]); err != nil {
				panic(err)
			}
			info["replication"]["role"] = "slave"
			conn := sendHandshake(hostPortArr[0], hostPortArr[1], persistence)

			handleSlaveCommands(*conn, memStore, persistence)

			defer (*conn).Close()
		}()
	}

	for {
		conn, err := listener.Accept()
		if err != nil {
			panic(err)
		}

		go connectionHandler(conn, memStore, persistence)
	}
}
