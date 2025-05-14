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
			val, ok, err := store.StringGet(args[0])
			if err != nil {
				conn.Write([]byte(err.Error() + "\r\n"))
				continue
			}
			if !ok {
				conn.Write([]byte("$-1\r\n"))
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

			conn.Write([]byte(store.StringSet(args[0], args[1], expiry, nx, xx, ttl, get)))
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

			storeVal, ok, _ := store.StringGet(args[0])
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
				continue
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
				conn.Write([]byte(err.Error() + "\r\n"))
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
		case "APPEND":
			if len(args) != 2 {
				conn.Write([]byte("-ERR wrong number of arguments for 'append' command\r\n"))
				continue
			}

			storeVal, ok, err := store.StringGet(args[0])
			if err != nil {
				conn.Write([]byte(err.Error() + "\r\n"))
				continue
			}
			if !ok {
				store.StringSet(args[0], args[1], nil, false, false, false, false)
				conn.Write(fmt.Appendf(nil, ":%d\r\n", len(args[1])))
				continue
			}

			store.StringSet(args[0], storeVal.Value+args[1], nil, false, false, false, false)
			conn.Write(fmt.Appendf(nil, ":%d\r\n", len(storeVal.Value+args[1])))
		case "MSET":
			if len(args) < 2 || len(args)%2 != 0 {
				conn.Write([]byte("-ERR wrong number of arguments for 'mset' command\r\n"))
				continue
			}
			for i := 0; i < len(args); i += 2 {
				store.StringSet(args[i], args[i+1], nil, false, false, false, false)
			}
			conn.Write([]byte("+OK\r\n"))
		case "MGET":
			if len(args) == 0 {
				conn.Write([]byte("-ERR wrong number of arguments for 'mget' command\r\n"))
				continue
			}
			var resp strings.Builder
			resp.WriteString(fmt.Sprintf("*%d\r\n", len(args)))
			for _, key := range args {
				storeVal, ok, err := store.StringGet(key)
				if err != nil {
					conn.Write([]byte(err.Error() + "\r\n"))
					continue
				}
				if !ok {
					resp.WriteString("$-1\r\n")
					continue
				}
				resp.WriteString(fmt.Sprintf("$%d\r\n%s\r\n", len(storeVal.Value), storeVal.Value))
			}
			conn.Write([]byte(resp.String()))
		case "LPUSH":
			if len(args) < 2 {
				conn.Write([]byte("-ERR wrong number of arguments for 'lpush' command\r\n"))
				continue
			}

			err := store.LPush(args[0], args[1:])
			if err != nil {
				conn.Write([]byte(err.Error() + "\r\n"))
				continue
			}

			conn.Write(fmt.Appendf(nil, ":%d\r\n", len(args)-1))
		case "RPUSH":
			if len(args) < 2 {
				conn.Write([]byte("-ERR wrong number of arguments for 'rpush' command\r\n"))
				continue
			}

			err := store.RPush(args[0], args[1:])
			if err != nil {
				conn.Write([]byte(err.Error() + "\r\n"))
				continue
			}

			conn.Write(fmt.Appendf(nil, ":%d\r\n", len(args)-1))
		case "LPOP":
			if len(args) < 1 || len(args) > 2 {
				conn.Write([]byte("-ERR wrong number of arguments for 'lpop' command\r\n"))
				continue
			}
			if len(args) == 1 {
				val, err := store.LPop(args[0])
				if err != nil {
					conn.Write([]byte(err.Error() + "\r\n"))
					continue
				}
				conn.Write(fmt.Appendf(nil, "$%d\r\n%s\r\n", len(val), val))
				continue
			}
			count, err := strconv.Atoi(args[1])
			if err != nil || count <= 0 {
				conn.Write([]byte("-ERR value is out of range, must be positive\r\n"))
				continue
			}
			size, err := store.LLen(args[0])

			if err != nil {
				conn.Write([]byte(err.Error() + "\r\n"))
				continue
			}
			if size == 0 {
				conn.Write([]byte("_\r\n"))
				continue
			}

			if count > size {
				count = size
			}

			var poppedValues []string
			for range count {
				val, err := store.LPop(args[0])
				if err != nil {
					conn.Write([]byte(err.Error() + "\r\n"))
					continue
				}
				poppedValues = append(poppedValues, val)
			}
			var resp strings.Builder
			resp.WriteString(fmt.Sprintf("*%d\r\n", len(poppedValues)))
			for _, val := range poppedValues {
				resp.WriteString(fmt.Sprintf("$%d\r\n%s\r\n", len(val), val))
			}
			conn.Write([]byte(resp.String()))
		case "BLPOP":
			if len(args) < 2 {
				conn.Write([]byte("-ERR wrong number of arguments for 'blpop' command\r\n"))
				continue
			}

			keys := args[:len(args)-1]
			var duration time.Duration
			if args[len(args)-1] == "0" {
				duration = time.Duration(0 * time.Second)
			} else {
				timeout, err := strconv.ParseFloat(args[len(args)-1], 64)
				if err != nil || timeout < 0 {
					conn.Write([]byte("-ERR timeout is not a float or out of range\r\n"))
					continue
				}
				duration = time.Duration(timeout * float64(time.Second))
			}

			key, value, err := store.BLPop(keys, duration, false)
			if err != nil {
				conn.Write([]byte(err.Error() + "\r\n"))
				continue
			}

			if value == "" {
				conn.Write([]byte("_\r\n"))
				continue
			}

			conn.Write(fmt.Appendf(nil, "*2\r\n$%d\r\n%s\r\n$%d\r\n%s\r\n", len(key), key, len(value), value))
		case "BRPOP":
			if len(args) < 2 {
				conn.Write([]byte("-ERR wrong number of arguments for 'brpop' command\r\n"))
				continue
			}

			keys := args[:len(args)-1]
			var duration time.Duration
			if args[len(args)-1] == "0" {
				duration = time.Duration(0 * time.Second)
			} else {
				timeout, err := strconv.ParseFloat(args[len(args)-1], 64)
				if err != nil || timeout < 0 {
					conn.Write([]byte("-ERR timeout is not a float or out of range\r\n"))
					continue
				}
				duration = time.Duration(timeout * float64(time.Second))
			}

			key, value, err := store.BLPop(keys, duration, true)
			if err != nil {
				conn.Write([]byte(err.Error() + "\r\n"))
				continue
			}

			if value == "" {
				conn.Write([]byte("_\r\n"))
				continue
			}

			conn.Write(fmt.Appendf(nil, "*2\r\n$%d\r\n%s\r\n$%d\r\n%s\r\n", len(key), key, len(value), value))
		case "RPOP":
			if len(args) < 1 || len(args) > 2 {
				conn.Write([]byte("-ERR wrong number of arguments for 'rpop' command\r\n"))
				continue
			}
			if len(args) == 1 {
				val, err := store.RPop(args[0])
				if err != nil {
					conn.Write([]byte(err.Error() + "\r\n"))
					continue
				}
				conn.Write(fmt.Appendf(nil, "$%d\r\n%s\r\n", len(val), val))
				continue
			}
			count, err := strconv.Atoi(args[1])
			if err != nil || count <= 0 {
				conn.Write([]byte("-ERR value is out of range, must be positive\r\n"))
				continue
			}
			size, err := store.LLen(args[0])

			if err != nil {
				conn.Write([]byte(err.Error() + "\r\n"))
				continue
			}
			if size == 0 {
				conn.Write([]byte("_\r\n"))
				continue
			}

			if count > size {
				count = size
			}

			var poppedValues []string
			for range count {
				val, err := store.RPop(args[0])
				if err != nil {
					conn.Write([]byte(err.Error() + "\r\n"))
					continue
				}
				poppedValues = append(poppedValues, val)
			}
			var resp strings.Builder
			resp.WriteString(fmt.Sprintf("*%d\r\n", len(poppedValues)))
			for _, val := range poppedValues {
				resp.WriteString(fmt.Sprintf("$%d\r\n%s\r\n", len(val), val))
			}
			conn.Write([]byte(resp.String()))
		case "LLEN":
			if len(args) != 1 {
				conn.Write([]byte("-ERR wrong number of arguments for 'llen' command\r\n"))
				continue
			}
			listLen, err := store.LLen(args[0])
			if err != nil {
				conn.Write([]byte(err.Error() + "\r\n"))
				continue
			}
			conn.Write(fmt.Appendf(nil, ":%d\r\n", listLen))
		case "LRANGE":
			if len(args) != 3 {
				conn.Write([]byte("-ERR wrong number of arguments for 'lrange' command\r\n"))
				continue
			}

			start, err := strconv.Atoi(args[1])
			if err != nil {
				conn.Write([]byte("-ERR value is not an integer or out of range\r\n"))
				continue
			}
			end, err := strconv.Atoi(args[2])
			if err != nil {
				conn.Write([]byte("-ERR value is not an integer or out of range\r\n"))
				continue
			}

			values, err := store.LRange(args[0], start, end)
			if err != nil {
				conn.Write([]byte(err.Error() + "\r\n"))
				continue
			}
			var resp strings.Builder
			resp.WriteString(fmt.Sprintf("*%d\r\n", len(values)))
			for _, val := range values {
				resp.WriteString(fmt.Sprintf("$%d\r\n%s\r\n", len(val), val))
			}
			conn.Write([]byte(resp.String()))
		case "LTRIM":
			if len(args) != 3 {
				conn.Write([]byte("-ERR wrong number of arguments for 'ltrim' command\r\n"))
				continue
			}
			start, err := strconv.Atoi(args[1])
			if err != nil {
				conn.Write([]byte("-ERR value is not an integer or out of range\r\n"))
				continue
			}
			stop, err := strconv.Atoi(args[2])
			if err != nil {
				conn.Write([]byte("-ERR value is not an integer or out of range\r\n"))
				continue
			}
			err = store.LTrim(args[0], start, stop)
			if err != nil {
				conn.Write([]byte(err.Error() + "\r\n"))
				continue
			}
			conn.Write([]byte("+OK\r\n"))
		case "LMOVE":
			if len(args) != 4 {
				conn.Write([]byte("-ERR wrong number of arguments for 'lmove' command\r\n"))
				continue
			}

			uppercaseSrcFlag := strings.ToUpper(args[2])
			uppercaseDestFlag := strings.ToUpper(args[3])
			if uppercaseSrcFlag != "LEFT" && uppercaseSrcFlag != "RIGHT" {
				conn.Write([]byte("-ERR syntax error\r\n"))
				continue
			}
			if uppercaseDestFlag != "LEFT" && uppercaseDestFlag != "RIGHT" {
				conn.Write([]byte("-ERR syntax error\r\n"))
				continue
			}
			if uppercaseSrcFlag == "LEFT" {
				if uppercaseDestFlag == "LEFT" {
					val, err := store.LMove(args[0], args[1], true, true)
					if err != nil {
						conn.Write([]byte(err.Error() + "\r\n"))
						continue
					}
					conn.Write(fmt.Appendf(nil, "$%d\r\n%s\r\n", len(val), val))
					continue
				}
				val, err := store.LMove(args[0], args[1], true, false)
				if err != nil {
					conn.Write([]byte(err.Error() + "\r\n"))
					continue
				}
				conn.Write(fmt.Appendf(nil, "$%d\r\n%s\r\n", len(val), val))
				continue
			}
			if uppercaseDestFlag == "LEFT" {
				val, err := store.LMove(args[0], args[1], false, true)
				if err != nil {
					conn.Write([]byte(err.Error() + "\r\n"))
					continue
				}
				conn.Write(fmt.Appendf(nil, "$%d\r\n%s\r\n", len(val), val))
				continue
			}
			val, err := store.LMove(args[0], args[1], false, false)
			if err != nil {
				conn.Write([]byte(err.Error() + "\r\n"))
				continue
			}
			conn.Write(fmt.Appendf(nil, "$%d\r\n%s\r\n", len(val), val))
		case "BLMOVE":
			if len(args) != 5 {
				conn.Write([]byte("-ERR wrong number of arguments for 'blmove' command\r\n"))
				continue
			}

			sourceKey := args[0]
			destinationKey := args[1]
			uppercaseSrcFlag := strings.ToUpper(args[2])
			uppercaseDestFlag := strings.ToUpper(args[3])
			timeoutStr := args[4]

			if uppercaseSrcFlag != "LEFT" && uppercaseSrcFlag != "RIGHT" {
				conn.Write([]byte("-ERR syntax error\r\n"))
				continue
			}
			if uppercaseDestFlag != "LEFT" && uppercaseDestFlag != "RIGHT" {
				conn.Write([]byte("-ERR syntax error\r\n"))
				continue
			}

			var duration time.Duration
			if timeoutStr == "0" {
				duration = time.Duration(0 * time.Second)
			} else {
				timeout, err := strconv.ParseFloat(timeoutStr, 64)
				if err != nil || timeout < 0 {
					conn.Write([]byte("-ERR timeout is not a float or out of range\r\n"))
					continue
				}
				duration = time.Duration(timeout * float64(time.Second))
			}

			if uppercaseSrcFlag == "LEFT" {
				if uppercaseDestFlag == "LEFT" {
					val, err := store.BLMove(sourceKey, destinationKey, true, true, duration)
					if err != nil {
						conn.Write([]byte(err.Error() + "\r\n"))
						continue
					}
					if val == "" {
						conn.Write([]byte("_\r\n"))
						continue
					}

					conn.Write(fmt.Appendf(nil, "$%d\r\n%s\r\n", len(val), val))
					continue
				}
				val, err := store.BLMove(sourceKey, destinationKey, true, false, duration)
				if err != nil {
					conn.Write([]byte(err.Error() + "\r\n"))
					continue
				}

				if val == "" {
					conn.Write([]byte("_\r\n"))
					continue
				}
				conn.Write(fmt.Appendf(nil, "$%d\r\n%s\r\n", len(val), val))
				continue
			}
			if uppercaseDestFlag == "LEFT" {
				val, err := store.BLMove(sourceKey, destinationKey, false, true, duration)
				if err != nil {
					conn.Write([]byte(err.Error() + "\r\n"))
					continue
				}
				if val == "" {
					conn.Write([]byte("_\r\n"))
					continue
				}

				conn.Write(fmt.Appendf(nil, "$%d\r\n%s\r\n", len(val), val))
				continue
			}
			val, err := store.BLMove(sourceKey, destinationKey, false, false, duration)
			if err != nil {
				conn.Write([]byte(err.Error() + "\r\n"))
				continue
			}
			if val == "" {
				conn.Write([]byte("_\r\n"))
				continue
			}

			conn.Write(fmt.Appendf(nil, "$%d\r\n%s\r\n", len(val), val))
		case "SADD":
			if len(args) < 2 {
				conn.Write([]byte("-ERR wrong number of arguments for 'sadd' command\r\n"))
				continue
			}

			addedValues := 0
			for _, val := range args[1:] {
				returnedVal, err := store.SAdd(args[0], val)
				if err != nil {
					conn.Write([]byte(err.Error() + "\r\n"))
					continue
				}
				addedValues += returnedVal
			}
			conn.Write(fmt.Appendf(nil, ":%d\r\n", addedValues))
		default:
			conn.Write([]byte("-ERR unknown command '" + strings.ToLower(command) + "', with args beginning with: " + strings.Join(args, " ") + "\r\n"))
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
