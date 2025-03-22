package main

import (
	"errors"
	"fmt"
	"net"
	"regexp"
	"strconv"
	"strings"
)

var readOnlyCmds map[string]bool = map[string]bool{
	"PING":     true,
	"ECHO":     true,
	"GET":      true,
	"TTL":      true,
	"CONFIG":   true,
	"KEYS":     true,
	"LASTSAVE": true,
	"INFO":     true,
}

func parsePattern(pattern string) string {
	pattern = regexp.QuoteMeta(pattern)
	pattern = strings.ReplaceAll(pattern, "\\?", ".")
	pattern = strings.ReplaceAll(pattern, "\\*", ".*")
	pattern = strings.ReplaceAll(pattern, "\\[\\^", "[^")
	pattern = strings.ReplaceAll(pattern, "\\[", "[")
	pattern = strings.ReplaceAll(pattern, "\\]", "]")
	return "^" + pattern + "$"
}

func validateHost(host string) error {
	if _, err := net.LookupIP(host); err != nil {
		return errors.New("invalid host")
	}
	return nil
}

func validatePort(port string) error {
	portNum, err := strconv.Atoi(port)
	if err != nil {
		return errors.New("invalid port (port must be a number)")
	}

	if portNum < 0 || portNum > 65535 {
		return errors.New("invalid port (port must be in range 0-65535)")
	}

	return nil
}

func convertCmdToResp(command string, args []string) []byte {
	var response strings.Builder
	response.WriteString(fmt.Sprintf("*%d\r\n$%d\r\n%s\r\n", len(args)+1, len(command), command))

	for _, arg := range args {
		response.WriteString(fmt.Sprintf("$%d\r\n%s\r\n", len(arg), arg))
	}

	fmt.Printf("%q\n", response.String())
	return []byte(response.String())
}

func readOnlyCommands(cmd string) bool {
	_, ok := readOnlyCmds[cmd]

	return ok
}
