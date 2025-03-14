package main

import (
	"errors"
	"net"
	"regexp"
	"strconv"
	"strings"
)

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
