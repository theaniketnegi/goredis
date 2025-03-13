package main

import (
	"regexp"
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
