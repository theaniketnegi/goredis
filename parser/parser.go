package parser

import (
	"bufio"
	"errors"
	"io"
	"strconv"
	"strings"
)

func ParseRESP(reader *bufio.Reader) (string, []string, error) {
	b, err := reader.ReadString('\n')

	if err != nil {
		return "", nil, err
	}
	line := strings.TrimSpace(b)
	switch line[0] {
	case '*':
		arrSize, err := strconv.Atoi(line[1:])
		if err != nil {
			return "", nil, err
		}

		var args []string
		for range arrSize {
			line, err := reader.ReadString('\n')
			if err != nil {
				return "", nil, err
			}

			line = strings.TrimSpace(line)

			if !strings.HasPrefix(line, "$") {
				return "", nil, errors.New("expected bulk string")
			}
			stringLength, err := strconv.Atoi(line[1:])

			if err != nil {
				return "", nil, errors.New("invalid length")
			}

			word := make([]byte, stringLength+2)
			_, err = reader.Read(word)
			if err != nil {
				return "", nil, errors.New("invalid string length")
			}

			word = []byte(strings.TrimSpace(string(word)))
			args = append(args, string(word))
		}

		if len(args) == 0 {
			return "", nil, nil
		}
		return strings.ToUpper(args[0]), args[1:], nil
	}
	return "", nil, nil
}

func ParseRDBMessage(reader *bufio.Reader) (int, string, error) {
	b, err := reader.ReadString('\n')
	if err != nil {
		return 0, "", err
	}

	line := strings.TrimSpace(b)

	fileLength, err := strconv.Atoi(line[1:])
	if err != nil {
		return 0, "", err
	}

	content := make([]byte, fileLength)
	_, err = io.ReadFull(reader, content)
	if err != nil {
		return 0, "", err
	}

	return fileLength, string(content), nil
}
