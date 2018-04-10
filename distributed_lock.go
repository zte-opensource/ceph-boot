package main

import (
	"bufio"
	"fmt"
	"io"
	"strings"
	"sync"

	"github.com/reconquest/lineflushwriter-go"
	"github.com/reconquest/prefixwriter-go"
	"github.com/reconquest/hierr-go"
	"github.com/zte-opensource/runcmd"
)

const (
	lockAcquiredString = `acquired`
	lockLockedString   = `locked`
)

type distributedLockNode struct {
	address address
	runner  runcmd.Runner

	connection *distributedLockConnection
}

type distributedLock struct {
	nodes []*distributedLockNode
}

func (node *distributedLockNode) String() string {
	return node.address.String()
}

type distributedLockConnection struct {
	stdin  io.WriteCloser
	stdout io.Reader
}

func (node *distributedLockNode) lock(
	filename string,
) error {
	lockCommandLine := []string{
		"sh", "-c", fmt.Sprintf(
			`flock -nx %s -c 'printf "%s\n" && cat' || printf "%s\n"`,
			filename, lockAcquiredString, lockLockedString,
		),
	}

	logMutex := &sync.Mutex{}

	traceln(hierr.Errorf(
		lockCommandLine,
		`%s running lock command`,
		node,
	))

	lockCommand := node.runner.Command(
		lockCommandLine[0],
		lockCommandLine[1:]...,
	)

	stdout, err := lockCommand.StdoutPipe()
	if err != nil {
		return hierr.Errorf(
			err,
			`can't get control stdout pipe from lock process`,
		)
	}

	stderr := lineflushwriter.New(
		prefixwriter.New(
			newDebugWriter(logger),
			fmt.Sprintf("%s {flock} <stderr> ", node.String()),
		),
		logMutex,
		true,
	)

	lockCommand.SetStderr(stderr)

	stdin, err := lockCommand.StdinPipe()
	if err != nil {
		return hierr.Errorf(
			err,
			`can't get control stdin pipe to lock process`,
		)
	}

	err = lockCommand.Start()
	if err != nil {
		return hierr.Errorf(
			err,
			`%s can't start lock command: '%s`,
			node, lockCommandLine,
		)
	}

	line, err := bufio.NewReader(stdout).ReadString('\n')
	if err != nil {
		return hierr.Errorf(
			err,
			`%s can't read lock status line from lock process`,
			node,
		)
	}

	switch strings.TrimSpace(line) {
	case lockAcquiredString:
		// pass

	case lockLockedString:
		return fmt.Errorf(
			`%s can't acquire lock, `+
				`lock already obtained by another process `+
				`or unavailable`,
			node,
		)

	default:
		return fmt.Errorf(
			`%s unexpected reply string encountered `+
				`instead of '%s' or '%s': '%s'`,
			node, lockAcquiredString, lockLockedString,
			line,
		)
	}

	tracef(`lock acquired: '%s' on '%s'`, node, filename)

	node.connection = &distributedLockConnection{
		stdin:  stdin,
		stdout: stdout,
	}

	return nil
}
