package put

import (
	"bufio"
	"bytes"
	"crypto/x509"
	"encoding/pem"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"os/user"
	"strconv"
	"strings"
	"sync"
	"time"

	"golang.org/x/crypto/ssh/terminal"

	"github.com/spf13/cobra"

	"github.com/mattn/go-shellwords"
	"github.com/zte-opensource/ceph-boot/hierr"
	"github.com/zte-opensource/ceph-boot/log"
	"github.com/zte-opensource/ceph-boot/remote"
	"github.com/zte-opensource/ceph-boot/runcmd"
)

const (
	// heartbeatTimeoutCoefficient will be multiplied to send timeout and
	// resulting value will be used as time interval between heartbeats.
	heartbeatTimeoutCoefficient = 0.8
)

var (
	verboseMap = map[int]log.Verbosity{
		0: log.VerbosityQuiet,
		1: log.VerbosityInfo,
		2: log.VerbosityDebug,
	}

	sshPasswordPrompt   = "Password: "
	sshPassphrasePrompt = "Key passphrase: "

	pool *remote.ThreadPool
)

var (
	verbose        int
	inventory      []string
	sshPrivateKey  string
	askPassword    bool
	sshAgent       bool
	sudo           bool
	sshDefaultUser string
	sshDefaultPort int
	stdin          string
	serial         bool
	noPrefix       bool
	parallel       int
	colorDark      bool
	colorLight     bool
	connTimeout    int
	sendTimeout    int
	recvTimeout    int
	keepAlive      int
	shell          string
	commandLine    string
	preserveUid    bool
	preserveGid    bool
)

func init() {
	curUser, _ := user.Current()

	defaultKey := curUser.HomeDir + "/.ssh/id_rsa"
	defaultUser := curUser.Username
	defaultPort := 22

	flags := PutCmd.Flags()

	flags.CountVarP(&verbose, "verbose", "v", "verbose output")
	flags.StringSliceVarP(&inventory, "inventory", "i", nil,
		"specify inventory host path or comma separated host list")
	PutCmd.MarkFlagRequired("inventory")
	flags.StringVar(&sshPrivateKey, "key", defaultKey,
		"identity file (private key), which will be used for authentication")
	flags.BoolVarP(&askPassword, "ask-pass", "k", false,
		"enable password authentication, interactive TTY is required for reading password")
	flags.BoolVar(&sshAgent, "ssh-agent", false,
		"enable ssh-agent forwarding")
	flags.BoolVar(&sudo, "sudo", false,
		"run operations with sudo (nopasswd)")
	flags.StringVarP(&sshDefaultUser, "user", "u", defaultUser,
		"default username used for connecting to all hosts")
	flags.IntVarP(&sshDefaultPort, "port", "p", defaultPort,
		"default ssh port used for connecting to all hosts")
	flags.StringVar(&stdin, "stdin", "",
		"pass specified file as input for the command")
	flags.BoolVar(&serial, "serial", false,
		"run commands in serial mode, so they output will not interleave each other")
	flags.BoolVar(&noPrefix, "no-prefix", false,
		"do not print prefixes for running commands")
	flags.IntVar(&parallel, "parellel", 16,
		"parallel remote execution count")
	flags.BoolVar(&colorDark, "color-dark", false,
		"set status bar theme to dark")
	flags.BoolVar(&colorLight, "color-light", true,
		"set status bar theme to light")
	flags.IntVar(&connTimeout, "conn-timeout", 10,
		"remote host connection timeout in seconds")
	flags.IntVar(&sendTimeout, "send-timeout", 60,
		"remote host connection data sending timeout in seconds")
	flags.IntVar(&recvTimeout, "recv-timeout", 60,
		"remote host connection data receiving timeout in seconds")
	flags.IntVar(&keepAlive, "keep-alive", 10,
		"how long to keep connection keeped alive after session ends")
	flags.StringVar(&shell, "shell", "bash -c {}",
		"'{}' will be replaced with properly escaped command")
	flags.StringVarP(&commandLine, "command", "c", "",
		"command to execute after upload finished")
	flags.BoolVar(&preserveUid, "preserve-uid", true,
		"preserve uid")
	flags.BoolVar(&preserveGid, "preserve-gid", true,
		"preserve gid")
}

var PutCmd = &cobra.Command{
	Use:   "put",
	Short: "Upload files via SSH",
	Long:  `Upload files via SSH.`,
	Args:  cobra.MinimumNArgs(2),
	Run: func(cmd *cobra.Command, args []string) {
		run(args)
	},
}

func run(args []string) error {
	src := args[:len(args)-1]
	dst := args[len(args)-1]

	var filesList []remote.File

	if verbose > 2 {
		verbose = 2
	}
	verbosity := verboseMap[verbose]

	theme := log.ThemeDefault
	switch {
	case colorLight:
		theme = log.ThemeLight
	case colorDark:
		theme = log.ThemeDark
	}

	log.SetupLogger(verbosity)
	if len(stdin) == 0 {
		log.SetupStatusBar(theme)
	}

	pool = remote.NewThreadPool(parallel)

	commandline := args

	canceler := sync.NewCond(&sync.Mutex{})

	addresses, err := parseAddresses(inventory, sshDefaultUser, sshDefaultPort)
	if err != nil {
		return hierr.Errorf(
			err,
			`can't parse all specified addresses`,
		)
	}

	timeouts := &runcmd.Timeouts{
		ConnectionTimeout: time.Second * time.Duration(connTimeout),
		SendTimeout:       time.Second * time.Duration(sendTimeout),
		ReceiveTimeout:    time.Second * time.Duration(recvTimeout),
		KeepAlive:         time.Second * time.Duration(keepAlive),
	}

	runnerFactory, err := createRunnerFactory(timeouts, sshPrivateKey, askPassword, sshAgent)
	if err != nil {
		return hierr.Errorf(
			err,
			`can't create runner factory`,
		)
	}

	log.Debugf(`using %d threads`, pool.Size)
	log.Debugf(`connecting to %d nodes`, len(addresses))

	heartbeatDuration := time.Duration(
		float64(sendTimeout)*heartbeatTimeoutCoefficient,
	) * time.Second

	config := remote.Config{
		Pool:         pool,
		Addresses:    addresses,
		LockFile:     dst,
		HbInterval:   heartbeatDuration,
		HbCancelCond: canceler,
	}

	cluster := remote.NewCluster(config)
	err = cluster.Connect(runnerFactory)
	if err != nil {
		return hierr.Errorf(
			err,
			`connecting to cluster failed`,
		)
	}

	log.Debugf(`global lock acquired on %d nodes`, len(cluster.Nodes))

	log.Debugf(`building files list from %d sources`, len(src))
	filesList, err = remote.GetFilesList(src...)
	if err != nil {
		return hierr.Errorf(
			err,
			`can't build files list`,
		)
	}

	log.Debugf(`file list contains %d files`, len(filesList))
	log.Debugf(`files to upload: %+v`, filesList)

	log.Debugf(`file upload started into: '%s'`, dst)

	// start tar command which waits files on stdin to extract
	err = remote.StartArchiveReceivers(cluster, dst, sudo, serial)
	if err != nil {
		return hierr.Errorf(
			err,
			`can't start archive receivers on the cluster`,
		)
	}

	err = remote.ArchiveFilesToWriter(
		cluster.Stdin,
		filesList,
		preserveUid,
		preserveGid,
	)
	if err != nil {
		return hierr.Errorf(
			err,
			`can't archive files and send to the remote nodes`,
		)
	}

	log.Debugf(`waiting file upload to finish`)

	err = cluster.Stdin.Close()
	if err != nil {
		return hierr.Errorf(
			err,
			`can't close archive receiver stdin`,
		)
	}

	err = cluster.Wait()
	if err != nil {
		return hierr.Errorf(
			err,
			`archive upload failed`,
		)
	}

	log.Debugf(`upload done`)

	if len(commandLine) == 0 {
		return nil
	}

	log.Debugf(`starting sync tool`)

	command, err := shellwords.NewParser().Parse(commandLine)
	if err != nil {
		return hierr.Errorf(
			err,
			`can't parse sync tool command: '%s'`,
			commandline,
		)
	}

	c, err := remote.New(sudo, shell, commandline, command)
	if err != nil {
		return hierr.Errorf(
			err,
			"invalid command line",
		)
	}

	err = cluster.RunCommand(c, serial)
	if err != nil {
		return hierr.Errorf(
			err,
			`can't run remote execution on %d nodes`,
			len(cluster.Nodes),
		)
	}

	if stdin != "" {
		var inputFile *os.File

		inputFile, err = os.Open(stdin)
		if err != nil {
			return hierr.Errorf(
				err,
				`can't open file for passing as stdin: '%s'`,
				inputFile,
			)
		}

		_, err = io.Copy(cluster.Stdin, inputFile)
		if err != nil {
			return hierr.Errorf(
				err,
				`can't copy input file to the execution processes`,
			)
		}
	}

	log.Debugf(`commands are running, waiting for finish`)

	err = cluster.Stdin.Close()
	if err != nil {
		return hierr.Errorf(
			err,
			`can't close stdin`,
		)
	}

	err = cluster.Wait()
	if err != nil {
		return hierr.Errorf(
			err,
			`remote execution failed, because one of `+
				`command has been exited with non-zero exit `+
				`code (or timed out) at least on one node`,
		)
	}

	return nil
}

func readSSHKey(path string) ([]byte, error) {
	data, err := ioutil.ReadFile(path)
	if err != nil {
		return nil, hierr.Errorf(
			err,
			`can't read SSH key from file`,
		)
	}

	decoded, extra := pem.Decode(data)

	if len(extra) != 0 {
		return nil, hierr.Errorf(
			errors.New(string(extra)),
			`extra data found in the SSH key`,
		)
	}

	if procType, ok := decoded.Headers[`Proc-Type`]; ok {
		// according to pem_decrypt.go in stdlib
		if procType == `4,ENCRYPTED` {
			passphrase, err := readPassword(sshPassphrasePrompt)
			if err != nil {
				return nil, hierr.Errorf(
					err,
					`can't read key passphrase`,
				)
			}

			data, err = x509.DecryptPEMBlock(decoded, []byte(passphrase))
			if err != nil {
				return nil, hierr.Errorf(
					err,
					`can't decrypt (using passphrase) SSH key`,
				)
			}

			rsa, err := x509.ParsePKCS1PrivateKey(data)
			if err != nil {
				return nil, hierr.Errorf(
					err,
					`can't parse decrypted key as RSA key`,
				)
			}

			pemBytes := bytes.Buffer{}
			err = pem.Encode(
				&pemBytes,
				&pem.Block{
					Type:  `RSA PRIVATE KEY`,
					Bytes: x509.MarshalPKCS1PrivateKey(rsa),
				},
			)
			if err != nil {
				return nil, hierr.Errorf(
					err,
					`can't convert decrypted RSA key into PEM format`,
				)
			}

			data = pemBytes.Bytes()
		}
	}

	return data, nil
}

func createRunnerFactory(
	timeouts *runcmd.Timeouts,
	sshKeyPath string,
	askPassword bool,
	sshForwarding bool,
) (remote.RunnerFactory, error) {
	switch {
	case askPassword:
		var password string

		password, err := readPassword(sshPasswordPrompt)
		if err != nil {
			return nil, hierr.Errorf(
				err,
				`can't read password`,
			)
		}

		return remote.CreateRemoteRunnerFactoryWithPassword(
			password,
			timeouts,
		), nil

	case sshForwarding:
		sock := os.Getenv("SSH_AUTH_SOCK")
		if sock == "" {
			return nil, fmt.Errorf(`can't find ssh-agent socket`)
		}

		return remote.CreateRemoteRunnerFactoryWithAgent(
			sock,
			timeouts,
		), nil

	case sshKeyPath != "":
		key, err := readSSHKey(sshKeyPath)
		if err != nil {
			return nil, hierr.Errorf(
				err,
				`can't read SSH key: '%s'`,
				sshKeyPath,
			)
		}

		return remote.CreateRemoteRunnerFactoryWithKey(
			string(key),
			timeouts,
		), nil

	}

	return nil, fmt.Errorf(
		`no matching runner factory found [password, publickey, ssh-agent]`,
	)
}

func parseAddresses(
	hosts []string,
	defaultUser string,
	defaultPort int,
) ([]remote.Address, error) {
	var (
		hostsToParse []string
		addresses    []remote.Address
	)

	for _, host := range hosts {
		if strings.HasPrefix(host, "/") || strings.HasPrefix(host, "./") {
			hostsFile, err := os.Open(host)
			if err != nil {
				return nil, hierr.Errorf(
					err,
					`can't open hosts file: '%s'`,
					host,
				)
			}

			scanner := bufio.NewScanner(hostsFile)
			for scanner.Scan() {
				hostsToParse = append(hostsToParse, scanner.Text())
			}
		} else {
			hostsToParse = append(hostsToParse, host)
		}
	}

	for _, host := range hostsToParse {
		parsedAddress, err := remote.ParseAddress(
			host, defaultUser, defaultPort,
		)
		if err != nil {
			return nil, hierr.Errorf(
				err,
				`can't parse specified address '%s'`,
				host,
			)
		}

		addresses = append(addresses, parsedAddress)
	}

	return remote.GetUniqueAddresses(addresses), nil
}

func readPassword(prompt string) (string, error) {
	fmt.Fprintf(os.Stderr, prompt)

	tty, err := os.Open("/dev/tty")
	if err != nil {
		return "", hierr.Errorf(
			err,
			`TTY is required for reading password, `+
				`but /dev/tty can't be opened`,
		)
	}

	password, err := terminal.ReadPassword(int(tty.Fd()))
	if err != nil {
		return "", hierr.Errorf(
			err,
			`can't read password`,
		)
	}

	if prompt != "" {
		fmt.Fprintln(os.Stderr)
	}

	return string(password), nil
}

func makeTimeouts(args map[string]interface{}) (*runcmd.Timeouts, error) {
	var (
		connectionTimeoutRaw = args["--conn-timeout"].(string)
		sendTimeoutRaw       = args["--send-timeout"].(string)
		receiveTimeoutRaw    = args["--recv-timeout"].(string)
		keepAliveRaw         = args["--keep-alive"].(string)
	)

	connectionTimeout, err := strconv.Atoi(connectionTimeoutRaw)
	if err != nil {
		return nil, hierr.Errorf(
			err,
			`can't convert specified connection timeout to number: '%s'`,
			connectionTimeoutRaw,
		)
	}

	sendTimeout, err := strconv.Atoi(sendTimeoutRaw)
	if err != nil {
		return nil, hierr.Errorf(
			err,
			`can't convert specified send timeout to number: '%s'`,
			sendTimeoutRaw,
		)
	}

	receiveTimeout, err := strconv.Atoi(receiveTimeoutRaw)
	if err != nil {
		return nil, hierr.Errorf(
			err,
			`can't convert specified receive timeout to number: '%s'`,
			receiveTimeoutRaw,
		)
	}

	keepAlive, err := strconv.Atoi(keepAliveRaw)
	if err != nil {
		return nil, hierr.Errorf(
			err,
			`can't convert specified keep alive time to number: '%s'`,
			keepAliveRaw,
		)
	}

	return &runcmd.Timeouts{
		ConnectionTimeout: time.Millisecond * time.Duration(connectionTimeout),
		SendTimeout:       time.Millisecond * time.Duration(sendTimeout),
		ReceiveTimeout:    time.Millisecond * time.Duration(receiveTimeout),
		KeepAlive:         time.Millisecond * time.Duration(keepAlive),
	}, nil
}
