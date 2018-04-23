package remote

import (
	"fmt"

	"github.com/zte-opensource/runcmd"
)

type (
	RunnerFactory func(address Address) (runcmd.Runner, error)
)

func CreateRemoteRunnerFactoryWithKey(
	key string,
	timeouts *runcmd.Timeouts,
) RunnerFactory {
	return func(address Address) (runcmd.Runner, error) {
		return createRunner(
			runcmd.NewRemoteRawKeyAuthRunnerWithTimeouts,
			key,
			address,
			*timeouts,
		)
	}
}

func CreateRemoteRunnerFactoryWithPassword(
	password string,
	timeouts *runcmd.Timeouts,
) RunnerFactory {
	return func(address Address) (runcmd.Runner, error) {
		return createRunner(
			runcmd.NewRemotePassAuthRunnerWithTimeouts,
			password,
			address,
			*timeouts,
		)
	}
}

func CreateRemoteRunnerFactoryWithAgent(
	sock string,
	timeouts *runcmd.Timeouts,
) RunnerFactory {
	return func(address Address) (runcmd.Runner, error) {
		return createRunner(
			runcmd.NewRemoteAgentAuthRunnerWithTimeouts,
			sock,
			address,
			*timeouts,
		)
	}
}

func createRunner(
	factory func(string, string, string, runcmd.Timeouts) (*runcmd.Remote, error),
	auth string,
	address Address,
	timeouts runcmd.Timeouts,
) (runcmd.Runner, error) {
	return factory(
		address.User,
		fmt.Sprintf("%s:%d", address.Domain, address.Port),
		auth,
		timeouts,
	)
}
