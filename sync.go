package main

import "github.com/reconquest/hierr-go"

func runSyncProtocol(
	cluster *Cluster,
	command []string,
	serial bool,
) error {
	protocol := newSyncProtocol()

	execution, err := cluster.RunCommand(
		command,
		func(remoteCommand *RemoteCommand) {
			remoteCommand.stdout = newProtocolNodeWriter(remoteCommand, protocol)
		},
		serial,
	)
	if err != nil {
		return hierr.Errorf(
			err,
			`can't run sync tool command`,
		)
	}

	tracef(`starting sync protocol with %d nodes`, len(execution.nodes))

	err = protocol.Init(execution.stdin)
	if err != nil {
		return hierr.Errorf(
			err,
			`can't init protocol with sync tool`,
		)
	}

	tracef(`sending information about %d nodes to each`, len(execution.nodes))

	for _, node := range execution.nodes {
		err = protocol.SendNode(node.remoteCommand)
		if err != nil {
			return hierr.Errorf(
				err,
				`can't send node to sync tool: '%s'`,
				node.String(),
			)
		}
	}

	tracef(`sending start message to sync tools`)

	err = protocol.SendStart()
	if err != nil {
		return hierr.Errorf(
			err,
			`can't start sync tool`,
		)
	}

	debugf(`waiting sync tool to finish`)

	err = execution.Wait()
	if err != nil {
		return hierr.Errorf(
			err,
			`failed to finish sync tool command`,
		)
	}

	return nil
}
