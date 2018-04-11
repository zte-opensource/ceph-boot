package main

import "github.com/reconquest/hierr-go"

func runSyncProtocol(
	cluster *Cluster,
	raw *rawCommand,
) error {
	protocol := newSyncProtocol()

	command, err := raw.parseCommand()
	if err != nil {
		return err
	}

	execution, err := runCommand(
		cluster,
		command,
		func(remoteNode *CommandSession) {
			remoteNode.stdout = newProtocolNodeWriter(remoteNode, protocol)
		},
		raw.serial,
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
		err = protocol.SendNode(node.session)
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

	err = execution.wait()
	if err != nil {
		return hierr.Errorf(
			err,
			`failed to finish sync tool command`,
		)
	}

	return nil
}
