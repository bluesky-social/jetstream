package main

import "github.com/urfave/cli/v3"

func newTestApp() *cli.Command {
	return newAppWithEnviron(func() []string { return nil })
}
