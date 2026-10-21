// AgentENV node storage plugin. Sandbox APIs remain in the original runtime.
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	adapter "ssf.dev/ssf/adapter/agentenvadapter"
	"syscall"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
func run() error {
	if len(os.Args) < 2 || os.Args[1] != "node" {
		return fmt.Errorf("usage: agentenvadapter node -config FILE")
	}
	flags := flag.NewFlagSet("node", flag.ContinueOnError)
	path := flags.String("config", "/etc/agentenvadapter/config.json", "native node plugin configuration")
	if err := flags.Parse(os.Args[2:]); err != nil {
		return err
	}
	cfg, err := adapter.Load(*path)
	if err != nil {
		return err
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	return adapter.Run(ctx, "node", cfg)
}
