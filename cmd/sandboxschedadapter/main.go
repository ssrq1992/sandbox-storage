// The SandboxSched node storage adapter. Sandbox APIs remain in the original
// runtime; this service runs next to the public SSF node agent.
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	adapter "ssf.dev/ssf/adapter/sandboxschedadapter"
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
		return fmt.Errorf("usage: sandboxschedadapter node -config FILE")
	}
	flags := flag.NewFlagSet("node", flag.ContinueOnError)
	path := flags.String("config", "/etc/sandboxschedadapter/config.json", "node service configuration")
	if err := flags.Parse(os.Args[2:]); err != nil {
		return err
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	return adapter.Start(ctx, *path)
}
