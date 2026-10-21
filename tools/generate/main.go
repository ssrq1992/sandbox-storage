// Command generate uses a pinned pure-Go parser and the official protobuf plugins.
package main

import (
	"bytes"
	"context"
	"flag"
	"fmt"
	"github.com/bufbuild/protocompile"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protodesc"
	"google.golang.org/protobuf/types/pluginpb"
	"os"
	"os/exec"
	"path/filepath"
)

func main() {
	if e := run(); e != nil {
		fmt.Fprintln(os.Stderr, e)
		os.Exit(1)
	}
}
func run() error {
	root := flag.String("root", ".", "proto source root")
	input := flag.String("proto", "api/ssf/v1/node.proto", "proto path relative to root")
	output := flag.String("out", ".", "generated output root")
	flag.Parse()
	name := *input
	compiler := protocompile.Compiler{Resolver: &protocompile.SourceResolver{ImportPaths: []string{*root}}}
	files, e := compiler.Compile(context.Background(), name)
	if e != nil {
		return e
	}
	req := &pluginpb.CodeGeneratorRequest{FileToGenerate: []string{name}, Parameter: proto.String("paths=source_relative")}
	for _, f := range files {
		req.ProtoFile = append(req.ProtoFile, protodesc.ToFileDescriptorProto(f))
	}
	b, e := proto.Marshal(req)
	if e != nil {
		return e
	}
	for _, plugin := range []string{"google.golang.org/protobuf/cmd/protoc-gen-go", "google.golang.org/grpc/cmd/protoc-gen-go-grpc"} {
		cmd := exec.Command("go", "run", "-mod=mod", plugin)
		cmd.Stdin = bytes.NewReader(b)
		cmd.Stderr = os.Stderr
		raw, e := cmd.Output()
		if e != nil {
			return e
		}
		var resp pluginpb.CodeGeneratorResponse
		if e = proto.Unmarshal(raw, &resp); e != nil {
			return e
		}
		if resp.GetError() != "" {
			return fmt.Errorf("plugin: %s", resp.GetError())
		}
		for _, file := range resp.File {
			if e = os.MkdirAll(filepath.Dir(filepath.Join(*output, file.GetName())), 0755); e != nil {
				return e
			}
			if e = os.WriteFile(filepath.Join(*output, file.GetName()), []byte(file.GetContent()), 0644); e != nil {
				return e
			}
		}
	}
	return nil
}
