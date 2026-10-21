// Package sdk wraps the generated transport without hiding operation IDs.
package sdk

import (
	"context"
	"crypto/tls"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	v1 "ssf.dev/ssf/api/ssf/v1"
)

type Client struct {
	Conn *grpc.ClientConn
	Node v1.RuntimeStorageNodeClient
}

func Dial(endpoint string, tlsConfig *tls.Config) (*Client, error) {
	c, e := grpc.NewClient(endpoint, grpc.WithTransportCredentials(credentials.NewTLS(tlsConfig)))
	if e != nil {
		return nil, e
	}
	return &Client{c, v1.NewRuntimeStorageNodeClient(c)}, nil
}
func (c *Client) Close() error { return c.Conn.Close() }
func (c *Client) Prepare(ctx context.Context, operation, plan string) (*v1.PrepareResponse, error) {
	return c.Node.PrepareVolumes(ctx, &v1.PrepareRequest{RequestId: operation, SignedNodePlan: plan})
}
