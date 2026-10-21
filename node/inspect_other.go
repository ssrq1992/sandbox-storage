//go:build !linux

package node

import (
	"errors"
	"ssf.dev/ssf/core"
)

func inspect(string, bool, bool) (core.Endpoint, error) {
	return core.Endpoint{}, errors.New("real mount verification requires Linux")
}
