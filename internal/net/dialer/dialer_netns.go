//go:build linux && !android

package dialer

import "errors"

// switchNetns retains the internal contract without changing OS threads or namespaces.
// Linux product nodes reject nonempty namespace requests before DialFunc or socket I/O.
func switchNetns(name string) (restore func(), err error) {
	if name != "" {
		return nil, errors.ErrUnsupported
	}
	return func() {}, nil
}
