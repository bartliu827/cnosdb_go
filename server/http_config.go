package server

import (
	"bytes"
	"fmt"
	"regexp"
	"strconv"
	"time"

	tls "github.com/cnosdb/cnosdb/pkg/tlsconfig"
	"github.com/cnosdb/cnosdb/vend/common/pkg/toml"

	"github.com/pkg/errors"
)

const (
	// DefaultBindAddress is the default address to bind to.
	DefaultBindAddress = ":8086"

	// DefaultRealm is the default realm sent back when issuing a basic auth challenge.
	DefaultRealm = "CnosDB"

	// DefaultBindSocket is the default unix socket to bind to.
	DefaultBindSocket = "/var/run/cnosdb.sock"

	// DefaultMaxBodySize is the default maximum size of a client request body, in bytes. Specify 0 for no limit.
	DefaultMaxBodySize = 25e6

	// DefaultEnqueuedWriteTimeout is the maximum time a write request can wait to be processed.
	DefaultEnqueuedWriteTimeout = 30 * time.Second
)

type HTTPConfig struct {
	Enabled                 bool           `toml:"enabled"`
	BindAddress             string         `toml:"bind-address"`
	AuthEnabled             bool           `toml:"auth-enabled"`
	LogEnabled              bool           `toml:"log-enabled"`
	SuppressWriteLog        bool           `toml:"suppress-write-log"`
	WriteTracing            bool           `toml:"write-tracing"`
	PprofEnabled            bool           `toml:"pprof-enabled"`
	DebugPprofEnabled       bool           `toml:"debug-pprof-enabled"`
	HTTPSEnabled            bool           `toml:"https-enabled"`
	HTTPSCertificate        string         `toml:"https-certificate"`
	HTTPSPrivateKey         string         `toml:"https-private-key"`
	MaxRowLimit             int            `toml:"max-row-limit"`
	MaxConnectionLimit      int            `toml:"max-connection-limit"`
	SharedSecret            string         `toml:"shared-secret"`
	Realm                   string         `toml:"realm"`
	UnixSocketEnabled       bool           `toml:"unix-socket-enabled"`
	UnixSocketGroup         *toml.Group    `toml:"unix-socket-group"`
	UnixSocketPermissions   toml.FileMode  `toml:"unix-socket-permissions"`
	BindSocket              string         `toml:"bind-socket"`
	MaxBodySize             int            `toml:"max-body-size"`
	AccessLogPath           string         `toml:"access-log-path"`
	AccessLogStatusFilters  []StatusFilter `toml:"access-log-status-filters"`
	MaxConcurrentWriteLimit int            `toml:"max-concurrent-write-limit"`
	MaxEnqueuedWriteLimit   int            `toml:"max-enqueued-write-limit"`
	EnqueuedWriteTimeout    time.Duration  `toml:"enqueued-write-timeout"`
	TLS                     *tls.Config    `toml:"-"`
}

func NewHTTPConfig() HTTPConfig {
	return HTTPConfig{
		Enabled:               true,
		BindAddress:           DefaultBindAddress,
		LogEnabled:            true,
		PprofEnabled:          true,
		DebugPprofEnabled:     false,
		HTTPSEnabled:          false,
		HTTPSCertificate:      "/etc/ssl/cnosdb.pem",
		MaxRowLimit:           0,
		Realm:                 DefaultRealm,
		UnixSocketEnabled:     false,
		UnixSocketPermissions: 0777,
		BindSocket:            DefaultBindSocket,
		MaxBodySize:           DefaultMaxBodySize,
		EnqueuedWriteTimeout:  DefaultEnqueuedWriteTimeout,
	}
}

// StatusFilter HTTP ????????????????????? statusCode % divisor = base ???
type StatusFilter struct {
	base    int
	divisor int
}

// reStatusFilter ????????? StatusFilter ???????????? ??? 1-5 ???????????????????????? base ???????????????????????? x ????????? divisor ?????????????????????
var reStatusFilter = regexp.MustCompile(`^([1-5]\d*)([xX]*)$`)

// ParseStatusFilter ??????????????? s ?????? StatusFilter
func ParseStatusFilter(s string) (StatusFilter, error) {
	m := reStatusFilter.FindStringSubmatch(s)
	if m == nil {
		return StatusFilter{}, fmt.Errorf("status filter must be a digit that starts with 1-5 optionally followed by X characters")
	} else if len(s) != 3 {
		return StatusFilter{}, fmt.Errorf("status filter must be exactly 3 characters long")
	}

	// ??????????????? s ???????????? 1 ??? x ????????? divisor ??? 10 ??? base ??? 2 ??? ?????????
	// ???????????? 2 ??? x ????????? divisor ??? 100 ??? base ??? 1 ????????????
	// ??????????????????????????? statusCode % divisor ???????????????????????? base ??????
	base, err := strconv.Atoi(m[1])
	if err != nil {
		return StatusFilter{}, err
	}

	divisor := 1
	switch len(m[2]) {
	case 1:
		divisor = 10
	case 2:
		divisor = 100
	}
	return StatusFilter{
		base:    base,
		divisor: divisor,
	}, nil
}

// Match ?????? HTTP ???????????????????????? Filter
func (sf StatusFilter) Match(statusCode int) bool {
	if sf.divisor == 0 {
		return false
	}
	return statusCode/sf.divisor == sf.base
}

// UnmarshalText ?????? TOML ????????????????????? Filter
func (sf *StatusFilter) UnmarshalText(text []byte) error {
	f, err := ParseStatusFilter(string(text))
	if err != nil {
		return err
	}
	*sf = f
	return nil
}

// MarshalText ?????? Filter ????????? TOML ??????
func (sf StatusFilter) MarshalText() (text []byte, err error) {
	var buf bytes.Buffer
	if sf.base != 0 {
		buf.WriteString(strconv.Itoa(sf.base))
	}

	switch sf.divisor {
	case 1:
	case 10:
		buf.WriteString("X")
	case 100:
		buf.WriteString("XX")
	default:
		return nil, errors.New("invalid status filter")
	}
	return buf.Bytes(), nil
}

type StatusFilters []StatusFilter

func (filters StatusFilters) Match(statusCode int) bool {
	if len(filters) == 0 {
		return true
	}

	for _, sf := range filters {
		if sf.Match(statusCode) {
			return true
		}
	}
	return false
}
