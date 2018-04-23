package remote

import (
	"fmt"
	"regexp"
	"strconv"

	"github.com/zte-opensource/ceph-boot/hierr"
)

var (
	hostRegexp = regexp.MustCompile(`^(?:([^@]+)@)?(.*?)(?::(\d+))?$`)
)

type Address struct {
	User   string
	Domain string
	Port   int
}

func (a Address) Equal(o Address) bool {
	if a.User != o.User {
		return false
	}

	if a.Domain != o.Domain {
		return false
	}

	if a.Port != o.Port {
		return false
	}

	return true
}

func (a Address) String() string {
	return fmt.Sprintf(
		"[%s@%s:%d]",
		a.User,
		a.Domain,
		a.Port,
	)
}

func ParseAddress(
	host string, defaultUser string, defaultPort int,
) (Address, error) {
	matches := hostRegexp.FindStringSubmatch(host)

	var (
		user    = defaultUser
		domain  = matches[2]
		rawPort = matches[3]
		port    = defaultPort
	)

	if matches[1] != "" {
		user = matches[1]
	}

	if rawPort != "" {
		var err error
		port, err = strconv.Atoi(rawPort)
		if err != nil {
			return Address{}, hierr.Errorf(
				err,
				`can't parse port number: '%s'`, rawPort,
			)
		}
	}

	return Address{
		User:   user,
		Domain: domain,
		Port:   port,
	}, nil
}

func GetUniqueAddresses(addresses []Address) []Address {
	var result []Address

	for _, origin := range addresses {
		keep := true

		for _, another := range result {
			if !origin.Equal(another) {
				continue
			}

			keep = false
		}

		if keep {
			result = append(result, origin)
		}
	}

	return result
}
