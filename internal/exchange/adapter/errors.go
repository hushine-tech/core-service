package adapter

import (
	"errors"
	"fmt"
)

const (
	CodeRouteUnsupported      = "route_unsupported"
	CodeCapabilityUnsupported = "capability_unsupported"
)

var (
	ErrRouteUnsupported      = errors.New(CodeRouteUnsupported)
	ErrCapabilityUnsupported = errors.New(CodeCapabilityUnsupported)
)

type Error struct {
	Code       string
	Route      Route
	Capability string
}

func (e Error) Error() string {
	switch e.Code {
	case CodeRouteUnsupported:
		return fmt.Sprintf("%s: exchange=%s environment=%s market=%s",
			e.Code,
			e.Route.Exchange.String(),
			e.Route.Environment.String(),
			e.Route.Market.String(),
		)
	case CodeCapabilityUnsupported:
		if e.Capability != "" {
			return fmt.Sprintf("%s: %s", e.Code, e.Capability)
		}
		return e.Code
	default:
		return e.Code
	}
}

func (e Error) Unwrap() error {
	switch e.Code {
	case CodeRouteUnsupported:
		return ErrRouteUnsupported
	case CodeCapabilityUnsupported:
		return ErrCapabilityUnsupported
	default:
		return nil
	}
}

func RouteUnsupported(route Route) error {
	return Error{Code: CodeRouteUnsupported, Route: route}
}

func CapabilityUnsupported(capability string) error {
	return Error{Code: CodeCapabilityUnsupported, Capability: capability}
}
