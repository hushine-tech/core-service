package okx

import "github.com/hushine-tech/core-service/internal/exchange/adapter"

type Factory struct {
	route adapter.Route
}

func NewFactory(route adapter.Route) *Factory {
	return &Factory{route: route}
}

func (f *Factory) CredentialValidator() (adapter.CredentialValidator, error) {
	return nil, adapter.CapabilityUnsupported("okx credential_validator")
}

func (f *Factory) AccountSnapshotReader() (adapter.AccountSnapshotReader, error) {
	return nil, adapter.CapabilityUnsupported("okx account_snapshot_reader")
}

func (f *Factory) SymbolRulesReader() (adapter.SymbolRulesReader, error) {
	return nil, adapter.CapabilityUnsupported("okx symbol_rules_reader")
}

func (f *Factory) OrderExecutor() (adapter.OrderExecutor, error) {
	return nil, adapter.CapabilityUnsupported("okx order_executor")
}

func (f *Factory) OrderStateReader() (adapter.OrderStateReader, error) {
	return nil, adapter.CapabilityUnsupported("okx order_state_reader")
}

func (f *Factory) OrderCanceller() (adapter.OrderCanceller, error) {
	return nil, adapter.CapabilityUnsupported("okx order_canceller")
}
