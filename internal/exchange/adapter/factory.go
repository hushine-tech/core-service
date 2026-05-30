package adapter

type Factory interface {
	CredentialValidator() (CredentialValidator, error)
	AccountSnapshotReader() (AccountSnapshotReader, error)
	SymbolRulesReader() (SymbolRulesReader, error)
	OrderExecutor() (OrderExecutor, error)
	OrderStateReader() (OrderStateReader, error)
	OrderCanceller() (OrderCanceller, error)
}
