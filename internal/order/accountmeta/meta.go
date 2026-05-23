package accountmeta

// Meta holds account configuration needed by the order domain to route and execute orders.
type Meta struct {
	AccountID      int64
	UserID         int64
	Mode           int32  // 0=backtest, 1=live, 2=testnet
	MarginMode     string // "cross" / "isolated"
	PositionMode   string // "one_way" / "hedge"
	APIKey         string
	APISecret      string
	DefaultFeeRate float64
	SlippageBps    float64
}
