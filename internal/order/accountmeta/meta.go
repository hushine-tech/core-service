package accountmeta

// Meta holds account configuration needed by the order domain to route and execute orders.
type Meta struct {
	AccountID      int64
	VenueID        int64
	UserID         int64
	Environment    int32
	Exchange       int32
	Market         int32
	MarginMode     string
	PositionMode   string
	APIKey         string
	APISecret      string
	CredentialJSON string
	DefaultFeeRate float64
	SlippageBps    float64
}
