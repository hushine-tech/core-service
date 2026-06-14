package mockserver

func mockExchangeInfoSymbol(symbol string) map[string]any {
	return map[string]any{
		"symbol": symbol,
		"filters": []map[string]any{
			{
				"filterType": "PRICE_FILTER",
				"tickSize":   "0.01",
			},
			{
				"filterType": "LOT_SIZE",
				"minQty":     "0.001",
				"stepSize":   "0.001",
			},
			{
				"filterType": "MIN_NOTIONAL",
				"notional":   "5",
			},
		},
	}
}
