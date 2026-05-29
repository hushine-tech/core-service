package domain

import "testing"

func TestEnvironmentLabels(t *testing.T) {
	cases := []struct {
		env  Environment
		want string
	}{
		{EnvironmentBacktest, "backtest"},
		{EnvironmentDemo, "demo"},
		{EnvironmentLive, "live"},
	}
	for _, tc := range cases {
		if got := tc.env.String(); got != tc.want {
			t.Fatalf("Environment(%d).String() = %q, want %q", tc.env, got, tc.want)
		}
	}
}

func TestMarketLabels(t *testing.T) {
	cases := []struct {
		market Market
		want   string
	}{
		{MarketSpot, "spot"},
		{MarketPerpetualFutures, "perpetual_futures"},
		{MarketDeliveryFutures, "delivery_futures"},
	}
	for _, tc := range cases {
		if got := tc.market.String(); got != tc.want {
			t.Fatalf("Market(%d).String() = %q, want %q", tc.market, got, tc.want)
		}
	}
}

func TestVenueMarketModes(t *testing.T) {
	spot := Venue{Market: MarketSpot, MarginMode: MarginModeNone, PositionMode: PositionModeNone}
	if err := spot.ValidateMarketModes(); err != nil {
		t.Fatalf("spot ValidateMarketModes: %v", err)
	}

	badSpot := Venue{Market: MarketSpot, MarginMode: MarginModeCross, PositionMode: PositionModeNone}
	if err := badSpot.ValidateMarketModes(); err == nil {
		t.Fatal("bad spot ValidateMarketModes = nil, want error")
	}

	perp := Venue{Market: MarketPerpetualFutures, MarginMode: MarginModeCross, PositionMode: PositionModeOneWay}
	if err := perp.ValidateMarketModes(); err != nil {
		t.Fatalf("perp ValidateMarketModes: %v", err)
	}

	badPerp := Venue{Market: MarketPerpetualFutures, MarginMode: MarginModeNone, PositionMode: PositionModeOneWay}
	if err := badPerp.ValidateMarketModes(); err == nil {
		t.Fatal("bad perp ValidateMarketModes = nil, want error")
	}
}
