package catalog

import (
	"reflect"
	"testing"
)

func TestFilterSymbols(t *testing.T) {
	all := []string{"BTCUSDT", "ETHUSDT", "BNBUSDT", "BTCEUR"}
	got := filterSymbols(all, "BTC", 10)
	want := []string{"BTCUSDT", "BTCEUR"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("filterSymbols: got %v want %v", got, want)
	}
	got2 := filterSymbols(all, "", 2)
	if len(got2) != 2 || got2[0] != "BTCUSDT" || got2[1] != "ETHUSDT" {
		t.Fatalf("limit: got %v", got2)
	}
}
