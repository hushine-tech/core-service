package venuekeys

import (
	"crypto/rand"
	"encoding/hex"
	"strings"
)

const BacktestPrefix = "sim_btv_"

func NewBacktestAPIKey() (string, error) {
	var raw [16]byte
	if _, err := rand.Read(raw[:]); err != nil {
		return "", err
	}
	return BacktestPrefix + hex.EncodeToString(raw[:]), nil
}

func IsBacktestAPIKey(value string) bool {
	value = strings.TrimSpace(value)
	if len(value) != len(BacktestPrefix)+32 {
		return false
	}
	if !strings.HasPrefix(value, BacktestPrefix) {
		return false
	}
	for _, ch := range value[len(BacktestPrefix):] {
		if (ch < '0' || ch > '9') && (ch < 'a' || ch > 'f') {
			return false
		}
	}
	return true
}
