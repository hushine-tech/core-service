package credential

import (
	"encoding/base64"
	"errors"
	"strings"
	"testing"
)

const rawTestKey = "0123456789abcdef0123456789abcdef"

func TestManagerRawKeyRoundTrip(t *testing.T) {
	mgr, err := NewManager(rawTestKey, "v1")
	if err != nil {
		t.Fatalf("NewManager: %v", err)
	}
	got, err := roundTrip(mgr, `{"api_key":"k","api_secret":"s"}`)
	if err != nil {
		t.Fatalf("roundTrip: %v", err)
	}
	if got != `{"api_key":"k","api_secret":"s"}` {
		t.Fatalf("roundTrip plaintext = %q", got)
	}
}

func TestManagerBase64KeyRoundTrip(t *testing.T) {
	key := base64.StdEncoding.EncodeToString([]byte(rawTestKey))
	mgr, err := NewManager(key, "v2")
	if err != nil {
		t.Fatalf("NewManager: %v", err)
	}
	ciphertext, err := mgr.Encrypt("secret")
	if err != nil {
		t.Fatalf("Encrypt: %v", err)
	}
	if !strings.HasPrefix(ciphertext, "v2:") {
		t.Fatalf("ciphertext prefix = %q, want v2:", ciphertext)
	}
	plaintext, err := mgr.Decrypt(ciphertext)
	if err != nil {
		t.Fatalf("Decrypt: %v", err)
	}
	if plaintext != "secret" {
		t.Fatalf("plaintext = %q", plaintext)
	}
}

func TestManagerWrongKeyFailsDecrypt(t *testing.T) {
	mgr, err := NewManager(rawTestKey, "v1")
	if err != nil {
		t.Fatalf("NewManager: %v", err)
	}
	ciphertext, err := mgr.Encrypt("secret")
	if err != nil {
		t.Fatalf("Encrypt: %v", err)
	}
	wrong, err := NewManager("abcdef0123456789abcdef0123456789", "v1")
	if err != nil {
		t.Fatalf("NewManager wrong: %v", err)
	}
	if _, err := wrong.Decrypt(ciphertext); err == nil {
		t.Fatal("Decrypt with wrong key = nil, want error")
	}
}

func TestManagerRejectsEmptyKey(t *testing.T) {
	if _, err := NewManager("", "v1"); !errors.Is(err, ErrEmptyKey) {
		t.Fatalf("NewManager empty key err = %v, want ErrEmptyKey", err)
	}
}

func TestManagerRejectsEmptyPlaintext(t *testing.T) {
	mgr, err := NewManager(rawTestKey, "v1")
	if err != nil {
		t.Fatalf("NewManager: %v", err)
	}
	if _, err := mgr.Encrypt("   "); !errors.Is(err, ErrEmptyPlaintext) {
		t.Fatalf("Encrypt empty err = %v, want ErrEmptyPlaintext", err)
	}
}

func TestFingerprintStableAndTrimmed(t *testing.T) {
	a := Fingerprint(" api-key ")
	b := Fingerprint("api-key")
	if a == "" {
		t.Fatal("Fingerprint returned empty string")
	}
	if a != b {
		t.Fatalf("Fingerprint should trim whitespace: %q != %q", a, b)
	}
}

func roundTrip(mgr *Manager, plaintext string) (string, error) {
	ciphertext, err := mgr.Encrypt(plaintext)
	if err != nil {
		return "", err
	}
	return mgr.Decrypt(ciphertext)
}
