package credential

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"strings"
)

const keySize = 32

var (
	ErrEmptyKey       = errors.New("credential encryption key is required")
	ErrInvalidKey     = errors.New("credential encryption key must be 32 bytes")
	ErrEmptyPlaintext = errors.New("credential plaintext is required")
)

type Manager struct {
	key        []byte
	keyVersion string
}

func NewManager(rawKey string, keyVersion string) (*Manager, error) {
	key, err := parseKey(rawKey)
	if err != nil {
		return nil, err
	}
	keyVersion = strings.TrimSpace(keyVersion)
	if keyVersion == "" {
		keyVersion = "v1"
	}
	return &Manager{key: key, keyVersion: keyVersion}, nil
}

func (m *Manager) Encrypt(plaintext string) (string, error) {
	plaintext = strings.TrimSpace(plaintext)
	if plaintext == "" {
		return "", ErrEmptyPlaintext
	}
	block, err := aes.NewCipher(m.key)
	if err != nil {
		return "", fmt.Errorf("new cipher: %w", err)
	}
	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return "", fmt.Errorf("new gcm: %w", err)
	}
	nonce := make([]byte, gcm.NonceSize())
	if _, err := io.ReadFull(rand.Reader, nonce); err != nil {
		return "", fmt.Errorf("read nonce: %w", err)
	}
	sealed := gcm.Seal(nonce, nonce, []byte(plaintext), nil)
	return m.keyVersion + ":" + base64.StdEncoding.EncodeToString(sealed), nil
}

func (m *Manager) Decrypt(ciphertext string) (string, error) {
	version, payload, ok := strings.Cut(strings.TrimSpace(ciphertext), ":")
	if !ok || strings.TrimSpace(version) == "" || strings.TrimSpace(payload) == "" {
		return "", errors.New("invalid credential ciphertext format")
	}
	raw, err := base64.StdEncoding.DecodeString(payload)
	if err != nil {
		return "", fmt.Errorf("decode credential ciphertext: %w", err)
	}
	block, err := aes.NewCipher(m.key)
	if err != nil {
		return "", fmt.Errorf("new cipher: %w", err)
	}
	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return "", fmt.Errorf("new gcm: %w", err)
	}
	if len(raw) <= gcm.NonceSize() {
		return "", errors.New("credential ciphertext too short")
	}
	nonce := raw[:gcm.NonceSize()]
	body := raw[gcm.NonceSize():]
	plaintext, err := gcm.Open(nil, nonce, body, nil)
	if err != nil {
		return "", fmt.Errorf("decrypt credential ciphertext: %w", err)
	}
	return string(plaintext), nil
}

func Fingerprint(apiKey string) string {
	apiKey = strings.TrimSpace(apiKey)
	if apiKey == "" {
		return ""
	}
	sum := sha256.Sum256([]byte(apiKey))
	return hex.EncodeToString(sum[:])
}

func (m *Manager) KeyVersion() string {
	return m.keyVersion
}

func parseKey(rawKey string) ([]byte, error) {
	rawKey = strings.TrimSpace(rawKey)
	if rawKey == "" {
		return nil, ErrEmptyKey
	}
	if len([]byte(rawKey)) == keySize {
		return []byte(rawKey), nil
	}
	decoded, err := base64.StdEncoding.DecodeString(rawKey)
	if err == nil && len(decoded) == keySize {
		return decoded, nil
	}
	return nil, ErrInvalidKey
}
