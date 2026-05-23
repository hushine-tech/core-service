package service

import (
	"context"
	"testing"
	"time"

	"github.com/hushine-tech/core-service/gen/accountv1"
	"github.com/hushine-tech/core-service/internal/domain"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestCreateUserSuccess(t *testing.T) {
	repo := &stubRepo{}
	svc := NewAccountGRPCService(repo, nil, nil, nil)

	resp, err := svc.CreateUser(context.Background(), &accountv1.CreateUserRequest{
		Username: "  Trader_01  ",
		Password: "secret-123",
	})
	if err != nil {
		t.Fatalf("CreateUser: %v", err)
	}
	if resp.GetUser().GetId() == 0 {
		t.Fatal("expected non-zero user id")
	}
	if resp.GetUser().GetUsername() != "trader_01" {
		t.Fatalf("username: got %q", resp.GetUser().GetUsername())
	}
	stored, err := repo.GetUserByUsername(context.Background(), "trader_01")
	if err != nil {
		t.Fatalf("stored user lookup: %v", err)
	}
	if stored.PasswordHash == "secret-123" || stored.PasswordHash == "" {
		t.Fatalf("expected bcrypt hash, got %q", stored.PasswordHash)
	}
}

func TestCreateUserDuplicateUsername(t *testing.T) {
	repo := &stubRepo{
		users: map[string]domain.User{
			"trader_01": {ID: 1, Username: "trader_01", PasswordHash: "hashed", CreatedAt: time.Now().UTC()},
		},
	}
	svc := NewAccountGRPCService(repo, nil, nil, nil)

	_, err := svc.CreateUser(context.Background(), &accountv1.CreateUserRequest{
		Username: "trader_01",
		Password: "secret-123",
	})
	if err == nil {
		t.Fatal("expected duplicate username error")
	}
	if status.Code(err) != codes.AlreadyExists {
		t.Fatalf("code: got %v", status.Code(err))
	}
}

func TestVerifyUserPasswordSuccess(t *testing.T) {
	repo := &stubRepo{}
	svc := NewAccountGRPCService(repo, nil, nil, nil)
	created, err := svc.CreateUser(context.Background(), &accountv1.CreateUserRequest{
		Username: "trader_01",
		Password: "secret-123",
	})
	if err != nil {
		t.Fatalf("CreateUser: %v", err)
	}

	resp, err := svc.VerifyUserPassword(context.Background(), &accountv1.VerifyUserPasswordRequest{
		Username: "Trader_01",
		Password: "secret-123",
	})
	if err != nil {
		t.Fatalf("VerifyUserPassword: %v", err)
	}
	if !resp.GetValid() {
		t.Fatal("expected valid=true")
	}
	if resp.GetUser().GetId() != created.GetUser().GetId() {
		t.Fatalf("user id: got %d want %d", resp.GetUser().GetId(), created.GetUser().GetId())
	}
}

func TestVerifyUserPasswordInvalidPassword(t *testing.T) {
	repo := &stubRepo{}
	svc := NewAccountGRPCService(repo, nil, nil, nil)
	if _, err := svc.CreateUser(context.Background(), &accountv1.CreateUserRequest{
		Username: "trader_01",
		Password: "secret-123",
	}); err != nil {
		t.Fatalf("CreateUser: %v", err)
	}

	resp, err := svc.VerifyUserPassword(context.Background(), &accountv1.VerifyUserPasswordRequest{
		Username: "trader_01",
		Password: "wrong-password",
	})
	if err != nil {
		t.Fatalf("VerifyUserPassword: %v", err)
	}
	if resp.GetValid() {
		t.Fatal("expected valid=false")
	}
	if resp.GetUser() != nil {
		t.Fatalf("expected no user on invalid password, got %+v", resp.GetUser())
	}
}

func TestListAccountsRequiresUserID(t *testing.T) {
	svc := NewAccountGRPCService(&stubRepo{}, nil, nil, nil)

	_, err := svc.ListAccounts(context.Background(), &accountv1.ListAccountsRequest{})
	if err == nil {
		t.Fatal("expected missing user_id error")
	}
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("code: got %v", status.Code(err))
	}
}
