package repository

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/hushine-tech/core-service/internal/domain"
)

func TestListStrategiesPageOrdersByCreatedAtDesc(t *testing.T) {
	repo, ctx := notificationTestRepo(t)
	user := createNotificationTestUser(t, ctx, repo)

	now := time.Now().UTC()
	fixtures := []struct {
		name      string
		createdAt time.Time
	}{
		{name: "aaa-old", createdAt: now.Add(-3 * time.Hour)},
		{name: "zzz-new", createdAt: now.Add(-1 * time.Hour)},
		{name: "mmm-middle", createdAt: now.Add(-2 * time.Hour)},
	}

	ids := make([]int64, 0, len(fixtures))
	prefix := fmt.Sprintf("strategy-order-%d-", time.Now().UnixNano())
	t.Cleanup(func() {
		for _, id := range ids {
			_, _ = repo.db.ExecContext(context.Background(), `DELETE FROM strategies WHERE strategy_id = $1`, id)
		}
	})

	for _, fixture := range fixtures {
		id, err := repo.CreateStrategy(ctx, domain.Strategy{
			UserID:      user.ID,
			Name:        prefix + fixture.name,
			Version:     "1.0.0",
			Description: "strategy list ordering regression",
			Code:        "class MyStrategy:\n    pass\n",
		})
		if err != nil {
			t.Fatalf("create strategy %s: %v", fixture.name, err)
		}
		ids = append(ids, id)
		if _, err := repo.db.ExecContext(ctx,
			`UPDATE strategies SET created_at = $1 WHERE strategy_id = $2`,
			fixture.createdAt, id,
		); err != nil {
			t.Fatalf("set created_at for %s: %v", fixture.name, err)
		}
	}

	got, meta, err := repo.ListStrategiesPage(ctx, user.ID, prefix, false, 10, 0)
	if err != nil {
		t.Fatalf("list strategies page: %v", err)
	}
	if meta.Total != int64(len(fixtures)) {
		t.Fatalf("total = %d, want %d", meta.Total, len(fixtures))
	}
	if len(got) != len(fixtures) {
		t.Fatalf("rows = %d, want %d", len(got), len(fixtures))
	}

	wantNames := []string{prefix + "zzz-new", prefix + "mmm-middle", prefix + "aaa-old"}
	for i, want := range wantNames {
		if got[i].Name != want {
			t.Fatalf("row %d name = %q, want %q; rows=%v", i, got[i].Name, want, []string{got[0].Name, got[1].Name, got[2].Name})
		}
	}
}
