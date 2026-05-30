package lifecycle

import "context"

type EventStore interface {
	SaveLifecycleEvent(ctx context.Context, event Event) (Event, error)
}

type Ingestor struct {
	store EventStore
}

func NewIngestor(store EventStore) *Ingestor {
	return &Ingestor{store: store}
}

func (i *Ingestor) Ingest(ctx context.Context, event Event) (Event, error) {
	return i.store.SaveLifecycleEvent(ctx, event)
}
