package notification

import (
	"context"
	"testing"
	"time"

	"github.com/IBM/sarama"
	"github.com/hushine-tech/core-service/internal/domain"
)

type fakeConsumerGroupSession struct {
	ctx           context.Context
	markedTopic   string
	markedPart    int32
	markedOffset  int64
	commitCount   int
	resetCount    int
	commitOffsets []int64
}

func (f *fakeConsumerGroupSession) Claims() map[string][]int32 { return nil }
func (f *fakeConsumerGroupSession) MemberID() string           { return "member-1" }
func (f *fakeConsumerGroupSession) GenerationID() int32        { return 1 }
func (f *fakeConsumerGroupSession) Context() context.Context {
	if f.ctx == nil {
		return context.Background()
	}
	return f.ctx
}

func (f *fakeConsumerGroupSession) MarkOffset(topic string, partition int32, offset int64, _ string) {
	f.markedTopic = topic
	f.markedPart = partition
	f.markedOffset = offset
}

func (f *fakeConsumerGroupSession) MarkMessage(msg *sarama.ConsumerMessage, metadata string) {
	f.MarkOffset(msg.Topic, msg.Partition, msg.Offset+1, metadata)
}

func (f *fakeConsumerGroupSession) ResetOffset(string, int32, int64, string) {
	f.resetCount++
}

func (f *fakeConsumerGroupSession) Commit() {
	f.commitCount++
	f.commitOffsets = append(f.commitOffsets, f.markedOffset)
}

type fakeConsumerGroupClaim struct {
	topic     string
	partition int32
	initial   int64
	high      int64
	messages  chan *sarama.ConsumerMessage
}

func (f *fakeConsumerGroupClaim) Topic() string              { return f.topic }
func (f *fakeConsumerGroupClaim) Partition() int32           { return f.partition }
func (f *fakeConsumerGroupClaim) InitialOffset() int64       { return f.initial }
func (f *fakeConsumerGroupClaim) HighWaterMarkOffset() int64 { return f.high }
func (f *fakeConsumerGroupClaim) Messages() <-chan *sarama.ConsumerMessage {
	return f.messages
}

func TestConsumerCommitsAfterEachNotificationMessage(t *testing.T) {
	repo := &fakeRepo{
		user:     domain.User{ID: 42, PlanCode: "pro"},
		settings: domain.NotificationSettings{UserID: 42, Enabled: true, SystemEnabled: true, StrategyEnabled: true, CustomEnabled: true},
		channel:  domain.NotificationChannel{UserID: 42, Channel: domain.NotificationChannelTelegram, Status: domain.NotificationChannelStatusBound, TargetID: "chat-1"},
		plan:     domain.NotificationPlan{PlanCode: "pro", NotificationEnabled: true, AllowSystem: true, AllowStrategy: true, AllowCustom: true},
	}
	sender := &fakeSender{}
	svc := NewService(repo, sender, Config{}, func() time.Time { return time.Unix(100, 0).UTC() })
	handler := &consumerHandler{svc: svc}

	messages := make(chan *sarama.ConsumerMessage, 1)
	messages <- &sarama.ConsumerMessage{
		Topic:     "notification.events",
		Partition: 0,
		Offset:    7,
		Value: []byte(`{
			"schema_version": 1,
			"user_id": 42,
			"category": "strategy",
			"event_type": "order.accepted",
			"severity": "info",
			"message": "Order accepted"
		}`),
	}
	close(messages)

	session := &fakeConsumerGroupSession{}
	claim := &fakeConsumerGroupClaim{topic: "notification.events", partition: 0, messages: messages}
	if err := handler.ConsumeClaim(session, claim); err != nil {
		t.Fatalf("ConsumeClaim: %v", err)
	}

	if len(sender.sent) != 1 {
		t.Fatalf("sent = %#v, want one Telegram message", sender.sent)
	}
	if session.markedTopic != "notification.events" || session.markedPart != 0 || session.markedOffset != 8 {
		t.Fatalf("marked offset = %s/%d/%d, want notification.events/0/8", session.markedTopic, session.markedPart, session.markedOffset)
	}
	if session.commitCount != 1 {
		t.Fatalf("commit count = %d, want 1", session.commitCount)
	}
	if got := session.commitOffsets[0]; got != 8 {
		t.Fatalf("committed offset = %d, want 8", got)
	}
}
