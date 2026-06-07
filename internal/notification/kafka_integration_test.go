//go:build integration

package notification

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/IBM/sarama"
	"github.com/hushine-tech/core-service/internal/domain"
)

type integrationSender struct {
	mu   sync.Mutex
	sent []string
}

func (s *integrationSender) SendMessage(_ context.Context, chatID string, text string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.sent = append(s.sent, chatID+":"+text)
	return nil
}

func (s *integrationSender) Count() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.sent)
}

func (s *integrationSender) Messages() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]string, len(s.sent))
	copy(out, s.sent)
	return out
}

type readyConsumerHandler struct {
	svc   *Service
	ready chan struct{}
	once  sync.Once
}

func (h *readyConsumerHandler) Setup(sarama.ConsumerGroupSession) error {
	h.once.Do(func() { close(h.ready) })
	return nil
}

func (h *readyConsumerHandler) Cleanup(sarama.ConsumerGroupSession) error { return nil }

func (h *readyConsumerHandler) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	return (&consumerHandler{svc: h.svc}).ConsumeClaim(session, claim)
}

type runningIntegrationConsumer struct {
	cancel context.CancelFunc
	group  sarama.ConsumerGroup
	done   chan struct{}
	errs   chan error
	ready  chan struct{}
}

func TestKafkaConsumerGroupDoesNotReplayCommittedNotificationAfterRestart(t *testing.T) {
	brokers := kafkaIntegrationBrokers()
	cfg := kafkaIntegrationConfig()
	admin, err := sarama.NewClusterAdmin(brokers, cfg)
	if err != nil {
		t.Skipf("kafka integration broker unavailable at %v: %v", brokers, err)
	}
	defer admin.Close()

	suffix := fmt.Sprintf("%d", time.Now().UnixNano())
	topic := "notification.commit.integration." + suffix
	groupID := "notification-commit-integration-" + suffix
	if err := admin.CreateTopic(topic, &sarama.TopicDetail{NumPartitions: 1, ReplicationFactor: 1}, false); err != nil && err != sarama.ErrTopicAlreadyExists {
		t.Fatalf("create topic %s: %v", topic, err)
	}
	defer func() { _ = admin.DeleteTopic(topic) }()

	firstSender := &integrationSender{}
	firstConsumer := startIntegrationConsumer(t, brokers, cfg, groupID, topic, notificationIntegrationService(firstSender))
	waitForConsumerReady(t, firstConsumer)

	firstOffset := publishIntegrationNotification(t, brokers, cfg, topic, "evt-first", "first order")
	waitUntil(t, 10*time.Second, func() bool { return firstSender.Count() == 1 }, "first consumer to receive first message")
	waitForCommittedOffset(t, admin, groupID, topic, firstOffset+1)
	stopIntegrationConsumer(t, firstConsumer)

	secondSender := &integrationSender{}
	secondConsumer := startIntegrationConsumer(t, brokers, cfg, groupID, topic, notificationIntegrationService(secondSender))
	waitForConsumerReady(t, secondConsumer)
	assertNoMessages(t, secondSender, 1500*time.Millisecond, "restarted consumer replayed committed first message")

	secondOffset := publishIntegrationNotification(t, brokers, cfg, topic, "evt-second", "second order")
	waitUntil(t, 10*time.Second, func() bool { return secondSender.Count() == 1 }, "restarted consumer to receive second message")
	messages := secondSender.Messages()
	if !strings.Contains(messages[0], "second order") || strings.Contains(messages[0], "first order") {
		t.Fatalf("restarted consumer messages = %#v, want only second order", messages)
	}
	waitForCommittedOffset(t, admin, groupID, topic, secondOffset+1)
	stopIntegrationConsumer(t, secondConsumer)
}

func kafkaIntegrationBrokers() []string {
	raw := strings.TrimSpace(os.Getenv("NOTIFICATION_KAFKA_BROKERS"))
	if raw == "" {
		raw = "192.168.88.10:19092"
	}
	parts := strings.Split(raw, ",")
	out := make([]string, 0, len(parts))
	for _, part := range parts {
		if trimmed := strings.TrimSpace(part); trimmed != "" {
			out = append(out, trimmed)
		}
	}
	return out
}

func kafkaIntegrationConfig() *sarama.Config {
	cfg := sarama.NewConfig()
	cfg.Version = sarama.V2_8_0_0
	cfg.ClientID = "core-service-notification-integration-test"
	cfg.Producer.Return.Successes = true
	cfg.Consumer.Offsets.Initial = sarama.OffsetNewest
	cfg.Consumer.Return.Errors = true
	return cfg
}

func notificationIntegrationService(sender *integrationSender) *Service {
	repo := &fakeRepo{
		user:     domain.User{ID: 42, PlanCode: "pro"},
		settings: domain.NotificationSettings{UserID: 42, Enabled: true, SystemEnabled: true, StrategyEnabled: true, CustomEnabled: true},
		channel:  domain.NotificationChannel{UserID: 42, Channel: domain.NotificationChannelTelegram, Status: domain.NotificationChannelStatusBound, TargetID: "chat-1"},
		plan:     domain.NotificationPlan{PlanCode: "pro", NotificationEnabled: true, AllowSystem: true, AllowStrategy: true, AllowCustom: true},
	}
	return NewService(repo, sender, Config{SendTimeout: 2 * time.Second}, time.Now)
}

func startIntegrationConsumer(t *testing.T, brokers []string, cfg *sarama.Config, groupID string, topic string, svc *Service) *runningIntegrationConsumer {
	t.Helper()
	group, err := sarama.NewConsumerGroup(brokers, groupID, cfg)
	if err != nil {
		t.Fatalf("create consumer group: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	rc := &runningIntegrationConsumer{
		cancel: cancel,
		group:  group,
		done:   make(chan struct{}),
		errs:   make(chan error, 1),
		ready:  make(chan struct{}),
	}
	handler := &readyConsumerHandler{svc: svc, ready: rc.ready}
	go func() {
		defer close(rc.done)
		for ctx.Err() == nil {
			if err := group.Consume(ctx, []string{topic}, handler); err != nil {
				if ctx.Err() == nil {
					rc.errs <- err
				}
				return
			}
		}
	}()
	return rc
}

func waitForConsumerReady(t *testing.T, rc *runningIntegrationConsumer) {
	t.Helper()
	select {
	case <-rc.ready:
	case err := <-rc.errs:
		t.Fatalf("consumer failed before ready: %v", err)
	case <-time.After(10 * time.Second):
		t.Fatal("consumer did not become ready")
	}
}

func stopIntegrationConsumer(t *testing.T, rc *runningIntegrationConsumer) {
	t.Helper()
	rc.cancel()
	select {
	case <-rc.done:
	case err := <-rc.errs:
		t.Fatalf("consumer failed while stopping: %v", err)
	case <-time.After(10 * time.Second):
		t.Fatal("consumer did not stop")
	}
	if err := rc.group.Close(); err != nil {
		t.Fatalf("close consumer group: %v", err)
	}
}

func publishIntegrationNotification(t *testing.T, brokers []string, cfg *sarama.Config, topic string, eventID string, message string) int64 {
	t.Helper()
	producer, err := sarama.NewSyncProducer(brokers, cfg)
	if err != nil {
		t.Fatalf("create producer: %v", err)
	}
	defer producer.Close()
	raw, err := json.Marshal(Event{
		SchemaVersion: SchemaVersion,
		EventID:       eventID,
		UserID:        42,
		Category:      CategoryStrategy,
		EventType:     EventOrderAccepted,
		Severity:      SeverityInfo,
		Message:       message,
	})
	if err != nil {
		t.Fatalf("marshal event: %v", err)
	}
	partition, offset, err := producer.SendMessage(&sarama.ProducerMessage{
		Topic: topic,
		Key:   sarama.StringEncoder("42"),
		Value: sarama.ByteEncoder(raw),
	})
	if err != nil {
		t.Fatalf("send event: %v", err)
	}
	if partition != 0 {
		t.Fatalf("event written to partition %d, want partition 0", partition)
	}
	return offset
}

func waitForCommittedOffset(t *testing.T, admin sarama.ClusterAdmin, groupID string, topic string, wantOffset int64) {
	t.Helper()
	waitUntil(t, 10*time.Second, func() bool {
		resp, err := admin.ListConsumerGroupOffsets(groupID, map[string][]int32{topic: []int32{0}})
		if err != nil {
			return false
		}
		block := resp.GetBlock(topic, 0)
		return block != nil && block.Err == sarama.ErrNoError && block.Offset >= wantOffset
	}, fmt.Sprintf("committed offset >= %d", wantOffset))
}

func waitUntil(t *testing.T, timeout time.Duration, ok func() bool, desc string) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if ok() {
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", desc)
}

func assertNoMessages(t *testing.T, sender *integrationSender, duration time.Duration, desc string) {
	t.Helper()
	timer := time.NewTimer(duration)
	defer timer.Stop()
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-timer.C:
			return
		case <-ticker.C:
			if sender.Count() != 0 {
				t.Fatalf("%s: %#v", desc, sender.Messages())
			}
		}
	}
}
