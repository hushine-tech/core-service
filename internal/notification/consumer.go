package notification

import (
	"context"
	"errors"
	"time"

	"github.com/IBM/sarama"
)

func RunKafkaConsumer(ctx context.Context, brokers []string, groupID string, topic string, svc *Service) error {
	if svc == nil {
		return errors.New("notification service is required")
	}
	if len(brokers) == 0 {
		return errors.New("notification kafka brokers are required")
	}
	if groupID == "" {
		groupID = "core-service-notification"
	}
	if topic == "" {
		topic = "notification.events"
	}
	cfg := sarama.NewConfig()
	cfg.Consumer.Offsets.Initial = sarama.OffsetNewest
	group, err := sarama.NewConsumerGroup(brokers, groupID, cfg)
	if err != nil {
		return err
	}
	defer group.Close()

	handler := &consumerHandler{svc: svc}
	for {
		if err := group.Consume(ctx, []string{topic}, handler); err != nil {
			if ctx.Err() != nil {
				return nil
			}
			time.Sleep(time.Second)
			continue
		}
		if ctx.Err() != nil {
			return nil
		}
	}
}

type consumerHandler struct {
	svc *Service
}

func (h *consumerHandler) Setup(sarama.ConsumerGroupSession) error   { return nil }
func (h *consumerHandler) Cleanup(sarama.ConsumerGroupSession) error { return nil }

func (h *consumerHandler) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for msg := range claim.Messages() {
		if event, err := ParseEvent(msg.Value); err == nil {
			_ = h.svc.DeliverEvent(session.Context(), event)
		}
		session.MarkMessage(msg, "")
	}
	return nil
}
