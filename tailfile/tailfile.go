package tailfile

import (
	"LogCollect/kafka"
	"context"
	"github.com/Shopify/sarama"
	"github.com/hpcloud/tail"
	"github.com/sirupsen/logrus"
)

type tailTask struct {
	path    string
	topic   string
	tailObj *tail.Tail
	ctx     context.Context
	cancel  context.CancelFunc
}

func (task *tailTask) run() {
	for {
		select {
		case <-task.ctx.Done():
			return
		default:

		}
		line, ok := <-task.tailObj.Lines
		if !ok {
			logrus.Warn("tail file close reopen, filename:%s\n", task.path)
			continue
		}
		if len(line.Text) == 0 {
			continue
		}
		msg := &sarama.ProducerMessage{}
		msg.Topic = task.topic
		msg.Value = sarama.StringEncoder(line.Text)
		kafka.MsgChan(msg)
	}
}
