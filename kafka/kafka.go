package kafka

import (
	"github.com/Shopify/sarama"
	"github.com/sirupsen/logrus"
)

var (
	client  sarama.SyncProducer
	msgChan chan *sarama.ProducerMessage
)

func Init(address []string, ChanSize int64) (err error) {
	config := sarama.NewConfig()
	config.Producer.RequiredAcks = sarama.WaitForAll          // 发送完数据需要leader和follow都确认
	config.Producer.Partitioner = sarama.NewRandomPartitioner // 新选出一个partition
	config.Producer.Return.Successes = true                   // 成功交付的消息将在success channel返回

	client, err = sarama.NewSyncProducer(address, config)
	if err != nil {
		logrus.Error("producer closed, err:", err)
		return
	}
	msgChan = make(chan *sarama.ProducerMessage, ChanSize)
	go SendMsg()
	return
}

func SendMsg() {
	for {
		select {
		case msg := <-msgChan:
			pid, offset, err := client.SendMessage(msg)
			if err != nil {
				logrus.Warning("send msg failed,err:", err)
				return
			}
			logrus.Info("send msg to kafka success, pid:%v, offset:%v", pid, offset)
		default:
		}
	}
}

func MsgChan(msg *sarama.ProducerMessage) {
	msgChan <- msg
}
