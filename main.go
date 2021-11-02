package main

import (
	"LogCollect/etcd"
	"LogCollect/kafka"
	"LogCollect/tailfile"
	"github.com/go-ini/ini"
	"github.com/sirupsen/logrus"
)

type Config struct {
	KafkaConfig   `ini:"kafka"`
	CollectConfig `ini:"collect"`
	EtcdConfig    `ini:"etcd"`
}

type KafkaConfig struct {
	Address  string `ini:"address"`
	ChanSize int64  `ini:"chan_size"`
}
type CollectConfig struct {
	Path string `ini:"log_path"`
}
type EtcdConfig struct {
	Address string `ini:"address"`
	Key     string `ini:"key"`
}

func run() {
	select {}
}

//log collection client
//收集指定目录下的日志文件，发送到kafka中
func main() {
	var configObject = new(Config)
	//读配置文件
	error := ini.MapTo(configObject, "./config/config.ini")
	if error != nil {
		logrus.Error("load config failed, err:%v", error)
		return
	}
	//初始化,连接kafka
	error = kafka.Init([]string{configObject.KafkaConfig.Address}, configObject.KafkaConfig.ChanSize)
	if error != nil {
		logrus.Error("init kafka failed, err:%v", error)
		return
	}
	logrus.Info("init kafka success")
	//初始etcd
	error = etcd.Init([]string{configObject.EtcdConfig.Address})
	if error != nil {
		logrus.Error("init etcd failed, err:%v", error)
		return
	}
	logrus.Info("init etcd success")
	//从etcd中拉取要收集日志的配置项
	allconfig, error := etcd.GetConfig(configObject.EtcdConfig.Key)
	if error != nil {
		logrus.Error("get all config failed, err:%v", error)
		return
	}
	go etcd.Watch(configObject.EtcdConfig.Key)
	//初始化tailfile
	error = tailfile.Init(allconfig)
	if error != nil {
		logrus.Error("init tailfile failed, err:%v", error)
		return
	}
	logrus.Info("init tailfile success")
	run()
}
