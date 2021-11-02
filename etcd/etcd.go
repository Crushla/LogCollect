package etcd

import (
	"LogCollect/common"
	"LogCollect/tailfile"
	"context"
	"encoding/json"
	"github.com/sirupsen/logrus"
	"go.etcd.io/etcd/clientv3"
	"time"
)

var (
	client *clientv3.Client
)

func Init(address []string) (err error) {
	client, err = clientv3.New(clientv3.Config{
		Endpoints:   address,
		DialTimeout: 5 * time.Second,
	})
	if err != nil {
		logrus.Error("connect to etcd failed, err:%v\n", err)
		return
	}
	return
}

func GetConfig(key string) (collectEntryList []common.CollectEntry, err error) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*5)
	defer cancel()
	value, err := client.Get(ctx, key)
	if err != nil {
		logrus.Error("get conf from etcd by :%s failed,err:%v", key, err)
		return
	}
	if len(value.Kvs) == 0 {
		logrus.Warning("get len 0 conf etcd by key:%s", key)
		return
	}
	ret := value.Kvs[0]
	json.Unmarshal(ret.Value, &collectEntryList)
	return
}

func Watch(key string) {
	rch := client.Watch(context.Background(), key)
	for wresp := range rch {
		for _, evt := range wresp.Events {
			var collectEntryList []common.CollectEntry
			err := json.Unmarshal(evt.Kv.Value, &collectEntryList)
			if err != nil {
				logrus.Error("json unmarshal new conf failed, err:%v", err)
				continue
			}
			//告诉tailfile用新的配置
			tailfile.SendNewConf(collectEntryList)
		}
	}
}
