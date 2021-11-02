package main

import (
	"context"
	"fmt"
	"time"

	"go.etcd.io/etcd/clientv3"
)

func main() {
	cli, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{"127.0.0.1:2379"},
		DialTimeout: 5 * time.Second,
	})
	if err != nil {
		// handle error!
		fmt.Printf("connect to etcd failed, err:%v\n", err)
		return
	}
	fmt.Println("connect to etcd success")
	defer cli.Close()
	// put
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	//_, err = cli.Put(ctx, "log_conf", "[{\"path\":\"/home/hwd/code/go/src/LogCollection/log1\",\"topic\":\"web1\"},{\"path\":\"/home/hwd/code/go/src/LogCollection/log2\",\"topic\":\"web2\"},{\"path\":\"/home/hwd/code/go/src/LogCollection/log3\",\"topic\":\"web3\"}]\n")
	_, err = cli.Put(ctx, "log_conf", "[{\"path\":\"/home/hwd/code/go/src/LogCollection/log1\",\"topic\":\"web1\"},{\"path\":\"/home/hwd/code/go/src/LogCollection/log2\",\"topic\":\"web2\"}]\n")
	cancel()
	if err != nil {
		fmt.Printf("put to etcd failed, err:%v\n", err)
		return
	}
	// get
	ctx, cancel = context.WithTimeout(context.Background(), time.Second)
	resp, err := cli.Get(ctx, "log_conf")
	cancel()
	if err != nil {
		fmt.Printf("get from etcd failed, err:%v\n", err)
		return
	}
	for _, ev := range resp.Kvs {
		fmt.Printf("%s:%s\n", ev.Key, ev.Value)
	}
	//	//watch
	//	rch := cli.Watch(context.Background(), "q1mi")
	//WATCH:
	//	for wresp := range rch{
	//		for _,ev := range wresp.Events {
	//			fmt.Printf("Type: %s Key:%s Value:%s\n", ev.Type, ev.Kv.Key, ev.Kv.Value)
	//			break WATCH
	//		}
	//	}
}
