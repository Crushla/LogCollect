package tailfile

import (
	"LogCollect/common"
	"github.com/hpcloud/tail"
	"github.com/sirupsen/logrus"
	"golang.org/x/net/context"
)

type tailTaskMgr struct {
	tailTaskMap map[string]tailTask
	entryList   []common.CollectEntry
	confChan    chan []common.CollectEntry
}

var (
	tailMgr tailTaskMgr
)

func Init(allConfig []common.CollectEntry) (err error) {
	tailMgr = tailTaskMgr{
		tailTaskMap: make(map[string]tailTask, 20),
		entryList:   allConfig,
		confChan:    make(chan []common.CollectEntry),
	}
	config := tail.Config{
		ReOpen:    true,
		Follow:    true,
		MustExist: false,
		Poll:      true,
	}
	for _, conf := range tailMgr.entryList {
		ctx, cancel := context.WithCancel(context.Background())
		task := tailTask{
			path:   conf.Path,
			topic:  conf.Topic,
			ctx:    ctx,
			cancel: cancel,
		}
		task.tailObj, err = tail.TailFile(task.path, config)
		if err != nil {
			logrus.Error("tailfile create TailObj for path:%s failed, err:%v\n", task.path, err)
			return
		}
		//保存任务，方便管理
		tailMgr.tailTaskMap[task.path] = task
		go task.run()
	}
	go tailMgr.Watch()
	return
}

func (t *tailTaskMgr) Watch() {
	for {
		config := tail.Config{
			ReOpen:    true,
			Follow:    true,
			MustExist: false,
			Poll:      true,
		}
		//等待新配置
		newConf := <-tailMgr.confChan
		//启用新配置
		logrus.Info("get new conf from etcd,conf:%v", newConf)
		for _, conf := range newConf {
			//原配置已有
			if t.isExist(conf) {
				continue
			}
			//原配置没有
			ctx, cancel := context.WithCancel(context.Background())
			task := tailTask{
				path:   conf.Path,
				topic:  conf.Topic,
				ctx:    ctx,
				cancel: cancel,
			}
			task.tailObj, _ = tail.TailFile(task.path, config)
			tailMgr.tailTaskMap[task.path] = task
			go task.run()
			continue
		}
		//如果新配置没有，但是原来配置中有
		for key, task := range t.tailTaskMap {
			var found bool
			for _, conf := range newConf {
				if key == conf.Path {
					found = true
					break
				}
			}
			if !found {
				delete(t.tailTaskMap, key)
				task.cancel()
			}
		}
	}
}

func (t *tailTaskMgr) isExist(newConf common.CollectEntry) bool {
	_, ok := t.tailTaskMap[newConf.Path]
	return ok
}

func SendNewConf(newConf []common.CollectEntry) {
	tailMgr.confChan <- newConf
}
