package main

import (
	"context"
	"fmt"
	"slices"
	"strings"
	"sync"

	"github.com/justarabbi-t/kube_example.git/services"
	appsv1 "k8s.io/api/apps/v1"
)

type Topic int

const (
	appList Topic = iota
	Topic2
	Topic3
)

func (t Topic) String() string {
	return [...]string{"appList"}[t]
}

func handleAppChannels(add_chan, upd_chan, del_chan chan appsv1.Deployment, ctx context.Context, wg *sync.WaitGroup) error {
	// consumerChan := make(chan map[string]string, 1)
	producerChan := make(chan map[string]string, 1)
	// kafkaConsumer := services.NewConsumerWithChan(consumerChan, "deploymentTracker", "")
	kafkaProducer := services.NewProducerWithChan(producerChan, "deploymentTracker", appList.String(), ctx)
	// go kafkaConsumer.Watch()

	go kafkaProducer.Watch(nil)
CheckDeplLoop:
	for {
		select {
		case dpl_add := <-add_chan:
			func() {
				// gAppList.mu.Lock()
				// defer gAppList.mu.Unlock()
				fmt.Printf("DEPLOYMENT ADDED: %s %s\n", dpl_add.Name, dpl_add.Labels)
				label_list := []string{}
				for k, v := range dpl_add.Labels {
					label_list = append(label_list, fmt.Sprintf("%s=%s", k, v))
				}
				producerChan <- map[string]string{
					"action": "add",
					"type":   "app",
					"name":   dpl_add.Name,
					"labels": strings.Join(label_list, ","),
				}
				// n:= NewApp(dpl_add)
				// gAppList.appList = append(gAppList.appList, NewApp(*dpl_add))
				// appChan <- &gAppList
			}()
		case dpl_del := <-del_chan:
			func() {

				gAppList.mu.Lock()
				defer gAppList.mu.Unlock()
				prevAppList.mu.Lock()
				defer prevAppList.mu.Unlock()
				logger.Info(fmt.Sprintf("DEPLOYMENT DELETED: %s %s\n", dpl_del.Name, dpl_del.Labels))

				prevAppList.appList = slices.DeleteFunc(prevAppList.appList, func(a AnApp) bool { return a.Equal(NewApp(*dpl_del)) })
				gAppList.appList = slices.DeleteFunc(gAppList.appList, func(a AnApp) bool { return a.Equal(NewApp(*dpl_del)) })
				appChan <- &gAppList
				delAppChan <- NewApp(*dpl_del)
			}()
		case dpl_upd := <-upd_chan:
			// pull current safeApp
			func() {

				gAppList.mu.Lock()

				defer gAppList.mu.Unlock()

				updatedApp := NewApp(*dpl_upd)

				logger.Info(fmt.Sprintf("DEPLOYMENT UPDATED: %s %s\n", dpl_upd.Name, dpl_upd.Labels))
				// remove app to be updated
				gAppList.appList = slices.DeleteFunc(gAppList.appList, func(a AnApp) bool { return a.Equal(updatedApp) })

				gAppList.appList = append(gAppList.appList, updatedApp)
				appChan <- &gAppList
			}()
		case <-ctx.Done():
			logger.Info(fmt.Sprintln("CheckDeplLoop All done!"))
			break CheckDeplLoop
		}
	}
	wg.Done()
}
