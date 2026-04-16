package message_handler

import (
	"context"
	"errors"
	"fmt"
	"time"

	kt "github.com/justarabbi-t/kube_example.git/kafka_talkers"
	"github.com/justarabbi-t/kube_example.git/models"
	appsv1 "k8s.io/api/apps/v1"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/tools/cache"
)

var baseErr = errors.New("HandleDepChan")

// Takes channels and handles actions based on messages to channels
// messageType expected to be of kafka_talkers.MessageLike, messageType's value is unused
func HandleKubeMessage(mh kt.MessageHandler, appList *SafeAppSlice, appCfg *models.AppCfg) {
	defer appCfg.Ctx.Done()
	kafkaProducer, err := kt.NewProducerWithJsonChan(appCfg.KafkaCfg, kt.AppList, appCfg.Ctx)
	if err != nil {
		appCfg.ErrChan <- err
		return
	}
	go kafkaProducer.Watch(appCfg.ErrChan)

	switch mh.GetMsgType() {
	case kt.DeployMsg:
		addChan := make(chan *appsv1.Deployment, 3)
		updChan := make(chan *appsv1.Deployment, 3)
		delChan := make(chan *appsv1.Deployment, 3)
		h := HandlerChannels[*appsv1.Deployment]{
			errChan:      appCfg.ErrChan,
			addChan:      addChan,
			updChan:      updChan,
			delChan:      delChan,
			producerChan: kafkaProducer.Channel,
			consumerChan: nil,
		}
		messageLoop[*appsv1.Deployment](h, appList, mh, appCfg.Ctx)
		StartKubeWatcher[*appsv1.Deployment](h, appCfg.Ctx, *appCfg)
	default:
		appCfg.ErrChan <- fmt.Errorf("%w type switch unrecognized type T %v", baseErr, v)
	}
}

func messageLoop[T KubeLike](h HandlerChannels[T], appList *SafeAppSlice, msgSender kt.Sender, ctx context.Context) {
CheckDeplLoop:
	for {
		select {
		case dplAdd := <-h.addChan:
			fmt.Printf("DEPLOYMENT ADDED: %s %s\n", dplAdd.GetName(), dplAdd.GetNamespace())
			msgSender.UpdateMsg(dplAdd.GetName(), dplAdd.GetLabels(), kt.Add)
			msgSender.SendMsg(h.producerChan)

		case dplDel := <-h.delChan:
			fmt.Printf("DEPLOYMENT DELETED: %s %s\n", dplDel.GetName(), dplDel.GetNamespace())
			msgSender.UpdateMsg(dplDel.GetName(), dplDel.GetLabels(), kt.Del)
			msgSender.SendMsg(h.producerChan)

		case dplUpd := <-h.updChan:
			fmt.Printf("DEPLOYMENT UPDATED: %s %s\n", dplUpd.GetName(), dplUpd.GetNamespace())
			needsUpdate, err := checkNeedsUpdate(AnApp{dplUpd.GetName(), dplUpd.GetLabels()}, appList)
			if err != nil {
				h.errChan <- err
			} else if needsUpdate {
				fmt.Println("\n\n NEEDS UPDATE \n\n")
				msgSender.UpdateMsg(dplUpd.GetName(), dplUpd.GetLabels(), kt.Upd)
				msgSender.SendMsg(h.producerChan)
			}
		case e := <-h.errChan:
			fmt.Printf("CheckDeplLoop err == %s\n", e)
		case <-ctx.Done():
			fmt.Println("CheckDeplLoop All done!")
			break CheckDeplLoop
		}
	}
}

func StartKubeWatcher[T KubeLike](h HandlerChannels[T], ctx context.Context, appCfg models.AppCfg) {
	factory := informers.NewSharedInformerFactoryWithOptions(appCfg.KubeClientset, 10*time.Minute, informers.WithNamespace(appCfg.KubeCfg.WatchNameSpace))

	depInform := factory.Apps().V1().Deployments().Informer()
	depInform.AddEventHandler(
		cache.ResourceEventHandlerFuncs{
			AddFunc: func(obj interface{}) {
				depl := obj.(T)
				fmt.Printf("sending to addChan, %s\n", depl.GetName())
				h.addChan <- depl
			},
			UpdateFunc: func(oldObj, newObj interface{}) {
				depl := newObj.(T)
				fmt.Printf("sending to updChan, %s\n", depl.GetName())
				h.updChan <- depl
			},
			DeleteFunc: func(obj interface{}) {
				depl := obj.(T)
				fmt.Printf("sending to delChan, %s\n", depl.GetName())
				h.delChan <- depl
			},
		},
	)
	factory.Start(ctx.Done())
}
