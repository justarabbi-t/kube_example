package message_handler

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"time"

	ciliumclientset "github.com/cilium/cilium/pkg/k8s/client/clientset/versioned"
	"github.com/justarabbi-t/kube_example.git/models"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/tools/cache"
)

var baseErr = errors.New("HandleDepChan")

// Takes channels and handles actions based on messages to channels
// messageType expected to be of kafka_talkers.MessageLike, messageType's value is unused
func HandleKubeMessage(mh SendMessageHandler[KubeLike], appList *SafeAppSlice, appCfg *models.AppCfg) {
	defer appCfg.Ctx.Done()
	kafkaProducer, err := NewProducerWithJsonChan(appCfg.KafkaCfg, AppList, appCfg.Ctx)
	if err != nil {
		appCfg.ErrChan <- err
		return
	}
	go kafkaProducer.Watch(appCfg.ErrChan)

	switch v := mh.GetMsgType(); v {
	case DeployMsg:
		// addChan := make(chan *appsv1.Deployment, 3)
		// updChan := make(chan *appsv1.Deployment, 3)
		// delChan := make(chan *appsv1.Deployment, 3)

		addChan := make(chan KubeLike, 3)
		updChan := make(chan KubeLike, 3)
		delChan := make(chan KubeLike, 3)
		h := HandlerChannels[KubeLike]{
			errChan:      appCfg.ErrChan,
			addChan:      addChan,
			updChan:      updChan,
			delChan:      delChan,
			producerChan: kafkaProducer.Channel,
			consumerChan: nil,
		}
		mh.MessageLoop(h, appList, appCfg.Ctx)
		StartKubeWatcher[KubeLike](h, appCfg.Ctx, *appCfg)
	default:
		appCfg.ErrChan <- fmt.Errorf("%w type switch unrecognized MessageType=%v", baseErr, v)
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

func GenericHandleUpdateLoop(appList *SafeAppSlice, appCfg *models.AppCfg) {
	defer appCfg.Ctx.Done()
	kafkaConsumer, err := NewConsumerWithJsonChan(appCfg.KafkaCfg, AppList, appCfg.Ctx)
	if err != nil {
		appCfg.ErrChan <- err
		return
	}
	kafkaProducer, err := NewProducerWithJsonChan(appCfg.KafkaCfg, AdvList, appCfg.Ctx)
	if err != nil {
		appCfg.ErrChan <- err
		return
	}

	go kafkaProducer.Watch(appCfg.ErrChan)
	go kafkaConsumer.Watch(appCfg.ErrChan)

UpdateListsLoop:
	for {
		select {
		case aMsg := <-kafkaConsumer.Channel:
			kMsg, ok := aMsg.(DeploymentMessage)
			if !ok {
				appCfg.ErrChan <- fmt.Errorf("UpdateListsLoop kafkaConsumer aMsg is not of type DeploymentMessage")
				continue
			}
			name, labels, err := handleAction(kMsg, appList, appCfg.CiliumClientset, appCfg.ErrChan, appCfg.Ctx)
			if err != nil {
				appCfg.ErrChan <- fmt.Errorf("case aMsg applyManifest %w", err)
			}
			sMsg := NewDeploymentMessage(name, labels, kMsg.Action)
			sMsg.Send(kafkaConsumer.Channel)
		case <-appCfg.Ctx.Done():
			fmt.Println("UpdateListsLoop All done!")
			break UpdateListsLoop
		}
	}
}

func handleAction(aMsg DeploymentMessage, appList *SafeAppSlice, clientset ciliumclientset.Interface, errChan chan error, ctx context.Context) (string, map[string]string, error) {

	tmpApp := NewApp(aMsg.Name, aMsg.Labels)
	switch aMsg.Action {
	case Add:
		return handleAdd(tmpApp, appList, clientset, errChan, ctx)
	case Del:
		return handleDel(tmpApp, appList, clientset, errChan, ctx)
	case Upd:
		return handleUpd(tmpApp, appList, clientset, errChan, ctx)
	default:
		return "", map[string]string{}, fmt.Errorf("handleAction uknown action: stringRepr=%s intRepr=%d", aMsg.Action, aMsg.Action)
	}
}

func handleAdd(a AnApp, appList *SafeAppSlice, clientset ciliumclientset.Interface, errChan chan error, ctx context.Context) (string, map[string]string, error) {
	appList.mu.Lock()
	defer appList.mu.Unlock()

	if a.isExportBgpTenant() && !slices.ContainsFunc(appList.appList, a.DeepEqual) {
		appList.appList = append(appList.appList, a)
		adv := NewCiliumBGPAdvert(a)
		err := adv.applyManifest(clientset, ctx)
		if err != nil {
			return "", map[string]string{}, err
		}
	}
	return a.Name, a.Tags, nil
}

func handleDel(a AnApp, appList *SafeAppSlice, clientset ciliumclientset.Interface, errChan chan error, ctx context.Context) (string, map[string]string, error) {
	appList.mu.Lock()
	defer appList.mu.Unlock()

	if slices.ContainsFunc(appList.appList, a.DeepEqual) {
		appList.appList = slices.DeleteFunc(appList.appList, a.DeepEqual)
		adv := NewCiliumBGPAdvert(a)
		err := adv.removeAdv(clientset, ctx)
		if err != nil {
			return "", map[string]string{}, err
		}
	}
	return a.Name, a.Tags, nil
}

func checkNeedsUpdate(a AnApp, appList *SafeAppSlice) (bool, error) {
	appList.mu.Lock()
	defer appList.mu.Unlock()
	if slices.ContainsFunc(appList.appList, a.DeepEqual) {
		return false, nil
	}
	return true, nil
}

func handleUpd(a AnApp, appList *SafeAppSlice, clientset ciliumclientset.Interface, errChan chan error, ctx context.Context) (string, map[string]string, error) {
	appList.mu.Lock()
	defer appList.mu.Unlock()

	if a.isExportBgpTenant() && slices.ContainsFunc(appList.appList, a.Equal) {
		appList.appList = slices.DeleteFunc(appList.appList, a.Equal)
		appList.appList = append(appList.appList, a)
		adv := NewCiliumBGPAdvert(a)
		err := adv.applyManifest(clientset, ctx)
		if err != nil {
			return "", map[string]string{}, err
		}
	} else if !a.isExportBgpTenant() && slices.ContainsFunc(appList.appList, a.Equal) {
		// cover edge where depl is changed and now unexportable i so smart
		appList.appList = slices.DeleteFunc(appList.appList, a.Equal)
		appList.appList = append(appList.appList, a)
		adv := NewCiliumBGPAdvert(a)
		err := adv.removeAdv(clientset, ctx)
		if err != nil {
			return "", map[string]string{}, err
		}
	} else {
		return "", map[string]string{}, fmt.Errorf("handleUpd else ... this shouldn't happen something is borked name=%s tags=%v", a.Name, a.Tags)
	}
	return a.Name, a.Tags, nil
}
