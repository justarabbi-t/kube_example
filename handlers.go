package main

import (
	"context"
	"fmt"
	"slices"

	kafka "github.com/confluentinc/confluent-kafka-go/kafka"

	ciliumclientset "github.com/cilium/cilium/pkg/k8s/client/clientset/versioned"
	"github.com/justarabbi-t/kube_example.git/services"
	appsv1 "k8s.io/api/apps/v1"
)

var AddDplBaseMessage = map[string]services.Message{
	"Add": {
		Action:        services.Add,
		ActionSubject: services.Deployment,
		Topic:         services.AppList,
	},
	"Del": {
		Action:        services.Del,
		ActionSubject: services.Deployment,
		Topic:         services.AppList,
	},
	"Upd": {
		Action:        services.Upd,
		ActionSubject: services.Deployment,
		Topic:         services.AppList,
	},
}

func sendAppMessage(name string, labels map[string]string, c chan services.DeploymentMessage, a services.Action) error {
	if c != nil {
		services.DeploymentMessage{
			Message: AddDplBaseMessage[a.String()],
			Name:    name,
			Labels:  labels,
		}.Send(c)
		return nil
	}
	return fmt.Errorf("name=% msg=%s chan c is nil %v", name, a, c)
}

func handleDepChannels(addChan, updChan, delChan chan *appsv1.Deployment, appList *SafeAppSlice, kafkaCfg kafka.ConfigMap, errChan chan error, ctx context.Context) {
	defer ctx.Done()
	kafkaProducer, err := services.NewProducerWithJsonChan[services.DeploymentMessage](kafkaCfg, services.AppList, ctx)
	if err != nil {
		errChan <- err
		return
	}
	go kafkaProducer.Watch(errChan)

CheckDeplLoop:
	for {
		select {
		case dplAdd := <-addChan:
			fmt.Printf("DEPLOYMENT ADDED: %s %s\n", dplAdd.Name, dplAdd.Namespace)
			err := sendAppMessage(dplAdd.Name, dplAdd.Labels, kafkaProducer.Channel, services.Add)
			if err != nil {
				errChan <- err
			}
		case dplDel := <-delChan:
			fmt.Printf("DEPLOYMENT DELETED: %s %s\n", dplDel.Name, dplDel.Namespace)
			err := sendAppMessage(dplDel.Name, dplDel.Labels, kafkaProducer.Channel, services.Del)
			if err != nil {
				errChan <- err
			}

		case dplUpd := <-updChan:
			fmt.Printf("DEPLOYMENT UPDATED: %s %s\n", dplUpd.Name, dplUpd.Namespace)
			needsUpdate, err := checkNeedsUpdate(AnApp{dplUpd.Name, dplUpd.Labels}, appList)
			if err != nil {
				errChan <- err
			} else if needsUpdate {
				fmt.Println("\n\n NEEDS UPDATE \n\n")
				err = sendAppMessage(dplUpd.Name, dplUpd.Labels, kafkaProducer.Channel, services.Upd)
				if err != nil {
					errChan <- err
				}
			}
		case e := <-errChan:
			fmt.Printf("CheckDeplLoop err == %s\n", e)
		case <-ctx.Done():
			fmt.Println("CheckDeplLoop All done!")
			break CheckDeplLoop
		}
	}
}

func handleUpdateLoop(appList *SafeAppSlice, clientset ciliumclientset.Interface, kafkaCfg kafka.ConfigMap, errChan chan error, ctx context.Context) {
	defer ctx.Done()
	kafkaConsumer, err := services.NewConsumerWithJsonChan[services.DeploymentMessage](kafkaCfg, services.AppList, ctx)
	if err != nil {
		errChan <- err
		return
	}
	kafkaProducer, err := services.NewProducerWithJsonChan[services.DeploymentMessage](kafkaCfg, services.AdvList, ctx)
	if err != nil {
		errChan <- err
		return
	}

	go kafkaProducer.Watch(errChan)
	go kafkaConsumer.Watch(errChan)

UpdateListsLoop:
	for {
		select {
		case aMsg := <-kafkaConsumer.Channel:
			name, tags, err := handleAction(aMsg, appList, clientset, errChan, ctx)
			if err != nil {
				errChan <- fmt.Errorf("case aMsg applyManifest %w", err)
			}
			sendAppMessage(name, tags, kafkaProducer.Channel, services.Add)
		// case e := <-errChan:
		// 	fmt.Printf("handleUpdateLoop UpdateListsLoop err == %s\n", e)
		case <-ctx.Done():
			fmt.Println("UpdateListsLoop All done!")
			break UpdateListsLoop
		}
	}
}

func handleAction(aMsg services.DeploymentMessage, appList *SafeAppSlice, clientset ciliumclientset.Interface, errChan chan error, ctx context.Context) (string, map[string]string, error) {

	tmpApp := NewApp(aMsg.Name, aMsg.Labels)
	switch aMsg.Action {
	case services.Add:
		return handleAdd(tmpApp, appList, clientset, errChan, ctx)
	case services.Del:
		return handleDel(tmpApp, appList, clientset, errChan, ctx)
	case services.Upd:
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
