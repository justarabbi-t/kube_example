// package message_handler

// import (
// 	"context"
// 	"fmt"
// 	"slices"

// 	ciliumclientset "github.com/cilium/cilium/pkg/k8s/client/clientset/versioned"
// 	kafka "github.com/confluentinc/confluent-kafka-go/kafka"

// 	appsv1 "k8s.io/api/apps/v1"
// )

// func GenericHandleDepChannels(addChan, updChan, delChan chan *appsv1.Deployment, appList *SafeAppSlice, kafkaCfg kafka.ConfigMap, errChan chan error, ctx context.Context) {
// 	defer ctx.Done()
// 	kafkaProducer, err := NewProducerWithJsonChan(kafkaCfg, AppList, ctx)
// 	if err != nil {
// 		errChan <- err
// 		return
// 	}
// 	go kafkaProducer.Watch(errChan)

// CheckDeplLoop:
// 	for {
// 		select {
// 		case dplAdd := <-addChan:
// 			fmt.Printf("DEPLOYMENT ADDED: %s %s\n", dplAdd.Name, dplAdd.Namespace)
// 			msg := NewDeploymentMessage(dplAdd.Name, dplAdd.Labels, Add)
// 			msg.Send(kafkaProducer.Channel)

// 		case dplDel := <-delChan:
// 			fmt.Printf("DEPLOYMENT DELETED: %s %s\n", dplDel.Name, dplDel.Namespace)
// 			msg := NewDeploymentMessage(dplDel.Name, dplDel.Labels, Del)
// 			msg.Send(kafkaProducer.Channel)

// 		case dplUpd := <-updChan:
// 			fmt.Printf("DEPLOYMENT UPDATED: %s %s\n", dplUpd.Name, dplUpd.Namespace)
// 			needsUpdate, err := checkNeedsUpdate(AnApp{dplUpd.Name, dplUpd.Labels}, appList)
// 			if err != nil {
// 				errChan <- err
// 			} else if needsUpdate {
// 				fmt.Println("\n\n NEEDS UPDATE \n\n")
// 				msg := NewDeploymentMessage(dplUpd.Name, dplUpd.Labels, Upd)
// 				msg.Send(kafkaProducer.Channel)
// 			}
// 		case e := <-errChan:
// 			fmt.Printf("CheckDeplLoop err == %s\n", e)
// 		case <-ctx.Done():
// 			fmt.Println("CheckDeplLoop All done!")
// 			break CheckDeplLoop
// 		}
// 	}
// }

// func GenericHandleUpdateLoop(appList *SafeAppSlice, clientset ciliumclientset.Interface, kafkaCfg kafka.ConfigMap, errChan chan error, ctx context.Context) {
// 	defer ctx.Done()
// 	kafkaConsumer, err := NewConsumerWithJsonChan(kafkaCfg, AppList, ctx)
// 	if err != nil {
// 		errChan <- err
// 		return
// 	}
// 	kafkaProducer, err := NewProducerWithJsonChan(kafkaCfg, AdvList, ctx)
// 	if err != nil {
// 		errChan <- err
// 		return
// 	}

// 	go kafkaProducer.Watch(errChan)
// 	go kafkaConsumer.Watch(errChan)

// UpdateListsLoop:
// 	for {
// 		select {
// 		case aMsg := <-kafkaConsumer.Channel:
// 			kMsg, ok := aMsg.(DeploymentMessage)
// 			if !ok {
// 				errChan <- fmt.Errorf("UpdateListsLoop kafkaConsumer aMsg is not of type DeploymentMessage")
// 				continue
// 			}
// 			name, labels, err := handleAction(kMsg, appList, clientset, errChan, ctx)
// 			if err != nil {
// 				errChan <- fmt.Errorf("case aMsg applyManifest %w", err)
// 			}
// 			sMsg := NewDeploymentMessage(name, labels, kMsg.Action)
// 			sMsg.Send(kafkaConsumer.Channel)
// 		case <-ctx.Done():
// 			fmt.Println("UpdateListsLoop All done!")
// 			break UpdateListsLoop
// 		}
// 	}
// }

// func handleAction(aMsg DeploymentMessage, appList *SafeAppSlice, clientset ciliumclientset.Interface, errChan chan error, ctx context.Context) (string, map[string]string, error) {

// 	tmpApp := NewApp(aMsg.Name, aMsg.Labels)
// 	switch aMsg.Action {
// 	case Add:
// 		return handleAdd(tmpApp, appList, clientset, errChan, ctx)
// 	case Del:
// 		return handleDel(tmpApp, appList, clientset, errChan, ctx)
// 	case Upd:
// 		return handleUpd(tmpApp, appList, clientset, errChan, ctx)
// 	default:
// 		return "", map[string]string{}, fmt.Errorf("handleAction uknown action: stringRepr=%s intRepr=%d", aMsg.Action, aMsg.Action)
// 	}
// }

// func handleAdd(a AnApp, appList *SafeAppSlice, clientset ciliumclientset.Interface, errChan chan error, ctx context.Context) (string, map[string]string, error) {
// 	appList.mu.Lock()
// 	defer appList.mu.Unlock()

// 	if a.isExportBgpTenant() && !slices.ContainsFunc(appList.appList, a.DeepEqual) {
// 		appList.appList = append(appList.appList, a)
// 		adv := NewCiliumBGPAdvert(a)
// 		err := adv.applyManifest(clientset, ctx)
// 		if err != nil {
// 			return "", map[string]string{}, err
// 		}
// 	}
// 	return a.Name, a.Tags, nil
// }

// func handleDel(a AnApp, appList *SafeAppSlice, clientset ciliumclientset.Interface, errChan chan error, ctx context.Context) (string, map[string]string, error) {
// 	appList.mu.Lock()
// 	defer appList.mu.Unlock()

// 	if slices.ContainsFunc(appList.appList, a.DeepEqual) {
// 		appList.appList = slices.DeleteFunc(appList.appList, a.DeepEqual)
// 		adv := NewCiliumBGPAdvert(a)
// 		err := adv.removeAdv(clientset, ctx)
// 		if err != nil {
// 			return "", map[string]string{}, err
// 		}
// 	}
// 	return a.Name, a.Tags, nil
// }

// func checkNeedsUpdate(a AnApp, appList *SafeAppSlice) (bool, error) {
// 	appList.mu.Lock()
// 	defer appList.mu.Unlock()
// 	if slices.ContainsFunc(appList.appList, a.DeepEqual) {
// 		return false, nil
// 	}
// 	return true, nil
// }

// func handleUpd(a AnApp, appList *SafeAppSlice, clientset ciliumclientset.Interface, errChan chan error, ctx context.Context) (string, map[string]string, error) {
// 	appList.mu.Lock()
// 	defer appList.mu.Unlock()

// 	if a.isExportBgpTenant() && slices.ContainsFunc(appList.appList, a.Equal) {
// 		appList.appList = slices.DeleteFunc(appList.appList, a.Equal)
// 		appList.appList = append(appList.appList, a)
// 		adv := NewCiliumBGPAdvert(a)
// 		err := adv.applyManifest(clientset, ctx)
// 		if err != nil {
// 			return "", map[string]string{}, err
// 		}
// 	} else if !a.isExportBgpTenant() && slices.ContainsFunc(appList.appList, a.Equal) {
// 		// cover edge where depl is changed and now unexportable i so smart
// 		appList.appList = slices.DeleteFunc(appList.appList, a.Equal)
// 		appList.appList = append(appList.appList, a)
// 		adv := NewCiliumBGPAdvert(a)
// 		err := adv.removeAdv(clientset, ctx)
// 		if err != nil {
// 			return "", map[string]string{}, err
// 		}
// 	} else {
// 		return "", map[string]string{}, fmt.Errorf("handleUpd else ... this shouldn't happen something is borked name=%s tags=%v", a.Name, a.Tags)
// 	}
// 	return a.Name, a.Tags, nil
// }
