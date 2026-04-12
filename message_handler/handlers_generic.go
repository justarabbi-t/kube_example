package message_handler

import (
	"context"
	"fmt"

	kafka "github.com/confluentinc/confluent-kafka-go/kafka"

	kt "github.com/justarabbi-t/kube_example.git/kafka_talkers"
	appsv1 "k8s.io/api/apps/v1"
)

func GenericHandleDepChannels[T kt.MessageLike](addChan, updChan, delChan chan *appsv1.Deployment, appList *SafeAppSlice, kafkaCfg kafka.ConfigMap, errChan chan error, ctx context.Context) {
	defer ctx.Done()
	kafkaProducer, err := kt.NewProducerWithJsonChan[T](kafkaCfg, kt.AppList, ctx)
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
			//////// Need kt.NewMsg generic switch on T? return type
			msg := kt.NewDeploymentMessage(dplAdd.Name, dplAdd.Labels, kt.Add)

			kt.SendMessage[T](kafkaProducer.Channel, msg)
			if err != nil {
				errChan <- err
			}
		case dplDel := <-delChan:
			fmt.Printf("DEPLOYMENT DELETED: %s %s\n", dplDel.Name, dplDel.Namespace)
			err := sendAppMessage(dplDel.Name, dplDel.Labels, kafkaProducer.Channel, kt.Del)
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
				err = sendAppMessage(dplUpd.Name, dplUpd.Labels, kafkaProducer.Channel, kt.Upd)
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
