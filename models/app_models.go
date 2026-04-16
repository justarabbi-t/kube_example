package models

import (
	"context"
	"fmt"

	ciliumclientset "github.com/cilium/cilium/pkg/k8s/client/clientset/versioned"
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"k8s.io/client-go/kubernetes"
)

type KubeConfig struct {
	WatchNameSpace string
	KubeConfigPath string
}

func (k KubeConfig) String() string {
	return fmt.Sprintf("watchNameSpace=%s kubeConfigPath=%s", k.WatchNameSpace, k.KubeConfigPath)
}

type AppCfg struct {
	KubeCfg         KubeConfig
	KafkaCfg        kafka.ConfigMap
	KubeClientset   *kubernetes.Clientset
	CiliumClientset *ciliumclientset.Clientset
	Ctx             context.Context
	ErrChan         chan error
}
