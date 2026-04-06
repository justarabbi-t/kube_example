package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"log/slog"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"

	ciliumclientset "github.com/cilium/cilium/pkg/k8s/client/clientset/versioned"
	kt "github.com/justarabbi-t/kube_example.git/kafka_talkers"
	mh "github.com/justarabbi-t/kube_example.git/message_handler"
	appsv1 "k8s.io/api/apps/v1"

	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/tools/clientcmd"
)

type appConfig struct {
	watchNameSpace string
	kubeConfigPath string
}

func (c appConfig) String() string {
	return fmt.Sprintf("watchNameSpace=%s kubeConfigPath=%s", c.watchNameSpace, c.kubeConfigPath)
}

func main() {
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()
	defer ctx.Done()

	var cfg appConfig
	flag.StringVar(&cfg.watchNameSpace, "watch", "default", "kube namespace to watch, defaut='default'")
	flag.StringVar(&cfg.kubeConfigPath, "kube-path", ".kube/config", "kube config to use, default='./kube/config', assumes path resides in HOME dir")
	flag.Parse()

	logger := *slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelDebug}))
	slog.SetDefault(&logger)

	logger.Info(fmt.Sprintf("start config=%s", cfg))

	kubeconfig := filepath.Join(os.Getenv("HOME"), cfg.kubeConfigPath)
	config, err := clientcmd.BuildConfigFromFlags("", kubeconfig)
	if err != nil {
		log.Fatalf("build config error: %w", err)
	}
	clientset, err := kubernetes.NewForConfig(config)
	if err != nil {
		log.Fatalf("build kube clientset error: %w", err)
	}

	ciliumClientSet, err := ciliumclientset.NewForConfig(config)
	if err != nil {
		log.Fatalf("build cilium clientset error: %w", err)
	}

	factory := informers.NewSharedInformerFactoryWithOptions(clientset, 10*time.Minute, informers.WithNamespace(cfg.watchNameSpace))

	// setup initial struct chan
	// var wg sync.WaitGroup
	// wg.Add(2)

	safeAppList := mh.NewSafeAppSlice()
	errChan := make(chan error, 3)
	addChan := make(chan *appsv1.Deployment, 3)
	delChan := make(chan *appsv1.Deployment, 3)
	updChan := make(chan *appsv1.Deployment, 3)

	depInform := factory.Apps().V1().Deployments().Informer()
	depInform.AddEventHandler(
		cache.ResourceEventHandlerFuncs{
			AddFunc: func(obj interface{}) {
				depl := obj.(*appsv1.Deployment)
				fmt.Printf("sending to addChan, %s\n", depl.Name)
				addChan <- depl
			},
			UpdateFunc: func(oldObj, newObj interface{}) {
				depl := newObj.(*appsv1.Deployment)
				fmt.Printf("sending to updChan, %s\n", depl.Name)
				updChan <- depl
			},
			DeleteFunc: func(obj interface{}) {
				depl := obj.(*appsv1.Deployment)
				fmt.Printf("sending to delChan, %s\n", depl.Name)
				delChan <- depl
			},
		},
	)

	factory.Start(ctx.Done())
	kafkaCfg := kt.NewCfgMap("kubeExample", "kubeExample")
	// CheckDeplLoop:
	go mh.HandleDepChannels(addChan, updChan, delChan, safeAppList, kafkaCfg, errChan, ctx)

	go mh.HandleUpdateLoop(safeAppList, ciliumClientSet, kafkaCfg, errChan, ctx)
MainSelect:
	for {
		select {
		case err := <-errChan:
			logger.Error("main select", "err", err)
		case <-ctx.Done():
			err := ctx.Err()
			defer close(errChan)
			defer close(addChan)
			defer close(delChan)
			defer close(updChan)
			logger.Info("Context cancelled! Exiting!", "ctx", err)
			break MainSelect
		}
	}

	// wg.Wait()

	if !cache.WaitForCacheSync(ctx.Done(), depInform.HasSynced) {
		log.Panic("failed to sync informer caches")
	}

}
