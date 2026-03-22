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
	"slices"
	"sync"
	"syscall"
	"time"

	ciliumclientset "github.com/cilium/cilium/pkg/k8s/client/clientset/versioned"
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
	var wg sync.WaitGroup
	wg.Add(4)

	prevAppList := SafeAppSlice{
		mu:      sync.Mutex{},
		appList: []AnApp{},
	}

	gAdvertList := SafeAdvSlice{
		mu:      sync.Mutex{},
		advList: []CiliumBgpAdvert{},
	}
	advChan := make(chan *SafeAdvSlice, 3)
	advChan <- &gAdvertList
	defer close(advChan)

	gAppList := SafeAppSlice{
		mu:      sync.Mutex{},
		appList: []AnApp{},
	}
	appChan := make(chan *SafeAppSlice, 3)
	appChan <- &gAppList
	defer close(appChan)

	delAppChan := make(chan AnApp, 3)
	defer close(delAppChan)

	add_ch := make(chan *appsv1.Deployment, 3)
	del_ch := make(chan *appsv1.Deployment, 3)
	upd_ch := make(chan *appsv1.Deployment, 3)

	depInform := factory.Apps().V1().Deployments().Informer()
	depInform.AddEventHandler(
		cache.ResourceEventHandlerFuncs{
			AddFunc: func(obj interface{}) {
				depl := obj.(*appsv1.Deployment)
				add_ch <- depl
			},
			UpdateFunc: func(oldObj, newObj interface{}) {
				depl := newObj.(*appsv1.Deployment)
				upd_ch <- depl
			},
			DeleteFunc: func(obj interface{}) {
				depl := obj.(*appsv1.Deployment)
				del_ch <- depl
			},
		},
	)

	factory.Start(ctx.Done())

	go func() {
	CheckDeplLoop:
		for {
			select {
			case dpl_add := <-add_ch:
				func() {

					gAppList.mu.Lock()
					defer gAppList.mu.Unlock()

					logger.Info(fmt.Sprintf("DEPLOYMENT ADDED: %s %s\n", dpl_add.Name, dpl_add.Labels))
					gAppList.appList = append(gAppList.appList, NewApp(*dpl_add))
					appChan <- &gAppList
				}()
			case dpl_del := <-del_ch:
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
			case dpl_upd := <-upd_ch:
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
	}()

	go func() {
	UpdateListsLoop:
		for {
			select {
			case safeApp := <-appChan:
				func() {

					safeApp.mu.Lock()
					defer safeApp.mu.Unlock()

					prevAppList.mu.Lock()
					defer prevAppList.mu.Unlock()

					for _, app := range safeApp.appList {
						// append if app is not equal to any in prevapplist, compares appname,tenantname, and tags
						if !slices.ContainsFunc(prevAppList.appList, app.DeepEqual) && app.isExportBgpTenant() {
							func() {
								// place inside anon func to ensure defer mu.unlock executed after ea loop iteration
								gAdvertList.mu.Lock()
								defer gAdvertList.mu.Unlock()

								cilBgpAdv := NewCiliumBGPAdvert(app)
								gAdvertList.advList = slices.DeleteFunc(gAdvertList.advList, func(a CiliumBgpAdvert) bool { return a.Equal(cilBgpAdv) })
								gAdvertList.advList = append(gAdvertList.advList, cilBgpAdv)
								// check for matching tenant and app name and delete, cover case where tags are diff
								// prevAppList.appList = slices.DeleteFunc(prevAppList.appList, func(a AnApp) bool { return a.Equal(app) })
								prevAppList.appList = append(prevAppList.appList, app)
								advChan <- &gAdvertList
							}()
						}
					}

				}()

			case <-ctx.Done():
				logger.Info(fmt.Sprintln("UpdateListsLoop All done!"))
				break UpdateListsLoop
			}
		}
		wg.Done()
	}()

	go func() {
	ReactToAdvListLoop:
		for {
			select {
			case adv := <-advChan:
				func() {
					adv.mu.Lock()
					defer adv.mu.Unlock()
					for _, a := range adv.advList {
						err := a.applyManifest(ciliumClientSet, ctx)
						if err != nil {
							wErr := fmt.Errorf("ReactToAdvListLoop labelSel=%s advTypes=%v wrappedErr=%w", a.getLabelSelector().String(), a.advertTypes, err)
							logger.Error("", "err", wErr)
							// continue for now, revisit this when done de//buggering it
							continue
						}
					}
				}()

			case <-ctx.Done():
				logger.Info(fmt.Sprintln("ReactToAdvListLoop All done!"))
				break ReactToAdvListLoop
			}
		}
		wg.Done()
	}()

	go func() {
	ReactToDelAppList:
		for {
			select {
			case delApp := <-delAppChan:
				func() {
					logger.Debug("ReactToDelAppList ", "delApp", delApp.Name)
					gAdvertList.mu.Lock()
					defer gAdvertList.mu.Unlock()
					removeIdx := []int{}
					for idx, a2 := range gAdvertList.advList {
						if delApp.DeepEqual(a2.app) {
							removable, err := a2.isRemovable(*clientset, cfg.watchNameSpace, ctx)
							if err != nil {
								logger.Error("ReactToDelAppList loop", "err", fmt.Errorf("isRemovable %w", err))
								continue
							} else if removable {
								err := a2.removeAdv(ciliumClientSet, ctx)
								if err != nil {
									logger.Error("ReactToDelAppList loop", "err", fmt.Errorf("removeAdv %w", err))
								} else {
									removeIdx = append(removeIdx, idx)
								}
							}
						}
					}

					for i := range removeIdx {
						gAdvertList.advList = slices.Delete(gAdvertList.advList, i, i)
					}
				}()

			case <-ctx.Done():
				logger.Info(fmt.Sprintln("ReactToDelAppList Loop All done!"))
				break ReactToDelAppList
			}
		}
		wg.Done()
	}()

	wg.Wait()

	if !cache.WaitForCacheSync(ctx.Done(), depInform.HasSynced) {
		log.Panic("failed to sync informer caches")
	}

}
