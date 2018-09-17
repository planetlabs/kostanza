package lister

import (
	"time"

	core_v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/client-go/informers"
	informersv1 "k8s.io/client-go/informers/core/v1"
	"k8s.io/client-go/kubernetes"
	listersv1 "k8s.io/client-go/listers/core/v1"
	"k8s.io/client-go/tools/cache"

	"github.com/jacobstr/kostanza/internal/log"
)

type PodLister interface {
	List(selector labels.Selector) ([]*core_v1.Pod, error)
	Run(stopCh <-chan struct{}) error
}

func NewKubernetesPodLister(client kubernetes.Interface) *kubernetesPodLister {
	informerFactory := informers.NewSharedInformerFactory(client, time.Second)
	pi := informerFactory.Core().V1().Pods()
	pl := pi.Lister()

	return &kubernetesPodLister{
		lister:   pl,
		informer: pi,
	}
}

type kubernetesPodLister struct {
	lister   listersv1.PodLister
	informer informersv1.PodInformer
}

func (k *kubernetesPodLister) List(selector labels.Selector) (ret []*core_v1.Pod, err error) {
	return k.lister.List(selector)
}

func (k *kubernetesPodLister) Run(stopCh <-chan struct{}) error {
	k.informer.Informer().Run(stopCh)
	log.Log.Debug("waiting for pod cache to sync")
	if ok := cache.WaitForCacheSync(stopCh, k.informer.Informer().HasSynced); !ok {
		log.Log.Error("pod cache did not sync")
		return ErrCacheSyncFailed
	}
	return nil
}

type FakePodLister struct {
	Pods []*core_v1.Pod
}

func (l *FakePodLister) List(selector labels.Selector) ([]*core_v1.Pod, error) {
	return l.Pods, nil
}

func (l *FakePodLister) Run(stopCh <-chan struct{}) error {
	select {
	case <-stopCh:
		return nil
	}
}
