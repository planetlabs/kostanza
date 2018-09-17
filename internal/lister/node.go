package lister

import (
	"time"

	"github.com/jacobstr/kostanza/internal/log"
	core_v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/client-go/informers"
	informersv1 "k8s.io/client-go/informers/core/v1"
	"k8s.io/client-go/kubernetes"
	listersv1 "k8s.io/client-go/listers/core/v1"
	"k8s.io/client-go/tools/cache"
)

type NodeLister interface {
	List(selector labels.Selector) (ret []*core_v1.Node, err error)
	Run(stopCh <-chan struct{}) error
}

// NewKubernetesNodeLister returns a NodeLister that provides simplified listing
// of nodes that leverages the underlying client-go SharedInformer API. We
// use the client-go machinery to maintain a cache for us and simply list
// all pods via this cache instead of being particularly interested in change events.
func NewKubernetesNodeLister(client kubernetes.Interface) *kubernetesNodeLister {
	informerFactory := informers.NewSharedInformerFactory(client, time.Second)
	ni := informerFactory.Core().V1().Nodes()
	nl := ni.Lister()

	return &kubernetesNodeLister{
		lister:   nl,
		informer: ni,
	}
}

type kubernetesNodeLister struct {
	lister   listersv1.NodeLister
	informer informersv1.NodeInformer
}

func (k *kubernetesNodeLister) List(selector labels.Selector) (ret []*core_v1.Node, err error) {
	return k.lister.List(selector)
}

func (k *kubernetesNodeLister) Run(stopCh <-chan struct{}) error {
	k.informer.Informer().Run(stopCh)
	log.Log.Debug("waiting for node cache to sync")
	if ok := cache.WaitForCacheSync(stopCh, k.informer.Informer().HasSynced); !ok {
		log.Log.Error("node cache did not sync")
		return ErrCacheSyncFailed
	}
	return nil
}

type FakeNodeLister struct {
	Nodes []*core_v1.Node
}

func (l *FakeNodeLister) List(selector labels.Selector) ([]*core_v1.Node, error) {
	return l.Nodes, nil
}

func (l *FakeNodeLister) Run(stopCh <-chan struct{}) error {
	select {
	case <-stopCh:
		return nil
	}
}
