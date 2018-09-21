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

var _ NodeLister = (*kubernetesNodeLister)(nil)
var _ NodeLister = (*FakeNodeLister)(nil)

// NodeLister lists nodes in a kubernetes cluster. The canonical implementation
// uses the kubernetes informer mechanism, which is expected to be started via a
// call to the Run method. Prior to this, a concrete implementation will
// generally not succesfully return nodes.
type NodeLister interface {
	List(selector labels.Selector) (ret []*core_v1.Node, err error)
	Run(stopCh <-chan struct{}) error
}

// NewKubernetesNodeLister returns a NodeLister that provides simplified
// listing of nodes via the underlying client-go SharedInformer APIs
func NewKubernetesNodeLister(client kubernetes.Interface) *kubernetesNodeLister { // nolint: golint
	informerFactory := informers.NewSharedInformerFactory(client, time.Second)
	ni := informerFactory.Core().V1().Nodes()
	nl := ni.Lister()

	return &kubernetesNodeLister{
		lister:   nl,
		informer: ni,
	}
}

// kubernetesNodeLister uses an underlying client-go informer to synchronize a
// local in-memory cache of kubernetes node resources.
type kubernetesNodeLister struct {
	lister   listersv1.NodeLister
	informer informersv1.NodeInformer
}

// List returns the slice of nodes matching the provided labels.
func (k *kubernetesNodeLister) List(selector labels.Selector) (ret []*core_v1.Node, err error) {
	return k.lister.List(selector)
}

// Run begins stars the asynchonrous watch loop using the underlying client-go
// informer. The stopCh can be used to signal when we we should cancel.
func (k *kubernetesNodeLister) Run(stopCh <-chan struct{}) error {
	k.informer.Informer().Run(stopCh)
	log.Log.Debug("waiting for node cache to sync")
	if ok := cache.WaitForCacheSync(stopCh, k.informer.Informer().HasSynced); !ok {
		log.Log.Error("node cache did not sync")
		return ErrCacheSyncFailed
	}
	return nil
}

// FakeNodeLister provides a mock NodeLister implementation.
type FakeNodeLister struct {
	Nodes []*core_v1.Node
}

// List returns the slice of nodes provided to this NodeLister.
func (l *FakeNodeLister) List(selector labels.Selector) ([]*core_v1.Node, error) {
	return l.Nodes, nil
}

// Run mimics the run loop of a concrete NodeLister.
func (l *FakeNodeLister) Run(stopCh <-chan struct{}) error {
	<-stopCh
	return nil
}
