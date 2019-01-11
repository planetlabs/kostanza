// Copyright 2018 Planet Labs Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package lister

import (
	"time"

	"github.com/planetlabs/kostanza/internal/log"
	core_v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/client-go/informers"
	informersv1 "k8s.io/client-go/informers/core/v1"
	"k8s.io/client-go/kubernetes"
	listersv1 "k8s.io/client-go/listers/core/v1"
	"k8s.io/client-go/tools/cache"
)

const nodeResyncPeriod = time.Minute * 15

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
	informerFactory := informers.NewSharedInformerFactory(client, nodeResyncPeriod)
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
