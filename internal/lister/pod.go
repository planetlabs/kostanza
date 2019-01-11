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

	core_v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/client-go/informers"
	informersv1 "k8s.io/client-go/informers/core/v1"
	"k8s.io/client-go/kubernetes"
	listersv1 "k8s.io/client-go/listers/core/v1"
	"k8s.io/client-go/tools/cache"

	"github.com/planetlabs/kostanza/internal/log"
)

const podResyncPeriod = time.Minute * 15

var _ PodLister = (*kubernetesPodLister)(nil)
var _ PodLister = (*FakePodLister)(nil)

// PodLister lists pods in a kubernetes cluster. The canonical implementation
// uses the kubernetes informer mechanism, which is expected to be started via a
// call to the Run method. Prior to this, a concrete implementation will
// generally not succesfully return pods.
type PodLister interface {
	List(selector labels.Selector) ([]*core_v1.Pod, error)
	Run(stopCh <-chan struct{}) error
}

// NewKubernetesPodLister returns a PodLister that provides simplified listing
// of pods via the underlying client-go SharedInformer APIs.
func NewKubernetesPodLister(client kubernetes.Interface) *kubernetesPodLister { // nolint: golint
	informerFactory := informers.NewSharedInformerFactory(client, podResyncPeriod)
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

// FakePodLister provides a mock PodLister implementation.
type FakePodLister struct {
	Pods []*core_v1.Pod
}

// List returns the list of pods provided to the FakePodLister.
func (l *FakePodLister) List(selector labels.Selector) ([]*core_v1.Pod, error) {
	return l.Pods, nil
}

// Run mimics the run loop of a concrete PodLister.
func (l *FakePodLister) Run(stopCh <-chan struct{}) error {
	<-stopCh
	return nil
}
