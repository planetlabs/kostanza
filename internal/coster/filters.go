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

package coster

import core_v1 "k8s.io/api/core/v1"

// PodFilter returns true if Pod should be included in filtered results.
type PodFilter func(p *core_v1.Pod) bool

// PodFilters augments a slice of PodFilter functions with additional collection
// related functionality.
type PodFilters []PodFilter

// All returns true if all predicate functions in match the provided pod.
func (pf PodFilters) All(p *core_v1.Pod) bool {
	for _, f := range pf {
		if !f(p) {
			return false
		}
	}
	return true
}

// RunningPodFilter returns true if the Pod is running.
func RunningPodFilter(p *core_v1.Pod) bool {
	return p.Status.Phase == core_v1.PodRunning
}
