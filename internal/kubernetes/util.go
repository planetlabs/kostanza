package kubernetes

import (
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/client-go/tools/clientcmd/api"
)

// BuildConfigFromFlags is clientcmd.BuildConfigFromFlags with no annoying
// dependencies on glog.
// https://godoc.org/k8s.io/client-go/tools/clientcmd#BuildConfigFromFlags
func BuildConfigFromFlags(apiserver, kubecfg string) (*rest.Config, error) {
	if kubecfg != "" || apiserver != "" {
		return clientcmd.NewNonInteractiveDeferredLoadingClientConfig(
			&clientcmd.ClientConfigLoadingRules{ExplicitPath: kubecfg},
			&clientcmd.ConfigOverrides{ClusterInfo: api.Cluster{Server: apiserver}}).ClientConfig()
	}
	return rest.InClusterConfig()
}
