/*
Copyright 2016 The Kubernetes Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package kubefedcluster

import (
	"context"
	"sync"
	"time"

	"github.com/pkg/errors"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/equality"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/apimachinery/pkg/util/wait"
	utilfeature "k8s.io/apiserver/pkg/util/feature"
	kubeclient "k8s.io/client-go/kubernetes"
	typedcorev1 "k8s.io/client-go/kubernetes/typed/core/v1"
	restclient "k8s.io/client-go/rest"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/tools/record"
	"k8s.io/klog"

	fedv1b1 "sigs.k8s.io/kubefed/pkg/apis/core/v1beta1"
	genericclient "sigs.k8s.io/kubefed/pkg/client/generic"
	genscheme "sigs.k8s.io/kubefed/pkg/client/generic/scheme"
	"sigs.k8s.io/kubefed/pkg/controller/util"
	"sigs.k8s.io/kubefed/pkg/features"
	"sigs.k8s.io/kubefed/pkg/metrics"
)

// Read-Note: 对于集群 Fed 的单一集群的信息
// ClusterData stores cluster client and previous health check probe results of individual cluster.
type ClusterData struct {
	// Read-Note: 集群通信的 Client
	// clusterKubeClient is the kube client for the cluster.
	clusterKubeClient *ClusterClient

	// Read-Note: 集群当前的状态
	// clusterStatus is the cluster status as of last sampling.
	clusterStatus *fedv1b1.KubeFedClusterStatus

	// Read-Note: 当前集群的最新状态连续被探测到的次数，主要用于集群状态的健康检查，判断集群是在线还是失联
	// How many times in a row the probe has returned the same result.
	resultRun int64

	// Read-Note: 缓存当前最新的 kubefed cluster 对象
	// cachedObj holds the last observer object from apiserver
	cachedObj *fedv1b1.KubeFedCluster
}

// Read-Note: 集群 Fed 控制器
// ClusterController is responsible for maintaining the health status of each
// KubeFedCluster in a particular namespace.
type ClusterController struct {
	client genericclient.Client

	// Read-Note: 关于集群健康检查的配置，探测周期、失败或成功阈值、超时时间
	// clusterHealthCheckConfig is the configurable parameters for cluster health check
	clusterHealthCheckConfig *util.ClusterHealthCheckConfig

	mu sync.RWMutex

	// Read-Note: 存储多个集群的状态信息
	// clusterDataMap is a mapping of clusterName and the cluster specific details.
	clusterDataMap map[string]*ClusterData

	// Read-Note: 用于 Event 相关事件的回调注册
	// clusterController is the cache.Controller where callbacks are registered
	// for events on KubeFedClusters.
	clusterController cache.Controller

	// Read-Note: 指定 fed cluster 相关信息存储的 Namespace
	// fedNamespace is the name of the namespace containing
	// KubeFedCluster resources and their associated secrets.
	fedNamespace string

	eventRecorder record.EventRecorder
}

// StartClusterController starts a new cluster controller.
func StartClusterController(config *util.ControllerConfig, clusterHealthCheckConfig *util.ClusterHealthCheckConfig, stopChan <-chan struct{}) error {
	controller, err := newClusterController(config, clusterHealthCheckConfig)
	if err != nil {
		return err
	}
	klog.Infof("Starting cluster controller")
	controller.Run(stopChan)
	return nil
}

// Read-Note: 创建多集群控制器的流程
// newClusterController returns a new cluster controller
func newClusterController(config *util.ControllerConfig, clusterHealthCheckConfig *util.ClusterHealthCheckConfig) (*ClusterController, error) {
	kubeConfig := restclient.CopyConfig(config.KubeConfig)
	kubeConfig.Timeout = clusterHealthCheckConfig.Timeout
	client := genericclient.NewForConfigOrDieWithUserAgent(kubeConfig, "cluster-controller")

	cc := &ClusterController{
		client:                   client,
		clusterHealthCheckConfig: clusterHealthCheckConfig,
		clusterDataMap:           make(map[string]*ClusterData),
		fedNamespace:             config.KubeFedNamespace,
	}

	kubeClient := kubeclient.NewForConfigOrDie(kubeConfig)
	broadcaster := record.NewBroadcaster()
	broadcaster.StartRecordingToSink(&typedcorev1.EventSinkImpl{Interface: kubeClient.CoreV1().Events("")})
	recorder := broadcaster.NewRecorder(genscheme.Scheme, corev1.EventSource{Component: "kubefedcluster-controller"})
	cc.eventRecorder = recorder

	var err error
	// Read-Note: 创建 Informer 用于监听 Kube Fed Cluster 的事件
	_, cc.clusterController, err = util.NewGenericInformerWithEventHandler(
		config.KubeConfig,
		config.KubeFedNamespace,
		&fedv1b1.KubeFedCluster{},
		util.NoResyncPeriod,
		&cache.ResourceEventHandlerFuncs{
			// Read-Note: 对于 delete 事件，删除 cluster data map 中的对应集群
			DeleteFunc: func(obj interface{}) {
				castObj, ok := obj.(*fedv1b1.KubeFedCluster)
				if !ok {
					tombstone, ok := obj.(cache.DeletedFinalStateUnknown)
					if !ok {
						klog.Errorf("Couldn't get object from tombstone %#v", obj)
						return
					}
					castObj, ok = tombstone.Obj.(*fedv1b1.KubeFedCluster)
					if !ok {
						klog.Errorf("Tombstone contained object that is not expected %#v", obj)
						return
					}
				}
				cc.delFromClusterSet(castObj)
			},
			// Read-Note: 对于 add 事件，存储到对应集群到 cluster data map 中
			AddFunc: func(obj interface{}) {
				castObj := obj.(*fedv1b1.KubeFedCluster)
				cc.addToClusterSet(castObj)
			},
			// Read-Note: 对于 update 事件，需要针对集群的变更情况确认是否更新集群信息
			UpdateFunc: func(oldObj, newObj interface{}) {
				var clusterChanged bool
				cluster := newObj.(*fedv1b1.KubeFedCluster)
				cc.mu.Lock()
				clusterData, ok := cc.clusterDataMap[cluster.Name]

				// Read-Note: 如果涉及到集群定义的 Spec 字段和元信息中的 Annotation 或 Label 变更，
				// 则需要通过先删后加的方式更新 cluster data map 中的集群信息，
				// 因为 status 的信息正是由 cluster controller 来更新，所以这里忽略相关变更，避免修改引发的连环震荡
				if !ok || !equality.Semantic.DeepEqual(clusterData.cachedObj.Spec, cluster.Spec) ||
					!equality.Semantic.DeepEqual(clusterData.cachedObj.ObjectMeta.Annotations, cluster.ObjectMeta.Annotations) ||
					!equality.Semantic.DeepEqual(clusterData.cachedObj.ObjectMeta.Labels, cluster.ObjectMeta.Labels) {
					clusterChanged = true
				}
				cc.mu.Unlock()
				// ignore update if there is no change between the cached object and new
				if !clusterChanged {
					return
				}
				cc.delFromClusterSet(cluster)
				cc.addToClusterSet(cluster)
			},
		},
	)
	return cc, err
}

// delFromClusterSet removes a cluster from the cluster data map
func (cc *ClusterController) delFromClusterSet(obj *fedv1b1.KubeFedCluster) {
	cc.mu.Lock()
	defer cc.mu.Unlock()
	klog.V(1).Infof("ClusterController observed a cluster deletion: %v", obj.Name)
	delete(cc.clusterDataMap, obj.Name)
}

// addToClusterSet creates a new client for the cluster and stores it in cluster data map.
func (cc *ClusterController) addToClusterSet(obj *fedv1b1.KubeFedCluster) {
	cc.mu.Lock()
	defer cc.mu.Unlock()
	clusterData := cc.clusterDataMap[obj.Name]
	if clusterData != nil && clusterData.clusterKubeClient != nil {
		return
	}

	klog.V(1).Infof("ClusterController observed a new cluster: %v", obj.Name)

	// create the restclient of cluster
	restClient, err := NewClusterClientSet(obj, cc.client, cc.fedNamespace, cc.clusterHealthCheckConfig.Timeout)
	if err != nil || restClient == nil {
		cc.RecordError(obj, "MalformedClusterConfig", errors.Wrap(err, "The configuration for this cluster may be malformed"))
		return
	}
	cc.clusterDataMap[obj.Name] = &ClusterData{clusterKubeClient: restClient, cachedObj: obj.DeepCopy()}
}

// Run begins watching and syncing.
func (cc *ClusterController) Run(stopChan <-chan struct{}) {
	defer utilruntime.HandleCrash()
	// Read-Note: 启动 Event Watcher 监听 kube fed cluster 的事件
	go cc.clusterController.Run(stopChan)
	// Read-Note: 周期性监控每一个集群的状态，包括 status 和健康检查状态
	// monitor cluster status periodically, in phase 1 we just get the health state from "/healthz"
	go wait.Until(func() {
		if err := cc.updateClusterStatus(); err != nil {
			klog.Errorf("Error monitoring cluster status: %v", err)
		}
	}, cc.clusterHealthCheckConfig.Period, stopChan)
}

// updateClusterStatus checks cluster health and updates status of all KubeFedClusters
func (cc *ClusterController) updateClusterStatus() error {
	clusters := &fedv1b1.KubeFedClusterList{}
	err := cc.client.List(context.TODO(), clusters, cc.fedNamespace)
	if err != nil {
		return err
	}

	var wg sync.WaitGroup
	for _, obj := range clusters.Items {
		cc.mu.RLock()
		cluster := obj.DeepCopy()
		clusterData := cc.clusterDataMap[cluster.Name]
		cc.mu.RUnlock()
		// Read-Note: 按理说之前的步骤就应该保障 cluster data map 中有集群数据，
		// 这里再一次验证，kube fed cluster list 和 data map 中多集群数据的一致性，如果不一致再次添加集群
		if clusterData == nil {
			// Retry adding cluster client
			cc.addToClusterSet(cluster)
			cc.mu.RLock()
			clusterData = cc.clusterDataMap[cluster.Name]
			cc.mu.RUnlock()
			if clusterData == nil {
				klog.Warningf("Failed to retrieve stored data for cluster %s", cluster.Name)
				continue
			}
		}

		wg.Add(1)
		// Read-Note: 更新每个单独集群的状态
		go cc.updateIndividualClusterStatus(cluster, clusterData, &wg)
	}

	wg.Wait()
	return nil
}

func (cc *ClusterController) updateIndividualClusterStatus(cluster *fedv1b1.KubeFedCluster,
	storedData *ClusterData, wg *sync.WaitGroup) {
	defer metrics.ClusterHealthStatusDurationFromStart(time.Now())

	clusterClient := storedData.clusterKubeClient

	// Read-Note: 通过监控检查接口 `/healthz` 判定集群 status 中的 condition 细节
	currentClusterStatus, err := clusterClient.GetClusterHealthStatus()
	if err != nil {
		cc.RecordError(cluster, "RetrievingClusterHealthFailed", errors.Wrap(err, "Failed to retrieve health of the cluster"))
	}

	// Read-Note: 通过集群最近探测同样 status 的连续次数，配合阈值（成功配合就绪，失败配合离线），判断集群的状态，
	currentClusterStatus = thresholdAdjustedClusterStatus(currentClusterStatus, storedData, cc.clusterHealthCheckConfig)

	// Read-Note: 如果开启了跨集群的 DNS 服务发现，则需要在集群 status 中额外更新更新 Region 和 AZ 相关的物理信息
	if utilfeature.DefaultFeatureGate.Enabled(features.CrossClusterServiceDiscovery) {
		currentClusterStatus = cc.updateClusterZonesAndRegion(currentClusterStatus, cluster, clusterClient)
	}

	// Read-Note: 将更新过的集群状态写回 API Server
	storedData.clusterStatus = currentClusterStatus
	cluster.Status = *currentClusterStatus
	if err := cc.client.UpdateStatus(context.TODO(), cluster); err != nil {
		klog.Warningf("Failed to update the status of cluster %q: %v", cluster.Name, err)
	}

	wg.Done()
}

func (cc *ClusterController) RecordError(cluster runtime.Object, errorCode string, err error) {
	cc.eventRecorder.Eventf(cluster, corev1.EventTypeWarning, errorCode, err.Error())
}

func thresholdAdjustedClusterStatus(clusterStatus *fedv1b1.KubeFedClusterStatus, storedData *ClusterData,
	clusterHealthCheckConfig *util.ClusterHealthCheckConfig) *fedv1b1.KubeFedClusterStatus {
	if storedData.clusterStatus == nil {
		storedData.resultRun = 1
		return clusterStatus
	}

	threshold := clusterHealthCheckConfig.FailureThreshold
	if util.IsClusterReady(clusterStatus) {
		threshold = clusterHealthCheckConfig.SuccessThreshold
	}

	if storedData.resultRun < threshold {
		// Success/Failure is below threshold - leave the probe state unchanged.
		probeTime := clusterStatus.Conditions[0].LastProbeTime
		clusterStatus = storedData.clusterStatus
		setProbeTime(clusterStatus, probeTime)
	} else if clusterStatusEqual(clusterStatus, storedData.clusterStatus) {
		// preserve the last transition time
		setTransitionTime(clusterStatus, *storedData.clusterStatus.Conditions[0].LastTransitionTime)
	}

	if clusterStatusEqual(clusterStatus, storedData.clusterStatus) {
		// Increment the result run has there is no change in cluster condition
		storedData.resultRun++
	} else {
		// Reset the result run
		storedData.resultRun = 1
	}

	return clusterStatus
}

func (cc *ClusterController) updateClusterZonesAndRegion(clusterStatus *fedv1b1.KubeFedClusterStatus, cluster *fedv1b1.KubeFedCluster,
	clusterClient *ClusterClient) *fedv1b1.KubeFedClusterStatus {
	if !util.IsClusterReady(clusterStatus) {
		return clusterStatus
	}

	zones, region, err := clusterClient.GetClusterZones()
	if err != nil {
		cc.RecordError(cluster, "RetrievingRegionZonesFailed", errors.Wrap(err, "Failed to get zones and region for the cluster"))
		return clusterStatus
	}

	// If new zone & region are empty, preserve the old ones so that user configured zone & region
	// labels are effective
	if len(zones) == 0 {
		zones = cluster.Status.Zones
	}
	if len(region) == 0 && cluster.Status.Region != nil {
		region = *cluster.Status.Region
	}
	clusterStatus.Zones = zones
	clusterStatus.Region = &region
	return clusterStatus
}

func clusterStatusEqual(newClusterStatus, oldClusterStatus *fedv1b1.KubeFedClusterStatus) bool {
	return util.IsClusterReady(newClusterStatus) == util.IsClusterReady(oldClusterStatus)
}

func setProbeTime(clusterStatus *fedv1b1.KubeFedClusterStatus, probeTime metav1.Time) {
	for i := 0; i < len(clusterStatus.Conditions); i++ {
		clusterStatus.Conditions[i].LastProbeTime = probeTime
	}
}

func setTransitionTime(clusterStatus *fedv1b1.KubeFedClusterStatus, transitionTime metav1.Time) {
	for i := 0; i < len(clusterStatus.Conditions); i++ {
		clusterStatus.Conditions[i].LastTransitionTime = &transitionTime
	}
}
