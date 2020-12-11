/*
Copyright 2018 The Kubernetes Authors.

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

package schedulingtypes

import (
	"bytes"
	"context"
	"fmt"
	"sort"
	"time"

	"github.com/pkg/errors"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	pkgruntime "k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/klog"
	crclient "sigs.k8s.io/controller-runtime/pkg/client"

	"sigs.k8s.io/kubefed/pkg/apis/core/typeconfig"
	fedschedulingv1a1 "sigs.k8s.io/kubefed/pkg/apis/scheduling/v1alpha1"
	genericclient "sigs.k8s.io/kubefed/pkg/client/generic"
	ctlutil "sigs.k8s.io/kubefed/pkg/controller/util"
	"sigs.k8s.io/kubefed/pkg/controller/util/planner"
	"sigs.k8s.io/kubefed/pkg/controller/util/podanalyzer"
)

const (
	RSPKind = "ReplicaSchedulingPreference"
)

func init() {
	schedulingType := SchedulingType{
		Kind:             RSPKind,
		SchedulerFactory: NewReplicaScheduler,
	}
	RegisterSchedulingType("deployments.apps", schedulingType)
	RegisterSchedulingType("replicasets.apps", schedulingType)
}

type ReplicaScheduler struct {
	controllerConfig *ctlutil.ControllerConfig

	eventHandlers SchedulerEventHandlers

	plugins *ctlutil.SafeMap

	client      genericclient.Client
	podInformer ctlutil.FederatedInformer
}

func NewReplicaScheduler(controllerConfig *ctlutil.ControllerConfig, eventHandlers SchedulerEventHandlers) (Scheduler, error) {
	client := genericclient.NewForConfigOrDieWithUserAgent(controllerConfig.KubeConfig, "replica-scheduler")
	scheduler := &ReplicaScheduler{
		plugins:          ctlutil.NewSafeMap(),
		controllerConfig: controllerConfig,
		eventHandlers:    eventHandlers,
		client:           client,
	}

	// TODO: Update this to use a typed client from single target informer.
	// As of now we have a separate informer for pods, whereas all we need
	// is a typed client.
	// We ignore the pod events in this informer from clusters.
	var err error
	scheduler.podInformer, err = ctlutil.NewFederatedInformer(
		controllerConfig,
		client,
		PodResource,
		func(pkgruntime.Object) {},
		eventHandlers.ClusterLifecycleHandlers,
	)
	if err != nil {
		return nil, err
	}

	return scheduler, nil
}

func (s *ReplicaScheduler) SchedulingKind() string {
	return RSPKind
}

func (s *ReplicaScheduler) StartPlugin(typeConfig typeconfig.Interface) error {
	kind := typeConfig.GetFederatedType().Kind
	// TODO(marun) Return an error if the kind is not supported

	plugin, err := NewPlugin(s.controllerConfig, s.eventHandlers, typeConfig)
	if err != nil {
		return errors.Wrapf(err, "Failed to initialize replica scheduling plugin for %q", kind)
	}

	plugin.Start()
	s.plugins.Store(kind, plugin)

	return nil
}

func (s *ReplicaScheduler) StopPlugin(kind string) {
	plugin, ok := s.plugins.Get(kind)
	if !ok {
		return
	}

	plugin.(*Plugin).Stop()
	s.plugins.Delete(kind)
}

func (s *ReplicaScheduler) ObjectType() pkgruntime.Object {
	return &fedschedulingv1a1.ReplicaSchedulingPreference{}
}

func (s *ReplicaScheduler) Start() {
	s.podInformer.Start()
}

func (s *ReplicaScheduler) HasSynced() bool {
	for _, plugin := range s.plugins.GetAll() {
		if !plugin.(*Plugin).HasSynced() {
			return false
		}
	}

	if !s.podInformer.ClustersSynced() {
		klog.V(2).Infof("Cluster list not synced")
		return false
	}
	clusters, err := s.podInformer.GetReadyClusters()
	if err != nil {
		runtime.HandleError(errors.Wrap(err, "Failed to get ready clusters"))
		return false
	}
	return s.podInformer.GetTargetStore().ClustersSynced(clusters)
}

func (s *ReplicaScheduler) Stop() {
	for _, plugin := range s.plugins.GetAll() {
		plugin.(*Plugin).Stop()
	}
	s.plugins.DeleteAll()
	s.podInformer.Stop()
}

// Read-Note: 目前对于 Scheduler 最重点的其实就在这了，对于 Replicas 字段的调协
// 而这段其实就是捞已经注册的 Type 的 Plugin ，执行他的调协，所以再往下扎一层，看 Plugin 的 Reconcile
func (s *ReplicaScheduler) Reconcile(obj pkgruntime.Object, qualifiedName ctlutil.QualifiedName) ctlutil.ReconciliationStatus {
	rsp, ok := obj.(*fedschedulingv1a1.ReplicaSchedulingPreference)
	if !ok {
		runtime.HandleError(errors.Errorf("Incorrect runtime object for RSP: %v", rsp))
		return ctlutil.StatusError
	}

	clusterNames, err := s.clusterNames()
	if err != nil {
		runtime.HandleError(errors.Wrap(err, "Failed to get cluster list"))
		return ctlutil.StatusError
	}
	if len(clusterNames) == 0 {
		// no joined clusters, nothing to do
		return ctlutil.StatusAllOK
	}

	kind := rsp.Spec.TargetKind
	if kind != "FederatedDeployment" && kind != "FederatedReplicaSet" {
		runtime.HandleError(errors.Wrapf(err, "RSP target kind: %s is incorrect", kind))
		return ctlutil.StatusNeedsRecheck
	}

	plugin, ok := s.plugins.Get(kind)
	if !ok {
		return ctlutil.StatusAllOK
	}

	if !plugin.(*Plugin).FederatedTypeExists(qualifiedName.String()) {
		// target FederatedType does not exist, nothing to do
		return ctlutil.StatusAllOK
	}

	key := qualifiedName.String()
	// Read-Note: 关键在于这个 result ，也就是 replicas map 的获取
	result, err := s.GetSchedulingResult(rsp, qualifiedName, clusterNames)
	if err != nil {
		runtime.HandleError(errors.Wrapf(err, "Failed to compute the schedule information while reconciling RSP named %q", key))
		return ctlutil.StatusError
	}

	// Read-Note: 其实就是把 placement + override + replicas schedule 的内容实际进行组合，进行必要的 update
	err = plugin.(*Plugin).Reconcile(qualifiedName, result)
	if err != nil {
		runtime.HandleError(errors.Wrapf(err, "Failed to reconcile federated targets for RSP named %q", key))
		return ctlutil.StatusError
	}

	return ctlutil.StatusAllOK
}

// The list of clusters could come from any target informer
func (s *ReplicaScheduler) clusterNames() ([]string, error) {
	clusters, err := s.podInformer.GetReadyClusters()
	if err != nil {
		return nil, err
	}
	clusterNames := []string{}
	for _, cluster := range clusters {
		clusterNames = append(clusterNames, cluster.Name)
	}

	return clusterNames, nil
}

// Read-Note: 关键中的关键，replicas 的数量是如何重新分配的
func (s *ReplicaScheduler) GetSchedulingResult(rsp *fedschedulingv1a1.ReplicaSchedulingPreference, qualifiedName ctlutil.QualifiedName, clusterNames []string) (map[string]int64, error) {
	key := qualifiedName.String()

	// Read-Note: 通过 key 从 Cache 获取 FT 指向的目标 Type 的 Resource，这里也就是某一集群的 Deployment 或者 RS
	objectGetter := func(clusterName, key string) (interface{}, bool, error) {
		plugin, ok := s.plugins.Get(rsp.Spec.TargetKind)
		if !ok {
			return nil, false, nil
		}
		return plugin.(*Plugin).targetInformer.GetTargetStore().GetByKey(clusterName, key)
	}
	// Read-Note: 通过 FT 指向的目标 Type 的 Resource（Deployment 或者 RS），获取 spec.selector.matchLabels
	// 获取在某一集群里由该 Resource 做 Controller 的 Pod 列表
	podsGetter := func(clusterName string, unstructuredObj *unstructured.Unstructured) (*corev1.PodList, error) {
		client, err := s.podInformer.GetClientForCluster(clusterName)
		if err != nil {
			return nil, err
		}
		selectorLabels, ok, err := unstructured.NestedStringMap(unstructuredObj.Object, "spec", "selector", "matchLabels")
		if !ok {
			return nil, errors.New("missing selector on object")
		}
		if err != nil {
			return nil, errors.Wrap(err, "error retrieving selector from object")
		}

		podList := &corev1.PodList{}
		err = client.List(context.Background(), podList, unstructuredObj.GetNamespace(), crclient.MatchingLabels(selectorLabels))
		if err != nil {
			return nil, err
		}
		return podList, nil
	}

	// Read-Note: 综合这些因素重新考虑如何重新排布各个集群的 replicas 占比和评估后的资源使用量（？）
	currentReplicasPerCluster, estimatedCapacity, err := clustersReplicaState(clusterNames, key, objectGetter, podsGetter)
	if err != nil {
		return nil, err
	}

	// Read-Note: 看到这个熟悉的比例分配结构，居然还套了 min max
	// TODO: Move this to API defaulting logic
	if len(rsp.Spec.Clusters) == 0 {
		rsp.Spec.Clusters = map[string]fedschedulingv1a1.ClusterPreferences{
			"*": {Weight: 1},
		}
	}

	plnr := planner.NewPlanner(rsp)
	return schedule(plnr, key, clusterNames, currentReplicasPerCluster, estimatedCapacity)
}

func schedule(planner *planner.Planner, key string, clusterNames []string, currentReplicasPerCluster map[string]int64, estimatedCapacity map[string]int64) (map[string]int64, error) {
	scheduleResult, overflow, err := planner.Plan(clusterNames, currentReplicasPerCluster, estimatedCapacity, key)
	if err != nil {
		return nil, err
	}

	// TODO: Check if we really need to place the federated type in clusters
	// with 0 replicas. Override replicas would be set to 0 in this case.
	result := make(map[string]int64)
	for clusterName := range currentReplicasPerCluster {
		result[clusterName] = 0
	}

	for clusterName, replicas := range scheduleResult {
		result[clusterName] = replicas
	}
	for clusterName, replicas := range overflow {
		result[clusterName] += replicas
	}

	if klog.V(4) {
		buf := bytes.NewBufferString(fmt.Sprintf("Schedule - %q\n", key))
		sort.Strings(clusterNames)
		for _, clusterName := range clusterNames {
			cur := currentReplicasPerCluster[clusterName]
			target := scheduleResult[clusterName]
			fmt.Fprintf(buf, "%s: current: %d target: %d", clusterName, cur, target)
			if over, found := overflow[clusterName]; found {
				fmt.Fprintf(buf, " overflow: %d", over)
			}
			if capacity, found := estimatedCapacity[clusterName]; found {
				fmt.Fprintf(buf, " capacity: %d", capacity)
			}
			fmt.Fprintf(buf, "\n")
		}
		klog.V(4).Infof(buf.String())
	}
	return result, nil
}

// clustersReplicaState returns information about the scheduling state of the pods running in the federated clusters.
func clustersReplicaState(
	clusterNames []string,
	key string,
	objectGetter func(clusterName string, key string) (interface{}, bool, error),
	podsGetter func(clusterName string, obj *unstructured.Unstructured) (*corev1.PodList, error)) (currentReplicasPerCluster map[string]int64, estimatedCapacity map[string]int64, err error) {
	currentReplicasPerCluster = make(map[string]int64)
	estimatedCapacity = make(map[string]int64)

	for _, clusterName := range clusterNames {
		// Read-Note: 获取 Deployment 或 RS 的 spec.replicas 和 status.readyReplicas
		obj, exists, err := objectGetter(clusterName, key)
		if err != nil {
			return nil, nil, err
		}
		if !exists {
			continue
		}

		unstructuredObj := obj.(*unstructured.Unstructured)
		// Read-Question：如果是找不到 spec.replicas 字段的情况就直接认为是 0 有点不太合理
		// 因为 RS 和 Deployment Spec 的 replicas 的缺省含义是以 1 作为默认值
		// 虽然理想的状态是该字段有值，但是缺省的字段和原定义个人认为最好保持一致
		replicas, ok, err := unstructured.NestedInt64(unstructuredObj.Object, "spec", "replicas")
		if err != nil {
			return nil, nil, errors.Wrap(err, "Error retrieving 'replicas' field")
		}
		if !ok {
			replicas = int64(0)
		}
		// Read-Question: 如果是找不到 status.readyReplicas 也直接认为是 0 也不太合理
		// 这个字段本身在 RS 和 Deployment 为非 ptr 字段，如果 not found 很大程度上可以怀疑是上层传入的 unstructed object 类型不符合预期
		// 更合理的做法应该是也返回 error 中止这次 replicas 重分配的计算
		readyReplicas, ok, err := unstructured.NestedInt64(unstructuredObj.Object, "status", "readyreplicas")
		if err != nil {
			return nil, nil, errors.Wrap(err, "Error retrieving 'readyreplicas' field")
		}
		if !ok {
			readyReplicas = int64(0)
		}

		if replicas == readyReplicas {
			// Read-Note: 如果当前集群的实例数即为期望状态，那维持现有的 replicas 就行
			currentReplicasPerCluster[clusterName] = readyReplicas
		} else {
			// Read-Note: 如果当前集群的实例数与期望状态有出入，那么就需要结合该 Controller 管理的所有 Pod 的状况来考虑
			// Read-Question: 看到这里会有和上文有类似的疑问，这里给 current replicas per cluster 设置初始值为 0 是否合理（当然这个初始化 0 最终肯定不会采用）
			// Read-Question：还有一个问题是这边额外统计 podStatus.RunningAndReady 可以认为和前文的 readyReplicas 等价，这边多做一次统计是考虑到 Status 状态可能没有及时更新？
			// Read-Question: 除此之外还想到了一个场景，如果是正在滚动升级的状态，running&ready > spec.replicas ，这边直接丢进去似乎问题…很大呀……
			// 这里先画个未完待续，看看外层具体怎么用这里的计算结果，具体在 `pkg/controller/util/planner/planner.go` 中 `planner.Plan` 是怎么实现的
			currentReplicasPerCluster[clusterName] = int64(0)
			podList, err := podsGetter(clusterName, unstructuredObj)
			if err != nil {
				return nil, nil, err
			}

			podStatus := podanalyzer.AnalyzePods(podList, time.Now())
			currentReplicasPerCluster[clusterName] = int64(podStatus.RunningAndReady) // include pending as well?
			// Read-Question: 这边统计的 unschedule 是以 condition 获取到 pod 被调度器 1min 内尝试调度，但是最终无法调度成功
			// 此处考虑到可能是集群资源原因、Quota 限制、调度条件（Node Selector、亲和和反亲和）
			// 而这些状况都是一个动态过程，这种情况如果考虑直接把这份实例量给到其他集群，并不是针对每一种情况都合理，
			// 比如 CA 可快速扩容，其他有一些不合理的调度条件被移除，而且就算调度到其他集群也有可能引发新的 Unschedulable 然后来回放
			// Read-Question: 这边也没有考虑到 pod 是否是 updated ，所以新旧 pod 是同时考虑的，可能只考虑 updated 的部分会更合理一些
			unschedulable := int64(podStatus.Unschedulable)
			if unschedulable > 0 {
				estimatedCapacity[clusterName] = replicas - unschedulable
			}
		}
	}
	return currentReplicasPerCluster, estimatedCapacity, nil
}
