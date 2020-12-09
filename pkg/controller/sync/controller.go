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

package sync

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/pkg/errors"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	pkgruntime "k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/apimachinery/pkg/util/wait"
	kubeclient "k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/scheme"
	typedcorev1 "k8s.io/client-go/kubernetes/typed/core/v1"
	"k8s.io/client-go/tools/record"
	"k8s.io/klog"

	"sigs.k8s.io/kubefed/pkg/apis/core/typeconfig"
	fedv1b1 "sigs.k8s.io/kubefed/pkg/apis/core/v1beta1"
	genericclient "sigs.k8s.io/kubefed/pkg/client/generic"
	"sigs.k8s.io/kubefed/pkg/controller/sync/dispatch"
	"sigs.k8s.io/kubefed/pkg/controller/sync/status"
	"sigs.k8s.io/kubefed/pkg/controller/util"
	finalizersutil "sigs.k8s.io/kubefed/pkg/controller/util/finalizers"
	"sigs.k8s.io/kubefed/pkg/metrics"
)

const (
	allClustersKey = "ALL_CLUSTERS"

	// If this finalizer is present on a federated resource, the sync
	// controller will have the opportunity to perform pre-deletion operations
	// (like deleting managed resources from member clusters).
	FinalizerSyncController = "kubefed.io/sync-controller"
)

// KubeFedSyncController synchronizes the state of federated resources
// in the host cluster with resources in member clusters.
type KubeFedSyncController struct {
	// TODO(marun) add comment
	worker util.ReconcileWorker

	// Read-Note: Deliverer 用于在集群状态变化是触发 fed 资源的调协
	// 本质其实是个封装的 Heap + target/update/stop channel
	// For triggering reconciliation of all target resources. This is
	// used when a new cluster becomes available.
	clusterDeliverer *util.DelayingDeliverer

	// Read-Note: 成员集群的资源 Informer
	// Informer for resources in member clusters
	informer util.FederatedInformer

	// For events
	eventRecorder record.EventRecorder

	clusterAvailableDelay   time.Duration
	clusterUnavailableDelay time.Duration
	smallDelay              time.Duration

	typeConfig typeconfig.Interface

	// Read-Note: fed 资源的遍历器，方便批量操作
	fedAccessor FederatedResourceAccessor

	// Read-Note: Host cluster 的 client
	hostClusterClient genericclient.Client

	skipAdoptingResources bool

	limitedScope bool
}

// Read-Note: FTC 资源其实是对 kubernetes 资源映射，当 FTC 控制器把 sync 控制器启动了之后，
// sync 控制器再去监控联邦 CRD （FederatedXXX），这些资源的创建其实才是 sync 控制器管理的真正对象。
// FTC 控制器的真正目的其实就是看那些 kubernetes 资源对象需要被联邦管理，哪些不需要。
// 需要的资源对象，就创建一个 FTC 对象声明下，那么 FTC 控制器会启动一个 sync 控制器来跟进这个 kubernetes 对象（或 kubernetes CRD）的变化。
// StartKubeFedSyncController starts a new sync controller for a type config
func StartKubeFedSyncController(controllerConfig *util.ControllerConfig, stopChan <-chan struct{}, typeConfig typeconfig.Interface, fedNamespaceAPIResource *metav1.APIResource) error {
	// Read-Note: 启动 fed sync 控制器，监听的对象为 Type Config 类型
	controller, err := newKubeFedSyncController(controllerConfig, typeConfig, fedNamespaceAPIResource)
	if err != nil {
		return err
	}
	if controllerConfig.MinimizeLatency {
		controller.minimizeLatency()
	}
	klog.Infof(fmt.Sprintf("Starting sync controller for %q", typeConfig.GetFederatedType().Kind))
	controller.Run(stopChan)
	return nil
}

// newKubeFedSyncController returns a new sync controller for the configuration
func newKubeFedSyncController(controllerConfig *util.ControllerConfig, typeConfig typeconfig.Interface, fedNamespaceAPIResource *metav1.APIResource) (*KubeFedSyncController, error) {
	federatedTypeAPIResource := typeConfig.GetFederatedType()
	userAgent := fmt.Sprintf("%s-controller", strings.ToLower(federatedTypeAPIResource.Kind))

	// Initialize non-dynamic clients first to avoid polluting config
	client := genericclient.NewForConfigOrDieWithUserAgent(controllerConfig.KubeConfig, userAgent)
	kubeClient := kubeclient.NewForConfigOrDie(controllerConfig.KubeConfig)

	broadcaster := record.NewBroadcaster()
	broadcaster.StartRecordingToSink(&typedcorev1.EventSinkImpl{Interface: kubeClient.CoreV1().Events("")})
	recorder := broadcaster.NewRecorder(scheme.Scheme, corev1.EventSource{Component: userAgent})

	s := &KubeFedSyncController{
		clusterAvailableDelay:   controllerConfig.ClusterAvailableDelay,
		clusterUnavailableDelay: controllerConfig.ClusterUnavailableDelay,
		smallDelay:              time.Second * 3,
		eventRecorder:           recorder,
		typeConfig:              typeConfig,
		hostClusterClient:       client,
		skipAdoptingResources:   controllerConfig.SkipAdoptingResources,
		limitedScope:            controllerConfig.LimitedScope(),
	}

	s.worker = util.NewReconcileWorker(s.reconcile, util.WorkerTiming{
		ClusterSyncDelay: s.clusterAvailableDelay,
	})

	// Build deliverer for triggering cluster reconciliations.
	s.clusterDeliverer = util.NewDelayingDeliverer()

	targetAPIResource := typeConfig.GetTargetType()

	// Federated informer for resources in member clusters
	var err error
	s.informer, err = util.NewFederatedInformer(
		controllerConfig,
		client,
		&targetAPIResource,
		// Read-Note: 触发 fed 资源调协的函数，这个函数被注入到一个 Event watcher 有任何对应 Object 的变更触发入队元素的调协
		func(obj pkgruntime.Object) {
			qualifiedName := util.NewQualifiedName(obj)
			s.worker.EnqueueForRetry(qualifiedName)
		},
		// Read-Note: 针对集群状态的变更，都触发所有目标资源的重新处理
		&util.ClusterLifecycleHandlerFuncs{
			// Read-Note: 当有新的集群可用（可能是新加入或者不可用集群恢复）
			ClusterAvailable: func(cluster *fedv1b1.KubeFedCluster) {
				// When new cluster becomes available process all the target resources again.
				s.clusterDeliverer.DeliverAt(allClustersKey, nil, time.Now().Add(s.clusterAvailableDelay))
			},
			// Read-Note: 当有新的集群不可用（可能是被移除或者状态失联）
			// When a cluster becomes unavailable process all the target resources again.
			ClusterUnavailable: func(cluster *fedv1b1.KubeFedCluster, _ []interface{}) {
				s.clusterDeliverer.DeliverAt(allClustersKey, nil, time.Now().Add(s.clusterUnavailableDelay))
			},
		},
	)
	if err != nil {
		return nil, err
	}

	// Read-Note: 处理实际处理 fed 资源的 Accessor
	s.fedAccessor, err = NewFederatedResourceAccessor(
		controllerConfig, typeConfig, fedNamespaceAPIResource,
		client, s.worker.EnqueueObject, recorder)
	if err != nil {
		return nil, err
	}

	return s, nil
}

// minimizeLatency reduces delays and timeouts to make the controller more responsive (useful for testing).
func (s *KubeFedSyncController) minimizeLatency() {
	s.clusterAvailableDelay = time.Second
	s.clusterUnavailableDelay = time.Second
	s.smallDelay = 20 * time.Millisecond
	s.worker.SetDelay(50*time.Millisecond, s.clusterAvailableDelay)
}

func (s *KubeFedSyncController) Run(stopChan <-chan struct{}) {
	s.fedAccessor.Run(stopChan)
	s.informer.Start()
	s.clusterDeliverer.StartWithHandler(func(_ *util.DelayingDelivererItem) {
		s.reconcileOnClusterChange()
	})

	s.worker.Run(stopChan)

	// Ensure all goroutines are cleaned up when the stop channel closes
	go func() {
		<-stopChan
		s.informer.Stop()
		s.clusterDeliverer.Stop()
	}()
}

// Check whether all data stores are in sync. False is returned if any of the informer/stores is not yet
// synced with the corresponding api server.
func (s *KubeFedSyncController) isSynced() bool {
	// Read-Note: 获取集群拓扑信息的 Controller 是否同步
	if !s.informer.ClustersSynced() {
		klog.V(2).Infof("Cluster list not synced")
		return false
	}
	// Read-Note: Accessor 是否同步，主要取决于里面的各类 manager 和 controller 是否初始化或者同步完毕
	if !s.fedAccessor.HasSynced() {
		// The fed accessor will have logged why sync is not yet
		// complete.
		return false
	}

	// Read-Note: 获取所有就绪集群，逐一确认对应 Target Type 相关的 controller 在所有就绪集群是否同步完成
	// TODO(marun) set clusters as ready in the test fixture?
	clusters, err := s.informer.GetReadyClusters()
	if err != nil {
		runtime.HandleError(errors.Wrap(err, "Failed to get ready clusters"))
		return false
	}
	if !s.informer.GetTargetStore().ClustersSynced(clusters) {
		return false
	}
	return true
}

// Read-Note: 针对集群状态变更的的调协，全部集群相关 fed 资源都遍历一遍（由 ClusterLifecycleHandlerFuncs 触发）
// The function triggers reconciliation of all target federated resources.
func (s *KubeFedSyncController) reconcileOnClusterChange() {
	if !s.isSynced() {
		s.clusterDeliverer.DeliverAt(allClustersKey, nil, time.Now().Add(s.clusterAvailableDelay))
	}
	s.fedAccessor.VisitFederatedResources(func(obj interface{}) {
		qualifiedName := util.NewQualifiedName(obj.(pkgruntime.Object))
		s.worker.EnqueueWithDelay(qualifiedName, s.smallDelay)
	})
}

// Read-Note: 同步器的主体——针对某个具体资源调协逻辑
func (s *KubeFedSyncController) reconcile(qualifiedName util.QualifiedName) util.ReconciliationStatus {
	// Read-Note: 判断 Informer 是否是最新同步内容
	if !s.isSynced() {
		return util.StatusNotSynced
	}

	kind := s.typeConfig.GetFederatedType().Kind

	// Read-Note: 构造需要可能需要更新的对象
	// 其中 possibleOrphan 为 true 的情况是对于本身就是 NS 或者非 NS 相关对象
	// 从后面的处理来看，这些对象的被 kubefed 接管之后如果不做主动的清理则可能成为孤儿，
	// 而 NS 关联对象则可以跟随 NS 的清理被删除
	// 清理的最终执行对象是 Dispatcher 的 Unmanaged 类
	fedResource, possibleOrphan, err := s.fedAccessor.FederatedResource(qualifiedName)
	if err != nil {
		runtime.HandleError(errors.Wrapf(err, "Error creating FederatedResource helper for %s %q", kind, qualifiedName))
		return util.StatusError
	}
	if possibleOrphan {
		apiResource := s.typeConfig.GetTargetType()
		gvk := apiResourceToGVK(&apiResource)
		klog.V(2).Infof("Ensuring the removal of the label %q from %s %q in member clusters.", util.ManagedByKubeFedLabelKey, gvk.Kind, qualifiedName)
		err = s.removeManagedLabel(gvk, qualifiedName)
		if err != nil {
			wrappedErr := errors.Wrapf(err, "failed to remove the label %q from %s %q in member clusters", util.ManagedByKubeFedLabelKey, gvk.Kind, qualifiedName)
			runtime.HandleError(wrappedErr)
			return util.StatusError
		}

		return util.StatusAllOK
	}
	if fedResource == nil {
		return util.StatusAllOK
	}

	key := fedResource.FederatedName().String()

	klog.V(4).Infof("Starting to reconcile %s %q", kind, key)
	startTime := time.Now()
	defer func() {
		klog.V(4).Infof("Finished reconciling %s %q (duration: %v)", kind, key, time.Since(startTime))
		metrics.ReconcileFederatedResourcesDurationFromStart(startTime)
	}()

	// Read-Note: 如果该对象已经被标记为删除，则检查一系列删除流程，确保其清理回收不会被阻塞：
	// 是否设置 Finalizer、其清理 Annotation 是否标记 Orphan 清理、是否移除标记 kubefed 接管的 label
	// 当以上检查都确认无误之后，进行对象在各个集群的清理，并统计有哪些集群因为清理异常有遗留
	// 其中会绕过一些关键或者特殊的对象，比如：
	// 1. host cluster 中的 NS（只会移除接管，host cluster 中 NS 的清理是由上层 Controller 负责，sync 不负责
	// 2. 已经被标记删除的对象（无需重复删除）
	if fedResource.Object().GetDeletionTimestamp() != nil {
		return s.ensureDeletion(fedResource)
	}

	// Read-Note: 如果该对象没有被标记删除，则需要确保 Finalizer 存在，阻拦其他组件对于该对象的删除
	err = s.ensureFinalizer(fedResource)
	if err != nil {
		fedResource.RecordError("EnsureFinalizerError", errors.Wrap(err, "Failed to ensure finalizer"))
		return util.StatusError
	}

	// Read-Note: 最后才是真正的同步所有集群环节，其中流程是：
	// 1. 获取全部集群，根据 fed resource placement 获取其中关联到的集群（这段路基再 placement）
	// 1.1. 选中的集群是已经注册的集群和 resource placement 中的集群取交集，也就是说 placement 中存在未注册集群会被忽略
	// 1.2. 如果对象本身是 NS 关联的，则获取的交集是需要带上 NS 再取交集
	// 2. 实际同步的工作由 Dispatcher 中的 Managed 类负责，Dispatcher 判断集群是否为选中、就绪
	// 2.1. 未选中的集群：未就绪的直接忽略；就绪的判断是否由之前的残留，需要进行清理对象；
	// 2.2. 选中的集群：未就绪的上报状态到 Status；就绪执行对象的更新（不存在则 Create，存在则 Update）；
	// 2.3.1. Create 中包含了从 fed resource 中获取 template，填充 override，然后是 Add 相关的错误判断，最后还会走一遍 Update 的流程确保 fed 接管的 label 存在
	// 2.3.2. Update 中包含了对于标记接管的 label 的判断，然后是依照 Retain（dispatch/retain）方式更新关联的 resource
	//（目前有 Service、Service Account、Replicas 相关，这块在 retain 部分详细分析），然后再填充一遍 override，在进行 version 更新
	// 3. 以上所有操作会有一个超时时间（目前是配置在 `newOperationDispatcher` 中的 30s），超时也会被认为失败记录事件
	// 4. 更新记录 fed resource status 到 host cluster：
	// 要么更新 status 成功则一次完成，否则会进行 interval 1s timeout 5s 的重试，超时仍认为更新 Status 失败
	// * 其他一些原因也会触发 Status 更新到 host cluster （setFederatedStatus 的调用位置，具体见传入的 reason ）
	// * 最终 status 更新的流程见 status.SetFederatedStatus
	return s.syncToClusters(fedResource)
}

// syncToClusters ensures that the state of the given object is
// synchronized to member clusters.
func (s *KubeFedSyncController) syncToClusters(fedResource FederatedResource) util.ReconciliationStatus {
	clusters, err := s.informer.GetClusters()
	if err != nil {
		fedResource.RecordError(string(status.ClusterRetrievalFailed), errors.Wrap(err, "Failed to retrieve list of clusters"))
		return s.setFederatedStatus(fedResource, status.ClusterRetrievalFailed, nil)
	}

	selectedClusterNames, err := fedResource.ComputePlacement(clusters)
	if err != nil {
		fedResource.RecordError(string(status.ComputePlacementFailed), errors.Wrap(err, "Failed to compute placement"))
		return s.setFederatedStatus(fedResource, status.ComputePlacementFailed, nil)
	}

	kind := fedResource.TargetKind()
	key := fedResource.TargetName().String()
	klog.V(4).Infof("Ensuring %s %q in clusters: %s", kind, key, strings.Join(selectedClusterNames.List(), ","))

	dispatcher := dispatch.NewManagedDispatcher(s.informer.GetClientForCluster, fedResource, s.skipAdoptingResources)

	for _, cluster := range clusters {
		clusterName := cluster.Name
		selectedCluster := selectedClusterNames.Has(clusterName)

		if !util.IsClusterReady(&cluster.Status) {
			if selectedCluster {
				// Cluster state only needs to be reported in resource
				// status for clusters selected for placement.
				err := errors.New("Cluster not ready")
				dispatcher.RecordClusterError(status.ClusterNotReady, clusterName, err)
			}
			continue
		}

		rawClusterObj, _, err := s.informer.GetTargetStore().GetByKey(clusterName, key)
		if err != nil {
			wrappedErr := errors.Wrap(err, "Failed to retrieve cached cluster object")
			dispatcher.RecordClusterError(status.CachedRetrievalFailed, clusterName, wrappedErr)
			continue
		}

		var clusterObj *unstructured.Unstructured
		if rawClusterObj != nil {
			clusterObj = rawClusterObj.(*unstructured.Unstructured)
		}

		// Resource should not exist in the named cluster
		if !selectedCluster {
			if clusterObj == nil {
				// Resource does not exist in the cluster
				continue
			}
			if clusterObj.GetDeletionTimestamp() != nil {
				// Resource is marked for deletion
				dispatcher.RecordStatus(clusterName, status.WaitingForRemoval)
				continue
			}
			if fedResource.IsNamespaceInHostCluster(clusterObj) {
				// Host cluster namespace needs to have the managed
				// label removed so it won't be cached anymore.
				dispatcher.RemoveManagedLabel(clusterName, clusterObj)
			} else {
				dispatcher.Delete(clusterName)
			}
			continue
		}

		// Resource should appear in the named cluster

		// TODO(marun) Consider waiting until the result of resource
		// creation has reached the target store before attempting
		// subsequent operations.  Otherwise the object won't be found
		// but an add operation will fail with AlreadyExists.
		if clusterObj == nil {
			dispatcher.Create(clusterName)
		} else {
			dispatcher.Update(clusterName, clusterObj)
		}
	}
	_, timeoutErr := dispatcher.Wait()
	if timeoutErr != nil {
		fedResource.RecordError("OperationTimeoutError", timeoutErr)
	}

	// Write updated versions to the API.
	updatedVersionMap := dispatcher.VersionMap()
	err = fedResource.UpdateVersions(selectedClusterNames.List(), updatedVersionMap)
	if err != nil {
		// Versioning of federated resources is an optimization to
		// avoid unnecessary updates, and failure to record version
		// information does not indicate a failure of propagation.
		runtime.HandleError(err)
	}

	collectedStatus := dispatcher.CollectedStatus()
	return s.setFederatedStatus(fedResource, status.AggregateSuccess, &collectedStatus)
}

func (s *KubeFedSyncController) setFederatedStatus(fedResource FederatedResource,
	reason status.AggregateReason, collectedStatus *status.CollectedPropagationStatus) util.ReconciliationStatus {
	if collectedStatus == nil {
		collectedStatus = &status.CollectedPropagationStatus{}
	}

	kind := fedResource.FederatedKind()
	name := fedResource.FederatedName()
	obj := fedResource.Object()

	// Only a single reason for propagation failure is reported at any one time, so only report
	// NamespaceNotFederated if no other explicit error has been indicated.
	if reason == status.AggregateSuccess {
		// For a cluster-scoped control plane, report when the containing namespace of a federated
		// resource is not federated.  The KubeFed system namespace is implicitly federated in a
		// namespace-scoped control plane.
		if !s.limitedScope && fedResource.NamespaceNotFederated() {
			reason = status.NamespaceNotFederated
		}
	}

	// If the underlying resource has changed, attempt to retrieve and
	// update it repeatedly.
	err := wait.PollImmediate(1*time.Second, 5*time.Second, func() (bool, error) {
		if updateRequired, err := status.SetFederatedStatus(obj, reason, *collectedStatus); err != nil {
			return false, errors.Wrapf(err, "failed to set the status")
		} else if !updateRequired {
			klog.V(4).Infof("No status update necessary for %s %q", kind, name)
			return true, nil
		}

		err := s.hostClusterClient.UpdateStatus(context.TODO(), obj)
		if err == nil {
			return true, nil
		}
		if apierrors.IsConflict(err) {
			klog.V(2).Infof("Failed to set propagation status for %s %q due to conflict (will retry): %v.", kind, name, err)
			err := s.hostClusterClient.Get(context.TODO(), obj, obj.GetNamespace(), obj.GetName())
			if err != nil {
				return false, errors.Wrapf(err, "failed to retrieve resource")
			}
			return false, nil
		}
		return false, errors.Wrapf(err, "failed to update resource")
	})
	if err != nil {
		runtime.HandleError(errors.Wrapf(err, "failed to set propagation status for %s %q", kind, name))
		return util.StatusError
	}

	return util.StatusAllOK
}

func (s *KubeFedSyncController) ensureDeletion(fedResource FederatedResource) util.ReconciliationStatus {
	fedResource.DeleteVersions()

	key := fedResource.FederatedName().String()
	kind := fedResource.FederatedKind()

	klog.V(2).Infof("Ensuring deletion of %s %q", kind, key)

	obj := fedResource.Object()

	finalizers := sets.NewString(obj.GetFinalizers()...)
	if !finalizers.Has(FinalizerSyncController) {
		klog.V(2).Infof("%s %q does not have the %q finalizer. Nothing to do.", kind, key, FinalizerSyncController)
		return util.StatusAllOK
	}

	if util.IsOrphaningEnabled(obj) {
		klog.V(2).Infof("Found %q annotation on %s %q. Removing the finalizer.",
			util.OrphanManagedResourcesAnnotation, kind, key)
		err := s.removeFinalizer(fedResource)
		if err != nil {
			wrappedErr := errors.Wrapf(err, "failed to remove finalizer %q from %s %q", FinalizerSyncController, kind, key)
			runtime.HandleError(wrappedErr)
			return util.StatusError
		}
		klog.V(2).Infof("Initiating the removal of the label %q from resources previously managed by %s %q.", util.ManagedByKubeFedLabelKey, kind, key)
		err = s.removeManagedLabel(fedResource.TargetGVK(), fedResource.TargetName())
		if err != nil {
			wrappedErr := errors.Wrapf(err, "failed to remove the label %q from all resources previously managed by %s %q", util.ManagedByKubeFedLabelKey, kind, key)
			runtime.HandleError(wrappedErr)
			return util.StatusError
		}
		return util.StatusAllOK
	}

	klog.V(2).Infof("Deleting resources managed by %s %q from member clusters.", kind, key)
	recheckRequired, err := s.deleteFromClusters(fedResource)
	if err != nil {
		wrappedErr := errors.Wrapf(err, "failed to delete %s %q", kind, key)
		runtime.HandleError(wrappedErr)
		return util.StatusError
	}
	if recheckRequired {
		return util.StatusNeedsRecheck
	}
	return util.StatusAllOK
}

// removeManagedLabel attempts to remove the managed label from
// resources with the given name in member clusters.
func (s *KubeFedSyncController) removeManagedLabel(gvk schema.GroupVersionKind, qualifiedName util.QualifiedName) error {
	// Read-Note: 此处 handleDeletionInClusters 只是套了一个多集群的便利、集群就绪状态验证、目标资源的构造，其实本质还是执行 deletion func
	// 而 deletion func 只是对于单集群的对象进行判断，如果被置了 deletion timestamp（标记删除）则移除 kubefed 接管 obj 的 label `handleDeletionInClusters==true`
	ok, err := s.handleDeletionInClusters(gvk, qualifiedName, func(dispatcher dispatch.UnmanagedDispatcher, clusterName string, clusterObj *unstructured.Unstructured) {
		if clusterObj.GetDeletionTimestamp() != nil {
			return
		}

		dispatcher.RemoveManagedLabel(clusterName, clusterObj)
	})
	if err != nil {
		return err
	}
	if !ok {
		return errors.Errorf("failed to remove the label from resources in one or more clusters.")
	}
	return nil
}

func (s *KubeFedSyncController) deleteFromClusters(fedResource FederatedResource) (bool, error) {
	gvk := fedResource.TargetGVK()
	qualifiedName := fedResource.TargetName()

	remainingClusters := []string{}
	ok, err := s.handleDeletionInClusters(gvk, qualifiedName, func(dispatcher dispatch.UnmanagedDispatcher, clusterName string, clusterObj *unstructured.Unstructured) {
		// If the containing namespace of a FederatedNamespace is
		// marked for deletion, it is impossible to require the
		// removal of the namespace in advance of removal of the sync
		// controller finalizer.  Return immediately and avoid
		// including the cluster in the list of remaining clusters.
		if fedResource.IsNamespaceInHostCluster(clusterObj) && clusterObj.GetDeletionTimestamp() != nil {
			return
		}

		remainingClusters = append(remainingClusters, clusterName)

		// Avoid attempting any operation on a deleted resource.
		if clusterObj.GetDeletionTimestamp() != nil {
			return
		}

		if fedResource.IsNamespaceInHostCluster(clusterObj) {
			// Creation or deletion of namespaces in the host cluster
			// is not the responsibility of the sync controller.
			// Removing the managed label will ensure a host cluster
			// namespace is no longer cached.
			dispatcher.RemoveManagedLabel(clusterName, clusterObj)
		} else {
			dispatcher.Delete(clusterName)
		}
	})
	if err != nil {
		return false, err
	}
	if !ok {
		return false, errors.Errorf("failed to remove managed resources from one or more clusters.")
	}
	if len(remainingClusters) > 0 {
		fedKind := fedResource.FederatedKind()
		fedName := fedResource.FederatedName()
		klog.V(2).Infof("Waiting for resources managed by %s %q to be removed from the following clusters: %s", fedKind, fedName, strings.Join(remainingClusters, ", "))
		return true, nil
	}
	err = s.ensureRemovedOrUnmanaged(fedResource)
	if err != nil {
		return false, errors.Wrapf(err, "failed to verify that managed resources no longer exist in any cluster")
	}
	// Managed resources no longer exist in any member cluster
	return false, s.removeFinalizer(fedResource)
}

// ensureRemovedOrUnmanaged ensures that no resources in member
// clusters that could be managed by the given federated resources are
// present or labeled as managed.  The checks are performed without
// the informer to cover the possibility that the resources have not
// yet been cached.
func (s *KubeFedSyncController) ensureRemovedOrUnmanaged(fedResource FederatedResource) error {
	clusters, err := s.informer.GetClusters()
	if err != nil {
		return errors.Wrap(err, "failed to get a list of clusters")
	}

	dispatcher := dispatch.NewCheckUnmanagedDispatcher(s.informer.GetClientForCluster, fedResource.TargetGVK(), fedResource.TargetName())
	unreadyClusters := []string{}
	for _, cluster := range clusters {
		if !util.IsClusterReady(&cluster.Status) {
			unreadyClusters = append(unreadyClusters, cluster.Name)
			continue
		}
		dispatcher.CheckRemovedOrUnlabeled(cluster.Name, fedResource.IsNamespaceInHostCluster)
	}
	ok, timeoutErr := dispatcher.Wait()
	if timeoutErr != nil {
		return timeoutErr
	}
	if len(unreadyClusters) > 0 {
		return errors.Errorf("the following clusters were not ready: %s", strings.Join(unreadyClusters, ", "))
	}
	if !ok {
		return errors.Errorf("one or more checks failed")
	}
	return nil
}

// handleDeletionInClusters invokes the provided deletion handler for
// each managed resource in member clusters.
func (s *KubeFedSyncController) handleDeletionInClusters(gvk schema.GroupVersionKind, qualifiedName util.QualifiedName,
	deletionFunc func(dispatcher dispatch.UnmanagedDispatcher, clusterName string, clusterObj *unstructured.Unstructured)) (bool, error) {
	clusters, err := s.informer.GetClusters()
	if err != nil {
		return false, errors.Wrap(err, "failed to get a list of clusters")
	}

	dispatcher := dispatch.NewUnmanagedDispatcher(s.informer.GetClientForCluster, gvk, qualifiedName)
	retrievalFailureClusters := []string{}
	unreadyClusters := []string{}
	for _, cluster := range clusters {
		clusterName := cluster.Name

		if !util.IsClusterReady(&cluster.Status) {
			unreadyClusters = append(unreadyClusters, clusterName)
			continue
		}

		key := util.QualifiedNameForCluster(clusterName, qualifiedName).String()
		rawClusterObj, _, err := s.informer.GetTargetStore().GetByKey(clusterName, key)
		if err != nil {
			wrappedErr := errors.Wrapf(err, "failed to retrieve %s %q for cluster %q", gvk.Kind, key, clusterName)
			runtime.HandleError(wrappedErr)
			retrievalFailureClusters = append(retrievalFailureClusters, clusterName)
			continue
		}
		if rawClusterObj == nil {
			continue
		}
		clusterObj := rawClusterObj.(*unstructured.Unstructured)
		deletionFunc(dispatcher, clusterName, clusterObj)
	}
	ok, timeoutErr := dispatcher.Wait()
	if timeoutErr != nil {
		return false, timeoutErr
	}
	if len(retrievalFailureClusters) > 0 {
		return false, errors.Errorf("failed to retrieve a managed resource for the following cluster(s): %s", strings.Join(retrievalFailureClusters, ", "))
	}
	if len(unreadyClusters) > 0 {
		return false, errors.Errorf("the following clusters were not ready: %s", strings.Join(unreadyClusters, ", "))
	}
	return ok, nil
}

func (s *KubeFedSyncController) ensureFinalizer(fedResource FederatedResource) error {
	obj := fedResource.Object()
	isUpdated, err := finalizersutil.AddFinalizers(obj, sets.NewString(FinalizerSyncController))
	if err != nil || !isUpdated {
		return err
	}
	klog.V(2).Infof("Adding finalizer %s to %s %q", FinalizerSyncController, fedResource.FederatedKind(), fedResource.FederatedName())
	return s.hostClusterClient.Update(context.TODO(), obj)
}

func (s *KubeFedSyncController) removeFinalizer(fedResource FederatedResource) error {
	obj := fedResource.Object()
	isUpdated, err := finalizersutil.RemoveFinalizers(obj, sets.NewString(FinalizerSyncController))
	if err != nil || !isUpdated {
		return err
	}
	klog.V(2).Infof("Removing finalizer %s from %s %q", FinalizerSyncController, fedResource.FederatedKind(), fedResource.FederatedName())
	return s.hostClusterClient.Update(context.TODO(), obj)
}
