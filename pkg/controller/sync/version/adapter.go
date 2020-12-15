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

package version

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	pkgruntime "k8s.io/apimachinery/pkg/runtime"

	fedv1a1 "sigs.k8s.io/kubefed/pkg/apis/core/v1alpha1"
	"sigs.k8s.io/kubefed/pkg/controller/util"
)

type VersionAdapter interface {
	TypeName() string

	// Create an empty instance of the version type
	NewObject() pkgruntime.Object
	// Create an empty instance of list version type
	NewListObject() pkgruntime.Object
	// Create a populated instance of the version type
	NewVersion(qualifiedName util.QualifiedName, ownerReference metav1.OwnerReference, status *fedv1a1.PropagatedVersionStatus) pkgruntime.Object

	// Type-agnostic access / mutation of the Status field of a version resource
	GetStatus(obj pkgruntime.Object) *fedv1a1.PropagatedVersionStatus
	SetStatus(obj pkgruntime.Object, status *fedv1a1.PropagatedVersionStatus)
}


// Read-Note: Adapter 没有复杂逻辑，只是针对是否 NS 相关，在生成 Version 的时候决定带不带上 NS
// 但从上层的调用来看有点多余，反正 cluster version 本身就是空 NS，区不区分其实都一样，不清楚是否在为后续实现做预留
func NewVersionAdapter(namespaced bool) VersionAdapter {
	if namespaced {
		return &namespacedVersionAdapter{}
	}
	return &clusterVersionAdapter{}
}
