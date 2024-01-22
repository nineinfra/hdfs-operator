/*
Copyright 2024 nineinfra.

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

package v1

import (
	"k8s.io/apimachinery/pkg/runtime"
	ctrl "sigs.k8s.io/controller-runtime"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/webhook"
	"sigs.k8s.io/controller-runtime/pkg/webhook/admission"
)

// log is for logging in this package.
var hdfsclusterlog = logf.Log.WithName("hdfscluster-resource")

func (r *HdfsCluster) SetupWebhookWithManager(mgr ctrl.Manager) error {
	return ctrl.NewWebhookManagedBy(mgr).
		For(r).
		Complete()
}

// TODO(user): EDIT THIS FILE!  THIS IS SCAFFOLDING FOR YOU TO OWN!

//+kubebuilder:webhook:path=/mutate-hdfs-nineinfra-tech-v1-hdfscluster,mutating=true,failurePolicy=fail,sideEffects=None,groups=hdfs.nineinfra.tech,resources=hdfsclusters,verbs=create;update,versions=v1,name=mhdfscluster.kb.io,admissionReviewVersions=v1

var _ webhook.Defaulter = &HdfsCluster{}

// Default implements webhook.Defaulter so a webhook will be registered for the type
func (r *HdfsCluster) Default() {
	hdfsclusterlog.Info("default", "name", r.Name)

	// TODO(user): fill in your defaulting logic.
}

// TODO(user): change verbs to "verbs=create;update;delete" if you want to enable deletion validation.
//+kubebuilder:webhook:path=/validate-hdfs-nineinfra-tech-v1-hdfscluster,mutating=false,failurePolicy=fail,sideEffects=None,groups=hdfs.nineinfra.tech,resources=hdfsclusters,verbs=create;update,versions=v1,name=vhdfscluster.kb.io,admissionReviewVersions=v1

var _ webhook.Validator = &HdfsCluster{}

// ValidateCreate implements webhook.Validator so a webhook will be registered for the type
func (r *HdfsCluster) ValidateCreate() (admission.Warnings, error) {
	hdfsclusterlog.Info("validate create", "name", r.Name)

	// TODO(user): fill in your validation logic upon object creation.
	return nil, nil
}

// ValidateUpdate implements webhook.Validator so a webhook will be registered for the type
func (r *HdfsCluster) ValidateUpdate(old runtime.Object) (admission.Warnings, error) {
	hdfsclusterlog.Info("validate update", "name", r.Name)

	// TODO(user): fill in your validation logic upon object update.
	return nil, nil
}

// ValidateDelete implements webhook.Validator so a webhook will be registered for the type
func (r *HdfsCluster) ValidateDelete() (admission.Warnings, error) {
	hdfsclusterlog.Info("validate delete", "name", r.Name)

	// TODO(user): fill in your validation logic upon object deletion.
	return nil, nil
}
