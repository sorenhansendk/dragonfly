//go:build !ignore_autogenerated
// +build !ignore_autogenerated

// Copyright 2022, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

// Code generated by controller-gen. DO NOT EDIT.

package v1alpha1

import (
	runtime "k8s.io/apimachinery/pkg/runtime"
)

// DeepCopyInto is an autogenerated deepcopy function, copying the receiver, writing into out. in must be non-nil.
func (in *DragonflyDb) DeepCopyInto(out *DragonflyDb) {
	*out = *in
	out.TypeMeta = in.TypeMeta
	in.ObjectMeta.DeepCopyInto(&out.ObjectMeta)
	out.Spec = in.Spec
	out.Status = in.Status
}

// DeepCopy is an autogenerated deepcopy function, copying the receiver, creating a new DragonflyDb.
func (in *DragonflyDb) DeepCopy() *DragonflyDb {
	if in == nil {
		return nil
	}
	out := new(DragonflyDb)
	in.DeepCopyInto(out)
	return out
}

// DeepCopyObject is an autogenerated deepcopy function, copying the receiver, creating a new runtime.Object.
func (in *DragonflyDb) DeepCopyObject() runtime.Object {
	if c := in.DeepCopy(); c != nil {
		return c
	}
	return nil
}

// DeepCopyInto is an autogenerated deepcopy function, copying the receiver, writing into out. in must be non-nil.
func (in *DragonflyDbList) DeepCopyInto(out *DragonflyDbList) {
	*out = *in
	out.TypeMeta = in.TypeMeta
	in.ListMeta.DeepCopyInto(&out.ListMeta)
	if in.Items != nil {
		in, out := &in.Items, &out.Items
		*out = make([]DragonflyDb, len(*in))
		for i := range *in {
			(*in)[i].DeepCopyInto(&(*out)[i])
		}
	}
}

// DeepCopy is an autogenerated deepcopy function, copying the receiver, creating a new DragonflyDbList.
func (in *DragonflyDbList) DeepCopy() *DragonflyDbList {
	if in == nil {
		return nil
	}
	out := new(DragonflyDbList)
	in.DeepCopyInto(out)
	return out
}

// DeepCopyObject is an autogenerated deepcopy function, copying the receiver, creating a new runtime.Object.
func (in *DragonflyDbList) DeepCopyObject() runtime.Object {
	if c := in.DeepCopy(); c != nil {
		return c
	}
	return nil
}

// DeepCopyInto is an autogenerated deepcopy function, copying the receiver, writing into out. in must be non-nil.
func (in *DragonflyDbSpec) DeepCopyInto(out *DragonflyDbSpec) {
	*out = *in
}

// DeepCopy is an autogenerated deepcopy function, copying the receiver, creating a new DragonflyDbSpec.
func (in *DragonflyDbSpec) DeepCopy() *DragonflyDbSpec {
	if in == nil {
		return nil
	}
	out := new(DragonflyDbSpec)
	in.DeepCopyInto(out)
	return out
}

// DeepCopyInto is an autogenerated deepcopy function, copying the receiver, writing into out. in must be non-nil.
func (in *DragonflyDbStatus) DeepCopyInto(out *DragonflyDbStatus) {
	*out = *in
}

// DeepCopy is an autogenerated deepcopy function, copying the receiver, creating a new DragonflyDbStatus.
func (in *DragonflyDbStatus) DeepCopy() *DragonflyDbStatus {
	if in == nil {
		return nil
	}
	out := new(DragonflyDbStatus)
	in.DeepCopyInto(out)
	return out
}
