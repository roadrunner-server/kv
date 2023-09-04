package kv

import (
	"context"

	kvv1 "github.com/roadrunner-server/api/v4/build/kv/v1"
	"github.com/roadrunner-server/api/v4/plugins/v1/kv"
	"github.com/roadrunner-server/errors"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
)

const (
	tracerName = "kv"
)

// Wrapper for the plugin
type rpc struct {
	// all available storages
	storages map[string]kv.Storage
	// tracer
	tracer *sdktrace.TracerProvider
}

// Has accepts []*kvv1.Payload proto payload with Storage and Item
func (r *rpc) Has(in *kvv1.Request, out *kvv1.Response) error {
	const op = errors.Op("rpc_has")

	_, span := r.tracer.Tracer(tracerName).Start(context.Background(), "kv:has")
	defer span.End()

	if in.GetStorage() == "" {
		span.RecordError(errors.Str("no storage provided"))
		return errors.E(op, errors.Str("no storage provided"))
	}

	if _, ok := r.storages[in.GetStorage()]; !ok {
		span.RecordError(errors.Errorf("no such storage: %s", in.GetStorage()))
		return errors.E(op, errors.Errorf("no such storage: %s", in.GetStorage()))
	}

	keys := make([]string, 0, len(in.GetItems()))

	for i := 0; i < len(in.GetItems()); i++ {
		keys = append(keys, in.Items[i].Key)
	}

	ret, err := r.storages[in.GetStorage()].Has(keys...)
	if err != nil {
		span.RecordError(err)
		return errors.E(op, err)
	}

	// update the value in the pointer
	// save the result
	out.Items = make([]*kvv1.Item, 0, len(ret))
	for k := range ret {
		out.Items = append(out.Items, &kvv1.Item{
			Key: k,
		})
	}

	return nil
}

// Set accept proto payload with Storage and Item
func (r *rpc) Set(in *kvv1.Request, _ *kvv1.Response) error {
	const op = errors.Op("rpc_set")

	_, span := r.tracer.Tracer(tracerName).Start(context.Background(), "kv:set")
	defer span.End()

	if _, ok := r.storages[in.GetStorage()]; !ok {
		span.RecordError(errors.Errorf("no such storage: %s", in.GetStorage()))
		return errors.E(op, errors.Errorf("no such storage: %s", in.GetStorage()))
	}

	items := in.GetItems()
	for i := 0; i < len(items); i++ {
		err := r.storages[in.GetStorage()].Set(from(items[i]))
		if err != nil {
			span.RecordError(err)
			return errors.E(op, err)
		}
	}

	// save the result
	return nil
}

// MGet accept proto payload with Storage and Item
func (r *rpc) MGet(in *kvv1.Request, out *kvv1.Response) error { //nolint:dupl
	const op = errors.Op("rpc_mget")

	_, span := r.tracer.Tracer(tracerName).Start(context.Background(), "kv:mget")
	defer span.End()

	if in.GetStorage() == "" {
		span.RecordError(errors.Str("no storage provided"))
		return errors.E(op, errors.Str("no storage provided"))
	}

	if _, ok := r.storages[in.GetStorage()]; !ok {
		span.RecordError(errors.Errorf("no such storage: %s", in.GetStorage()))
		return errors.E(op, errors.Errorf("no such storage: %s", in.GetStorage()))
	}

	keys := make([]string, 0, len(in.GetItems()))

	for i := 0; i < len(in.GetItems()); i++ {
		keys = append(keys, in.Items[i].Key)
	}

	ret, err := r.storages[in.GetStorage()].MGet(keys...)
	if err != nil {
		span.RecordError(err)
		return errors.E(op, err)
	}

	out.Items = make([]*kvv1.Item, 0, len(ret))
	for k := range ret {
		out.Items = append(out.Items, &kvv1.Item{
			Key:   k,
			Value: ret[k],
		})
	}

	return nil
}

// MExpire accept proto payload with Storage and Item
func (r *rpc) MExpire(in *kvv1.Request, _ *kvv1.Response) error {
	const op = errors.Op("rpc_mexpire")

	_, span := r.tracer.Tracer(tracerName).Start(context.Background(), "kv:mexpire")
	defer span.End()

	if in.GetStorage() == "" {
		span.RecordError(errors.Str("no storage provided"))
		return errors.E(op, errors.Str("no storage provided"))
	}

	if _, ok := r.storages[in.GetStorage()]; !ok {
		span.RecordError(errors.Errorf("no such storage: %s", in.GetStorage()))
		return errors.E(op, errors.Errorf("no such storage: %s", in.GetStorage()))
	}

	items := in.GetItems()
	for i := 0; i < len(items); i++ {
		err := r.storages[in.GetStorage()].MExpire(from(items[i]))
		if err != nil {
			span.RecordError(err)
			return errors.E(op, err)
		}
	}

	return nil
}

// TTL accept proto payload with Storage and Item
func (r *rpc) TTL(in *kvv1.Request, out *kvv1.Response) error { //nolint:dupl
	const op = errors.Op("rpc_ttl")

	_, span := r.tracer.Tracer(tracerName).Start(context.Background(), "kv:ttl")
	defer span.End()

	if in.GetStorage() == "" {
		span.RecordError(errors.Str("no storage provided"))
		return errors.E(op, errors.Str("no storage provided"))
	}

	if _, ok := r.storages[in.GetStorage()]; !ok {
		span.RecordError(errors.Errorf("no such storage: %s", in.GetStorage()))
		return errors.E(op, errors.Errorf("no such storage: %s", in.GetStorage()))
	}

	keys := make([]string, 0, len(in.GetItems()))

	for i := 0; i < len(in.GetItems()); i++ {
		keys = append(keys, in.Items[i].Key)
	}

	ret, err := r.storages[in.GetStorage()].TTL(keys...)
	if err != nil {
		span.RecordError(err)
		return errors.E(op, err)
	}

	out.Items = make([]*kvv1.Item, 0, len(ret))
	for k := range ret {
		out.Items = append(out.Items, &kvv1.Item{
			Key:     k,
			Timeout: ret[k],
		})
	}

	return nil
}

// Delete accept proto payload with Storage and Item
func (r *rpc) Delete(in *kvv1.Request, _ *kvv1.Response) error {
	const op = errors.Op("rcp_delete")

	_, span := r.tracer.Tracer(tracerName).Start(context.Background(), "kv:delete")
	defer span.End()

	if in.GetStorage() == "" {
		span.RecordError(errors.Str("no storage provided"))
		return errors.E(op, errors.Str("no storage provided"))
	}

	if _, ok := r.storages[in.GetStorage()]; !ok {
		span.RecordError(errors.Errorf("no such storage: %s", in.GetStorage()))
		return errors.E(op, errors.Errorf("no such storage: %s", in.GetStorage()))
	}

	keys := make([]string, 0, len(in.GetItems()))

	for i := 0; i < len(in.GetItems()); i++ {
		keys = append(keys, in.Items[i].Key)
	}

	err := r.storages[in.GetStorage()].Delete(keys...)
	if err != nil {
		span.RecordError(err)
		return errors.E(op, err)
	}

	return nil
}

// Clear clean the storage
func (r *rpc) Clear(in *kvv1.Request, _ *kvv1.Response) error {
	const op = errors.Op("rcp_delete")

	_, span := r.tracer.Tracer(tracerName).Start(context.Background(), "kv:clear")
	defer span.End()

	if in.GetStorage() == "" {
		span.RecordError(errors.Str("no storage provided"))
		return errors.E(op, errors.Str("no storage provided"))
	}

	if _, ok := r.storages[in.GetStorage()]; !ok {
		span.RecordError(errors.Errorf("no such storage: %s", in.GetStorage()))
		return errors.E(op, errors.Errorf("no such storage: %s", in.GetStorage()))
	}

	err := r.storages[in.GetStorage()].Clear()
	if err != nil {
		span.RecordError(err)
		return errors.E(op, err)
	}

	return nil
}

func from(tr *kvv1.Item) *Item {
	return &Item{
		key:     tr.GetKey(),
		val:     tr.GetValue(),
		timeout: tr.GetTimeout(),
	}
}
