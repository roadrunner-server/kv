package kv

import (
	"context"

	kvv1 "github.com/roadrunner-server/api/v4/build/kv/v1"
	"github.com/roadrunner-server/api/v4/plugins/v1/kv"
	"github.com/roadrunner-server/errors"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"
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

	storage, e := getStorage(in, span, r, op)
	if e != nil {
		return e
	}

	ret, err := storage.Has(composeKeys(in)...)
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

// Set accepts proto payload with Storage and Item
func (r *rpc) Set(in *kvv1.Request, _ *kvv1.Response) error {
	const op = errors.Op("rpc_set")

	_, span := r.tracer.Tracer(tracerName).Start(context.Background(), "kv:set")
	defer span.End()

	storage, e := getStorage(in, span, r, op)
	if e != nil {
		return e
	}

	err := storage.Set(from(in.GetItems())...)
	if err != nil {
		span.RecordError(err)
		return errors.E(op, err)
	}

	return nil
}

// MGet accept proto payload with Storage and Item
func (r *rpc) MGet(in *kvv1.Request, out *kvv1.Response) error { //nolint:dupl
	const op = errors.Op("rpc_mget")

	_, span := r.tracer.Tracer(tracerName).Start(context.Background(), "kv:mget")
	defer span.End()

	storage, e := getStorage(in, span, r, op)
	if e != nil {
		return e
	}

	ret, err := storage.MGet(composeKeys(in)...)
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

	storage, e := getStorage(in, span, r, op)
	if e != nil {
		return e
	}

	err := storage.MExpire(from(in.GetItems())...)
	if err != nil {
		span.RecordError(err)
		return errors.E(op, err)
	}

	return nil
}

// TTL accept proto payload with Storage and Item
func (r *rpc) TTL(in *kvv1.Request, out *kvv1.Response) error { //nolint:dupl
	const op = errors.Op("rpc_ttl")

	_, span := r.tracer.Tracer(tracerName).Start(context.Background(), "kv:ttl")
	defer span.End()

	storage, e := getStorage(in, span, r, op)

	if e != nil {
		return e
	}

	ret, err := storage.TTL(composeKeys(in)...)
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
	const op = errors.Op("rpc_delete")

	_, span := r.tracer.Tracer(tracerName).Start(context.Background(), "kv:delete")
	defer span.End()

	storage, e := getStorage(in, span, r, op)

	if e != nil {
		return e
	}

	err := storage.Delete(composeKeys(in)...)
	if err != nil {
		span.RecordError(err)
		return errors.E(op, err)
	}

	return nil
}

// Clear clean the storage
func (r *rpc) Clear(in *kvv1.Request, _ *kvv1.Response) error {
	const op = errors.Op("rpc_clear")

	_, span := r.tracer.Tracer(tracerName).Start(context.Background(), "kv:clear")
	defer span.End()

	storage, e := getStorage(in, span, r, op)

	if e != nil {
		return e
	}

	err := storage.Clear()
	if err != nil {
		span.RecordError(err)
		return errors.E(op, err)
	}

	return nil
}

func getStorage(in *kvv1.Request, span trace.Span, r *rpc, op errors.Op) (kv.Storage, error) {
	if in.GetStorage() == "" {
		e := errors.Str("no storage provided")
		span.RecordError(e)
		return nil, errors.E(op, e)
	}

	storage, ok := r.storages[in.GetStorage()]

	if !ok {
		e := errors.Errorf("no such storage: %s", in.GetStorage())
		span.RecordError(e)
		return nil, errors.E(op, e)
	}

	return storage, nil
}

func composeKeys(in *kvv1.Request) []string {
	ln := len(in.GetItems())
	keys := make([]string, 0, ln)

	for i := 0; i < ln; i++ {
		keys = append(keys, in.Items[i].Key)
	}

	return keys
}

func from(tr []*kvv1.Item) []kv.Item {
	items := make([]kv.Item, 0, len(tr))
	for i := range tr {
		items = append(items, &Item{
			key:     tr[i].GetKey(),
			val:     tr[i].GetValue(),
			timeout: tr[i].GetTimeout(),
		})
	}

	return items
}
