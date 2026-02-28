package kv

import (
	"context"
	"time"

	kvv2 "github.com/roadrunner-server/api-go/v5/kv/v2"
	"github.com/roadrunner-server/api-plugins/v5/kv"
	"github.com/roadrunner-server/errors"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"google.golang.org/protobuf/types/known/durationpb"
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

// Has accepts []*kvv2.Payload proto payload with Storage and Item
func (r *rpc) Has(in *kvv2.KvRequest, out *kvv2.KvResponse) error {
	const op = errors.Op("rpc_has")

	ctx, span := r.tracer.Tracer(tracerName).Start(context.Background(), "kv:has")
	defer span.End()

	if in.GetStorage() == "" {
		err := errors.Str("no storage provided")
		span.RecordError(err)
		return errors.E(op, err)
	}

	st, ok := r.storages[in.GetStorage()]
	if !ok {
		err := errors.Errorf("no such storage: %s", in.GetStorage())
		span.RecordError(err)
		return errors.E(op, err)
	}

	keys := make([]string, 0, len(in.GetItems()))

	for i := range in.GetItems() {
		keys = append(keys, in.Items[i].Key)
	}

	ret, err := st.Has(ctx, keys...)
	if err != nil {
		span.RecordError(err)
		return errors.E(op, err)
	}

	// update the value in the pointer
	// save the result
	out.Items = make([]*kvv2.KvItem, 0, len(ret))
	for k := range ret {
		out.Items = append(out.Items, &kvv2.KvItem{
			Key: k,
		})
	}

	return nil
}

// Set accepts proto payload with Storage and Item
func (r *rpc) Set(in *kvv2.KvRequest, _ *kvv2.KvResponse) error {
	const op = errors.Op("rpc_set")

	ctx, span := r.tracer.Tracer(tracerName).Start(context.Background(), "kv:set")
	defer span.End()

	if in.GetStorage() == "" {
		err := errors.Str("no storage provided")
		span.RecordError(err)
		return errors.E(op, err)
	}

	st, ok := r.storages[in.GetStorage()]
	if !ok {
		err := errors.Errorf("no such storage: %s", in.GetStorage())
		span.RecordError(err)
		return errors.E(op, err)
	}

	err := st.Set(ctx, from(in.GetItems())...)
	if err != nil {
		span.RecordError(err)
		return errors.E(op, err)
	}

	return nil
}

// MGet accept proto payload with Storage and Item
func (r *rpc) MGet(in *kvv2.KvRequest, out *kvv2.KvResponse) error { //nolint:dupl
	const op = errors.Op("rpc_mget")

	ctx, span := r.tracer.Tracer(tracerName).Start(context.Background(), "kv:mget")
	defer span.End()

	if in.GetStorage() == "" {
		err := errors.Str("no storage provided")
		span.RecordError(err)
		return errors.E(op, err)
	}

	st, ok := r.storages[in.GetStorage()]
	if !ok {
		err := errors.Errorf("no such storage: %s", in.GetStorage())
		span.RecordError(err)
		return errors.E(op, err)
	}

	keys := make([]string, 0, len(in.GetItems()))

	for i := range in.GetItems() {
		keys = append(keys, in.Items[i].Key)
	}

	ret, err := st.MGet(ctx, keys...)
	if err != nil {
		span.RecordError(err)
		return errors.E(op, err)
	}

	out.Items = make([]*kvv2.KvItem, 0, len(ret))
	for k := range ret {
		out.Items = append(out.Items, &kvv2.KvItem{
			Key:   k,
			Value: ret[k],
		})
	}

	return nil
}

// MExpire accept proto payload with Storage and Item
func (r *rpc) MExpire(in *kvv2.KvRequest, _ *kvv2.KvResponse) error {
	const op = errors.Op("rpc_mexpire")

	ctx, span := r.tracer.Tracer(tracerName).Start(context.Background(), "kv:mexpire")
	defer span.End()

	if in.GetStorage() == "" {
		err := errors.Str("no storage provided")
		span.RecordError(err)
		return errors.E(op, err)
	}

	st, ok := r.storages[in.GetStorage()]
	if !ok {
		err := errors.Errorf("no such storage: %s", in.GetStorage())
		span.RecordError(err)
		return errors.E(op, err)
	}

	err := st.MExpire(ctx, from(in.GetItems())...)
	if err != nil {
		span.RecordError(err)
		return errors.E(op, err)
	}

	return nil
}

// TTL accept proto payload with Storage and Item
func (r *rpc) TTL(in *kvv2.KvRequest, out *kvv2.KvResponse) error { //nolint:dupl
	const op = errors.Op("rpc_ttl")

	ctx, span := r.tracer.Tracer(tracerName).Start(context.Background(), "kv:ttl")
	defer span.End()

	if in.GetStorage() == "" {
		err := errors.Str("no storage provided")
		span.RecordError(err)
		return errors.E(op, err)
	}

	st, ok := r.storages[in.GetStorage()]
	if !ok {
		err := errors.Errorf("no such storage: %s", in.GetStorage())
		span.RecordError(err)
		return errors.E(op, err)
	}

	keys := make([]string, 0, len(in.GetItems()))

	for i := range in.GetItems() {
		keys = append(keys, in.Items[i].Key)
	}

	ret, err := st.TTL(ctx, keys...)
	if err != nil {
		span.RecordError(err)
		return errors.E(op, err)
	}

	out.Items = make([]*kvv2.KvItem, 0, len(ret))
	for k := range ret {
		item := &kvv2.KvItem{Key: k}
		if ret[k] != "" {
			t, err := time.Parse(time.RFC3339, ret[k])
			if err == nil {
				item.Ttl = durationpb.New(time.Until(t))
			}
		}
		out.Items = append(out.Items, item)
	}

	return nil
}

// Delete accept proto payload with Storage and Item
func (r *rpc) Delete(in *kvv2.KvRequest, _ *kvv2.KvResponse) error {
	const op = errors.Op("rpc_delete")

	ctx, span := r.tracer.Tracer(tracerName).Start(context.Background(), "kv:delete")
	defer span.End()

	if in.GetStorage() == "" {
		err := errors.Str("no storage provided")
		span.RecordError(err)
		return errors.E(op, err)
	}

	st, ok := r.storages[in.GetStorage()]
	if !ok {
		err := errors.Errorf("no such storage: %s", in.GetStorage())
		span.RecordError(err)
		return errors.E(op, err)
	}

	keys := make([]string, 0, len(in.GetItems()))

	for i := range in.GetItems() {
		keys = append(keys, in.Items[i].Key)
	}

	err := st.Delete(ctx, keys...)
	if err != nil {
		span.RecordError(err)
		return errors.E(op, err)
	}

	return nil
}

// Clear clean the storage
func (r *rpc) Clear(in *kvv2.KvRequest, _ *kvv2.KvResponse) error {
	const op = errors.Op("rpc_clear")

	ctx, span := r.tracer.Tracer(tracerName).Start(context.Background(), "kv:clear")
	defer span.End()

	if in.GetStorage() == "" {
		err := errors.Str("no storage provided")
		span.RecordError(err)
		return errors.E(op, err)
	}

	st, ok := r.storages[in.GetStorage()]
	if !ok {
		err := errors.Errorf("no such storage: %s", in.GetStorage())
		span.RecordError(err)
		return errors.E(op, err)
	}

	err := st.Clear(ctx)
	if err != nil {
		span.RecordError(err)
		return errors.E(op, err)
	}

	return nil
}

func from(tr []*kvv2.KvItem) []kv.Item {
	items := make([]kv.Item, 0, len(tr))
	for i := range tr {
		var timeout string
		if ttl := tr[i].GetTtl(); ttl != nil {
			timeout = time.Now().Add(ttl.AsDuration()).Format(time.RFC3339)
		}
		items = append(items, &Item{
			key:     tr[i].GetKey(),
			val:     tr[i].GetValue(),
			timeout: timeout,
		})
	}

	return items
}
