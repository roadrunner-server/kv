package kv

import (
	"context"
	stderr "errors"
	"fmt"

	kvV1 "github.com/roadrunner-server/api-go/v6/kv/v1"
	"github.com/roadrunner-server/api-plugins/v6/kv"
	"github.com/roadrunner-server/errors"
	"go.opentelemetry.io/otel/trace"
)

const tracerName = "kv"

var (
	errEmptyStorage = stderr.New("no storage provided")
	errNoSuchStore  = stderr.New("no such storage")
)

type rpc struct {
	pl     *Plugin
	tracer trace.Tracer
}

func (r *rpc) lookupStorage(name string) (kv.Storage, error) {
	if name == "" {
		return nil, errEmptyStorage
	}
	st, ok := r.pl.storages[name]
	if !ok {
		return nil, fmt.Errorf("%w: %s", errNoSuchStore, name)
	}
	return st, nil
}

func keysOf(items []*kvV1.Item) []string {
	keys := make([]string, 0, len(items))
	for _, it := range items {
		keys = append(keys, it.GetKey())
	}
	return keys
}

func (r *rpc) Has(in *kvV1.Request, out *kvV1.Response) error {
	const op = errors.Op("rpc_has")

	ctx, span := r.tracer.Start(context.Background(), "kv:has")
	defer span.End()

	st, err := r.lookupStorage(in.GetStorage())
	if err != nil {
		span.RecordError(err)
		return err
	}

	keys := keysOf(in.GetItems())

	ret, err := st.Has(ctx, keys...)
	if err != nil {
		span.RecordError(err)
		return errors.E(op, err)
	}

	out.Items = make([]*kvV1.Item, 0, len(ret))
	for k := range ret {
		out.Items = append(out.Items, &kvV1.Item{Key: k})
	}
	return nil
}

func (r *rpc) Set(in *kvV1.Request, _ *kvV1.Response) error {
	const op = errors.Op("rpc_set")

	ctx, span := r.tracer.Start(context.Background(), "kv:set")
	defer span.End()

	st, err := r.lookupStorage(in.GetStorage())
	if err != nil {
		span.RecordError(err)
		return err
	}

	if err := st.Set(ctx, from(in.GetItems())...); err != nil {
		span.RecordError(err)
		return errors.E(op, err)
	}
	return nil
}

func (r *rpc) MGet(in *kvV1.Request, out *kvV1.Response) error {
	const op = errors.Op("rpc_mget")

	ctx, span := r.tracer.Start(context.Background(), "kv:mget")
	defer span.End()

	st, err := r.lookupStorage(in.GetStorage())
	if err != nil {
		span.RecordError(err)
		return err
	}

	keys := keysOf(in.GetItems())

	ret, err := st.MGet(ctx, keys...)
	if err != nil {
		span.RecordError(err)
		return errors.E(op, err)
	}

	out.Items = make([]*kvV1.Item, 0, len(ret))
	for k := range ret {
		out.Items = append(out.Items, &kvV1.Item{Key: k, Value: ret[k]})
	}
	return nil
}

func (r *rpc) MExpire(in *kvV1.Request, _ *kvV1.Response) error {
	const op = errors.Op("rpc_mexpire")

	ctx, span := r.tracer.Start(context.Background(), "kv:mexpire")
	defer span.End()

	st, err := r.lookupStorage(in.GetStorage())
	if err != nil {
		span.RecordError(err)
		return err
	}

	if err := st.MExpire(ctx, from(in.GetItems())...); err != nil {
		span.RecordError(err)
		return errors.E(op, err)
	}
	return nil
}

func (r *rpc) TTL(in *kvV1.Request, out *kvV1.Response) error {
	const op = errors.Op("rpc_ttl")

	ctx, span := r.tracer.Start(context.Background(), "kv:ttl")
	defer span.End()

	st, err := r.lookupStorage(in.GetStorage())
	if err != nil {
		span.RecordError(err)
		return err
	}

	keys := keysOf(in.GetItems())

	ret, err := st.TTL(ctx, keys...)
	if err != nil {
		span.RecordError(err)
		return errors.E(op, err)
	}

	out.Items = make([]*kvV1.Item, 0, len(ret))
	for k := range ret {
		out.Items = append(out.Items, &kvV1.Item{Key: k, Timeout: ret[k]})
	}
	return nil
}

func (r *rpc) Delete(in *kvV1.Request, _ *kvV1.Response) error {
	const op = errors.Op("rpc_delete")

	ctx, span := r.tracer.Start(context.Background(), "kv:delete")
	defer span.End()

	st, err := r.lookupStorage(in.GetStorage())
	if err != nil {
		span.RecordError(err)
		return err
	}

	keys := keysOf(in.GetItems())

	if err := st.Delete(ctx, keys...); err != nil {
		span.RecordError(err)
		return errors.E(op, err)
	}
	return nil
}

func (r *rpc) Clear(in *kvV1.Request, _ *kvV1.Response) error {
	const op = errors.Op("rpc_clear")

	ctx, span := r.tracer.Start(context.Background(), "kv:clear")
	defer span.End()

	st, err := r.lookupStorage(in.GetStorage())
	if err != nil {
		span.RecordError(err)
		return err
	}

	if err := st.Clear(ctx); err != nil {
		span.RecordError(err)
		return errors.E(op, err)
	}
	return nil
}

func from(tr []*kvV1.Item) []kv.Item {
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
