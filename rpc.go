package kv

import (
	"context"
	stderr "errors"
	"fmt"
	"time"

	"connectrpc.com/connect"
	kvV2 "github.com/roadrunner-server/api-go/v6/kv/v2"
	"github.com/roadrunner-server/api-plugins/v6/kv"
	"github.com/roadrunner-server/errors"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"google.golang.org/protobuf/types/known/durationpb"
)

const tracerName = "kv"

var (
	errEmptyStorage = stderr.New("no storage provided")
	errNoSuchStore  = stderr.New("no such storage")
)

type rpc struct {
	storages map[string]kv.Storage
	tracer   *sdktrace.TracerProvider
}

func (r *rpc) lookupStorage(name string) (kv.Storage, error) {
	if name == "" {
		return nil, connect.NewError(connect.CodeInvalidArgument, errEmptyStorage)
	}
	st, ok := r.storages[name]
	if !ok {
		return nil, connect.NewError(connect.CodeNotFound, fmt.Errorf("%w: %s", errNoSuchStore, name))
	}
	return st, nil
}

func (r *rpc) Has(ctx context.Context, req *connect.Request[kvV2.KvRequest]) (*connect.Response[kvV2.KvResponse], error) {
	const op = errors.Op("rpc_has")
	msg := req.Msg

	ctx, span := r.tracer.Tracer(tracerName).Start(ctx, "kv:has")
	defer span.End()

	st, err := r.lookupStorage(msg.GetStorage())
	if err != nil {
		span.RecordError(err)
		return nil, err
	}

	keys := make([]string, 0, len(msg.GetItems()))
	for i := range msg.GetItems() {
		keys = append(keys, msg.GetItems()[i].GetKey())
	}

	ret, err := st.Has(ctx, keys...)
	if err != nil {
		span.RecordError(err)
		return nil, connect.NewError(connect.CodeInternal, errors.E(op, err))
	}

	out := &kvV2.KvResponse{Items: make([]*kvV2.KvItem, 0, len(ret))}
	for k := range ret {
		out.Items = append(out.Items, &kvV2.KvItem{Key: k})
	}
	return connect.NewResponse(out), nil
}

func (r *rpc) Set(ctx context.Context, req *connect.Request[kvV2.KvRequest]) (*connect.Response[kvV2.KvResponse], error) {
	const op = errors.Op("rpc_set")
	msg := req.Msg

	ctx, span := r.tracer.Tracer(tracerName).Start(ctx, "kv:set")
	defer span.End()

	st, err := r.lookupStorage(msg.GetStorage())
	if err != nil {
		span.RecordError(err)
		return nil, err
	}

	if err := st.Set(ctx, from(msg.GetItems())...); err != nil {
		span.RecordError(err)
		return nil, connect.NewError(connect.CodeInternal, errors.E(op, err))
	}
	return connect.NewResponse(&kvV2.KvResponse{}), nil
}

func (r *rpc) MGet(ctx context.Context, req *connect.Request[kvV2.KvRequest]) (*connect.Response[kvV2.KvResponse], error) {
	const op = errors.Op("rpc_mget")
	msg := req.Msg

	ctx, span := r.tracer.Tracer(tracerName).Start(ctx, "kv:mget")
	defer span.End()

	st, err := r.lookupStorage(msg.GetStorage())
	if err != nil {
		span.RecordError(err)
		return nil, err
	}

	keys := make([]string, 0, len(msg.GetItems()))
	for i := range msg.GetItems() {
		keys = append(keys, msg.GetItems()[i].GetKey())
	}

	ret, err := st.MGet(ctx, keys...)
	if err != nil {
		span.RecordError(err)
		return nil, connect.NewError(connect.CodeInternal, errors.E(op, err))
	}

	out := &kvV2.KvResponse{Items: make([]*kvV2.KvItem, 0, len(ret))}
	for k := range ret {
		out.Items = append(out.Items, &kvV2.KvItem{Key: k, Value: ret[k]})
	}
	return connect.NewResponse(out), nil
}

func (r *rpc) MExpire(ctx context.Context, req *connect.Request[kvV2.KvRequest]) (*connect.Response[kvV2.KvResponse], error) {
	const op = errors.Op("rpc_mexpire")
	msg := req.Msg

	ctx, span := r.tracer.Tracer(tracerName).Start(ctx, "kv:mexpire")
	defer span.End()

	st, err := r.lookupStorage(msg.GetStorage())
	if err != nil {
		span.RecordError(err)
		return nil, err
	}

	if err := st.MExpire(ctx, from(msg.GetItems())...); err != nil {
		span.RecordError(err)
		return nil, connect.NewError(connect.CodeInternal, errors.E(op, err))
	}
	return connect.NewResponse(&kvV2.KvResponse{}), nil
}

func (r *rpc) TTL(ctx context.Context, req *connect.Request[kvV2.KvRequest]) (*connect.Response[kvV2.KvResponse], error) {
	const op = errors.Op("rpc_ttl")
	msg := req.Msg

	ctx, span := r.tracer.Tracer(tracerName).Start(ctx, "kv:ttl")
	defer span.End()

	st, err := r.lookupStorage(msg.GetStorage())
	if err != nil {
		span.RecordError(err)
		return nil, err
	}

	keys := make([]string, 0, len(msg.GetItems()))
	for i := range msg.GetItems() {
		keys = append(keys, msg.GetItems()[i].GetKey())
	}

	ret, err := st.TTL(ctx, keys...)
	if err != nil {
		span.RecordError(err)
		return nil, connect.NewError(connect.CodeInternal, errors.E(op, err))
	}

	out := &kvV2.KvResponse{Items: make([]*kvV2.KvItem, 0, len(ret))}
	for k := range ret {
		item := &kvV2.KvItem{Key: k}
		if ret[k] != "" {
			t, err := time.Parse(time.RFC3339, ret[k])
			if err != nil {
				span.RecordError(err)
				return nil, connect.NewError(connect.CodeInternal, errors.E(op, err))
			}
			d := time.Until(t)
			if d < 0 {
				d = 0
			}
			item.Ttl = durationpb.New(d)
		}
		out.Items = append(out.Items, item)
	}
	return connect.NewResponse(out), nil
}

func (r *rpc) Delete(ctx context.Context, req *connect.Request[kvV2.KvRequest]) (*connect.Response[kvV2.KvResponse], error) {
	const op = errors.Op("rpc_delete")
	msg := req.Msg

	ctx, span := r.tracer.Tracer(tracerName).Start(ctx, "kv:delete")
	defer span.End()

	st, err := r.lookupStorage(msg.GetStorage())
	if err != nil {
		span.RecordError(err)
		return nil, err
	}

	keys := make([]string, 0, len(msg.GetItems()))
	for i := range msg.GetItems() {
		keys = append(keys, msg.GetItems()[i].GetKey())
	}

	if err := st.Delete(ctx, keys...); err != nil {
		span.RecordError(err)
		return nil, connect.NewError(connect.CodeInternal, errors.E(op, err))
	}
	return connect.NewResponse(&kvV2.KvResponse{}), nil
}

func (r *rpc) Clear(ctx context.Context, req *connect.Request[kvV2.KvRequest]) (*connect.Response[kvV2.KvResponse], error) {
	const op = errors.Op("rpc_clear")
	msg := req.Msg

	ctx, span := r.tracer.Tracer(tracerName).Start(ctx, "kv:clear")
	defer span.End()

	st, err := r.lookupStorage(msg.GetStorage())
	if err != nil {
		span.RecordError(err)
		return nil, err
	}

	if err := st.Clear(ctx); err != nil {
		span.RecordError(err)
		return nil, connect.NewError(connect.CodeInternal, errors.E(op, err))
	}
	return connect.NewResponse(&kvV2.KvResponse{}), nil
}

func from(tr []*kvV2.KvItem) []kv.Item {
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
