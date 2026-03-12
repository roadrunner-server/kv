// Package kv is a RoadRunner plugin that provides a unified key-value storage
// gateway. It manages multiple named storage backends — memory, boltdb,
// memcached, and redis — registered through the Endure dependency-injection
// framework. Clients interact with every backend through a single RPC service,
// and all operations are instrumented with OpenTelemetry tracing.
package kv
