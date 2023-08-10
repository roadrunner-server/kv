module github.com/roadrunner-server/kv/v4

go 1.21

require (
	buf.build/gen/go/roadrunner-server/api/protocolbuffers/go v1.31.0-20230627200035-4e59a69f79a2.1
	github.com/roadrunner-server/api/v4 v4.6.2
	github.com/roadrunner-server/endure/v2 v2.4.1
	github.com/roadrunner-server/errors v1.3.0
	go.uber.org/zap v1.25.0
)

require (
	go.uber.org/multierr v1.11.0 // indirect
	google.golang.org/protobuf v1.31.0 // indirect
)
