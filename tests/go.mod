module tests

go 1.27

toolchain go1.27.0

require (
	github.com/roadrunner-server/api-go/v6 v6.0.0-beta.14
	github.com/roadrunner-server/boltdb/v6 v6.0.0-beta.4
	github.com/roadrunner-server/config/v6 v6.0.0-beta.3
	github.com/roadrunner-server/endure/v2 v2.6.2
	github.com/roadrunner-server/goridge/v4 v4.0.0-beta.3
	github.com/roadrunner-server/kv/v6 v6.0.0
	github.com/roadrunner-server/logger/v6 v6.0.0-beta.4
	github.com/roadrunner-server/memcached/v6 v6.0.0-beta.4
	github.com/roadrunner-server/memory/v6 v6.0.0-beta.4
	github.com/roadrunner-server/rpc/v6 v6.0.0-beta.5
	github.com/stretchr/testify v1.12.1
)

replace github.com/roadrunner-server/kv/v6 => ../

require (
	github.com/bradfitz/gomemcache v0.0.0-20260422231931-4d751bb6e37c // indirect
	github.com/cespare/xxhash/v2 v2.3.0 // indirect
	github.com/fatih/color v1.19.0 // indirect
	github.com/fsnotify/fsnotify v1.10.1 // indirect
	github.com/go-logr/logr v1.4.4 // indirect
	github.com/go-logr/stdr v1.2.2 // indirect
	github.com/go-viper/mapstructure/v2 v2.5.0 // indirect
	github.com/google/uuid v1.6.0 // indirect
	github.com/joho/godotenv v1.5.1 // indirect
	github.com/mattn/go-colorable v0.1.15 // indirect
	github.com/mattn/go-isatty v0.0.24 // indirect
	github.com/pelletier/go-toml/v2 v2.4.3 // indirect
	github.com/roadrunner-server/api-plugins/v6 v6.0.0-beta.2 // indirect
	github.com/roadrunner-server/errors v1.5.0 // indirect
	github.com/roadrunner-server/tcplisten v1.5.2 // indirect
	github.com/sagikazarmark/locafero v0.12.0 // indirect
	github.com/spf13/afero v1.15.0 // indirect
	github.com/spf13/cast v1.10.0 // indirect
	github.com/spf13/pflag v1.0.10 // indirect
	github.com/spf13/viper v1.21.0 // indirect
	github.com/subosito/gotenv v1.6.0 // indirect
	go.etcd.io/bbolt v1.5.0 // indirect
	go.opentelemetry.io/auto/sdk v1.2.1 // indirect
	go.opentelemetry.io/contrib/instrumentation/github.com/bradfitz/gomemcache/memcache/otelmemcache v0.43.0 // indirect
	go.opentelemetry.io/contrib/propagators/jaeger v1.45.0 // indirect
	go.opentelemetry.io/otel v1.45.0 // indirect
	go.opentelemetry.io/otel/metric v1.45.0 // indirect
	go.opentelemetry.io/otel/sdk v1.45.0 // indirect
	go.opentelemetry.io/otel/trace v1.45.0 // indirect
	go.uber.org/multierr v1.11.0 // indirect
	go.uber.org/zap v1.28.0 // indirect
	go.yaml.in/yaml/v3 v3.0.5 // indirect
	golang.org/x/sys v0.47.0 // indirect
	golang.org/x/text v0.41.0 // indirect
	google.golang.org/genproto v0.0.0-20260819154853-08b0e4226688 // indirect
	google.golang.org/protobuf v1.36.12 // indirect
)
