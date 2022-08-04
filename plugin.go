package kv

import (
	"fmt"

	"github.com/roadrunner-server/api/v2/plugins/config"
	"github.com/roadrunner-server/api/v2/plugins/kv"
	endure "github.com/roadrunner-server/endure/pkg/container"
	"github.com/roadrunner-server/errors"
	"go.uber.org/zap"
)

const (
	// PluginName linked to the memory, boltdb, memcached, redis plugins. DO NOT change w/o sync.
	PluginName string = "kv"
	// driver is the mandatory field which should present in every storage
	driver string = "driver"
	// config key used to detect local configuration for the driver
	cfg string = "config"
)

// Plugin for the unified storage
type Plugin struct {
	log *zap.Logger
	// constructors contains general storage constructors, such as boltdb, memory, memcached, redis.
	constructors map[string]kv.Constructor
	// storages contains user-defined storages, such as boltdb-north, memcached-us and so on.
	storages map[string]kv.Storage
	// KV configuration
	cfg       Config
	cfgPlugin config.Configurer
}

func (p *Plugin) Init(cfg config.Configurer, log *zap.Logger) error {
	const op = errors.Op("kv_plugin_init")
	if !cfg.Has(PluginName) {
		return errors.E(errors.Disabled)
	}

	err := cfg.UnmarshalKey(PluginName, &p.cfg.Data)
	if err != nil {
		return errors.E(op, err)
	}
	p.constructors = make(map[string]kv.Constructor, 5)
	p.storages = make(map[string]kv.Storage, 5)
	p.log = new(zap.Logger)
	*p.log = *log
	p.cfgPlugin = cfg
	return nil
}

func (p *Plugin) Serve() chan error {
	errCh := make(chan error, 1)
	const op = errors.Op("kv_plugin_serve")
	// key - storage name in the config
	// value - storage
	// For this config we should have 3 constructors: memory, boltdb and memcached but 4 KVs: default, boltdb-south, boltdb-north and memcached
	// when user requests for example boltdb-south, we should provide that particular preconfigured storage

	for k, v := range p.cfg.Data {
		// for example if the key not properly formatted (yaml)
		if v == nil {
			continue
		}

		// check type of the v
		// should be a map[string]any
		switch t := v.(type) {
		// correct type
		case map[string]any:
			if _, ok := t[driver]; !ok {
				errCh <- errors.E(op, errors.Errorf("could not find mandatory driver field in the %s storage", k))
				return errCh
			}
		default:
			p.log.Warn("wrong type detected in the configuration, please, check yaml indentation")
			continue
		}

		// config key for the particular sub-driver kv.memcached.config
		configKey := fmt.Sprintf("%s.%s.%s", PluginName, k, cfg)
		// at this point we know, that driver field present in the configuration
		drName := v.(map[string]any)[driver]

		// driver name should be a string
		if drStr, ok := drName.(string); ok {
			switch {
			// local configuration section key
			case p.cfgPlugin.Has(configKey):
				if _, ok := p.constructors[drStr]; !ok {
					p.log.Warn("no such constructor was registered", zap.String("requested", drStr), zap.Any("registered", p.constructors))
					continue
				}

				storage, err := p.constructors[drStr].KvFromConfig(configKey)
				if err != nil {
					errCh <- errors.E(op, err)
					return errCh
				}

				// save the storage
				p.storages[k] = storage
				// try global then
			case p.cfgPlugin.Has(k):
				if _, ok := p.constructors[drStr]; !ok {
					p.log.Warn("no such constructor was registered", zap.String("requested", drStr), zap.Any("registered", p.constructors))
					continue
				}

				// use only key for the driver registration, for example rr-boltdb should be globally available
				storage, err := p.constructors[drStr].KvFromConfig(k)
				if err != nil {
					errCh <- errors.E(op, err)
					return errCh
				}

				// save the storage
				p.storages[k] = storage
			default:
				p.log.Error("can't find local or global configuration, this section will be skipped", zap.String("local", configKey), zap.String("global", k))
				continue
			}
		}
		continue
	}

	return errCh
}

func (p *Plugin) Stop() error {
	// stop all attached storages
	for k := range p.storages {
		p.storages[k].Stop()
	}

	for k := range p.storages {
		delete(p.storages, k)
	}

	for k := range p.constructors {
		delete(p.constructors, k)
	}

	return nil
}

// Collects will get all plugins which implement Storage interface
func (p *Plugin) Collects() []any {
	return []any{
		p.GetAllStorageDrivers,
	}
}

func (p *Plugin) Name() string {
	return PluginName
}

func (p *Plugin) GetAllStorageDrivers(name endure.Named, constructor kv.Constructor) {
	// save the storage constructor
	p.constructors[name.Name()] = constructor
}

// RPC returns associated rpc service.
func (p *Plugin) RPC() any {
	return &rpc{srv: p, storages: p.storages}
}
