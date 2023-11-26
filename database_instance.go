package pg

import (
	"errors"
	"sync"
)

var (
	instances        map[string]*Database
	instancesMu      sync.RWMutex
	ErrNoInstance    = errors.New("there is no active database instance")
	ErrManyInstances = errors.New("there is more than one active database instance")
)

func GetInstance() (*Database, error) {
	instancesMu.RLock()
	defer instancesMu.RUnlock()

	if len(instances) == 0 {
		return nil, ErrNoInstance
	}

	if len(instances) > 1 {
		return nil, ErrManyInstances
	}

	var instance *Database

	for _, d := range instances {
		instance = d
		break
	}

	return instance, nil
}
