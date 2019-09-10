package exec

import "github.com/breathbath/dumper/config"

type Executor interface {
	GetValidConfig(generalConfig config.Config) (interface{}, error)
	Execute(generalConfig config.Config, execConfig interface{}) error
}
