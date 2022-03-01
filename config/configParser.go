package config

import (
	"encoding/json"
	"fmt"
	"github.com/breathbath/go_utils/v3/pkg/env"
	"github.com/breathbath/go_utils/v3/pkg/fs"
	"os"
)

func ParseConfig() ([]*Config, error) {
	var conf []*Config

	configPath, err := env.ReadEnvOrError("CONFIG_PATH")
	if err != nil {
		return nil, err
	}

	if !fs.FileExists(configPath) {
		return conf, fmt.Errorf("config file doesn't exit under '%s'", configPath)
	}

	yamlFile, err := os.ReadFile(configPath)
	if err != nil {
		return conf, err
	}

	err = json.Unmarshal(yamlFile, &conf)
	if err != nil {
		return conf, fmt.Errorf("cannot parse config file '%s': %v", configPath, err)
	}

	return conf, nil
}
