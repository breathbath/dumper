package config

import (
	"encoding/json"
	"fmt"
	"github.com/breathbath/go_utils/utils/env"
	"github.com/breathbath/go_utils/utils/fs"
	"io/ioutil"
)

func ParseConfig() ([]Config, error) {
	var conf []Config

	configPath, err := env.ReadEnvOrError("CONFIG_PATH")
	if err != nil {
		return conf, err
	}

	if !fs.FileExists(configPath) {
		return conf, fmt.Errorf("Config file doesn't exit under '%s'", configPath)
	}

	yamlFile, err := ioutil.ReadFile(configPath)
	if err != nil {
		return conf, err
	}

	err = json.Unmarshal(yamlFile, &conf)
	if err != nil {
		return conf, err
	}

	return conf, nil
}
