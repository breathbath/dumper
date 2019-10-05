package config

import "encoding/json"

type Config struct {
	Name    string           `json:"name"`
	Kind    string           `json:"kind"`
	Context *json.RawMessage `json:"context"`
	Period  string           `json:"period,omitempty"`
}
