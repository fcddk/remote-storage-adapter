package config

// GlobalConfig configures values that are used across other configuration
// objects.
type GlobalConfig struct {
	MeasurementsWhitelist string `yaml:"measurements_whitelist,omitempty"`
	TagsWhitelist         string `yaml:"tags_whitelist,omitempty"`
}

// UnmarshalYAML implements the yaml.Unmarshaler interface.
func (c *GlobalConfig) UnmarshalYAML(unmarshal func(interface{}) error) error {
	// Create a clean global config as the previous one was already populated
	// by the default due to the YAML parser behavior for empty blocks.
	gc := &GlobalConfig{}
	type plain GlobalConfig
	if err := unmarshal((*plain)(gc)); err != nil {
		return err
	}
	return nil
}
