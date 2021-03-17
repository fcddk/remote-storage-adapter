package config

type MeasurementConfig struct {
	Name             string   `yaml:"name"`
	Tags             []string `yaml:"tags,omitempty"`
	Database         string   `yaml:"database,omitempty"`
	Fields           []string `yaml:"fields,omitempty"`
	IgnoreOtherLabel bool     `yaml:"ignore_other_label"`
	DropLabels       []string `yaml:"drop_labels,omitempty"`
}

// UnmarshalYAML implements the yaml.Unmarshaler interface.
func (c *MeasurementConfig) UnmarshalYAML(unmarshal func(interface{}) error) error {
	// Create a clean global config as the previous one was already populated
	// by the default due to the YAML parser behavior for empty blocks.
	mc := &MeasurementConfig{}
	type plain MeasurementConfig
	if err := unmarshal((*plain)(mc)); err != nil {
		return err
	}
	*c = *mc
	return nil
}
