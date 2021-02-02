package influxdb

var (
	tagsWhitelist = make(map[string]bool)
)

func UpdateTagsWhitelist(tagsList []string) {
	for _, tag := range tagsList {
		tagsWhitelist[tag] = true
	}
}
