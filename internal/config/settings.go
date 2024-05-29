package config

// Settings contains the application config.
type Settings struct {
	StartTime                      string `yaml:"START_TIME"`
	StopTime                       string `yaml:"STOP_TIME"`
	BatchSize                      int    `yaml:"BATCH_SIZE"`
	TokenIDs                       string `yaml:"TOKEN_IDS"`
	Signals                        string `yaml:"SIGNALS"`
	MonPort                        int    `yaml:"MON_PORT"`
	ClickHouseHost                 string `yaml:"CLICKHOUSE_HOST"`
	ClickHouseTCPPort              int    `yaml:"CLICKHOUSE_TCP_PORT"`
	ClickHouseUser                 string `yaml:"CLICKHOUSE_USER"`
	ClickHousePassword             string `yaml:"CLICKHOUSE_PASSWORD"`
	ClickHouseDatabase             string `yaml:"CLICKHOUSE_DATABASE"`
	DeviceAPIAddr                  string `yaml:"DEVICES_API_GRPC_ADDR"`
	DeviceDataIndexName            string `yaml:"DEVICE_DATA_INDEX_NAME"`
	ElasticSearchAnalyticsHost     string `yaml:"ELASTIC_SEARCH_ANALYTICS_HOST"`
	ElasticSearchAnalyticsUsername string `yaml:"ELASTIC_SEARCH_ANALYTICS_USERNAME"`
	ElasticSearchAnalyticsPassword string `yaml:"ELASTIC_SEARCH_ANALYTICS_PASSWORD"`
}
