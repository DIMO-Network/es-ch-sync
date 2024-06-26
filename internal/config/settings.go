package config

// Settings contains the application config.
type Settings struct {
	StartTime                      string `yaml:"START_TIME"`
	StopTime                       string `yaml:"STOP_TIME"`
	BatchSize                      int    `yaml:"BATCH_SIZE"`
	TokenIDs                       string `yaml:"TOKEN_IDS"`
	Signals                        string `yaml:"SIGNALS"`
	Parallel                       int    `yaml:"PARALLEL"`
	MonPort                        int    `yaml:"MON_PORT"`
	ClickHouseHost                 string `yaml:"CLICKHOUSE_HOST"`
	ClickHouseTCPPort              int    `yaml:"CLICKHOUSE_TCP_PORT"`
	ClickHouseUser                 string `yaml:"CLICKHOUSE_USER"`
	ClickHousePassword             string `yaml:"CLICKHOUSE_PASSWORD"`
	ClickHouseDatabase             string `yaml:"CLICKHOUSE_DATABASE"`
	DeviceAPIAddr                  string `yaml:"DEVICES_API_GRPC_ADDR"`
	DeviceDataIndexName            string `yaml:"DEVICE_DATA_INDEX_NAME"`
	ElasticSearchAnalyticsHost     string `yaml:"ELASTICSEARCH_URL"`
	ElasticSearchAnalyticsUsername string `yaml:"ELASTICSEARCH_USER"`
	ElasticSearchAnalyticsPassword string `yaml:"ELASTICSEARCH_PASSWORD"`
}
