package config

import "time"

// AppConfig is the root configuration struct for TTDust.
type AppConfig struct {
	Server        ServerConfig        `mapstructure:"server"`
	Storage       StorageConfig       `mapstructure:"storage"`
	Ingestion     IngestionConfig     `mapstructure:"ingestion"`
	Processing    ProcessingConfig    `mapstructure:"processing"`
	Observability ObservabilityConfig `mapstructure:"observability"`
}

type ServerConfig struct {
	GRPCAddress     string        `mapstructure:"grpc_address"`
	RESTAddress     string        `mapstructure:"rest_address"`
	ShutdownTimeout time.Duration `mapstructure:"shutdown_timeout"`
}

type StorageConfig struct {
	Postgres PostgresConfig `mapstructure:"postgres"`
	Redis    RedisConfig    `mapstructure:"redis"`
	S3       S3Config       `mapstructure:"s3"`
	WAL      WALConfig      `mapstructure:"wal"`
}

type PostgresConfig struct {
	DSN             string        `mapstructure:"dsn"`
	MaxOpenConns    int           `mapstructure:"max_open_conns"`
	MaxIdleConns    int           `mapstructure:"max_idle_conns"`
	ConnMaxLifetime time.Duration `mapstructure:"conn_max_lifetime"`
}

type RedisConfig struct {
	Address  string `mapstructure:"address"`
	Password string `mapstructure:"password"`
	DB       int    `mapstructure:"db"`
	PoolSize int    `mapstructure:"pool_size"`
}

type S3Config struct {
	Endpoint        string `mapstructure:"endpoint"`
	Bucket          string `mapstructure:"bucket"`
	Region          string `mapstructure:"region"`
	AccessKeyID     string `mapstructure:"access_key_id"`
	SecretAccessKey string `mapstructure:"secret_access_key"`
	UsePathStyle    bool   `mapstructure:"use_path_style"`
}

type WALConfig struct {
	Dir               string        `mapstructure:"dir"`
	SegmentSizeBytes  int64         `mapstructure:"segment_size_bytes"`
	SegmentAge        time.Duration `mapstructure:"segment_age"`
	SyncMode          string        `mapstructure:"sync_mode"`
	RetentionSegments int           `mapstructure:"retention_segments"`
}

type IngestionConfig struct {
	MaxBatchSize          int           `mapstructure:"max_batch_size"`
	FlushInterval         time.Duration `mapstructure:"flush_interval"`
	BackpressureThreshold int           `mapstructure:"backpressure_threshold"`
	MaxMessageSizeBytes   int           `mapstructure:"max_message_size_bytes"`
}

type ProcessingConfig struct {
	MaxConcurrentPipelines int `mapstructure:"max_concurrent_pipelines"`
	DLQRetentionDays       int `mapstructure:"dlq_retention_days"`
}

type CompactionConfig struct {
	TargetFileSizeBytes   int64         `mapstructure:"target_file_size_bytes"`
	MinFilesForCompaction int           `mapstructure:"min_files_for_compaction"`
	CompactionInterval    time.Duration `mapstructure:"compaction_interval"`
	MaxConcurrentCompact  int           `mapstructure:"max_concurrent_compactions"`
}

type ObservabilityConfig struct {
	MetricsAddress string `mapstructure:"metrics_address"`
	LogLevel       string `mapstructure:"log_level"`
	LogFormat      string `mapstructure:"log_format"`
	TracingEnabled bool   `mapstructure:"tracing_enabled"`
}

// DefaultConfig returns the default configuration.
func DefaultConfig() *AppConfig {
	return &AppConfig{
		Server: ServerConfig{
			GRPCAddress:     ":50051",
			RESTAddress:     ":8080",
			ShutdownTimeout: 30 * time.Second,
		},
		Storage: StorageConfig{
			Postgres: PostgresConfig{
				DSN:             "postgres://localhost:5432/ttdust?sslmode=disable",
				MaxOpenConns:    25,
				MaxIdleConns:    5,
				ConnMaxLifetime: 5 * time.Minute,
			},
			Redis: RedisConfig{
				Address:  "localhost:6379",
				DB:       0,
				PoolSize: 10,
			},
			S3: S3Config{
				Region:       "us-east-1",
				Bucket:       "ttdust-data",
				UsePathStyle: true,
			},
			WAL: WALConfig{
				Dir:               "/var/lib/ttdust/wal",
				SegmentSizeBytes:  67108864, // 64MB
				SegmentAge:        10 * time.Minute,
				SyncMode:          "fsync",
				RetentionSegments: 10,
			},
		},
		Ingestion: IngestionConfig{
			MaxBatchSize:          10000,
			FlushInterval:         10 * time.Second,
			BackpressureThreshold: 10000,
			MaxMessageSizeBytes:   1048576, // 1MB
		},
		Processing: ProcessingConfig{
			MaxConcurrentPipelines: 4,
			DLQRetentionDays:       7,
		},
		Observability: ObservabilityConfig{
			MetricsAddress: ":9090",
			LogLevel:       "info",
			LogFormat:      "json",
			TracingEnabled: false,
		},
	}
}
