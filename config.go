package realtime

import "github.com/sieniven/xlayer-realtime/kafka"

type RealtimeConfig struct {
	Enable               bool
	EnableSubscribe      bool
	CacheHeightThreshold uint64
	Kafka                kafka.KafkaConfig
	CacheDumpPath        string
}
