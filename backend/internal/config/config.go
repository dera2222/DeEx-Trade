package config

import (
	"os"
	"strconv"
)

type Config struct {
	// Server
	Port        string
	Environment string

	// Database (Supabase PostgreSQL)
	DBHost     string
	DBPort     int
	DBUser     string
	DBPassword string
	DBName     string
	DBSSLMode  string
	DBMaxConns int

	// Redis (Upstash / Redis Cloud)
	RedisHost     string
	RedisPort     int
	RedisPassword string
	RedisURL      string
	RedisTLS      bool

	// JWT
	JWTSecret string

	// Settlement Contract
	SettlementContractBSC  string
	SettlementContractBase string

	// Executor
	ExecutorEnabled       bool
	ExecutorRPCURL        string
	ExecutorRPCURLBase    string
	ExecutorPrivateKey    string
	ExecutorIntervalMs    int
	ChainID               int64
	ChainIDBase           int64
	SettlementAddress     string
	SettlementAddressBase string

	// API Keys
	InfuraURL          string
	AlchemyURL         string
	EtherscanKey       string
	SolanaRPCURL       string
	SolanaCustodyAddr  string
	SolanaCustodyPrivateKey string
}

func Load() *Config {
	return &Config{
		Port:                   getEnv("PORT", "8080"),
		Environment:            getEnv("ENVIRONMENT", "development"),
		DBHost:                 getEnv("DB_HOST", "localhost"),
		DBPort:                 getEnvAsInt("DB_PORT", 5432),
		DBUser:                 getEnv("DB_USER", "postgres"),
		DBPassword:             getEnv("DB_PASSWORD", ""),
		DBName:                 getEnv("DB_NAME", "postgres"),
		DBSSLMode:              getEnv("DB_SSL_MODE", "require"),
		DBMaxConns:             getEnvAsInt("DB_MAX_CONNS", 25),
		RedisHost:              getEnv("REDIS_HOST", "localhost"),
		RedisPort:              getEnvAsInt("REDIS_PORT", 6379),
		RedisPassword:          getEnv("REDIS_PASSWORD", ""),
		RedisURL:               getEnv("REDIS_URL", ""),
		RedisTLS:               getEnvAsBool("REDIS_TLS", false),
		JWTSecret:              getEnv("JWT_SECRET", "your-secret-key-change-in-production"),
		SettlementContractBSC:  getEnv("SETTLEMENT_CONTRACT_BSC", "0x4896ebe3EE1436a58c690A8021301A6bFD6BD4E7"),
		SettlementContractBase: getEnv("SETTLEMENT_CONTRACT_BASE", "0x723da0ef5eea8370015465e9Cf2513D7e48e1b61"),
		ExecutorEnabled:        getEnvAsBool("EXECUTOR_ENABLED", false),
		ExecutorRPCURL:         getEnv("EXECUTOR_RPC_URL", "https://bsc-dataseed.binance.org"),
		ExecutorRPCURLBase:     getEnv("EXECUTOR_RPC_URL_BASE", "https://base-mainnet.infura.io/v3/f4c82c2334c043678a712a5e860c7edf"),
		ExecutorPrivateKey:     getEnv("EXECUTOR_PRIVATE_KEY", ""),
		ExecutorIntervalMs:     getEnvAsInt("EXECUTOR_INTERVAL_MS", 5000),
		ChainID:                getEnvAsInt64("CHAIN_ID", 56),
		ChainIDBase:            getEnvAsInt64("CHAIN_ID_BASE", 8453),
		SettlementAddress:      getEnv("SETTLEMENT_ADDRESS", "0x4896ebe3EE1436a58c690A8021301A6bFD6BD4E7"),
		SettlementAddressBase:  getEnv("SETTLEMENT_ADDRESS_BASE", "0x723da0ef5eea8370015465e9Cf2513D7e48e1b61"),
		InfuraURL:              getEnv("INFURA_URL", ""),
		AlchemyURL:             getEnv("ALCHEMY_URL", ""),
		EtherscanKey:           getEnv("ETHERSCAN_KEY", ""),
		SolanaRPCURL:           getEnv("SOLANA_RPC_URL", "https://mainnet.helius-rpc.com/?api-key=1bc62453-705c-40ea-81d7-37fea004e5fa"),
		SolanaCustodyAddr:      getEnv("SOLANA_CUSTODY_ADDRESS", "5xayu3sbV3xD6fRUapYo5geZecnFzjRnqrHDdSXfbQLr"),
		SolanaCustodyPrivateKey: getEnv("SOLANA_CUSTODY_PRIVATE_KEY", "51rWEunB7K9ZKVDr7gXq8R1Xh1mhvJztkU7q8DWLPg4mccVwUEs2gxKinYtW8fJyr35FEoj2MGMm8DgeWmHyTxMQ"),
	}
}

func getEnvAsBool(key string, defaultValue bool) bool {
	valueStr := getEnv(key, "")
	if valueStr == "" {
		return defaultValue
	}
	return valueStr == "true" || valueStr == "1"
}

func getEnvAsInt64(key string, defaultValue int64) int64 {
	valueStr := getEnv(key, "")
	if value, err := strconv.ParseInt(valueStr, 10, 64); err == nil {
		return value
	}
	return defaultValue
}

func getEnv(key, defaultValue string) string {
	if value, exists := os.LookupEnv(key); exists {
		return value
	}
	return defaultValue
}

func getEnvAsInt(key string, defaultValue int) int {
	valueStr := getEnv(key, "")
	if value, err := strconv.Atoi(valueStr); err == nil {
		return value
	}
	return defaultValue
}
