package db

import (
	"context"
	"fmt"
	"log"
	"net/url"
	"time"

	"github.com/nexus/backend/internal/config"
	_ "github.com/lib/pq"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

type DB struct {
	*gorm.DB
}

func New(cfg *config.Config) (*DB, error) {
	// Use PostgreSQL URL format with URL-encoded password
	// PreferSimpleProtocol disables prepared statements at the driver level
	encodedPassword := url.QueryEscape(cfg.DBPassword)
	dsn := fmt.Sprintf(
		"postgres://%s:%s@%s:%d/%s?sslmode=%s",
		cfg.DBUser, encodedPassword, cfg.DBHost, cfg.DBPort, cfg.DBName, cfg.DBSSLMode,
	)

	log.Printf("Connecting to database: host=%s port=%d dbname=%s user=%s", cfg.DBHost, cfg.DBPort, cfg.DBName, cfg.DBUser)
	log.Printf("Password: %q (length: %d)", cfg.DBPassword, len(cfg.DBPassword))

	// Use PreferSimpleProtocol to disable prepared statements for Supabase pooler compatibility
	gormCfg := &gorm.Config{
		Logger:                 logger.Default.LogMode(logger.Warn),
		NowFunc:                func() time.Time { return time.Now().UTC() },
		SkipDefaultTransaction: true,
	}

	db, err := gorm.Open(postgres.New(postgres.Config{
		DSN:                  dsn,
		PreferSimpleProtocol: true, // Disable prepared statements
	}), gormCfg)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to database: %w", err)
	}

	sqlDB, err := db.DB()
	if err != nil {
		return nil, fmt.Errorf("failed to get sql.DB: %w", err)
	}

	// Set connection pool based on config, with reasonable defaults for Supabase pooler
	maxConns := cfg.DBMaxConns
	if maxConns <= 0 {
		maxConns = 25 // Default from config
	}
	// For Supabase pooler, allow higher connections for better performance
	if maxConns > 25 {
		maxConns = 25
	}
	sqlDB.SetMaxIdleConns(maxConns / 2)
	sqlDB.SetMaxOpenConns(maxConns)
	sqlDB.SetConnMaxLifetime(10 * time.Minute)

	return &DB{db}, nil
}

func (d *DB) Close() error {
	sqlDB, err := d.DB.DB()
	if err != nil {
		return err
	}
	return sqlDB.Close()
}

func (d *DB) Ping(ctx context.Context) error {
	sqlDB, err := d.DB.DB()
	if err != nil {
		return err
	}
	return sqlDB.PingContext(ctx)
}

// Transaction helper
func (d *DB) Transaction(fn func(*gorm.DB) error) error {
	return d.DB.Transaction(fn)
}

func (d *DB) Raw(db *gorm.DB, sql string, values ...interface{}) *gorm.DB {
	return db.Raw(sql, values...)
}
