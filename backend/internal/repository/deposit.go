package repository

import (
	"context"
	"time"

	"github.com/nexus/backend/internal/db"
	"github.com/nexus/backend/internal/models"
)

type DepositRepository struct {
	db *db.DB
}

func NewDepositRepository(db *db.DB, _ *db.RedisClient) *DepositRepository {
	return &DepositRepository{db: db}
}

func (r *DepositRepository) Create(ctx context.Context, deposit *models.SolanaDeposit) error {
	deposit.CreatedAt = time.Now()
	deposit.UpdatedAt = time.Now()
	return r.db.WithContext(ctx).Create(deposit).Error
}

func (r *DepositRepository) GetByMemo(ctx context.Context, memo string) (*models.SolanaDeposit, error) {
	var deposit models.SolanaDeposit
	if err := r.db.WithContext(ctx).Where("memo = ?", memo).First(&deposit).Error; err != nil {
		return nil, err
	}
	return &deposit, nil
}

func (r *DepositRepository) GetByTxHash(ctx context.Context, txHash string) (*models.SolanaDeposit, error) {
	var deposit models.SolanaDeposit
	if err := r.db.WithContext(ctx).Where("tx_hash = ?", txHash).First(&deposit).Error; err != nil {
		return nil, err
	}
	return &deposit, nil
}

func (r *DepositRepository) ExistsByTxHash(ctx context.Context, txHash string) (bool, error) {
	var count int64
	err := r.db.WithContext(ctx).Model(&models.SolanaDeposit{}).Where("tx_hash = ?", txHash).Count(&count).Error
	return count > 0, err
}

func (r *DepositRepository) MarkCredited(ctx context.Context, id uint, txHash string, status string) error {
	return r.db.WithContext(ctx).Model(&models.SolanaDeposit{}).
		Where("id = ? AND tx_hash = ?", id, txHash).
		Update("status", status).Error
}
