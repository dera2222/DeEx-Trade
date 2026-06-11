package repository

import (
	"context"
	"time"

	"github.com/nexus/backend/internal/db"
	"github.com/nexus/backend/internal/models"
	"gorm.io/gorm"
)

type RefundRepository struct {
	db *db.DB
}

func NewRefundRepository(db *db.DB) *RefundRepository {
	return &RefundRepository{db: db}
}

// Create creates a new refund request
func (r *RefundRepository) Create(ctx context.Context, refund *models.RefundRequest) error {
	return r.db.WithContext(ctx).Create(refund).Error
}

// CreateBatch creates multiple refund requests in a single transaction
func (r *RefundRepository) CreateBatch(ctx context.Context, refunds []*models.RefundRequest) error {
	if len(refunds) == 0 {
		return nil
	}
	return r.db.WithContext(ctx).Create(refunds).Error
}

// GetByID retrieves a refund request by ID
func (r *RefundRepository) GetByID(ctx context.Context, id uint) (*models.RefundRequest, error) {
	var refund models.RefundRequest
	if err := r.db.WithContext(ctx).First(&refund, id).Error; err != nil {
		return nil, err
	}
	return &refund, nil
}

// GetPending retrieves pending refund requests with limit
func (r *RefundRepository) GetPending(ctx context.Context, limit int) ([]models.RefundRequest, error) {
	var refunds []models.RefundRequest
	err := r.db.WithContext(ctx).
		Where("status = ?", models.RefundStatusPending).
		Order("created_at asc").
		Limit(limit).
		Find(&refunds).Error
	return refunds, err
}

// GetByOrderID retrieves refund requests for a specific order
func (r *RefundRepository) GetByOrderID(ctx context.Context, orderID uint) ([]models.RefundRequest, error) {
	var refunds []models.RefundRequest
	err := r.db.WithContext(ctx).
		Where("order_id = ?", orderID).
		Order("created_at desc").
		Find(&refunds).Error
	return refunds, err
}

// UpdateStatus updates the status of a refund request
func (r *RefundRepository) UpdateStatus(ctx context.Context, id uint, status models.RefundStatus, txHash, errorMsg string) error {
	updateData := map[string]interface{}{
		"status":     status,
		"updated_at": time.Now(),
	}

	if txHash != "" {
		updateData["tx_hash"] = txHash
	}
	if errorMsg != "" {
		updateData["error_msg"] = errorMsg
	}

	return r.db.WithContext(ctx).
		Model(&models.RefundRequest{}).
		Where("id = ?", id).
		Updates(updateData).Error
}

// IncrementRetryCount increments the retry count for a refund request
func (r *RefundRepository) IncrementRetryCount(ctx context.Context, id uint) error {
	return r.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		var refund models.RefundRequest
		if err := tx.Where("id = ?", id).First(&refund).Error; err != nil {
			return err
		}

		refund.RetryCount++
		refund.UpdatedAt = time.Now()

		return tx.Save(&refund).Error
	})
}

// GetFailedForRetry retrieves failed refund requests that should be retried
func (r *RefundRepository) GetFailedForRetry(ctx context.Context, maxRetries int, limit int) ([]models.RefundRequest, error) {
	var refunds []models.RefundRequest
	err := r.db.WithContext(ctx).
		Where("status = ? AND retry_count < ?", models.RefundStatusFailed, maxRetries).
		Order("updated_at asc").
		Limit(limit).
		Find(&refunds).Error
	return refunds, err
}

// GetByUserID retrieves refund requests for a user
func (r *RefundRepository) GetByUserID(ctx context.Context, userID uint, limit, offset int) ([]models.RefundRequest, error) {
	var refunds []models.RefundRequest
	err := r.db.WithContext(ctx).
		Where("user_id = ?", userID).
		Order("created_at desc").
		Limit(limit).
		Offset(offset).
		Find(&refunds).Error
	return refunds, err
}

// ExistsByOrderID checks if a refund request exists for an order
func (r *RefundRepository) ExistsByOrderID(ctx context.Context, orderID uint) (bool, error) {
	var count int64
	err := r.db.WithContext(ctx).
		Model(&models.RefundRequest{}).
		Where("order_id = ?", orderID).
		Count(&count).Error
	return count > 0, err
}

// GetStats returns refund statistics
func (r *RefundRepository) GetStats(ctx context.Context) (map[string]int64, error) {
	var stats struct {
		Pending    int64
		Processing int64
		Completed  int64
		Failed     int64
		Total      int64
	}

	query := `
		SELECT
			COUNT(CASE WHEN status = 'pending' THEN 1 END) as pending,
			COUNT(CASE WHEN status = 'processing' THEN 1 END) as processing,
			COUNT(CASE WHEN status = 'completed' THEN 1 END) as completed,
			COUNT(CASE WHEN status = 'failed' THEN 1 END) as failed,
			COUNT(*) as total
		FROM refund_requests
	`

	err := r.db.WithContext(ctx).Raw(query).Scan(&stats).Error
	if err != nil {
		return nil, err
	}

	return map[string]int64{
		"pending":    stats.Pending,
		"processing": stats.Processing,
		"completed":  stats.Completed,
		"failed":     stats.Failed,
		"total":      stats.Total,
	}, nil
}