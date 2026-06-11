package repository

import (
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"math/big"
	"strings"
	"time"

	"github.com/nexus/backend/internal/db"
	"github.com/nexus/backend/internal/models"
	"gorm.io/gorm"
)

type UserRepository struct {
	db    *db.DB
	redis *db.RedisClient
}

func NewUserRepository(db *db.DB, redis *db.RedisClient) *UserRepository {
	return &UserRepository{db: db, redis: redis}
}

func (r *UserRepository) Create(ctx context.Context, user *models.User) error {
	user.CreatedAt = time.Now()
	user.UpdatedAt = time.Now()
	return r.db.WithContext(ctx).Create(user).Error
}

func (r *UserRepository) GetByID(ctx context.Context, id uint) (*models.User, error) {
	var user models.User
	if err := r.db.WithContext(ctx).First(&user, id).Error; err != nil {
		return nil, err
	}
	return &user, nil
}

func (r *UserRepository) GetByAddress(ctx context.Context, address string) (*models.User, error) {
	var user models.User
	// Case-insensitive address lookup
	err := r.db.WithContext(ctx).Where("LOWER(address) = LOWER(?)", address).First(&user).Error
	return &user, err
}

func (r *UserRepository) UpdateProfile(ctx context.Context, userID uint, email, username string) error {
	updates := map[string]interface{}{
		"updated_at": time.Now(),
	}
	if email != "" {
		updates["email"] = email
	}
	if username != "" {
		updates["username"] = username
	}
	return r.db.WithContext(ctx).Model(&models.User{}).Where("id = ?", userID).Updates(updates).Error
}

func (r *UserRepository) GetOrCreate(ctx context.Context, address string) (*models.User, error) {
	user, err := r.GetByAddress(ctx, address)
	if err == nil {
		return user, nil
	}

	if !errors.Is(err, gorm.ErrRecordNotFound) {
		return nil, err
	}

	var lastErr error
	for attempt := 0; attempt < 5; attempt++ {
		user = &models.User{
			Address:      address,
			ReferralCode: generateReferralCode(),
			CreatedAt:    time.Now(),
			UpdatedAt:    time.Now(),
		}

		if err := r.Create(ctx, user); err != nil {
			lastErr = err
			if isUniqueConstraintError(err, "users_referral_code_key", "referral_code") {
				continue
			}
			if isUniqueConstraintError(err, "users_address_key", "address") {
				return r.GetByAddress(ctx, address)
			}
			return nil, err
		}

		return user, nil
	}

	return nil, fmt.Errorf("failed to create user after retries: %w", lastErr)
}

func isUniqueConstraintError(err error, keys ...string) bool {
	errMsg := strings.ToLower(err.Error())
	if !strings.Contains(errMsg, "duplicate key") && !strings.Contains(errMsg, "unique constraint") {
		return false
	}

	for _, key := range keys {
		if strings.Contains(errMsg, strings.ToLower(key)) {
			return true
		}
	}

	return false
}

func generateReferralCode() string {
	return "CEX" + randomString(8)
}

func randomString(n int) string {
	const letters = "ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
	result := make([]byte, n)
	max := big.NewInt(int64(len(letters)))
	for i := range result {
		num, err := rand.Int(rand.Reader, max)
		if err != nil {
			result[i] = letters[time.Now().UnixNano()%int64(len(letters))]
			continue
		}
		result[i] = letters[num.Int64()]
	}
	return string(result)
}

// GetOrCreateGuestUser returns or creates a guest user for unauthenticated orders
func (r *UserRepository) GetOrCreateGuestUser(ctx context.Context) (*models.User, error) {
	// Try to find guest user by known address
	guestAddress := "0x0000000000000000000000000000000000000000"
	user, err := r.GetByAddress(ctx, guestAddress)
	if err == nil {
		return user, nil
	}

	if !errors.Is(err, gorm.ErrRecordNotFound) {
		return nil, err
	}

	var lastErr error
	for attempt := 0; attempt < 5; attempt++ {
		user = &models.User{
			Address:      guestAddress,
			Username:     "guest",
			ReferralCode: generateReferralCode(),
			Network:      models.NetworkBSC,
			CreatedAt:    time.Now(),
			UpdatedAt:    time.Now(),
		}

		if err := r.Create(ctx, user); err != nil {
			lastErr = err
			if isUniqueConstraintError(err, "users_referral_code_key", "referral_code") {
				continue
			}
			if isUniqueConstraintError(err, "users_address_key", "address") {
				return r.GetByAddress(ctx, guestAddress)
			}
			return nil, err
		}

		return user, nil
	}

	return nil, fmt.Errorf("failed to create guest user after retries: %w", lastErr)
}
