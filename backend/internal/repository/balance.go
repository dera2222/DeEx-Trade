package repository

import (
	"context"
	"errors"
	"time"

	"github.com/nexus/backend/internal/db"
	"github.com/nexus/backend/internal/models"
	"github.com/shopspring/decimal"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

type BalanceRepository struct {
	db *db.DB
}

func NewBalanceRepository(db *db.DB, _ *db.RedisClient) *BalanceRepository {
	return &BalanceRepository{db: db}
}

func (r *BalanceRepository) GetByUserID(ctx context.Context, userID uint) ([]models.UserBalance, error) {
	var balances []models.UserBalance
	err := r.db.WithContext(ctx).
		Where("user_id = ?", userID).
		Order("token_mint asc, network asc").
		Find(&balances).Error
	return balances, err
}

func (r *BalanceRepository) GetByUserToken(ctx context.Context, userID uint, network models.Network, tokenMint string) (*models.UserBalance, error) {
	var balance models.UserBalance
	err := r.db.WithContext(ctx).
		Where("user_id = ? AND network = ? AND token_mint = ?", userID, network, tokenMint).
		First(&balance).Error
	return &balance, err
}

func (r *BalanceRepository) EnsureBalance(ctx context.Context, userID uint, network models.Network, tokenMint string) (*models.UserBalance, error) {
	balance := models.UserBalance{
		UserID:    userID,
		Network:   network,
		TokenMint: tokenMint,
		Available: decimal.Zero,
		Locked:    decimal.Zero,
		Total:     decimal.Zero,
		CreatedAt: time.Now(),
		UpdatedAt: time.Now(),
	}

	if err := r.db.WithContext(ctx).
		Where("user_id = ? AND network = ? AND token_mint = ?", userID, network, tokenMint).
		FirstOrCreate(&balance).Error; err != nil {
		return nil, err
	}

	return &balance, nil
}

func (r *BalanceRepository) ReserveFunds(ctx context.Context, userID uint, network models.Network, tokenMint string, amount decimal.Decimal) error {
	if amount.LessThanOrEqual(decimal.Zero) {
		return errors.New("invalid reservation amount")
	}

	tx := r.db.WithContext(ctx).Begin()
	if tx.Error != nil {
		return tx.Error
	}
	defer func() {
		if r := recover(); r != nil {
			tx.Rollback()
		}
	}()

	var balance models.UserBalance
	if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).
		Where("user_id = ? AND network = ? AND token_mint = ?", userID, network, tokenMint).
		First(&balance).Error; err != nil {
		tx.Rollback()
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return errors.New("insufficient balance")
		}
		return err
	}

	if balance.Available.LessThan(amount) {
		tx.Rollback()
		return errors.New("insufficient available balance")
	}

	balance.Available = balance.Available.Sub(amount)
	balance.Locked = balance.Locked.Add(amount)
	balance.Total = balance.Available.Add(balance.Locked)
	balance.UpdatedAt = time.Now()

	if err := tx.Save(&balance).Error; err != nil {
		tx.Rollback()
		return err
	}

	return tx.Commit().Error
}

func (r *BalanceRepository) ReleaseLockedFunds(ctx context.Context, userID uint, network models.Network, tokenMint string, amount decimal.Decimal) error {
	if amount.LessThanOrEqual(decimal.Zero) {
		return errors.New("invalid release amount")
	}

	return r.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		var balance models.UserBalance
		if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).
			Where("user_id = ? AND network = ? AND token_mint = ?", userID, network, tokenMint).
			First(&balance).Error; err != nil {
			return err
		}

		if balance.Locked.LessThan(amount) {
			return errors.New("insufficient locked balance")
		}

		balance.Locked = balance.Locked.Sub(amount)
		balance.Available = balance.Available.Add(amount)
		balance.Total = balance.Available.Add(balance.Locked)
		balance.UpdatedAt = time.Now()

		return tx.Save(&balance).Error
	})
}

func (r *BalanceRepository) CreditAvailable(ctx context.Context, userID uint, network models.Network, tokenMint string, amount decimal.Decimal) error {
	if amount.LessThanOrEqual(decimal.Zero) {
		return errors.New("invalid credit amount")
	}

	return r.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		var balance models.UserBalance
		if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).
			Where("user_id = ? AND network = ? AND token_mint = ?", userID, network, tokenMint).
			First(&balance).Error; err != nil {
			if errors.Is(err, gorm.ErrRecordNotFound) {
				balance = models.UserBalance{
					UserID:    userID,
					Network:   network,
					TokenMint: tokenMint,
					Available: decimal.Zero,
					Locked:    decimal.Zero,
					Total:     decimal.Zero,
					CreatedAt: time.Now(),
					UpdatedAt: time.Now(),
				}
			} else {
				return err
			}
		}

		balance.Available = balance.Available.Add(amount)
		balance.Total = balance.Available.Add(balance.Locked)
		balance.UpdatedAt = time.Now()

		return tx.Save(&balance).Error
	})
}

func (r *BalanceRepository) SettleFillBalances(ctx context.Context, network models.Network, makerUserID, takerUserID uint,
	makerTokenIn, makerTokenOut string, makerLocked, makerCredit decimal.Decimal,
	takerTokenIn, takerTokenOut string, takerLocked, takerCredit decimal.Decimal) error {
	if makerLocked.LessThanOrEqual(decimal.Zero) && makerCredit.LessThanOrEqual(decimal.Zero) &&
		takerLocked.LessThanOrEqual(decimal.Zero) && takerCredit.LessThanOrEqual(decimal.Zero) {
		return errors.New("nothing to settle")
	}

	return r.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		updateBalance := func(userID uint, tokenMint string, lockedDelta, availableDelta decimal.Decimal) error {
			var balance models.UserBalance
			err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).
				Where("user_id = ? AND network = ? AND token_mint = ?", userID, network, tokenMint).
				First(&balance).Error
			if err != nil {
				if errors.Is(err, gorm.ErrRecordNotFound) {
					if availableDelta.LessThanOrEqual(decimal.Zero) {
						return errors.New("balance not found")
					}
					balance = models.UserBalance{
						UserID:    userID,
						Network:   network,
						TokenMint: tokenMint,
						Available: decimal.Zero,
						Locked:    decimal.Zero,
						Total:     decimal.Zero,
						CreatedAt: time.Now(),
						UpdatedAt: time.Now(),
					}
				} else {
					return err
				}
			}

			if lockedDelta.LessThan(decimal.Zero) {
				lockedDelta = lockedDelta.Abs()
				if balance.Locked.LessThan(lockedDelta) {
					return errors.New("insufficient locked balance")
				}
				balance.Locked = balance.Locked.Sub(lockedDelta)
			} else if lockedDelta.GreaterThan(decimal.Zero) {
				balance.Locked = balance.Locked.Add(lockedDelta)
			}

			if availableDelta.GreaterThan(decimal.Zero) {
				balance.Available = balance.Available.Add(availableDelta)
			}

			balance.Total = balance.Available.Add(balance.Locked)
			balance.UpdatedAt = time.Now()

			return tx.Save(&balance).Error
		}

		if makerLocked.GreaterThan(decimal.Zero) {
			if err := updateBalance(makerUserID, makerTokenIn, makerLocked.Neg(), decimal.Zero); err != nil {
				return err
			}
		}
		if makerCredit.GreaterThan(decimal.Zero) {
			if err := updateBalance(makerUserID, makerTokenOut, decimal.Zero, makerCredit); err != nil {
				return err
			}
		}
		if takerLocked.GreaterThan(decimal.Zero) {
			if err := updateBalance(takerUserID, takerTokenIn, takerLocked.Neg(), decimal.Zero); err != nil {
				return err
			}
		}
		if takerCredit.GreaterThan(decimal.Zero) {
			if err := updateBalance(takerUserID, takerTokenOut, decimal.Zero, takerCredit); err != nil {
				return err
			}
		}

		return nil
	})
}
