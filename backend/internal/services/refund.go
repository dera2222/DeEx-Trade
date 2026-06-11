package services

import (
	"context"
	"fmt"
	"math"
	"sync"
	"time"

	"github.com/nexus/backend/internal/config"
	"github.com/nexus/backend/internal/models"
	"github.com/nexus/backend/internal/repository"
	"github.com/shopspring/decimal"
)

type RefundService struct {
	cfg               *config.Config
	refundRepo        *repository.RefundRepository
	depositRepo       *repository.DepositRepository
	fillRepo          *repository.FillRepository
	solanaSettlement  *SolanaSettlementService
	queue             chan *models.RefundRequest
	quitChan          chan struct{}
	wg                sync.WaitGroup
	maxRetries        int
	batchSize         int
	processingDelay   time.Duration
}

type RefundResult struct {
	RefundID uint
	Success  bool
	TxHash   string
	Error    error
}

// NewRefundService creates a new refund service
func NewRefundService(cfg *config.Config, refundRepo *repository.RefundRepository, depositRepo *repository.DepositRepository, fillRepo *repository.FillRepository, solanaSettlement *SolanaSettlementService) *RefundService {
	return &RefundService{
		cfg:              cfg,
		refundRepo:       refundRepo,
		depositRepo:      depositRepo,
		fillRepo:         fillRepo,
		solanaSettlement: solanaSettlement,
		queue:            make(chan *models.RefundRequest, 1000), // Buffer for 1000 refund requests
		quitChan:         make(chan struct{}),
		maxRetries:       3,
		batchSize:        10,   // Process 10 refunds at a time
		processingDelay:  2 * time.Second, // Delay between batches
	}
}

// Start starts the refund processing worker
func (s *RefundService) Start(ctx context.Context) {
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		s.runWorker(ctx)
	}()

	fmt.Println("[RefundService] Started refund processing worker")
}

// Stop stops the refund processing worker
func (s *RefundService) Stop() {
	close(s.quitChan)
	s.wg.Wait()
	fmt.Println("[RefundService] Stopped refund processing worker")
}

// QueueRefund queues a refund request for processing
func (s *RefundService) QueueRefund(ctx context.Context, refund *models.RefundRequest) error {
	// Create the refund request in database first
	if err := s.refundRepo.Create(ctx, refund); err != nil {
		return fmt.Errorf("failed to create refund request: %w", err)
	}

	// Queue for processing
	select {
	case s.queue <- refund:
		fmt.Printf("[RefundService] Queued refund for order %d, amount %s\n", refund.OrderID, refund.Amount.String())
		return nil
	case <-ctx.Done():
		return ctx.Err()
	default:
		// Queue is full, but refund is still created in DB for later processing
		fmt.Printf("[RefundService] Queue full, refund %d created but queued for later processing\n", refund.ID)
		return nil
	}
}

// QueueRefundBatch queues multiple refund requests for processing
func (s *RefundService) QueueRefundBatch(ctx context.Context, refunds []*models.RefundRequest) error {
	for _, refund := range refunds {
		if err := s.QueueRefund(ctx, refund); err != nil {
			return err
		}
	}
	return nil
}

// runWorker runs the main refund processing loop
func (s *RefundService) runWorker(ctx context.Context) {
	ticker := time.NewTicker(s.processingDelay)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-s.quitChan:
			return
		case refund := <-s.queue:
			s.processRefund(ctx, refund)
		case <-ticker.C:
			// Process any pending refunds from database
			s.processPendingRefunds(ctx)
		}
	}
}

// processRefund processes a single refund request
func (s *RefundService) processRefund(ctx context.Context, refund *models.RefundRequest) {
	// Update status to processing
	if err := s.refundRepo.UpdateStatus(ctx, refund.ID, models.RefundStatusProcessing, "", ""); err != nil {
		fmt.Printf("[RefundService] Failed to update refund %d to processing: %v\n", refund.ID, err)
		return
	}

	// Execute the refund transfer
	txHash, err := s.executeRefund(ctx, refund)
	if err != nil {
		fmt.Printf("[RefundService] Refund %d failed: %v\n", refund.ID, err)

		// Increment retry count
		if incErr := s.refundRepo.IncrementRetryCount(ctx, refund.ID); incErr != nil {
			fmt.Printf("[RefundService] Failed to increment retry count for refund %d: %v\n", refund.ID, incErr)
		}

		// Check if we should retry or mark as failed
		if refund.RetryCount >= s.maxRetries-1 {
			if updateErr := s.refundRepo.UpdateStatus(ctx, refund.ID, models.RefundStatusFailed, "", err.Error()); updateErr != nil {
				fmt.Printf("[RefundService] Failed to mark refund %d as failed: %v\n", refund.ID, updateErr)
			}
		} else {
			// Reset status to pending for retry
			if resetErr := s.refundRepo.UpdateStatus(ctx, refund.ID, models.RefundStatusPending, "", ""); resetErr != nil {
				fmt.Printf("[RefundService] Failed to reset refund %d to pending: %v\n", refund.ID, resetErr)
			}
		}
		return
	}

	// Mark as completed
	if err := s.refundRepo.UpdateStatus(ctx, refund.ID, models.RefundStatusCompleted, txHash, ""); err != nil {
		fmt.Printf("[RefundService] Failed to mark refund %d as completed: %v\n", refund.ID, err)
		return
	}

	fmt.Printf("[RefundService] Refund %d completed successfully, tx: %s\n", refund.ID, txHash)
}

// processPendingRefunds processes refunds that are stuck in pending state
func (s *RefundService) processPendingRefunds(ctx context.Context) {
	refunds, err := s.refundRepo.GetPending(ctx, s.batchSize)
	if err != nil {
		fmt.Printf("[RefundService] Failed to get pending refunds: %v\n", err)
		return
	}

	if len(refunds) == 0 {
		return
	}

	fmt.Printf("[RefundService] Processing %d pending refunds\n", len(refunds))

	for _, refund := range refunds {
		select {
		case s.queue <- &refund:
			// Successfully queued
		case <-ctx.Done():
			return
		default:
			// Queue is full, process synchronously
			s.processRefund(ctx, &refund)
		}
	}
}

// executeRefund executes the actual token transfer for a refund
func (s *RefundService) executeRefund(ctx context.Context, refund *models.RefundRequest) (string, error) {
	if refund.Network != models.NetworkSolana {
		return "", fmt.Errorf("refund service only supports Solana network")
	}

	fmt.Printf("[RefundService] Executing refund of %s %s to %s\n",
		refund.Amount.String(), refund.TokenMint, refund.UserAddress)

	// Fetch token decimals and convert UI amount to raw token units (lamports, etc.)
	decimals, err := s.solanaSettlement.GetTokenDecimals(ctx, refund.TokenMint)
	if err != nil {
		return "", fmt.Errorf("failed to get token decimals: %w", err)
	}

	multiplier := decimal.NewFromFloat(math.Pow10(decimals))
	rawAmount := refund.Amount.Mul(multiplier)

	// Use the existing SolanaSettlementService transfer logic
	txHash, err := s.solanaSettlement.transferTokenToUser(ctx, refund.TokenMint, rawAmount, refund.UserAddress)
	if err != nil {
		return "", fmt.Errorf("failed to transfer tokens: %w", err)
	}

	return txHash, nil
}

// CreateRefundForOrder creates a refund request for an order's deposited amount.
// Used for both expired orders (called by the expiry processor) and cancelled orders (called by CancelOrder handler).
func (s *RefundService) CreateRefundForOrder(ctx context.Context, order *models.Order) error {
	if order.Network != models.NetworkSolana {
		// Skip non-Solana orders — only Solana uses the custody refund model
		return nil
	}

	// ── Step 1: Determine the refundable amount ──────────────────────────────
	// Priority:
	//   1. order.DepositAmount  — explicitly set when the deposit was linked to the order
	//   2. order.AmountIn       — the tokens the user locked when placing the order
	//                            (buy order → quote token; sell order → base token)
	// Track exact token amounts spent across all fills.
	var refundAmount decimal.Decimal

	spent := decimal.Zero
	if s.fillRepo != nil {
		fills, err := s.fillRepo.GetByOrderID(ctx, order.ID)
		if err == nil {
			for _, f := range fills {
				if order.Side == models.OrderSideBuy {
					spent = spent.Add(f.AmountIn) // Quote token spent
				} else {
					spent = spent.Add(f.Amount) // Base token spent
				}
			}
		} else {
			fmt.Printf("[RefundService] Warning: failed to fetch fills for order %d: %v\n", order.ID, err)
		}
	}

	if order.DepositAmount.GreaterThan(decimal.Zero) {
		refundAmount = order.DepositAmount.Sub(spent)
	} else if order.AmountIn.GreaterThan(decimal.Zero) {
		refundAmount = order.AmountIn.Sub(spent)
	}

	if refundAmount.LessThanOrEqual(decimal.Zero) {
		fmt.Printf("[RefundService] No refund needed for order %d (zero refund amount — fully filled or no deposit recorded, spent: %s)\n", order.ID, spent.String())
		return nil
	}

	// ── Step 2: Resolve the token mint ───────────────────────────────────────
	// Priority: deposit_token_mint → token_in → deposit table lookup
	// DepositTokenMint is now a *string — dereference safely.
	var tokenMint string
	if order.DepositTokenMint != nil {
		tokenMint = *order.DepositTokenMint
	}
	userAddress := order.Maker

	if tokenMint == "" {
		// token_in is the token the user deposited (quote for buys, base for sells)
		tokenMint = order.TokenIn
	}

	// If still unresolved, look up in the SolanaDeposit table
	if tokenMint == "" {
		var deposit *models.SolanaDeposit
		var lookupErr error

		depositTxHash := ""
		if order.DepositTxHash != nil {
			depositTxHash = *order.DepositTxHash
		}
		depositMemo := ""
		if order.DepositMemo != nil {
			depositMemo = *order.DepositMemo
		}

		if depositTxHash != "" {
			deposit, lookupErr = s.depositRepo.GetByTxHash(ctx, depositTxHash)
		} else if depositMemo != "" {
			deposit, lookupErr = s.depositRepo.GetByMemo(ctx, depositMemo)
		}

		if lookupErr != nil {
			return fmt.Errorf("failed to lookup deposit for order %d: %w", order.ID, lookupErr)
		}
		if deposit != nil {
			tokenMint = deposit.TokenMint
			userAddress = deposit.UserAddress
			fmt.Printf("[RefundService] Found deposit record for order %d: token_mint=%s, user_address=%s\n",
				order.ID, tokenMint, userAddress)
		}
	}

	if tokenMint == "" {
		return fmt.Errorf("could not determine token mint for order %d refund", order.ID)
	}

	// ── Step 3: Idempotency check ─────────────────────────────────────────────
	// Only skip if there is already a pending, processing, or completed refund.
	// A *failed* refund must be retried, so we don't skip on that status.
	existingRefunds, err := s.refundRepo.GetByOrderID(ctx, order.ID)
	if err == nil {
		for _, r := range existingRefunds {
			if r.Status == models.RefundStatusPending ||
				r.Status == models.RefundStatusProcessing ||
				r.Status == models.RefundStatusCompleted {
				fmt.Printf("[RefundService] Refund already %s for order %d — skipping duplicate\n", r.Status, order.ID)
				return nil
			}
		}
	}

	// ── Step 4: Create and queue the refund ─────────────────────────────────
	refund := &models.RefundRequest{
		OrderID:     order.ID,
		UserID:      order.UserID,
		Network:     order.Network,
		TokenMint:   tokenMint,
		Amount:      refundAmount,
		UserAddress: userAddress,
		Status:      models.RefundStatusPending,
		CreatedAt:   time.Now(),
		UpdatedAt:   time.Now(),
	}

	fmt.Printf("[RefundService] Creating refund for order %d: token=%s, amount=%s, to=%s\n",
		order.ID, tokenMint, refundAmount.String(), userAddress)

	return s.QueueRefund(ctx, refund)
}

// GetStats returns refund processing statistics
func (s *RefundService) GetStats(ctx context.Context) (map[string]interface{}, error) {
	stats, err := s.refundRepo.GetStats(ctx)
	if err != nil {
		return nil, err
	}

	return map[string]interface{}{
		"queue_size": len(s.queue),
		"stats":      stats,
	}, nil
}