package services

import (
	"fmt"
	"time"

	"github.com/nexus/backend/internal/config"
	"github.com/golang-jwt/jwt/v5"
)

type AuthService struct {
	cfg    *config.Config
	jwtKey []byte
}

type Claims struct {
	UserID  uint   `json:"user_id"`
	Address string `json:"address"`
	Network string `json:"network"`
	jwt.RegisteredClaims
}

func NewAuthService(cfg *config.Config) *AuthService {
	return &AuthService{
		cfg:    cfg,
		jwtKey: []byte(cfg.JWTSecret),
	}
}

// GenerateToken generates a JWT token for a user
func (s *AuthService) GenerateToken(userID uint, address, network string) (string, error) {
	expiration := time.Now().Add(24 * time.Hour)

	claims := &Claims{
		UserID:  userID,
		Address: address,
		Network: network,
		RegisteredClaims: jwt.RegisteredClaims{
			ExpiresAt: jwt.NewNumericDate(expiration),
			IssuedAt:  jwt.NewNumericDate(time.Now()),
			NotBefore: jwt.NewNumericDate(time.Now()),
		},
	}

	token := jwt.NewWithClaims(jwt.SigningMethodHS256, claims)
	return token.SignedString(s.jwtKey)
}

// VerifyToken verifies a JWT token and returns the user ID
func (s *AuthService) VerifyToken(tokenString string) (uint, error) {
	claims := &Claims{}

	token, err := jwt.ParseWithClaims(tokenString, claims, func(token *jwt.Token) (interface{}, error) {
		if _, ok := token.Method.(*jwt.SigningMethodHMAC); !ok {
			return nil, fmt.Errorf("unexpected signing method: %v", token.Header["alg"])
		}
		return s.jwtKey, nil
	})

	if err != nil {
		return 0, err
	}

	if !token.Valid {
		return 0, fmt.Errorf("invalid token")
	}

	return claims.UserID, nil
}

// RefreshToken refreshes a JWT token
func (s *AuthService) RefreshToken(tokenString string) (string, error) {
	claims := &Claims{}

	token, err := jwt.ParseWithClaims(tokenString, claims, func(token *jwt.Token) (interface{}, error) {
		return s.jwtKey, nil
	})

	if err != nil {
		return "", err
	}

	if !token.Valid {
		return "", fmt.Errorf("invalid token")
	}

	// Generate new token with same claims but new expiration
	return s.GenerateToken(claims.UserID, claims.Address, claims.Network)
}

// ValidateToken validates a JWT token without returning claims
func (s *AuthService) ValidateToken(tokenString string) bool {
	_, err := s.VerifyToken(tokenString)
	return err == nil
}
