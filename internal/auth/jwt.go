package auth

import (
	"crypto/rsa"
	"fmt"
	"os"
	"time"

	"github.com/golang-jwt/jwt/v5"
)

// JWTConfig holds JWT validation configuration.
type JWTConfig struct {
	PublicKeyPath string
	Issuer        string
	Audience      string
}

// JWTClaims represents the expected JWT payload.
type JWTClaims struct {
	jwt.RegisteredClaims
	Roles       []string               `json:"roles"`
	Permissions map[string]interface{} `json:"permissions"`
}

// JWTValidator validates JWT tokens.
type JWTValidator struct {
	publicKey *rsa.PublicKey
	issuer    string
	audience  string
}

// NewJWTValidator creates a new JWT validator from config.
func NewJWTValidator(cfg JWTConfig) (*JWTValidator, error) {
	if cfg.PublicKeyPath == "" {
		return nil, nil
	}

	keyData, err := os.ReadFile(cfg.PublicKeyPath)
	if err != nil {
		return nil, fmt.Errorf("read JWT public key: %w", err)
	}

	pubKey, err := jwt.ParseRSAPublicKeyFromPEM(keyData)
	if err != nil {
		return nil, fmt.Errorf("parse JWT public key: %w", err)
	}

	return &JWTValidator{
		publicKey: pubKey,
		issuer:    cfg.Issuer,
		audience:  cfg.Audience,
	}, nil
}

// Validate parses and validates a JWT token, returning the authenticated principal.
func (v *JWTValidator) Validate(tokenString string) (*Principal, error) {
	claims := &JWTClaims{}

	token, err := jwt.ParseWithClaims(tokenString, claims, func(token *jwt.Token) (interface{}, error) {
		if _, ok := token.Method.(*jwt.SigningMethodRSA); !ok {
			return nil, fmt.Errorf("unexpected signing method: %v", token.Header["alg"])
		}
		return v.publicKey, nil
	})
	if err != nil {
		return nil, fmt.Errorf("invalid token: %w", err)
	}
	if !token.Valid {
		return nil, fmt.Errorf("token validation failed")
	}

	// Verify issuer
	if v.issuer != "" {
		iss, _ := claims.GetIssuer()
		if iss != v.issuer {
			return nil, fmt.Errorf("invalid issuer: %s", iss)
		}
	}

	// Verify audience
	if v.audience != "" {
		aud, _ := claims.GetAudience()
		found := false
		for _, a := range aud {
			if a == v.audience {
				found = true
				break
			}
		}
		if !found {
			return nil, fmt.Errorf("invalid audience")
		}
	}

	// Verify not expired
	exp, _ := claims.GetExpirationTime()
	if exp != nil && exp.Before(time.Now()) {
		return nil, fmt.Errorf("token expired")
	}

	// Verify iat not in future
	iat, _ := claims.GetIssuedAt()
	if iat != nil && iat.After(time.Now().Add(1*time.Minute)) {
		return nil, fmt.Errorf("token issued in the future")
	}

	// Determine role (take highest)
	role := "reader"
	if len(claims.Roles) > 0 {
		role = claims.Roles[0]
	}

	sub, _ := claims.GetSubject()

	return &Principal{
		ID:         sub,
		Name:       sub,
		Role:       role,
		AuthMethod: "jwt",
	}, nil
}
