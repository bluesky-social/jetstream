package world

import (
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"

	"github.com/cockroachdb/pebble"
	"github.com/jcalabro/atmos"
	"github.com/jcalabro/atmos/crypto"
)

// account is the in-memory shape of a single simulated user's
// identity. Repo state is loaded lazily by repos.go.
type account struct {
	Index        int
	DID          atmos.DID
	PrivKeyBytes []byte // 32-byte k256 private key
	priv         *crypto.K256PrivateKey
}

// deriveAccount produces a deterministic account from a global seed
// and account index. Same (seed, idx) always returns the same DID +
// signing key, regardless of OS or compilation flags.
//
// k256 keygen requires a 32-byte scalar in [1, n-1]. We derive a
// candidate via SHA-256(seed_bytes || idx_bytes || counter) and retry
// until atmos accepts it. With overwhelming probability the first
// candidate works.
func deriveAccount(seed uint64, idx int) (account, error) {
	for counter := range 256 {
		raw := deriveScalar(seed, idx, counter)
		priv, err := crypto.ParsePrivateK256(raw)
		if err != nil {
			continue
		}
		k256Pub, ok := priv.PublicKey().(*crypto.K256PublicKey)
		if !ok {
			return account{}, errors.New("world: ParsePrivateK256 returned non-K256 public key")
		}
		pubBytes := k256Pub.Bytes()
		didStr := didFromPubkey(pubBytes)
		did, err := atmos.ParseDID(didStr)
		if err != nil {
			return account{}, fmt.Errorf("world: derived DID rejected: %w", err)
		}
		return account{
			Index:        idx,
			DID:          did,
			PrivKeyBytes: raw,
			priv:         priv,
		}, nil
	}
	return account{}, errors.New("world: failed to derive valid k256 key after 256 attempts")
}

func deriveScalar(seed uint64, idx, counter int) []byte {
	h := sha256.New()
	var seedBuf [8]byte
	binary.BigEndian.PutUint64(seedBuf[:], seed)
	h.Write(seedBuf[:])
	var idxBuf [8]byte
	binary.BigEndian.PutUint64(idxBuf[:], uint64(idx))
	h.Write(idxBuf[:])
	var ctrBuf [4]byte
	binary.BigEndian.PutUint32(ctrBuf[:], uint32(counter))
	h.Write(ctrBuf[:])
	return h.Sum(nil)
}

// saveAccount writes (key, did) for an account; safe to call inside a
// pebble batch via batch.Set, but here we use the db directly.
func (w *World) saveAccount(b *pebble.Batch, a account) error {
	if err := b.Set(keyAccountKey(a.Index), a.PrivKeyBytes, nil); err != nil {
		return fmt.Errorf("world: save account key: %w", err)
	}
	if err := b.Set(keyAccountDID(a.Index), []byte(a.DID), nil); err != nil {
		return fmt.Errorf("world: save account did: %w", err)
	}
	return nil
}

// loadAccount reads (key, did) for an account.
func (w *World) loadAccount(idx int) (account, error) {
	keyVal, kc, err := w.db.Get(keyAccountKey(idx))
	if err != nil {
		return account{}, fmt.Errorf("world: load account %d key: %w", idx, err)
	}
	defer func() { _ = kc.Close() }()
	priv, err := crypto.ParsePrivateK256(keyVal)
	if err != nil {
		return account{}, fmt.Errorf("world: parse account %d key: %w", idx, err)
	}
	didVal, dc, err := w.db.Get(keyAccountDID(idx))
	if err != nil {
		return account{}, fmt.Errorf("world: load account %d did: %w", idx, err)
	}
	defer func() { _ = dc.Close() }()
	did, err := atmos.ParseDID(string(didVal))
	if err != nil {
		return account{}, fmt.Errorf("world: parse account %d did: %w", idx, err)
	}
	return account{
		Index:        idx,
		DID:          did,
		PrivKeyBytes: append([]byte(nil), keyVal...),
		priv:         priv,
	}, nil
}
