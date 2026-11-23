package app

import (
	"crypto/rand"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"sync"
	"time"

	"github.com/google/uuid"
)

// MessageID is a version 1 UUID, stored in the ordered-time layout of the PHP server
// (ramsey's UuidBinaryOrderedTimeType): the time's high, middle and low parts first, so
// ids sort by the time they were made.
type MessageID [16]byte

// UUIDv1Pattern is a lowercase version 1 UUID.
const UUIDv1Pattern = `[0-9a-f]{8}-[0-9a-f]{4}-1[0-9a-f]{3}-[0-9a-f]{4}-[0-9a-f]{12}`

var (
	idMu   sync.Mutex
	idNode []byte
)

// NewMessageID makes a new id: the time now, a random clock sequence and this node.
func NewMessageID() MessageID {
	idMu.Lock()
	if idNode == nil {
		idNode = make([]byte, 6)
		rand.Read(idNode)
		idNode[0] |= 0x01 // a random node id, as RFC 4122 marks one
		uuid.SetNodeID(idNode)
	}
	idMu.Unlock()
	u, err := uuid.NewUUID()
	if err != nil {
		panic(err)
	}
	return MessageID(u)
}

// MessageIDAt is the smallest id of a second: what an id range starts at.
func MessageIDAt(t time.Time) MessageID {
	// 100-nanosecond intervals since 1582-10-15, the UUID epoch.
	ticks := uint64(t.Unix())*10_000_000 + uint64(t.Nanosecond()/100) + 0x01B21DD213814000
	var u MessageID
	binary.BigEndian.PutUint32(u[0:4], uint32(ticks))
	binary.BigEndian.PutUint16(u[4:6], uint16(ticks>>32))
	binary.BigEndian.PutUint16(u[6:8], uint16(ticks>>48)&0x0fff|0x1000)
	u[8] = 0x80 // variant, clock sequence 0
	return u
}

// ParseMessageID reads a version 1 UUID; ok is false for anything else.
func ParseMessageID(s string) (MessageID, bool) {
	u, err := uuid.Parse(s)
	if err != nil || u.Version() != 1 || len(s) != 36 {
		return MessageID{}, false
	}
	return MessageID(u), true
}

func (id MessageID) String() string { return uuid.UUID(id).String() }

// Bytes are the id as stored.
func (id MessageID) Bytes() []byte {
	b := make([]byte, 16)
	copy(b[0:2], id[6:8])
	copy(b[2:4], id[4:6])
	copy(b[4:8], id[0:4])
	copy(b[8:16], id[8:16])
	return b
}

// MessageIDFromBytes reads a stored id.
func MessageIDFromBytes(b []byte) (MessageID, error) {
	var id MessageID
	if len(b) != 16 {
		return id, fmt.Errorf("message id of %d bytes", len(b))
	}
	copy(id[6:8], b[0:2])
	copy(id[4:6], b[2:4])
	copy(id[0:4], b[4:8])
	copy(id[8:16], b[8:16])
	return id, nil
}

// RandomHex is n random bytes as hex.
func RandomHex(n int) string {
	b := make([]byte, n)
	rand.Read(b)
	return hex.EncodeToString(b)
}

// SecretHash is how a key's secret is kept: a secret is 192 random bits, so a plain
// SHA-256 is enough; there is nothing to brute-force that a slow hash would protect.
func SecretHash(secret string) string {
	h := sha256.Sum256([]byte(secret))
	return hex.EncodeToString(h[:])
}
