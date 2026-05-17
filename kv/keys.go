// Package kv is the placefs key/value substrate. It wraps cockroachdb/pebble
// so the rest of the code can stay engine-agnostic, and it owns the keyspace
// layout that index + store share. Pebble has no buckets — keys are
// prefix-tagged with a one-byte discriminator instead.
//
// Key scheme:
//
//	'M' next_inode               store-side inode counter (was _l4_meta/next_inode)
//	'n' uint64(inode-be)         per-inode node metadata (was nodes/<inode>)
//	'd' uint64(parent-be) name   dirent (was dirents/<parent><name>)
//	's' uint64(inode-be) uint32(stripe-be)
//	                             stripe fragment list (was stripes/<inode><stripe>)
//
// Single-byte tags are dense and let Pebble do a single-byte prefix scan for
// iteration. Big-endian inode/stripe encoding keeps lexicographic order ==
// numeric order, which matters for the per-inode and per-parent prefix walks
// used by readdir / truncate / DeleteInode / GC.
package kv

import "encoding/binary"

// Single-byte key tags. The grouping is intentional: lowercase = per-record
// data, uppercase = singleton/metadata. Reserve more uppercase letters for
// future singletons; lowercase ones are cheap because they sort together by
// "kind" in a Pebble scan.
const (
	tagStoreMeta    byte = 'M'
	tagNode         byte = 'n'
	tagDirent       byte = 'd'
	tagStripe       byte = 's'
)

// StoreNextInodeKey is the single key holding the next-inode counter.
// Defined as a singleton; the store mutates it under its own coordination.
var StoreNextInodeKey = []byte{tagStoreMeta, 'n', 'i'} // M + "ni" (next_inode)

// NodeKey returns the key for a per-inode node record.
func NodeKey(inode uint64) []byte {
	k := make([]byte, 1+8)
	k[0] = tagNode
	binary.BigEndian.PutUint64(k[1:], inode)
	return k
}

// DirentKey returns the key for a directory entry under parent.
func DirentKey(parent uint64, name string) []byte {
	k := make([]byte, 1+8+len(name))
	k[0] = tagDirent
	binary.BigEndian.PutUint64(k[1:], parent)
	copy(k[9:], name)
	return k
}

// DirentPrefix returns the prefix that covers every dirent under parent.
// Used as Pebble iterator LowerBound; pair with PrefixUpperBound for
// UpperBound.
func DirentPrefix(parent uint64) []byte {
	k := make([]byte, 1+8)
	k[0] = tagDirent
	binary.BigEndian.PutUint64(k[1:], parent)
	return k
}

// DirentParentFromKey extracts the parent inode embedded in a dirent key.
// Caller is responsible for length validation; the helper assumes the key
// came from DirentKey.
func DirentParentFromKey(k []byte) uint64 {
	return binary.BigEndian.Uint64(k[1:9])
}

// DirentNameFromKey returns the trailing name bytes of a dirent key.
func DirentNameFromKey(k []byte) string {
	return string(k[9:])
}

// LegacyStripeKey returns the bbolt-era key that stored every fragment for
// (inode, stripeID) as one concatenated value. Used only by the
// blob-to-per-fragment migration at startup.
func LegacyStripeKey(inode uint64, stripeID uint32) []byte {
	k := make([]byte, 1+8+4)
	k[0] = tagStripe
	binary.BigEndian.PutUint64(k[1:], inode)
	binary.BigEndian.PutUint32(k[9:], stripeID)
	return k
}

// FragmentKey returns the per-fragment key. Length is 21 bytes: the
// trailing 8-byte seq lets concurrent appenders write to distinct keys
// without read-merge-write, which is the entire point of moving away
// from the old blob-per-stripe layout.
func FragmentKey(inode uint64, stripeID uint32, seq uint64) []byte {
	k := make([]byte, 1+8+4+8)
	k[0] = tagStripe
	binary.BigEndian.PutUint64(k[1:], inode)
	binary.BigEndian.PutUint32(k[9:], stripeID)
	binary.BigEndian.PutUint64(k[13:], seq)
	return k
}

// FragmentStripePrefix returns the prefix covering every fragment under
// (inode, stripeID). Pair with PrefixUpperBound to scan one stripe.
//
// Note this is the same byte sequence as LegacyStripeKey — old-format
// blobs sit under their stripe prefix too. Callers can detect format by
// key length: 13 bytes == legacy blob, 21 bytes == per-fragment record.
// The migration deletes the legacy keys at startup so the data path
// only sees the new format.
func FragmentStripePrefix(inode uint64, stripeID uint32) []byte {
	k := make([]byte, 1+8+4)
	k[0] = tagStripe
	binary.BigEndian.PutUint64(k[1:], inode)
	binary.BigEndian.PutUint32(k[9:], stripeID)
	return k
}

// StripePrefix returns the prefix covering every stripe key for inode.
func StripePrefix(inode uint64) []byte {
	k := make([]byte, 1+8)
	k[0] = tagStripe
	binary.BigEndian.PutUint64(k[1:], inode)
	return k
}

// StripeAllPrefix returns the prefix that covers every fragment key in
// the DB. Used by GC, which scans every fragment to build the live-
// segment set.
func StripeAllPrefix() []byte { return []byte{tagStripe} }

// StripeIDFromFragmentKey extracts the stripeID embedded in a fragment key.
func StripeIDFromFragmentKey(k []byte) uint32 {
	return binary.BigEndian.Uint32(k[9:13])
}

// StripeInodeFromFragmentKey extracts the inode embedded in a fragment key.
func StripeInodeFromFragmentKey(k []byte) uint64 {
	return binary.BigEndian.Uint64(k[1:9])
}

// SeqFromFragmentKey extracts the seq embedded in a fragment key.
func SeqFromFragmentKey(k []byte) uint64 {
	return binary.BigEndian.Uint64(k[13:21])
}

// IsFragmentKey reports whether k has the per-fragment length, vs. the
// legacy blob length. Cheap classifier for the iterator paths that may
// observe both shapes during/after migration.
func IsFragmentKey(k []byte) bool { return len(k) == 21 }

// IsLegacyStripeKey reports whether k has the bbolt-era blob length.
func IsLegacyStripeKey(k []byte) bool { return len(k) == 13 }

// PrefixUpperBound returns the smallest byte slice that sorts strictly above
// every key starting with prefix. Used as Pebble iterator UpperBound to
// scope a prefix scan; nil means "no upper bound" (the prefix was all 0xff).
//
// Mechanics: copy prefix, then bump the last byte; if it overflowed, drop
// it and keep bumping. If everything overflows, the prefix is at the very
// end of the keyspace and there's nothing to bound by.
func PrefixUpperBound(prefix []byte) []byte {
	end := make([]byte, len(prefix))
	copy(end, prefix)
	for i := len(end) - 1; i >= 0; i-- {
		end[i]++
		if end[i] != 0 {
			return end[:i+1]
		}
	}
	return nil
}
