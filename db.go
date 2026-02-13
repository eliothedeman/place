package place

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log/slog"

	"github.com/dgraph-io/badger/v4"
)

type DB struct {
	db *badger.DB
}

func kind(t any, key string) []byte {
	var buff bytes.Buffer
	fmt.Fprintf(&buff, "%T://", t)
	fmt.Fprintf(&buff, "%s", key)
	return buff.Bytes()
}

func get[T any](db *badger.Txn, key string) *T {
	out := new(T)
	kkey := kind(out, key)
	itm, err := db.Get(kkey)
	if err != nil {
		slog.Error("Unable to get key", "err", err, "key", key)
		return nil
	}
	err = itm.Value(func(val []byte) error {
		err := json.Unmarshal(val, &out)
		if err != nil {
			slog.Error("Unable to decode value", "err", err, "key", key)
		}
		return err
	})
	if err != nil {
		return nil
	}
	return out
}

func set[T any](db *badger.Txn, key string, value *T) error {
	buff, err := json.Marshal(value)
	if err != nil {
		slog.Error("Unable to marshal value", "err", err, "key", key, "value", value)
		return err
	}
	kkey := kind(value, key)
	return db.Set(kkey, buff)
}
