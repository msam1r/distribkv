package db

import (
	bolt "go.etcd.io/bbolt"
)

type Database struct {
	db *bolt.DB
}

var (
	defaultBucket = []byte("default")
)

// NewDatabase returns an instance of our database we can work with.
func NewDatabase(dbPath string) (db *Database, closeFunc func() error, err error) {
	boltDb, err := bolt.Open(dbPath, 0600, nil)
	if err != nil {
		return nil, nil, err
	}

	db = &Database{db: boltDb}
	closeFunc = boltDb.Close

	if err := db.createDefaultBucket(); err != nil {
		closeFunc()
		return nil, nil, err
	}

	return db, closeFunc, nil
}

func (d *Database) createDefaultBucket() error {
	return d.db.Update(func(t *bolt.Tx) error {
		da, err := t.CreateBucketIfNotExists(defaultBucket)
		_ = da
		return err
	})
}

func (d *Database) Set(key string, value []byte) error {
	return d.db.Update(func(t *bolt.Tx) error {
		b := t.Bucket(defaultBucket)
		return b.Put([]byte(key), value)
	})
}

func (d *Database) Get(key string) ([]byte, error) {
	var result []byte

	err := d.db.View(func(t *bolt.Tx) error {
		b := t.Bucket(defaultBucket)
		result = b.Get([]byte(key))
		return nil
	})

	if err != nil {
		return nil, err
	}

	return result, err
}

// Delete removes the given key from the database.
func (d *Database) Delete(key string) error {
	return d.db.Update(func(t *bolt.Tx) error {
		b := t.Bucket(defaultBucket)
		return b.Delete([]byte(key))
	})
}

type extraFunc func(key string) bool

// DeleteExtraKeys removes all the keys that are not in the current shard.
func (d *Database) DeleteExtraKeys(isExtra extraFunc) error {
	var keys []string

	err := d.db.View(func(t *bolt.Tx) error {
		b := t.Bucket(defaultBucket)

		b.ForEach(func(k, v []byte) error {
			key := string(k)
			if isExtra(key) {
				keys = append(keys, key)
			}
			return nil
		})
		return nil
	})

	for _, key := range keys {
		d.Delete(key)
	}

	return err
}
