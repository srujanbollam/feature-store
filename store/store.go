package store

import (
	"fmt"

	bbolt "go.etcd.io/bbolt"
)

// FeatureStore wraps a BoltDB connection.
type FeatureStore struct {
	db *bbolt.DB
}

const bucketName = "features"

// NewFeatureStore opens (or creates) the database file at the given path
// and ensures the features bucket exists.
func NewFeatureStore(path string) (*FeatureStore, error) {
	db, err := bbolt.Open(path, 0600, nil)
	if err != nil {
		return nil, fmt.Errorf("open db: %w", err)
	}

	err = db.Update(func(tx *bbolt.Tx) error {
		_, err := tx.CreateBucketIfNotExists([]byte(bucketName))
		return err
	})
	if err != nil {
		return nil, fmt.Errorf("create bucket: %w", err)
	}

	return &FeatureStore{db: db}, nil
}

// Set stores a key-value pair.
func (s *FeatureStore) Set(key, value string) error {
	return s.db.Update(func(tx *bbolt.Tx) error {
		bucket := tx.Bucket([]byte(bucketName))
		return bucket.Put([]byte(key), []byte(value))
	})
}

// Get retrieves a value by key.
func (s *FeatureStore) Get(key string) (string, error) {
	var result string
	err := s.db.View(func(tx *bbolt.Tx) error {
		bucket := tx.Bucket([]byte(bucketName))
		value := bucket.Get([]byte(key))
		if value == nil {
			return fmt.Errorf("key not found: %s", key)
		}
		result = string(value)
		return nil
	})
	return result, err
}

// Delete removes a key.
func (s *FeatureStore) Delete(key string) error {
	return s.db.Update(func(tx *bbolt.Tx) error {
		bucket := tx.Bucket([]byte(bucketName))
		return bucket.Delete([]byte(key))
	})
}

// Close closes the database connection.
func (s *FeatureStore) Close() error {
	return s.db.Close()
}