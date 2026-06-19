package ml

import (
	"fmt"
	"strconv"

	"feature-store/store"
)

// RawFeatures represents unprocessed input data for a single user.
type RawFeatures struct {
	UserID string
	Age    float64
	Income float64
	Clicks float64
}

// bounds defines the expected min/max range for a feature, used for
// min-max normalization. These are reasonable defaults for demo data;
// in a real system these would be computed from historical data.
type bounds struct {
	min float64
	max float64
}

var fieldBounds = map[string]bounds{
	"age":    {min: 0, max: 100},
	"income": {min: 0, max: 200000},
	"clicks": {min: 0, max: 1000},
}

// Pipeline ingests raw features, normalizes them, and writes them to
// the store.
type Pipeline struct {
	store *store.FeatureStore
}

// NewPipeline creates a Pipeline backed by the given store.
func NewPipeline(s *store.FeatureStore) *Pipeline {
	return &Pipeline{store: s}
}

// normalize scales a value to the range [0, 1] given a min and max.
func normalize(value float64, b bounds) float64 {
	if b.max == b.min {
		return 0
	}
	return (value - b.min) / (b.max - b.min)
}

// Ingest normalizes each field in raw and writes it to the store under
// a key of the form feature:<user_id>:<field>.
func (p *Pipeline) Ingest(raw RawFeatures) error {
	fields := map[string]float64{
		"age":    raw.Age,
		"income": raw.Income,
		"clicks": raw.Clicks,
	}

	for field, value := range fields {
		normalized := normalize(value, fieldBounds[field])
		key := fmt.Sprintf("feature:%s:%s", raw.UserID, field)
		strValue := strconv.FormatFloat(normalized, 'f', 4, 64)

		if err := p.store.Set(key, strValue); err != nil {
			return fmt.Errorf("store feature %s: %w", key, err)
		}
	}

	return nil
}