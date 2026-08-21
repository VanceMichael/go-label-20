package quality

import (
	"context"
	"fmt"
	"math"
	"sort"
	"strings"
	"time"

	"go-base/internal/domain"
)

type Sample struct {
	ID          string
	TenantID    string
	ObjectType  string
	ObjectID    string
	CollectedBy string
	CollectedAt time.Time
	Temperature float64
	Seal        string
	Status      string
}

type Measurement struct {
	ID         string
	SampleID   string
	Metric     string
	Value      float64
	Unit       string
	Method     string
	MeasuredAt time.Time
	LabID      string
}

type Specification struct {
	Metric         string
	Unit           string
	Minimum        *float64
	Maximum        *float64
	RequiredMethod string
	MaxAge         time.Duration
}

type Decision struct {
	SampleID  string
	Passed    bool
	Failures  []Failure
	Warnings  []string
	DecidedAt time.Time
}

type Failure struct {
	Metric   string
	Code     string
	Expected string
	Actual   string
}

type ChainEntry struct {
	Sequence      int
	SampleID      string
	FromActor     string
	ToActor       string
	Location      string
	SealBefore    string
	SealAfter     string
	TransferredAt time.Time
}

type MeasurementCursor interface {
	Next() bool
	Measurement() Measurement
	Err() error
	Close() error
}

type MeasurementSource interface {
	OpenMeasurements(context.Context, string) (MeasurementCursor, error)
}

func CollectMeasurements(ctx context.Context, source MeasurementSource, sampleIDs []string) ([]Measurement, error) {
	if source == nil || len(sampleIDs) == 0 {
		return nil, fmt.Errorf("%w: measurement collection request", domain.ErrInvalid)
	}
	collected := make([]Measurement, 0)
	for _, sampleID := range sampleIDs {
		if sampleID == "" {
			return nil, fmt.Errorf("%w: sample ID", domain.ErrInvalid)
		}
		cursor, err := source.OpenMeasurements(ctx, sampleID)
		if err != nil {
			return nil, fmt.Errorf("open measurements for %s: %w", sampleID, err)
		}
		defer cursor.Close()
		for cursor.Next() {
			collected = append(collected, cursor.Measurement())
		}
		if err := cursor.Err(); err != nil {
			return nil, fmt.Errorf("read measurements for %s: %w", sampleID, err)
		}
	}
	return collected, nil
}

func (sample Sample) Validate(now time.Time) error {
	if sample.ID == "" || sample.TenantID == "" || sample.ObjectType == "" || sample.ObjectID == "" || sample.CollectedBy == "" {
		return fmt.Errorf("%w: sample identity", domain.ErrInvalid)
	}
	if sample.CollectedAt.After(now.Add(2*time.Minute)) || now.Sub(sample.CollectedAt) > 30*24*time.Hour {
		return fmt.Errorf("%w: sample collection time", domain.ErrInvalid)
	}
	if strings.TrimSpace(sample.Seal) == "" {
		return fmt.Errorf("%w: sample seal", domain.ErrInvalid)
	}
	if sample.Temperature < -20 || sample.Temperature > 80 {
		return fmt.Errorf("%w: sample temperature", domain.ErrInvalid)
	}
	return nil
}

func Evaluate(sample Sample, measurements []Measurement, specifications []Specification, now time.Time) (Decision, error) {
	if err := sample.Validate(now); err != nil {
		return Decision{}, err
	}
	decision := Decision{SampleID: sample.ID, Passed: true, DecidedAt: now}
	byMetric := make(map[string][]Measurement)
	for _, measurement := range measurements {
		if measurement.SampleID != sample.ID {
			continue
		}
		if measurement.ID == "" || measurement.Metric == "" || measurement.Unit == "" || measurement.Method == "" || measurement.LabID == "" {
			return Decision{}, fmt.Errorf("%w: quality measurement", domain.ErrInvalid)
		}
		if math.IsNaN(measurement.Value) || math.IsInf(measurement.Value, 0) {
			return Decision{}, fmt.Errorf("%w: non-finite quality measurement", domain.ErrInvalid)
		}
		byMetric[measurement.Metric] = append(byMetric[measurement.Metric], measurement)
	}
	for _, specification := range specifications {
		if specification.Metric == "" || specification.Unit == "" {
			return Decision{}, fmt.Errorf("%w: quality specification", domain.ErrInvalid)
		}
		candidates := byMetric[specification.Metric]
		sort.SliceStable(candidates, func(i, j int) bool {
			if candidates[i].MeasuredAt.Equal(candidates[j].MeasuredAt) {
				return candidates[i].ID < candidates[j].ID
			}
			return candidates[i].MeasuredAt.After(candidates[j].MeasuredAt)
		})
		if len(candidates) == 0 {
			decision.Passed = false
			decision.Failures = append(decision.Failures, Failure{Metric: specification.Metric, Code: "missing", Expected: "a current measurement", Actual: "none"})
			continue
		}
		measurement := candidates[0]
		if measurement.Unit != specification.Unit {
			decision.Passed = false
			decision.Failures = append(decision.Failures, Failure{Metric: specification.Metric, Code: "unit_mismatch", Expected: specification.Unit, Actual: measurement.Unit})
			continue
		}
		if specification.RequiredMethod != "" && measurement.Method != specification.RequiredMethod {
			decision.Passed = false
			decision.Failures = append(decision.Failures, Failure{Metric: specification.Metric, Code: "method_mismatch", Expected: specification.RequiredMethod, Actual: measurement.Method})
			continue
		}
		if specification.MaxAge > 0 && now.Sub(measurement.MeasuredAt) > specification.MaxAge {
			decision.Passed = false
			decision.Failures = append(decision.Failures, Failure{Metric: specification.Metric, Code: "expired", Expected: specification.MaxAge.String(), Actual: now.Sub(measurement.MeasuredAt).String()})
			continue
		}
		if specification.Minimum != nil && measurement.Value < *specification.Minimum {
			decision.Passed = false
			decision.Failures = append(decision.Failures, Failure{Metric: specification.Metric, Code: "below_minimum", Expected: fmt.Sprintf(">= %.3f", *specification.Minimum), Actual: fmt.Sprintf("%.3f", measurement.Value)})
		}
		if specification.Maximum != nil && measurement.Value > *specification.Maximum {
			decision.Passed = false
			decision.Failures = append(decision.Failures, Failure{Metric: specification.Metric, Code: "above_maximum", Expected: fmt.Sprintf("<= %.3f", *specification.Maximum), Actual: fmt.Sprintf("%.3f", measurement.Value)})
		}
		if len(candidates) > 1 && candidates[0].MeasuredAt.Equal(candidates[1].MeasuredAt) && candidates[0].Value != candidates[1].Value {
			decision.Warnings = append(decision.Warnings, "conflicting measurements at the same timestamp for "+specification.Metric)
		}
	}
	sort.Slice(decision.Failures, func(i, j int) bool {
		if decision.Failures[i].Metric == decision.Failures[j].Metric {
			return decision.Failures[i].Code < decision.Failures[j].Code
		}
		return decision.Failures[i].Metric < decision.Failures[j].Metric
	})
	sort.Strings(decision.Warnings)
	return decision, nil
}

func VerifyChain(sample Sample, entries []ChainEntry) error {
	if len(entries) == 0 {
		return fmt.Errorf("%w: custody chain is empty", domain.ErrInvalid)
	}
	ordered := append([]ChainEntry(nil), entries...)
	sort.SliceStable(ordered, func(i, j int) bool { return ordered[i].Sequence < ordered[j].Sequence })
	seal := sample.Seal
	var previous time.Time
	for i, entry := range ordered {
		if entry.Sequence != i+1 || entry.SampleID != sample.ID || entry.FromActor == "" || entry.ToActor == "" || entry.Location == "" {
			return fmt.Errorf("%w: custody entry %d", domain.ErrInvalid, i+1)
		}
		if entry.SealBefore != seal {
			return fmt.Errorf("%w: custody seal changed before entry %d", domain.ErrConflict, entry.Sequence)
		}
		if strings.TrimSpace(entry.SealAfter) == "" {
			return fmt.Errorf("%w: empty custody seal after entry %d", domain.ErrInvalid, entry.Sequence)
		}
		if !previous.IsZero() && entry.TransferredAt.Before(previous) {
			return fmt.Errorf("%w: custody time moved backwards", domain.ErrConflict)
		}
		seal = entry.SealAfter
		previous = entry.TransferredAt
	}
	return nil
}

func MergeMeasurements(existing, incoming []Measurement) ([]Measurement, error) {
	byID := make(map[string]Measurement, len(existing)+len(incoming))
	for _, measurement := range existing {
		if measurement.ID == "" {
			return nil, fmt.Errorf("%w: measurement ID", domain.ErrInvalid)
		}
		byID[measurement.ID] = measurement
	}
	for _, measurement := range incoming {
		if measurement.ID == "" {
			return nil, fmt.Errorf("%w: measurement ID", domain.ErrInvalid)
		}
		if current, exists := byID[measurement.ID]; exists {
			if current.SampleID != measurement.SampleID || current.Metric != measurement.Metric {
				return nil, fmt.Errorf("%w: measurement identity reuse", domain.ErrConflict)
			}
			if measurement.MeasuredAt.Before(current.MeasuredAt) {
				continue
			}
		}
		byID[measurement.ID] = measurement
	}
	result := make([]Measurement, 0, len(byID))
	for _, measurement := range byID {
		result = append(result, measurement)
	}
	sort.Slice(result, func(i, j int) bool {
		if result[i].MeasuredAt.Equal(result[j].MeasuredAt) {
			return result[i].ID < result[j].ID
		}
		return result[i].MeasuredAt.Before(result[j].MeasuredAt)
	})
	return result, nil
}
