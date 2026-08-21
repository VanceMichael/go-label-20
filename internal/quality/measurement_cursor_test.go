package quality

import (
	"context"
	"errors"
	"testing"
)

var (
	errLabCapacity = errors.New("lab cursor capacity exhausted")
	errLabRead     = errors.New("lab cursor read failed")
	errLabClose    = errors.New("lab cursor close failed")
)

type cursorSpec struct {
	measurements []Measurement
	readErr      error
	closeErr     error
}

type fakeMeasurementSource struct {
	specs     map[string]cursorSpec
	active    int
	maxActive int
	maxSeen   int
}

func (source *fakeMeasurementSource) OpenMeasurements(_ context.Context, sampleID string) (MeasurementCursor, error) {
	if source.active >= source.maxActive {
		return nil, errLabCapacity
	}
	spec := source.specs[sampleID]
	source.active++
	if source.active > source.maxSeen {
		source.maxSeen = source.active
	}
	return &fakeMeasurementCursor{source: source, spec: spec}, nil
}

type fakeMeasurementCursor struct {
	source *fakeMeasurementSource
	spec   cursorSpec
	index  int
	closed bool
}

func (cursor *fakeMeasurementCursor) Next() bool {
	if cursor.index >= len(cursor.spec.measurements) {
		return false
	}
	cursor.index++
	return true
}

func (cursor *fakeMeasurementCursor) Measurement() Measurement {
	return cursor.spec.measurements[cursor.index-1]
}

func (cursor *fakeMeasurementCursor) Err() error { return cursor.spec.readErr }

func (cursor *fakeMeasurementCursor) Close() error {
	if !cursor.closed {
		cursor.closed = true
		cursor.source.active--
	}
	return cursor.spec.closeErr
}

func TestMeasurementCollectionReleasesEachLabCursorAndPreservesErrors(t *testing.T) {
	t.Run("sequential samples stay within cursor capacity", func(t *testing.T) {
		source := &fakeMeasurementSource{maxActive: 1, specs: map[string]cursorSpec{
			"sample-a": {measurements: []Measurement{{ID: "a", SampleID: "sample-a"}}},
			"sample-b": {measurements: []Measurement{{ID: "b", SampleID: "sample-b"}}},
		}}
		measurements, err := CollectMeasurements(context.Background(), source, []string{"sample-a", "sample-b"})
		if err != nil || len(measurements) != 2 {
			t.Fatalf("measurements=%+v error=%v", measurements, err)
		}
		if source.active != 0 || source.maxSeen != 1 {
			t.Fatalf("cursor usage active=%d max=%d", source.active, source.maxSeen)
		}
	})

	t.Run("close failure keeps its identity", func(t *testing.T) {
		source := &fakeMeasurementSource{maxActive: 1, specs: map[string]cursorSpec{
			"sample-a": {closeErr: errLabClose},
		}}
		_, err := CollectMeasurements(context.Background(), source, []string{"sample-a"})
		if !errors.Is(err, errLabClose) || source.active != 0 {
			t.Fatalf("error=%v active=%d", err, source.active)
		}
	})

	t.Run("read failure closes its cursor", func(t *testing.T) {
		source := &fakeMeasurementSource{maxActive: 1, specs: map[string]cursorSpec{
			"sample-a": {readErr: errLabRead},
		}}
		_, err := CollectMeasurements(context.Background(), source, []string{"sample-a"})
		if !errors.Is(err, errLabRead) || source.active != 0 {
			t.Fatalf("error=%v active=%d", err, source.active)
		}
	})
}
