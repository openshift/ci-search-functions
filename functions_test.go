// Package cisearch provides GCS indexer functions
package cisearch

import (
	"context"
	"reflect"
	"testing"
)

func TestIndexJobs(t *testing.T) {
	tests := []struct {
		name    string
		e       GCSEvent
		wantErr bool
	}{
		// TODO: make the actual write a no-op in tests
		// {
		// 	e: GCSEvent{
		// 		Bucket: "origin-ci-test",
		// 		Name:   "logs/release-openshift-origin-installer-e2e-gcp-upgrade-4.8/1366716541889941504/artifacts/e2e-gcp-upgrade/gather-extra/artifacts/metrics/job_metrics.json",
		// 	},
		// },
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := IndexJobs(context.TODO(), tt.e); (err != nil) != tt.wantErr {
				t.Errorf("IndexJobs() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestPrometheusValue_UnmarshalJSON(t *testing.T) {
	tests := []struct {
		name    string
		data    []byte
		wantErr bool
		initial *PrometheusValue
		expect  *PrometheusValue
	}{
		{
			data:    []byte(`null`),
			initial: &PrometheusValue{},
			expect:  &PrometheusValue{},
		},
		{
			data:    []byte(`null`),
			initial: &PrometheusValue{Value: "test"},
			expect:  &PrometheusValue{Value: "test"},
		},
		{
			data:    []byte(`null`),
			initial: nil,
			expect:  nil,
		},
		{
			data:    []byte(`[]`),
			initial: &PrometheusValue{Value: "test"},
			expect:  &PrometheusValue{Value: "test"},
			wantErr: true,
		},
		{
			data:    []byte(`[1]`),
			initial: &PrometheusValue{Value: "test"},
			expect:  &PrometheusValue{Value: "test"},
			wantErr: true,
		},
		{
			data:    []byte(`[1, 2]`),
			initial: &PrometheusValue{Value: "test"},
			expect:  &PrometheusValue{Timestamp: 1, Value: "test"},
			wantErr: true,
		},
		{
			data:    []byte(`[1,`),
			initial: &PrometheusValue{Value: "test"},
			expect:  &PrometheusValue{Timestamp: 1, Value: "test"},
			wantErr: true,
		},
		{
			data:    []byte(`[1,]`),
			initial: &PrometheusValue{Value: "test"},
			expect:  &PrometheusValue{Timestamp: 1, Value: "test"},
			wantErr: true,
		},
		{
			data:    []byte(`[1, `),
			initial: &PrometheusValue{Value: "test"},
			expect:  &PrometheusValue{Timestamp: 1, Value: "test"},
			wantErr: true,
		},
		{
			data:    []byte(`[1, ]`),
			initial: &PrometheusValue{Value: "test"},
			expect:  &PrometheusValue{Timestamp: 1, Value: "test"},
			wantErr: true,
		},
		{
			data:    []byte(`[1, "]`),
			initial: &PrometheusValue{Value: "test"},
			expect:  &PrometheusValue{Timestamp: 1, Value: "test"},
			wantErr: true,
		},
		{
			data:    []byte(`[1, ""]`),
			initial: &PrometheusValue{Value: "test"},
			expect:  &PrometheusValue{Timestamp: 1, Value: "test"},
			wantErr: true,
		},
		{
			data:    []byte(`[1, " 1 "]`),
			initial: &PrometheusValue{Value: "test"},
			expect:  &PrometheusValue{Timestamp: 1, Value: "test"},
			wantErr: true,
		},
		{
			data:    []byte(`[1, "1"]`),
			initial: &PrometheusValue{Value: "test"},
			expect:  &PrometheusValue{Timestamp: 1, Value: "1"},
		},
		{
			data:    []byte(`[1, "1.1"]`),
			initial: &PrometheusValue{Value: "test"},
			expect:  &PrometheusValue{Timestamp: 1, Value: "1.1"},
		},
	}
	for _, tt := range tests {
		n := tt.name
		if len(n) == 0 {
			n = string(tt.data)
		}
		t.Run(n, func(t *testing.T) {
			err := tt.initial.UnmarshalJSON(tt.data)
			if (err != nil) != tt.wantErr {
				t.Errorf("PrometheusValue.UnmarshalJSON() error = %v, wantErr %v", err, tt.wantErr)
			}
			t.Logf("%v", err)
			if !reflect.DeepEqual(tt.expect, tt.initial) {
				t.Errorf("Unexpected output value: %#v", tt.initial)
			}
		})
	}
}
