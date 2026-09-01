// Package metrics exposes low-cardinality KIMERA domain state metrics.
package metrics

import (
	"sync/atomic"

	"github.com/prometheus/client_golang/prometheus"
	ctrlmetrics "sigs.k8s.io/controller-runtime/pkg/metrics"

	platformv1alpha1 "github.com/hashemzargari/kimera-operator/api/v1alpha1"
)

var (
	environmentCount atomic.Int64
	readyCount       atomic.Int64
	suspendedCount   atomic.Int64
)

func init() {
	ctrlmetrics.Registry.MustRegister(
		prometheus.NewGaugeFunc(prometheus.GaugeOpts{Name: "kimera_development_environments", Help: "Current number of DevelopmentEnvironment resources."}, func() float64 { return float64(environmentCount.Load()) }),
		prometheus.NewGaugeFunc(prometheus.GaugeOpts{Name: "kimera_development_environments_ready", Help: "Current number of ready DevelopmentEnvironment resources."}, func() float64 { return float64(readyCount.Load()) }),
		prometheus.NewGaugeFunc(prometheus.GaugeOpts{Name: "kimera_development_environments_suspended", Help: "Current number of suspended DevelopmentEnvironment resources."}, func() float64 { return float64(suspendedCount.Load()) }),
	)
}

// Observe replaces aggregate state from one authoritative API list.
func Observe(environments []platformv1alpha1.DevelopmentEnvironment) {
	var ready, suspended int64
	for i := range environments {
		if environments[i].Status.Phase == platformv1alpha1.PhaseReady {
			ready++
		}
		if environments[i].Spec.Suspended {
			suspended++
		}
	}
	environmentCount.Store(int64(len(environments)))
	readyCount.Store(ready)
	suspendedCount.Store(suspended)
}
