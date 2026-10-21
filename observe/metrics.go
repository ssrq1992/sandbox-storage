// Package observe provides bounded-cardinality Prometheus text metrics.
package observe

import (
	"fmt"
	"net/http"
	"sort"
	"strings"
	"sync"
	"time"
)

type sample struct {
	Count, Failures uint64
	Seconds         float64
}
type Registry struct {
	mu     sync.Mutex
	values map[string]sample
}

func (r *Registry) Observe(stage string, d time.Duration, e error) {
	stage = strings.Map(func(c rune) rune {
		if c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c == '_' {
			return c
		}
		return '_'
	}, stage)
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.values == nil {
		r.values = map[string]sample{}
	}
	v := r.values[stage]
	v.Count++
	if e != nil {
		v.Failures++
	}
	v.Seconds += d.Seconds()
	r.values[stage] = v
}
func (r *Registry) ServeHTTP(w http.ResponseWriter, _ *http.Request) {
	r.mu.Lock()
	defer r.mu.Unlock()
	w.Header().Set("Content-Type", "text/plain; version=0.0.4")
	keys := make([]string, 0, len(r.values))
	for key := range r.values {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, key := range keys {
		v := r.values[key]
		fmt.Fprintf(w, "ssf_step_duration_seconds_sum{step=%q} %g\nssf_step_duration_seconds_count{step=%q} %d\nssf_step_errors_total{step=%q} %d\n", key, v.Seconds, key, v.Count, key, v.Failures)
	}
}
