// Package hook runs a verify-then-reload command sequence so a bad config is
// never reloaded into a live load balancer. The verify command (e.g. "nginx
// -t") must succeed before the reload command (e.g. "nginx -s reload") runs.
package hook

import (
	"context"
	"fmt"
	"os/exec"
	"time"

	"github.com/badbuka/lbsync/internal/metrics"
)

// Runner executes an optional verify command followed by an optional reload
// command. It is invoked once per reconcile tick by a provider's Flush.
type Runner struct {
	Kind    string
	Verify  []string
	Reload  []string
	Timeout time.Duration
	Metrics *metrics.Metrics
}

// Run executes verify (if set) and only on success executes reload (if set).
// A verify failure returns an error so the caller can roll back the writes; a
// reload failure also returns an error so it is retried on the next tick.
func (r *Runner) Run(ctx context.Context) error {
	if len(r.Verify) > 0 {
		if err := r.exec(ctx, r.Verify); err != nil {
			r.bump(r.metricsVerify())
			return fmt.Errorf("verify failed: %w", err)
		}
	}
	if len(r.Reload) > 0 {
		if err := r.exec(ctx, r.Reload); err != nil {
			r.bump(r.metricsReload())
			return fmt.Errorf("reload failed: %w", err)
		}
	}
	return nil
}

func (r *Runner) exec(ctx context.Context, args []string) error {
	timeout := r.Timeout
	if timeout <= 0 {
		timeout = 30 * time.Second
	}
	cctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	cmd := exec.CommandContext(cctx, args[0], args[1:]...) //nolint:gosec // operator-configured hook command
	out, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("%v: %w: %s", args, err, string(out))
	}
	return nil
}

func (r *Runner) bump(c func()) {
	if c != nil {
		c()
	}
}

func (r *Runner) metricsVerify() func() {
	if r.Metrics == nil {
		return nil
	}
	return r.Metrics.VerifyErrors.WithLabelValues(r.Kind).Inc
}

func (r *Runner) metricsReload() func() {
	if r.Metrics == nil {
		return nil
	}
	return r.Metrics.ReloadErrors.WithLabelValues(r.Kind).Inc
}
