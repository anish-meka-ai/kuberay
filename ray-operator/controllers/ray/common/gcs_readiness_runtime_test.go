package common

import (
	"context"
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestShouldStartWhenDashboardIsHealthy(t *testing.T) {
	// Precondition.
	cmd, _, requests, _ := countedDashboardGateCommand(t, []healthResponse{{http.StatusOK, "success"}})
	// Under test.
	output, err := cmd.CombinedOutput()
	// Postcondition.
	require.NoError(t, err, "healthy dashboard must allow startup: %s", output)
	require.EqualValues(t, 1, requests.Load(), "a successful poll must make one request")
	require.Contains(t, string(output), "GCS is ready.", "success must be logged")
	require.NotContains(t, string(output), "retrying", "healthy dashboard should not require a retry")
}

func TestShouldAcceptSuccessWithTrailingNewline(t *testing.T) {
	// Precondition.
	cmd, _ := dashboardGateCommand(t, []healthResponse{{http.StatusOK, "success\n"}})
	// Under test.
	output, err := cmd.CombinedOutput()
	// Postcondition.
	require.NoError(t, err, "success with a trailing newline must allow startup: %s", output)
}

func TestShouldRetryUnhealthyDashboardResponsesWithoutLegacyCheck(t *testing.T) {
	for _, first := range []healthResponse{
		{http.StatusUnauthorized, "success"}, {http.StatusForbidden, "success"},
		{http.StatusNotFound, "success"}, {http.StatusServiceUnavailable, "success"},
		{http.StatusOK, "not ready"}, {http.StatusOK, "success\nnot ready\n"},
		{http.StatusOK, "success\x00  HTTP/1.1 200 OK\x000\x00"},
	} {
		t.Run(http.StatusText(first.status)+first.body, func(t *testing.T) {
			// Precondition.
			cmd, _, requests, _ := countedDashboardGateCommand(t, []healthResponse{first, {http.StatusOK, "success"}})
			cmd.Env = unexpectedRayCommandEnvironment(t, true)
			// Under test.
			output, err := cmd.CombinedOutput()
			// Postcondition.
			require.NoError(t, err, "gate should recover after dashboard is healthy: %s", output)
			require.EqualValues(t, 2, requests.Load(), "unhealthy responses must be retried")
			require.Contains(t, string(output), "retrying", "unhealthy responses must be logged")
			require.NotContains(t, string(output), "unexpected legacy check", "no failure may invoke the legacy CLI")
		})
	}
}

func TestShouldRetryOnceAfterOneSecondWhenDashboardRecovers(t *testing.T) {
	// Precondition.
	cmd, _, requests, times := countedDashboardGateCommand(t, []healthResponse{{http.StatusServiceUnavailable, "down"}, {http.StatusOK, "success"}})
	// Under test.
	output, err := cmd.CombinedOutput()
	// Postcondition.
	require.NoError(t, err, "recovery must allow startup: %s", output)
	require.EqualValues(t, 2, requests.Load(), "each poll must make one request")
	first, second := <-times, <-times
	require.GreaterOrEqual(t, second.Sub(first), 900*time.Millisecond, "polls must be spaced by about one second")
	require.Less(t, second.Sub(first), 4*time.Second, "retry must not retain the old five-second delay")
}

func TestShouldRetryWithoutFollowingDashboardRedirects(t *testing.T) {
	// Precondition.
	cmd, targets := redirectDashboardGateCommand(t)
	// Under test.
	output, err := cmd.CombinedOutput()
	// Postcondition.
	require.NoError(t, err, "dashboard must recover after a redirect response: %s", output)
	require.Zero(t, targets.Load(), "one poll must not issue a redirected request")
	require.Contains(t, string(output), "retrying", "redirect response must be logged")
}

func TestShouldAcceptSlowHealthyDashboardResponse(t *testing.T) {
	// Precondition.
	cmd, requests := delayedResponseDashboardGateCommand(t)
	// Under test.
	output, err := cmd.CombinedOutput()
	// Postcondition.
	require.NoError(t, err, "three-second healthy response must be accepted: %s", output)
	require.EqualValues(t, 1, requests.Load(), "slow healthy response must not be retried")
}

func TestShouldTimeoutAndRetryWhenDashboardDoesNotRespond(t *testing.T) {
	// Precondition.
	cmd, requests := slowDashboardGateCommand(t)
	// Under test.
	output, err := cmd.CombinedOutput()
	// Postcondition.
	require.NoError(t, err, "gate must recover from a stalled request: %s", output)
	require.EqualValues(t, 2, requests.Load(), "the stalled request must time out and be retried")
	require.Contains(t, string(output), "retrying", "timeout must be logged")
}

func TestShouldRetryWhenDashboardStreamsUnexpectedBody(t *testing.T) {
	// Precondition.
	cmd, requests := streamingDashboardGateCommand(t)
	// Under test.
	output, err := cmd.CombinedOutput()
	// Postcondition.
	require.NoError(t, err, "gate must recover from an oversized response: %s", output)
	require.EqualValues(t, 2, requests.Load(), "oversized response must be rejected and retried")
	require.Contains(t, string(output), "retrying", "rejected response must be logged")
}

func TestShouldKeepWaitingWhenDashboardIsUnreachable(t *testing.T) {
	// Precondition.
	cmd, ctx := unreachableDashboardGateCommand(t)
	cmd.Env = unexpectedRayCommandEnvironment(t, true)
	// Under test.
	output, err := cmd.CombinedOutput()
	// Postcondition.
	require.Error(t, err, "unreachable dashboard must not allow startup")
	require.ErrorIs(t, ctx.Err(), context.DeadlineExceeded, "gate must keep retrying")
	require.Contains(t, string(output), "retrying", "network failure must be logged")
	require.NotContains(t, string(output), "unexpected legacy check", "network errors must not invoke the legacy CLI")
}

func TestShouldFailClearlyWhenWgetIsMissing(t *testing.T) {
	// Precondition.
	cmd, ctx := gateCommand(t, 8265, 3*time.Second)
	cmd.Env = unexpectedRayCommandEnvironment(t, false)
	// Under test.
	output, err := cmd.CombinedOutput()
	// Postcondition.
	require.Error(t, err, "missing wget must not allow startup")
	require.NoError(t, ctx.Err(), "missing wget must fail promptly")
	require.Contains(t, string(output), "wget is required", "missing wget must be logged")
	require.NotContains(t, string(output), "unexpected legacy check", "missing wget must not invoke the legacy CLI")
}
