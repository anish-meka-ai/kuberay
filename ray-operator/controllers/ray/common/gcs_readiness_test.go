package common

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestShouldUseDashboardReadinessForLilypadWorkers(t *testing.T) {
	// Precondition.
	cluster := readinessCluster(t)
	// Under test.
	pod := renderReadinessWorker(t, cluster)
	// Postcondition.
	gate := pod.Spec.InitContainers[0]
	require.Contains(t, gate.Args[0], "http://127.0.0.1:8265/api/gcs_healthz", "Lilypad workers must use the dashboard check")
	require.NotContains(t, gate.Args[0], "ray health-check", "the init container must not launch the Ray CLI")
}

func TestShouldKeepDirectReadinessForNonLilypadWorkers(t *testing.T) {
	// Precondition.
	cluster := readinessCluster(t)
	cluster.Spec.WorkerGroupSpecs[0].Template.Labels = nil
	// Under test.
	pod := renderReadinessWorker(t, cluster)
	// Postcondition.
	require.Contains(t, pod.Spec.InitContainers[0].Args[0], "ray health-check --address 127.0.0.1:6379", "non-Lilypad workers must retain the direct GCS check")
}

func TestShouldPreserveWorkerInitSettingsWhenWaitingForGCS(t *testing.T) {
	// Precondition.
	cluster := readinessClusterWithInitSettings(t)
	worker := cluster.Spec.WorkerGroupSpecs[0].Template.Spec.Containers[0]
	// Under test.
	pod := renderReadinessWorker(t, cluster)
	// Postcondition.
	require.Len(t, pod.Spec.InitContainers, 2, "gate must follow the existing init container")
	require.Equal(t, "before", pod.Spec.InitContainers[0].Name, "existing init order must be preserved")
	gate := pod.Spec.InitContainers[1]
	require.Equal(t, worker.Image, gate.Image, "gate must use the worker image")
	require.Equal(t, worker.ImagePullPolicy, gate.ImagePullPolicy, "gate must use the worker image pull policy")
	require.Equal(t, worker.Env, gate.Env, "gate must retain the worker environment")
	require.Equal(t, worker.VolumeMounts, gate.VolumeMounts, "gate must retain the worker mounts")
	require.Equal(t, worker.SecurityContext, gate.SecurityContext, "gate must retain the worker security context")
}

func TestShouldSkipInjectedReadinessWhenInjectionDisabled(t *testing.T) {
	// Precondition.
	cluster := readinessCluster(t)
	t.Setenv(EnableInitContainerInjectionEnvKey, "false")
	// Under test.
	pod := renderReadinessWorker(t, cluster)
	// Postcondition.
	require.Empty(t, pod.Spec.InitContainers, "disabled injection must not add a gate")
}
