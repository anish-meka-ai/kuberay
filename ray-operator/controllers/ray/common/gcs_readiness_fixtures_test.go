package common

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"

	rayv1 "github.com/ray-project/kuberay/ray-operator/apis/ray/v1"
)

type healthResponse struct {
	status int
	body   string
}

func readinessCluster(t *testing.T) *rayv1.RayCluster {
	t.Helper()
	t.Setenv(EnableInitContainerInjectionEnvKey, "true")
	cluster := instance.DeepCopy()
	cluster.Spec.WorkerGroupSpecs[0].Template.Labels = map[string]string{"group_workload_class": "lilypad"}
	return cluster
}

func readinessClusterWithInitSettings(t *testing.T) *rayv1.RayCluster {
	t.Helper()
	cluster := readinessCluster(t)
	worker := &cluster.Spec.WorkerGroupSpecs[0]
	worker.Template.Spec.InitContainers = []corev1.Container{{Name: "before"}}
	worker.Template.Spec.Containers[0].SecurityContext = &corev1.SecurityContext{RunAsNonRoot: new(bool)}
	worker.Template.Spec.Containers[0].VolumeMounts = []corev1.VolumeMount{{Name: "tls", MountPath: "/tls"}}
	worker.Template.Spec.Containers[0].ImagePullPolicy = corev1.PullAlways
	return cluster
}

func renderReadinessWorker(t *testing.T, cluster *rayv1.RayCluster) corev1.PodTemplateSpec {
	t.Helper()
	worker := cluster.Spec.WorkerGroupSpecs[0]
	return DefaultWorkerPodTemplate(t.Context(), *cluster, worker, "worker-", "127.0.0.1", "6379")
}

func gateCommand(t *testing.T, port int32, timeout time.Duration) (*exec.Cmd, context.Context) {
	t.Helper()
	cluster := readinessCluster(t)
	gate := renderReadinessWorker(t, cluster).Spec.InitContainers[0]
	const defaultURL = "http://127.0.0.1:8265/api/gcs_healthz"
	require.Equal(t, 1, strings.Count(gate.Args[0], defaultURL), "gate must target the Lilypad dashboard")
	gate.Args[0] = strings.Replace(gate.Args[0], defaultURL, fmt.Sprintf("http://127.0.0.1:%d/api/gcs_healthz", port), 1)
	ctx, cancel := context.WithTimeout(t.Context(), timeout)
	t.Cleanup(cancel)
	cmd := exec.CommandContext(ctx, gate.Command[0], append(gate.Command[1:], gate.Args...)...)
	cmd.WaitDelay = time.Second
	return cmd, ctx
}

func dashboardGateCommand(t *testing.T, responses []healthResponse) (*exec.Cmd, context.Context) {
	t.Helper()
	cmd, ctx, _, _ := countedDashboardGateCommand(t, responses)
	return cmd, ctx
}

func countedDashboardGateCommand(t *testing.T, responses []healthResponse) (*exec.Cmd, context.Context, *atomic.Int32, <-chan time.Time) {
	t.Helper()
	var requests atomic.Int32
	requestTimes := make(chan time.Time, 100)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/gcs_healthz" {
			t.Errorf("unexpected dashboard request path %q", r.URL.Path)
			w.WriteHeader(http.StatusNotFound)
			return
		}
		requestTimes <- time.Now()
		index := int(requests.Add(1)) - 1
		if index >= len(responses) {
			index = len(responses) - 1
		}
		response := responses[index]
		w.WriteHeader(response.status)
		if _, err := io.WriteString(w, response.body); err != nil {
			t.Errorf("write dashboard response: %v", err)
		}
	}))
	t.Cleanup(server.Close)
	port := server.Listener.Addr().(*net.TCPAddr).Port
	cmd, ctx := gateCommand(t, int32(port), 12*time.Second)
	return cmd, ctx, &requests, requestTimes
}

func redirectDashboardGateCommand(t *testing.T) (*exec.Cmd, *atomic.Int32) {
	t.Helper()
	var targetRequests atomic.Int32
	var requests atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.RawQuery != "" {
			targetRequests.Add(1)
		} else if requests.Add(1) == 1 {
			w.Header().Set("Location", "/api/gcs_healthz?redirected=1")
			w.WriteHeader(http.StatusFound)
			return
		}
		if _, err := io.WriteString(w, "success"); err != nil {
			t.Errorf("write dashboard response: %v", err)
		}
	}))
	t.Cleanup(server.Close)
	cmd, _ := gateCommand(t, int32(server.Listener.Addr().(*net.TCPAddr).Port), 8*time.Second)
	return cmd, &targetRequests
}

func delayedResponseDashboardGateCommand(t *testing.T) (*exec.Cmd, *atomic.Int32) {
	t.Helper()
	var requests atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests.Add(1)
		select {
		case <-time.After(3 * time.Second):
			if _, err := io.WriteString(w, "success"); err != nil {
				t.Errorf("write dashboard response: %v", err)
			}
		case <-r.Context().Done():
		}
	}))
	t.Cleanup(server.Close)
	cmd, _ := gateCommand(t, int32(server.Listener.Addr().(*net.TCPAddr).Port), 8*time.Second)
	return cmd, &requests
}

func slowDashboardGateCommand(t *testing.T) (*exec.Cmd, *atomic.Int32) {
	t.Helper()
	var requests atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if requests.Add(1) == 1 {
			<-r.Context().Done()
			return
		}
		if _, err := io.WriteString(w, "success"); err != nil {
			t.Errorf("write dashboard response: %v", err)
		}
	}))
	t.Cleanup(server.Close)
	port := server.Listener.Addr().(*net.TCPAddr).Port
	cmd, _ := gateCommand(t, int32(port), 8*time.Second)
	return cmd, &requests
}

func streamingDashboardGateCommand(t *testing.T) (*exec.Cmd, *atomic.Int32) {
	t.Helper()
	var requests atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if requests.Add(1) > 1 {
			if _, err := io.WriteString(w, "success"); err != nil {
				t.Errorf("write dashboard response: %v", err)
			}
			return
		}
		if _, err := io.WriteString(w, "success\n"+strings.Repeat("x", 256*1024)); err != nil {
			if !errors.Is(err, syscall.EPIPE) && !errors.Is(err, syscall.ECONNRESET) && r.Context().Err() == nil {
				t.Errorf("write streaming response: %v", err)
			}
			return
		}
		w.(http.Flusher).Flush()
		<-r.Context().Done()
	}))
	t.Cleanup(server.Close)
	port := server.Listener.Addr().(*net.TCPAddr).Port
	cmd, _ := gateCommand(t, int32(port), 4*time.Second)
	return cmd, &requests
}

func unreachableDashboardGateCommand(t *testing.T) (*exec.Cmd, context.Context) {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err, "test port must be available")
	port := listener.Addr().(*net.TCPAddr).Port
	require.NoError(t, listener.Close(), "test port must close")
	return gateCommand(t, int32(port), 300*time.Millisecond)
}

func unexpectedRayCommandEnvironment(t *testing.T, includeSystemPath bool) []string {
	t.Helper()
	bin := t.TempDir()
	script := "#!/bin/sh\nprintf 'unexpected legacy check\\n' >&2\nexit 0\n"
	require.NoError(t, os.WriteFile(filepath.Join(bin, "ray"), []byte(script), 0755), "test Ray command must exist")
	path := bin
	if includeSystemPath {
		path += ":" + os.Getenv("PATH")
	}
	return append(os.Environ(), "PATH="+path)
}
