package common

import (
	"fmt"
	"net"
	"strconv"
	"strings"

	"github.com/ray-project/kuberay/ray-operator/controllers/ray/utils"
)

// Lilypad heads always serve the dashboard on Ray's default port.
func dashboardGCSReadinessCommand(headHost string) string {
	url := "http://" + net.JoinHostPort(headHost, strconv.Itoa(utils.DefaultDashboardPort)) + "/" + utils.RayDashboardGCSHealthPath
	return fmt.Sprintf(`
		export LC_ALL=C
		if ! command -v wget >/dev/null 2>&1; then
			echo "wget is required for GCS readiness" >&2
			exit 1
		fi
		while true; do
			response=""
			response_complete=false
			exec 3< <(http_proxy= https_proxy= HTTP_PROXY= HTTPS_PROXY= wget -nv -T 5 -t 1 --max-redirect=0 -O - %s)
			wget_pid=$!
			if IFS= read -r -n 8 -d '' response <&3; then
				if [ "${#response}" -eq 8 ]; then
					extra=""
					if ! IFS= read -r -n 1 -d '' extra <&3; then
						response_complete=true
					fi
				fi
			else
				response_complete=true
			fi
			exec 3<&-
			if wait "$wget_pid" && [ "$response_complete" = true ] && { [ "$response" = success ] || [ "$response" = $'success\n' ]; }; then
				echo "GCS is ready."
				break
			fi
			echo "Dashboard GCS health check failed; retrying." >&2
			sleep 1
		done
	`, shellQuote(url))
}

func shellQuote(value string) string {
	return "'" + strings.ReplaceAll(value, "'", "'\"'\"'") + "'"
}
