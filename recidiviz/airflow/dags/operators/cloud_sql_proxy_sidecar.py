# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2023 Recidiviz, Inc.
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <https://www.gnu.org/licenses/>.
# =============================================================================
"""
Helper that enables an arbitrary Kubernetes pod to connect to CloudSQL via the Cloud SQL proxy.
For more info about the proxy, see: https://cloud.google.com/sql/docs/mysql/sql-proxy#how-works.
"""
from typing import List

from kubernetes.client.models import (
    V1Container,
    V1EmptyDirVolumeSource,
    V1EnvVar,
    V1HTTPGetAction,
    V1Pod,
    V1Probe,
    V1ResourceRequirements,
    V1SecurityContext,
    V1Volume,
    V1VolumeMount,
)
from more_itertools import one

CLOUD_SQL_PROXY_HTTP_HOST = "0.0.0.0"  # nosec
CLOUD_SQL_PROXY_HTTP_PORT = 9801
CLOUD_SQL_PROXY_HTTP_ADMIN_PORT = 9091

CLOUD_SQL_PROXY_VOLUME = V1Volume(
    name="cloudsql-unix-sockets", empty_dir=V1EmptyDirVolumeSource()
)


SHARED_CLOUD_SQL_PROXY_VOLUME_MOUNT = V1VolumeMount(
    # This is the expected path for Cloud SQL Unix sockets in most, if not all, GCP runtimes
    # as defined here: https://cloud.google.com/sql/docs/postgres/connect-run#public-ip-default_1
    mount_path="/cloudsql",
    name=CLOUD_SQL_PROXY_VOLUME.name,
)


def configure_cloud_sql_proxy_for_pod(
    pod: V1Pod, app_container_name: str, connection_strings: List[str]
) -> V1Pod:
    """Given a Kubernetes pod, adds a "sidecar" container which runs the CloudSQL proxy inside the pod.

    Other containers in the pod can communicate with this container via any ports exposed on 0.0.0.0.
    Unix sockets mounted in a new shared volume (shared storage for all containers to reference). The
    Unix sockets are configured in a location that the Google CloudSQL proxy specifically expects.

    The container lifecycles are configured such that the CloudSQL proxy will start
    first and all other containers will wait to start until the proxy container is
    healthy.

    This implementation was modeled closely based on these docs:
    https://cloud.google.com/sql/docs/mysql/connect-kubernetes-engine#run_the_in_a_sidecar_pattern
    """
    pod.spec.volumes = pod.spec.volumes or []
    pod.spec.volumes.insert(0, CLOUD_SQL_PROXY_VOLUME)

    pod.spec.containers = pod.spec.containers or []
    app_container = one(
        container
        for container in pod.spec.containers
        if container.name == app_container_name
    )
    app_container.volume_mounts = app_container.volume_mounts or []
    app_container.volume_mounts.insert(0, SHARED_CLOUD_SQL_PROXY_VOLUME_MOUNT)

    app_container.env = app_container.env or []
    app_container.env.append(
        V1EnvVar(
            name="K8S_CLOUD_SQL_PROXY_ADMIN_HOST",
            value=CLOUD_SQL_PROXY_HTTP_HOST,
        )
    )
    app_container.env.append(
        V1EnvVar(
            name="K8S_CLOUD_SQL_PROXY_ADMIN_PORT",
            value=str(CLOUD_SQL_PROXY_HTTP_ADMIN_PORT),
        ),
    )

    sidecar = V1Container(
        name="cloud-sql-proxy",
        image="gcr.io/cloud-sql-connectors/cloud-sql-proxy:2.14.0",
        args=[
            "--structured-logs",
            # Expose both health check and admin control services to other containers in the pod
            "--http-address",
            CLOUD_SQL_PROXY_HTTP_HOST,
            # Configure health check port binding
            "--http-port",
            str(CLOUD_SQL_PROXY_HTTP_PORT),
            # Enable health check endpoints at /startup and /liveness
            "--health-check",
            # Configure the admin control service port
            "--admin-port",
            str(CLOUD_SQL_PROXY_HTTP_ADMIN_PORT),
            # This option enables the quitquitquit endpoint on the admin control service
            # Sending a POST request to /quitquitquit shuts down the proxy
            "--quitquitquit",
            # Verify the proxy can reach the Cloud SQL instance on startup
            "--run-connection-test",
            # Enable debug logging
            "--debug-logs",
            # --- Mount unix sockets in the shared volume
            "--unix-socket",
            SHARED_CLOUD_SQL_PROXY_VOLUME_MOUNT.mount_path,
            *connection_strings,
        ],
        volume_mounts=[SHARED_CLOUD_SQL_PROXY_VOLUME_MOUNT],
        security_context=V1SecurityContext(
            run_as_non_root=True,
        ),
        # The startup probe verifies that the Cloud SQL Proxy is correctly configured and is ready to accept connections
        startup_probe=V1Probe(
            failure_threshold=60,
            http_get=V1HTTPGetAction(
                path="/startup", port=CLOUD_SQL_PROXY_HTTP_PORT, scheme="HTTP"
            ),
            period_seconds=1,
            success_threshold=1,
            timeout_seconds=10,
        ),
        resources=V1ResourceRequirements(
            limits={
                "cpu": "500m",
                "memory": "500Mi",
            },
        ),
        restart_policy="Always",
    )

    # Handle pod startup sequencing. Pods are started serially in order
    # https://medium.com/@marko.luksa/delaying-application-start-until-sidecar-is-ready-2ec2d21a7b74
    pod.spec.init_containers = pod.spec.init_containers or []
    pod.spec.init_containers.insert(0, sidecar)

    return pod
