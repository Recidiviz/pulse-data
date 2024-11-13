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
"""Functionality for cleaning up completed pods"""
import logging
from datetime import datetime, timedelta

from airflow.configuration import conf
from airflow.providers.cncf.kubernetes.hooks.kubernetes import KubernetesHook
from dateutil.tz import tzlocal
from kubernetes import client

from recidiviz.airflow.dags.operators.recidiviz_kubernetes_pod_operator import (
    COMPOSER_USER_WORKLOADS,
)

POD_HISTORY_TTL = timedelta(hours=36)


def get_client() -> client.CoreV1Api:
    """Returns a K8s client"""
    hook = KubernetesHook(config_file=conf.get("kubernetes", "config_file"))
    return hook.core_v1_client


def cleanup_exited_pods() -> None:
    """Deletes pods that have not been active in the last POD_HISTORY_TTL"""
    k8s_client = get_client()

    pod_list = k8s_client.list_namespaced_pod(
        namespace=COMPOSER_USER_WORKLOADS,
        field_selector="status.phase!=Running",
    )

    now = datetime.now(tz=tzlocal())
    for pod in pod_list.items:
        pod_name = pod.metadata.name

        if not pod.status.start_time:
            logging.warning("skipping pod without start_time %s", pod)
            continue

        if now - pod.status.start_time > POD_HISTORY_TTL:
            logging.info("deleting %s", pod_name)
            k8s_client.delete_namespaced_pod(
                namespace=COMPOSER_USER_WORKLOADS,
                name=pod_name,
            )
