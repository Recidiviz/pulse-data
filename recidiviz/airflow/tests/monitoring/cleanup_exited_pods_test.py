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
"""Tests for cleaning up exited pods"""
import datetime
import unittest
from unittest.mock import MagicMock, patch

import freezegun
from dateutil.tz import tzlocal
from kubernetes.client import V1ObjectMeta, V1Pod, V1PodList, V1PodStatus

from recidiviz.airflow.dags.monitoring.cleanup_exited_pods import (
    POD_HISTORY_TTL,
    cleanup_exited_pods,
)
from recidiviz.airflow.dags.operators.recidiviz_kubernetes_pod_operator import (
    COMPOSER_USER_WORKLOADS,
)


class TestCleanupExitedPods(unittest.TestCase):
    """Tests for cleaning up exited pods"""

    @patch("recidiviz.airflow.dags.monitoring.cleanup_exited_pods.get_client")
    @freezegun.freeze_time(datetime.datetime(2023, 8, 24, 0, 0, 0, 0, tzinfo=tzlocal()))
    def test_cleanup_exited_pods(self, mock_get_client: MagicMock) -> None:
        """Pods that have not been running for more than POD_TTL_HOURS"""
        mock_client = mock_get_client.return_value

        recent_pod = V1Pod(
            metadata=V1ObjectMeta(name="recent-pod"),
            status=V1PodStatus(
                phase="Failed",
                start_time=datetime.datetime.now(tz=tzlocal())
                - datetime.timedelta(hours=10),
            ),
        )

        old_pod = V1Pod(
            metadata=V1ObjectMeta(name="old-pod"),
            status=V1PodStatus(
                phase="Succeeded",
                start_time=datetime.datetime.now(tz=tzlocal())
                - POD_HISTORY_TTL
                - datetime.timedelta(hours=4),
            ),
        )

        fresh_pod = V1Pod(
            metadata=V1ObjectMeta(name="fresh-pod"),
            status=V1PodStatus(
                phase="Pending",
                start_time=None,
            ),
        )

        mock_client.list_namespaced_pod.return_value = V1PodList(
            items=[recent_pod, old_pod, fresh_pod]
        )

        cleanup_exited_pods()

        mock_client.delete_namespaced_pod.assert_called_with(
            namespace=COMPOSER_USER_WORKLOADS, name=old_pod.metadata.name
        )
