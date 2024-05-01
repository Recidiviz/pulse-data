# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2024 Recidiviz, Inc.
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
"""Tests for task_queue_helpers.py."""
import unittest

from google.cloud.tasks_v2 import Queue

from recidiviz.airflow.dags.utils.task_queue_helpers import get_running_queue_instances


class TestSftpPipelineDag(unittest.TestCase):
    """Tests for task_queue_helpers.py."""

    def test_get_running_queue_instances(self) -> None:
        primary_queue_status = {
            "name": "projects/recidiviz-staging/locations/us-east1/queues/direct-ingest-state-us-me-scheduler",
            "rate_limits": {
                "max_dispatches_per_second": 100.0,
                "max_burst_size": 20,
                "max_concurrent_dispatches": 1,
            },
            "retry_config": {
                "max_attempts": 5,
                "min_backoff": "0.100s",
                "max_backoff": "3600s",
                "max_doublings": 16,
            },
            "state": Queue.State.RUNNING,
            "purge_time": "2022-01-24T18:39:30.777006Z",
            "stackdriver_logging_config": {"sampling_ratio": 1.0},
        }

        secondary_queue_status = {
            "name": "projects/recidiviz-staging/locations/us-east1/queues/direct-ingest-state-us-me-scheduler-secondary",
            "rate_limits": {
                "max_dispatches_per_second": 100.0,
                "max_burst_size": 20,
                "max_concurrent_dispatches": 1,
            },
            "retry_config": {
                "max_attempts": 5,
                "min_backoff": "0.100s",
                "max_backoff": "3600s",
                "max_doublings": 16,
            },
            "state": Queue.State.RUNNING,
            "purge_time": "2022-01-26T01:07:46.518478Z",
            "stackdriver_logging_config": {"sampling_ratio": 1.0},
        }

        instances = get_running_queue_instances(
            primary_queue_status, secondary_queue_status
        )
        self.assertEqual(["primary", "secondary"], instances)

        # Now start with primary paused
        primary_queue_status["state"] = Queue.State.PAUSED

        instances = get_running_queue_instances(
            primary_queue_status, secondary_queue_status
        )
        self.assertEqual(["secondary"], instances)

        # Now start with secondary paused as well
        secondary_queue_status["state"] = Queue.State.PAUSED
        instances = get_running_queue_instances(
            primary_queue_status, secondary_queue_status
        )
        self.assertEqual([], instances)
