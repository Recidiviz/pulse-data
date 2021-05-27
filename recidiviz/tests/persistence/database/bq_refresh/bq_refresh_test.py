# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2019 Recidiviz, Inc.
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

"""Tests for bq_refresh.py."""

import unittest
from unittest import mock

from google.cloud import bigquery
from google.cloud import exceptions
from mock import create_autospec

from recidiviz.big_query.big_query_client import BigQueryClient
from recidiviz.persistence.database.bq_refresh import bq_refresh

BQ_REFRESH_PACKAGE_NAME = bq_refresh.__name__


class BqLoadTest(unittest.TestCase):
    """Tests for bq_refresh.py."""

    def setUp(self) -> None:
        self.mock_project_id = "fake-recidiviz-project"
        self.mock_dataset_id = "fake-dataset"
        self.mock_table_id = "test_table"
        self.mock_dataset = bigquery.dataset.DatasetReference(
            self.mock_project_id, self.mock_dataset_id
        )
        self.mock_table = self.mock_dataset.table(self.mock_table_id)

        self.project_id_patcher = mock.patch("recidiviz.utils.metadata.project_id")
        self.project_id_patcher.start().return_value = self.mock_project_id

        self.mock_load_job_patcher = mock.patch("google.cloud.bigquery.job.LoadJob")
        self.mock_load_job = self.mock_load_job_patcher.start()
        self.mock_load_job.destination.return_value = self.mock_table

        self.mock_bq_client = create_autospec(BigQueryClient)

    def tearDown(self) -> None:
        self.mock_load_job_patcher.stop()
        self.project_id_patcher.stop()

    def test_wait_for_table_load_calls_result(self) -> None:
        """Test that wait_for_table_load calls load_job.result()"""
        bq_refresh.wait_for_table_load(self.mock_bq_client, self.mock_load_job)
        self.mock_load_job.result.assert_called()

    def test_wait_for_table_load_fail(self) -> None:
        """Test wait_for_table_load logs and exits if there is an error."""
        self.mock_load_job.result.side_effect = exceptions.NotFound("!")
        with self.assertLogs(level="ERROR"):
            success = bq_refresh.wait_for_table_load(
                self.mock_bq_client, self.mock_load_job
            )
            self.assertFalse(success)
