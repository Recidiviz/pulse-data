# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2022 Recidiviz, Inc.
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
"""An implementation of TestCase that can be used for tests that talk to the BigQuery
emulator.
"""
import unittest
from typing import Any, Dict, Iterable, List
from unittest.mock import Mock, patch

import pytest
from google.cloud import bigquery

from recidiviz.big_query.big_query_address import BigQueryAddress
from recidiviz.big_query.big_query_client import BigQueryClientImpl
from recidiviz.big_query.big_query_results_contents_handle import (
    BigQueryResultsContentsHandle,
)

BQ_EMULATOR_PROJECT_ID = "recidiviz-bq-emulator-project"


# TODO(#15020): Migrate all usages of  BigQueryViewTestCase to use this test case
#  instead (once the emulator has reached feature parity).
@pytest.mark.uses_bq_emulator
class BigQueryEmulatorTestCase(unittest.TestCase):
    """An implementation of TestCase that can be used for tests that talk to the
    BigQuery emulator.

    In order to run tests that extend this TestCase, you must first download the latest
    version of the BQ emulator by running:

    pipenv run pull-bq-emulator

    Then, before running the tests, you must launch the BQ emulator in a separate
    terminal:

    $ pipenv run start-bq-emulator

    DISCLAIMER: The BQ emulator currently supports a large subset of BigQuery SQL
    features, but not all of them. If you are trying to use the emulator and running
    into issues, you should post in #platform-team.
    """

    def setUp(self) -> None:
        self.project_id_patcher = patch(
            "recidiviz.utils.metadata.project_id",
            Mock(return_value=BQ_EMULATOR_PROJECT_ID),
        )
        self.project_id = self.project_id_patcher.start().return_value
        self.bq_client = BigQueryClientImpl()
        self._wipe_emulator_data()

    def tearDown(self) -> None:
        self._wipe_emulator_data()
        self.project_id_patcher.stop()

    def _wipe_emulator_data(self) -> None:
        for dataset_list_item in self.bq_client.list_datasets():
            self.bq_client.delete_dataset(
                self.bq_client.dataset_ref_for_id(dataset_list_item.dataset_id),
                delete_contents=True,
                not_found_ok=True,
            )

    def run_query_test(
        self, query_str: str, expected_result: Iterable[Dict[str, Any]]
    ) -> None:
        query_job = self.bq_client.run_query_async(
            query_str=query_str,
        )
        contents_iterator: Iterable[Dict[str, Any]] = BigQueryResultsContentsHandle(
            query_job
        ).get_contents_iterator()

        self.assertEqual(expected_result, list(contents_iterator))

    def create_mock_table(
        self,
        address: BigQueryAddress,
        schema: List[bigquery.SchemaField],
    ) -> None:
        dataset_ref = self.bq_client.dataset_ref_for_id(address.dataset_id)
        self.bq_client.create_dataset_if_necessary(dataset_ref)

        if self.bq_client.table_exists(dataset_ref, address.table_id):
            raise ValueError(
                f"Table [{address}] already exists. Test cleanup not working properly."
            )

        self.bq_client.create_table_with_schema(
            dataset_id=address.dataset_id,
            table_id=address.table_id,
            schema_fields=schema,
        )

    def load_rows_into_table(
        self,
        address: BigQueryAddress,
        data: List[Dict[str, Any]],
    ) -> None:
        dataset_ref = self.bq_client.dataset_ref_for_id(address.dataset_id)
        self.bq_client.stream_into_table(
            dataset_ref,
            address.table_id,
            rows=data,
        )
