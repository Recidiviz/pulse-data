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
from concurrent import futures
from typing import Any, Dict, Iterable, List
from unittest.mock import Mock, patch

import pandas as pd
import pytest
from google.cloud import bigquery

from recidiviz.big_query.big_query_address import BigQueryAddress
from recidiviz.big_query.big_query_client import (
    BQ_CLIENT_MAX_POOL_SIZE,
    BigQueryClientImpl,
)
from recidiviz.big_query.big_query_results_contents_handle import (
    BigQueryResultsContentsHandle,
)
from recidiviz.tests.big_query.big_query_test_helper import BigQueryTestHelper
from recidiviz.tests.test_setup_utils import BQ_EMULATOR_PROJECT_ID
from recidiviz.tests.utils.big_query_emulator_control import BigQueryEmulatorControl


# TODO(#15020): Migrate all usages of  BigQueryViewTestCase to use this test case
#  instead (once the emulator has reached feature parity).
@pytest.mark.uses_bq_emulator
class BigQueryEmulatorTestCase(unittest.TestCase, BigQueryTestHelper):
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

    control: BigQueryEmulatorControl

    # Deletes all tables / views in the emulator after each test
    # Subclasses can choose to override this as it may not always be necessary
    wipe_emulator_data_on_teardown = True

    @classmethod
    def get_input_schema_json_path(cls) -> str | None:
        return None

    @classmethod
    def setUpClass(cls) -> None:
        cls.control = BigQueryEmulatorControl.build()
        cls.control.pull_image()
        cls.control.start_emulator(
            input_schema_json_path=cls.get_input_schema_json_path()
        )

    def setUp(self) -> None:
        self.project_id_patcher = patch(
            "recidiviz.utils.metadata.project_id",
            Mock(return_value=BQ_EMULATOR_PROJECT_ID),
        )
        self.project_id = self.project_id_patcher.start().return_value
        self.bq_client = BigQueryClientImpl()

    def tearDown(self) -> None:
        self.project_id_patcher.stop()
        if self.wipe_emulator_data_on_teardown:
            self._wipe_emulator_data()

    @classmethod
    def tearDownClass(cls) -> None:
        # If the test failed, output the emulator logs prior to exiting
        print(cls.control.get_logs())
        cls.control.stop_emulator()

    def query(self, query: str) -> pd.DataFrame:
        return self.bq_client.run_query_async(
            query_str=query, use_query_cache=True
        ).to_dataframe()

    def _wipe_emulator_data(self) -> None:
        with futures.ThreadPoolExecutor(
            # Conservatively allow only half as many workers as allowed connections.
            # Lower this number if we see "urllib3.connectionpool:Connection pool is
            # full, discarding connection" errors.
            max_workers=int(BQ_CLIENT_MAX_POOL_SIZE / 2)
        ) as executor:
            to_delete = [
                executor.submit(
                    self.bq_client.delete_dataset,
                    self.bq_client.dataset_ref_for_id(dataset_list_item.dataset_id),
                    delete_contents=True,
                    not_found_ok=True,
                )
                for dataset_list_item in self.bq_client.list_datasets()
            ]

        for future in futures.as_completed(to_delete):
            future.result()

    def run_query_test(
        self, query_str: str, expected_result: Iterable[Dict[str, Any]]
    ) -> None:
        query_job = self.bq_client.run_query_async(
            query_str=query_str, use_query_cache=True
        )
        contents_iterator: Iterable[Dict[str, Any]] = BigQueryResultsContentsHandle(
            query_job
        ).get_contents_iterator()

        self.assertEqual(expected_result, list(contents_iterator))

    def create_mock_table(
        self,
        address: BigQueryAddress,
        schema: List[bigquery.SchemaField],
        check_exists: bool | None = True,
        create_dataset: bool | None = True,
    ) -> None:
        dataset_ref = self.bq_client.dataset_ref_for_id(address.dataset_id)
        if create_dataset:
            self.bq_client.create_dataset_if_necessary(dataset_ref)

        if check_exists and self.bq_client.table_exists(dataset_ref, address.table_id):
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
