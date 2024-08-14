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
"""Tests basic functionality of the create_or_update_dataflow_sandbox tool"""
from unittest.mock import patch

import pytest

from recidiviz.common.constants.states import StateCode
from recidiviz.pipelines.pipeline_names import (
    METRICS_PIPELINE_NAME,
    NORMALIZATION_PIPELINE_NAME,
    SUPPLEMENTAL_PIPELINE_NAME,
)
from recidiviz.source_tables import normalization_pipeline_output_table_collector
from recidiviz.tests.big_query.big_query_emulator_test_case import (
    BigQueryEmulatorTestCase,
)
from recidiviz.tools.calculator import (
    create_or_update_dataflow_sandbox as create_or_update_dataflow_sandbox_module,
)
from recidiviz.tools.calculator.create_or_update_dataflow_sandbox import (
    create_or_update_dataflow_sandbox,
)


@pytest.mark.uses_bq_emulator
class CreateOrUpdateDataflowSandboxTest(BigQueryEmulatorTestCase):
    """Tests basic functionality of the create_or_update_dataflow_sandbox tool"""

    def setUp(self) -> None:
        super().setUp()
        self.existing_states = [StateCode.US_XX, StateCode.US_YY]
        self.existing_states_patchers = [
            patch(
                f"{normalization_pipeline_output_table_collector.__name__}.get_direct_ingest_states_existing_in_env",
                return_value=self.existing_states,
            ),
            patch(
                f"{create_or_update_dataflow_sandbox_module.__name__}.get_direct_ingest_states_existing_in_env",
                return_value=self.existing_states,
            ),
        ]
        for patcher in self.existing_states_patchers:
            patcher.start()

    def tearDown(self) -> None:
        super().tearDown()
        for patcher in self.existing_states_patchers:
            patcher.stop()

    def test_create_supplemental(self) -> None:
        create_or_update_dataflow_sandbox(
            sandbox_dataset_prefix="sandboxed",
            pipelines=[SUPPLEMENTAL_PIPELINE_NAME],
            allow_overwrite=False,
        )

        self.assertListEqual(
            [dataset.dataset_id for dataset in self.bq_client.list_datasets()],
            ["sandboxed_supplemental_data"],
        )

        with self.assertRaisesRegex(
            ValueError, r"^Dataset sandboxed_supplemental_data already exists.*"
        ):
            create_or_update_dataflow_sandbox(
                sandbox_dataset_prefix="sandboxed",
                pipelines=[SUPPLEMENTAL_PIPELINE_NAME],
                allow_overwrite=False,
            )

    def test_create_metrics(self) -> None:
        create_or_update_dataflow_sandbox(
            sandbox_dataset_prefix="sandboxed",
            pipelines=[METRICS_PIPELINE_NAME],
            allow_overwrite=False,
        )

        self.assertListEqual(
            [dataset.dataset_id for dataset in self.bq_client.list_datasets()],
            ["sandboxed_dataflow_metrics"],
        )

        with self.assertRaisesRegex(
            ValueError, r"^Dataset sandboxed_dataflow_metrics already exists.*"
        ):
            create_or_update_dataflow_sandbox(
                sandbox_dataset_prefix="sandboxed",
                pipelines=[METRICS_PIPELINE_NAME],
                allow_overwrite=False,
            )

    def test_create_normalization_with_filter(self) -> None:
        create_or_update_dataflow_sandbox(
            sandbox_dataset_prefix="sandboxed",
            pipelines=[NORMALIZATION_PIPELINE_NAME],
            allow_overwrite=False,
            state_code_filter=StateCode.US_XX,
        )

        self.assertListEqual(
            [dataset.dataset_id for dataset in self.bq_client.list_datasets()],
            ["sandboxed_us_xx_normalized_state"],
        )

        with self.assertRaisesRegex(
            ValueError, r"^Dataset sandboxed_us_xx_normalized_state already exists.*"
        ):
            create_or_update_dataflow_sandbox(
                sandbox_dataset_prefix="sandboxed",
                pipelines=[NORMALIZATION_PIPELINE_NAME],
                allow_overwrite=False,
                state_code_filter=StateCode.US_XX,
            )

    def test_create_normalization_without_filter(self) -> None:
        create_or_update_dataflow_sandbox(
            sandbox_dataset_prefix="sandboxed",
            pipelines=[NORMALIZATION_PIPELINE_NAME],
            allow_overwrite=False,
        )

        self.assertListEqual(
            [dataset.dataset_id for dataset in self.bq_client.list_datasets()],
            ["sandboxed_us_xx_normalized_state", "sandboxed_us_yy_normalized_state"],
        )

        with self.assertRaisesRegex(
            ValueError, r"^Dataset sandboxed_us_xx_normalized_state already exists.*"
        ):
            create_or_update_dataflow_sandbox(
                sandbox_dataset_prefix="sandboxed",
                pipelines=[NORMALIZATION_PIPELINE_NAME],
                allow_overwrite=False,
                state_code_filter=StateCode.US_XX,
            )

    # TODO(#30495): Write a test for creating an ingest sandbox once we don't have to
    #  load all raw data tables in order to do so.
