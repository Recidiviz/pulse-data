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
from recidiviz.ingest.direct.direct_ingest_regions import get_direct_ingest_region
from recidiviz.pipelines.pipeline_names import (
    INGEST_PIPELINE_NAME,
    METRICS_PIPELINE_NAME,
    SUPPLEMENTAL_PIPELINE_NAME,
)
from recidiviz.source_tables import ingest_pipeline_output_table_collector
from recidiviz.tests.big_query.big_query_emulator_test_case import (
    BigQueryEmulatorTestCase,
)
from recidiviz.tests.ingest.direct import fake_regions as fake_regions_module
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
                f"{create_or_update_dataflow_sandbox_module.__name__}.get_direct_ingest_states_existing_in_env",
                return_value=self.existing_states,
            ),
            patch(
                f"{ingest_pipeline_output_table_collector.__name__}.get_direct_ingest_states_existing_in_env",
                return_value=self.existing_states,
            ),
        ]
        for patcher in self.existing_states_patchers:
            patcher.start()

        self.direct_ingest_regions_patcher = patch(
            f"{ingest_pipeline_output_table_collector.__name__}.direct_ingest_regions",
            autospec=True,
        )
        self.mock_direct_ingest_regions = self.direct_ingest_regions_patcher.start()
        self.mock_direct_ingest_regions.get_direct_ingest_region.side_effect = (
            lambda region_code: get_direct_ingest_region(
                region_code, region_module_override=fake_regions_module
            )
        )
        # TODO(#36168) Remove this once we have a better way to handle row access policy queries in the emulator
        self.bq_client_patcher = patch(
            f"{create_or_update_dataflow_sandbox_module.__name__}.BigQueryClientImpl",
            return_value=self.bq_client,
        )
        self.bq_client_patcher.start()

    def tearDown(self) -> None:
        super().tearDown()
        self.direct_ingest_regions_patcher.stop()
        for patcher in self.existing_states_patchers:
            patcher.stop()
        self.bq_client_patcher.stop()

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

    def test_create_ingest(self) -> None:
        create_or_update_dataflow_sandbox(
            sandbox_dataset_prefix="sandboxed",
            pipelines=[INGEST_PIPELINE_NAME],
            allow_overwrite=False,
        )

        self.assertListEqual(
            sorted(dataset.dataset_id for dataset in self.bq_client.list_datasets()),
            [
                "sandboxed_us_xx_ingest_view_results",
                "sandboxed_us_xx_normalized_state",
                "sandboxed_us_xx_state",
                "sandboxed_us_yy_ingest_view_results",
                "sandboxed_us_yy_normalized_state",
                "sandboxed_us_yy_state",
            ],
        )

        with self.assertRaisesRegex(
            ValueError, r"^Dataset sandboxed_.* already exists.*"
        ):
            create_or_update_dataflow_sandbox(
                sandbox_dataset_prefix="sandboxed",
                pipelines=[INGEST_PIPELINE_NAME],
                allow_overwrite=False,
            )
