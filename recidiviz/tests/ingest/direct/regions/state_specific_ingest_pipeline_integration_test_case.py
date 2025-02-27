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
"""Defines a based class for ingest pipeline integration tests"""
from typing import Any, Iterable
from unittest.mock import patch

import apache_beam as beam
from apache_beam.pvalue import PBegin

from recidiviz.common.constants.states import StateCode
from recidiviz.ingest.direct.ingest_mappings.ingest_view_contents_context import (
    IngestViewContentsContext,
)
from recidiviz.ingest.direct.types.direct_ingest_instance import DirectIngestInstance
from recidiviz.pipelines.ingest.state.generate_ingest_view_results import (
    GenerateIngestViewResults,
)
from recidiviz.tests.pipelines.ingest.state.pipeline_test_case import (
    StateIngestPipelineTestCase,
)

PIPELINE_INTEGRATION_TEST_NAME = "pipeline_integration"


class FakeGenerateIngestViewResults(GenerateIngestViewResults):
    def __init__(
        self,
        project_id: str,
        state_code: StateCode,
        ingest_view_name: str,
        raw_data_tables_to_upperbound_dates: dict[str, str],
        raw_data_source_instance: DirectIngestInstance,
        fake_ingest_view_results: Iterable[dict[str, Any]],
    ) -> None:
        super().__init__(
            project_id,
            state_code,
            ingest_view_name,
            raw_data_tables_to_upperbound_dates,
            raw_data_source_instance,
        )
        self.fake_ingest_view_results = fake_ingest_view_results

    def expand(self, input_or_inputs: PBegin) -> beam.PCollection[dict[str, Any]]:
        return input_or_inputs | beam.Create(self.fake_ingest_view_results)


class StateSpecificIngestPipelineIntegrationTestCase(StateIngestPipelineTestCase):
    """
    This class provides an integration test to be used by all states that do ingest.

    It currently reads in fixture files of ingest view output, rather than beginning with
    raw data.
    TODO(#22059): Standardize fixture formats and consolidate this with StateIngestPipelineTestCase
    """

    def setUp(self) -> None:
        super().setUp()
        self.maxDiff = None

        self.environment_patcher = patch("recidiviz.utils.environment.in_gcp_staging")
        self.mock_environment_fn = self.environment_patcher.start()
        self.mock_environment_fn.return_value = True

        self.context_patcher = patch(
            "recidiviz.pipelines.ingest.state.pipeline.IngestViewContentsContext.build_for_project"
        )
        self.context_patcher_fn = self.context_patcher.start()
        self.context_patcher_fn.return_value = (
            IngestViewContentsContext.build_for_tests()
        )

    def tearDown(self) -> None:
        self.environment_patcher.stop()
        self.context_patcher_fn.stop()
        super().tearDown()

    def generate_ingest_view_results_for_one_date(
        self,
        project_id: str,
        state_code: StateCode,
        ingest_view_name: str,
        raw_data_tables_to_upperbound_dates: dict[str, str],
        raw_data_source_instance: DirectIngestInstance,
        # pylint: disable=unused-argument
        resource_labels: dict[str, str],
    ) -> FakeGenerateIngestViewResults:
        """Returns a constructor that generates ingest view results for a given ingest view assuming a single date."""
        return FakeGenerateIngestViewResults(
            project_id,
            state_code,
            ingest_view_name,
            raw_data_tables_to_upperbound_dates,
            raw_data_source_instance,
            self.get_ingest_view_results_from_fixture(
                ingest_view_name=ingest_view_name,
                test_name=ingest_view_name,
                fixture_has_metadata_columns=False,
                generate_default_metadata=True,
                use_results_fixture=False,
            ),
        )

    # TODO(#22059): Remove this method and replace with the implementation on
    # StateIngestPipelineTestCase when fixture formats and data loading is standardized.
    def run_test_state_pipeline(self, create_expected: bool = False) -> None:
        """This runs a test for an ingest pipeline where the ingest view results are
        passed in directly.
        """
        with patch(
            "recidiviz.pipelines.ingest.state.pipeline.GenerateIngestViewResults",
            self.generate_ingest_view_results_for_one_date,
        ):
            self.run_test_ingest_pipeline(
                test_name=PIPELINE_INTEGRATION_TEST_NAME,
                create_expected=create_expected,
                build_for_integration_test=True,
            )
