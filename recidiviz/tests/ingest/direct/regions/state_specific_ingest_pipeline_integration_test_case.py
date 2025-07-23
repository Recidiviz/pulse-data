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
import os
from typing import Any, Iterable
from unittest.mock import patch

import apache_beam as beam
import pandas as pd
from apache_beam.pvalue import PBegin

from recidiviz.ingest.direct.ingest_mappings.ingest_view_contents_context import (
    IngestViewContentsContext,
)
from recidiviz.ingest.direct.types.direct_ingest_instance import DirectIngestInstance
from recidiviz.ingest.direct.views.direct_ingest_view_query_builder import (
    DirectIngestViewQueryBuilder,
)
from recidiviz.pipelines.ingest.state.generate_ingest_view_results import (
    GenerateIngestViewResults,
)
from recidiviz.tests.ingest.direct.fixture_util import (
    ingest_view_results_directory,
    read_ingest_view_results_fixture,
)
from recidiviz.tests.pipelines.ingest.state.pipeline_test_case import (
    StateIngestPipelineTestCase,
)

PIPELINE_INTEGRATION_TEST_NAME = "pipeline_integration"


class FakeGenerateIngestViewResults(GenerateIngestViewResults):
    def __init__(
        self,
        ingest_view_builder: DirectIngestViewQueryBuilder,
        raw_data_tables_to_upperbound_dates: dict[str, str],
        raw_data_source_instance: DirectIngestInstance,
        resource_labels: dict[str, str],
        fake_ingest_view_results: Iterable[dict[str, Any]],
    ) -> None:
        super().__init__(
            ingest_view_builder,
            raw_data_tables_to_upperbound_dates,
            raw_data_source_instance,
            resource_labels,
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

    @classmethod
    def INGEST_VIEWS_TO_USE_ALL_RESULT_FIXTURES_AS_INPUT(cls) -> set[str]:
        """
        Returns a set of ingest view names. For each ingest view (name) in
        this set, we will use the results of our ingest view tests as the input
        for the integration test.

        For example, if this set contains "sentence" in US_MO, then
        the integration test will use all CSVs in
        us_mo_ingest_view_results/sentence/ as the input to the test.
        (before it would have pulled sentence.csv, a single file that wasn't
        necessarily representative of the full set of data).

        TODO(#36159) we can eliminate this method when
        legacy_generate_ingest_view_results_for_one_date is removed.
        """
        return set()

    # TODO(#38321): Delete this when all ingest view and mapping tests are migrated.
    def legacy_generate_ingest_view_results_for_one_date(
        self,
        ingest_view_builder: DirectIngestViewQueryBuilder,
        raw_data_tables_to_upperbound_dates: dict[str, str],
        raw_data_source_instance: DirectIngestInstance,
        resource_labels: dict[str, str],
    ) -> FakeGenerateIngestViewResults:
        """Returns a constructor that generates ingest view results for a given ingest view assuming a single date."""
        if (
            ingest_view_builder.ingest_view_name
            in self.INGEST_VIEWS_TO_USE_ALL_RESULT_FIXTURES_AS_INPUT()
        ):
            return self.generate_ingest_view_results_for_one_date(
                ingest_view_builder,
                raw_data_tables_to_upperbound_dates,
                raw_data_source_instance,
                resource_labels,
            )

        return FakeGenerateIngestViewResults(
            ingest_view_builder,
            raw_data_tables_to_upperbound_dates,
            raw_data_source_instance,
            resource_labels,
            self.read_legacy_extract_and_merge_fixture(
                ingest_view_name=ingest_view_builder.ingest_view_name,
                test_name=ingest_view_builder.ingest_view_name,
            ),
        )

    def generate_ingest_view_results_for_one_date(
        self,
        ingest_view_builder: DirectIngestViewQueryBuilder,
        raw_data_tables_to_upperbound_dates: dict[str, str],
        raw_data_source_instance: DirectIngestInstance,
        resource_labels: dict[str, str],
    ) -> FakeGenerateIngestViewResults:
        """
        Returns a Transform that generates ingest view results for
        a given ingest view assuming a single date.

        Data is taken from the results of our ingest view tests, specifically
        from fixtures in direct_ingest_fixtures/us_xx/us_xx_ingest_view_results
        """
        # TODO(#38258) Decide on if/how we subset the amount of
        # data used for integration tests.
        ingest_view_name = ingest_view_builder.ingest_view_name
        view_dir = os.path.join(
            ingest_view_results_directory(self.state_code()), ingest_view_name
        )
        # We can stack the data here because we assume each ingest view test
        # case an independent root entity. Using the same root entity in
        # multiple ingest view tests means they should be able to merge
        # in our integration tests (and in production!)
        ingest_view_results = pd.concat(
            [
                read_ingest_view_results_fixture(
                    self.state_code(), ingest_view_name, fixture_name
                )
                for fixture_name in os.listdir(view_dir)
                if fixture_name.endswith(".csv")
            ],
            axis=0,  # stack data
        )
        return FakeGenerateIngestViewResults(
            ingest_view_builder,
            raw_data_tables_to_upperbound_dates,
            raw_data_source_instance,
            resource_labels,
            ingest_view_results.to_dict("records"),
        )

    # TODO(#38321): Delete this when all ingest view and mapping tests are migrated.
    # TODO(#22059): Remove this method and replace with the implementation on
    # StateIngestPipelineTestCase when fixture formats and data loading is standardized.
    def run_legacy_test_state_pipeline_from_deprecated_fixtures(
        self, create_expected: bool = False
    ) -> None:
        """This runs a test for an ingest pipeline where the ingest view results are
        passed in directly.
        """
        with patch(
            "recidiviz.pipelines.ingest.state.process_ingest_view.GenerateIngestViewResults",
            self.legacy_generate_ingest_view_results_for_one_date,
        ):
            self.run_test_ingest_pipeline(
                test_name=PIPELINE_INTEGRATION_TEST_NAME,
                create_expected=create_expected,
                build_for_integration_test=True,
            )

    def run_state_ingest_pipeline_integration_test(
        self, create_expected_output: bool = False
    ) -> None:
        """
        This runs a an ingest pipeline against us_xx_ingest_view_results
        data passed in directly (for performance). These fixtures are
        the results of individual ingest view tests.
        """
        with patch(
            "recidiviz.pipelines.ingest.state.process_ingest_view.GenerateIngestViewResults",
            self.generate_ingest_view_results_for_one_date,
        ):
            self.run_test_ingest_pipeline(
                test_name=PIPELINE_INTEGRATION_TEST_NAME,
                create_expected=create_expected_output,
                build_for_integration_test=True,
            )
