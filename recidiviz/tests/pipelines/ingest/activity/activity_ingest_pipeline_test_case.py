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
"""Base TestCase class for tests that run the activity ingest pipeline."""
from recidiviz.big_query.big_query_address import BigQueryAddress
from recidiviz.ingest.direct.types.direct_ingest_constants import (
    MATERIALIZATION_TIME_COL_NAME,
)
from recidiviz.pipelines.ingest.activity.dataset_config import (
    ingest_view_materialization_results_dataset,
    normalized_state_dataset_for_state_code,
    state_dataset_for_state_code,
)
from recidiviz.pipelines.ingest.activity.pipeline import StateIngestPipeline
from recidiviz.source_tables.ingest_pipeline_output_table_collector import (
    build_ingest_view_results_source_table_collection,
    build_normalized_state_output_source_table_collection,
    build_state_output_source_table_collection,
)
from recidiviz.source_tables.source_table_config import SourceTableCollection
from recidiviz.tests.ingest.direct.fixture_util import fixture_path_for_address
from recidiviz.tests.pipelines.ingest.activity.activity_ingest_region_test_mixin import (
    ActivityIngestRegionTestMixin,
)
from recidiviz.tests.pipelines.ingest.ingest_pipeline_test_case import (
    IngestPipelineTestCase,
)
from recidiviz.tests.pipelines.utils.run_pipeline_test_utils import (
    DEFAULT_TEST_PIPELINE_OUTPUT_SANDBOX_PREFIX,
    run_test_pipeline,
)
from recidiviz.tests.test_setup_utils import BQ_EMULATOR_PROJECT_ID
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override


class ActivityIngestPipelineTestCase(
    IngestPipelineTestCase, ActivityIngestRegionTestMixin
):
    """Base TestCase class for tests that run the activity ingest pipeline."""

    @classmethod
    def expected_output_collections(cls) -> list[SourceTableCollection]:
        with local_project_id_override(GCP_PROJECT_STAGING):
            ingest_view_results_collection = (
                build_ingest_view_results_source_table_collection(cls.state_code())
            )

        collections = [
            ingest_view_results_collection,
            build_state_output_source_table_collection(cls.state_code()),
            build_normalized_state_output_source_table_collection(cls.state_code()),
        ]

        return [
            c.as_sandbox_collection(DEFAULT_TEST_PIPELINE_OUTPUT_SANDBOX_PREFIX)
            for c in collections
        ]

    @classmethod
    def expected_ingest_view_dataset(cls) -> str:
        return ingest_view_materialization_results_dataset(
            cls.state_code(), DEFAULT_TEST_PIPELINE_OUTPUT_SANDBOX_PREFIX
        )

    @classmethod
    def expected_state_dataset(cls) -> str:
        return state_dataset_for_state_code(
            cls.state_code(), DEFAULT_TEST_PIPELINE_OUTPUT_SANDBOX_PREFIX
        )

    @classmethod
    def expected_normalized_state_dataset(cls) -> str:
        return normalized_state_dataset_for_state_code(
            cls.state_code(), DEFAULT_TEST_PIPELINE_OUTPUT_SANDBOX_PREFIX
        )

    def run_test_activity_ingest_pipeline(
        self,
        test_name: str,
        create_expected: bool = False,
        ingest_view_results_only: bool = False,
        pre_normalization_only: bool = False,
        ingest_views_to_run: str | None = None,
        build_for_integration_test: bool = False,
        raw_data_upper_bound_dates_json_override: str | None = None,
    ) -> None:
        """Runs the activity ingest pipeline against the BQ emulator and
        compares the output against fixture files.

        If you are updating activity ingest logic and expect outputs to change,
        set ``create_expected=True`` to regenerate the fixture files.
        """
        run_test_pipeline(
            pipeline_cls=StateIngestPipeline,
            state_code=self.state_code().value,
            project_id=BQ_EMULATOR_PROJECT_ID,
            read_from_bq_constructor=self.create_fake_bq_read_source_constructor,
            write_to_bq_constructor=self.create_fake_bq_write_sink_constructor,
            # Additional pipeline arguments
            ingest_view_results_only=ingest_view_results_only,
            pre_normalization_only=pre_normalization_only,
            ingest_views_to_run=ingest_views_to_run,
            raw_data_upper_bound_dates_json=(
                raw_data_upper_bound_dates_json_override
                if raw_data_upper_bound_dates_json_override
                else self.raw_fixture_loader.default_upper_bound_dates_json
            ),
            build_for_integration_test=build_for_integration_test,
        )

        for collection in self.expected_output_collections():
            for (
                address,
                source_table,
            ) in collection.source_tables_by_address.items():
                # We run everything as a sandbox, so we remove the sandbox prefix for fixture purposes
                fixture_address = BigQueryAddress(
                    dataset_id=address.dataset_id.removeprefix(
                        DEFAULT_TEST_PIPELINE_OUTPUT_SANDBOX_PREFIX + "_"
                    ),
                    table_id=address.table_id,
                )
                fixture_path = fixture_path_for_address(
                    self.state_code(), fixture_address, test_name
                )
                columns = [field.name for field in source_table.schema_fields]

                columns_to_ignore = []
                if MATERIALIZATION_TIME_COL_NAME in columns:
                    columns_to_ignore.append(MATERIALIZATION_TIME_COL_NAME)

                try:
                    self.compare_table_to_fixture(
                        address,
                        expected_output_fixture_path=fixture_path,
                        columns_to_ignore=columns_to_ignore,
                        create_expected=create_expected,
                        expect_missing_fixtures_on_empty_results=True,
                        expect_unique_output_rows=True,
                    )
                except FileNotFoundError as e:
                    raise ValueError(
                        f"No fixture file found corresponding to results for "
                        f"[{address.to_str()}]. Expected to find fixture at path "
                        f"[{fixture_path}]. If you expect the test to produce results "
                        f"for [{address.to_str()}], then rerun this test with "
                        f"create_expected=True to generate the fixture."
                    ) from e
