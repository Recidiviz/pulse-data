# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2026 Recidiviz, Inc.
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
"""Base TestCase class for tests that run the identity ingest pipeline."""
from recidiviz.big_query.big_query_address import BigQueryAddress
from recidiviz.pipelines.ingest.identity.dataset_config import (
    identity_cluster_dataset_for_tenant,
)
from recidiviz.pipelines.ingest.identity.pipeline import IdentityIngestPipeline
from recidiviz.source_tables.identity_pipeline_output_table_collector import (
    build_identity_cluster_output_source_table_collection,
)
from recidiviz.source_tables.source_table_config import SourceTableCollection
from recidiviz.tests.ingest.direct.fixture_util import fixture_path_for_address
from recidiviz.tests.pipelines.ingest.identity.identity_ingest_region_test_mixin import (
    IdentityIngestRegionTestMixin,
)
from recidiviz.tests.pipelines.ingest.ingest_pipeline_test_case import (
    IngestPipelineTestCase,
)
from recidiviz.tests.pipelines.utils.run_pipeline_test_utils import (
    DEFAULT_TEST_PIPELINE_OUTPUT_SANDBOX_PREFIX,
    run_test_pipeline,
)
from recidiviz.tests.test_setup_utils import BQ_EMULATOR_PROJECT_ID


class IdentityIngestPipelineTestCase(
    IngestPipelineTestCase, IdentityIngestRegionTestMixin
):
    """Base TestCase class for tests that run the identity ingest pipeline."""

    @classmethod
    def expected_output_collections(cls) -> list[SourceTableCollection]:
        return [
            build_identity_cluster_output_source_table_collection(
                cls.tenant().value
            ).as_sandbox_collection(DEFAULT_TEST_PIPELINE_OUTPUT_SANDBOX_PREFIX)
        ]

    @classmethod
    def expected_clustering_output_dataset(cls) -> str:
        return identity_cluster_dataset_for_tenant(
            cls.tenant().value,
            sandbox_dataset_prefix=DEFAULT_TEST_PIPELINE_OUTPUT_SANDBOX_PREFIX,
        )

    def run_test_identity_ingest_pipeline(
        self,
        test_name: str,
        create_expected: bool = False,
        raw_data_upper_bound_dates_json_override: str | None = None,
    ) -> None:
        """Runs the identity ingest pipeline against the BQ emulator and
        compares the output against fixture files.

        If you are updating identity ingest logic and expect outputs to change,
        set ``create_expected=True`` to regenerate the fixture files.
        """
        run_test_pipeline(
            pipeline_cls=IdentityIngestPipeline,
            state_code=self.state_code().value,
            project_id=BQ_EMULATOR_PROJECT_ID,
            read_from_bq_constructor=self.create_fake_bq_read_source_constructor,
            write_to_bq_constructor=self.create_fake_bq_write_sink_constructor,
            raw_data_upper_bound_dates_json=(
                raw_data_upper_bound_dates_json_override
                if raw_data_upper_bound_dates_json_override
                else self.raw_fixture_loader.default_upper_bound_dates_json
            ),
        )

        for collection in self.expected_output_collections():
            for address in collection.source_tables_by_address:
                # We run everything as a sandbox, so we remove the sandbox
                # prefix for fixture purposes.
                fixture_address = BigQueryAddress(
                    dataset_id=address.dataset_id.removeprefix(
                        DEFAULT_TEST_PIPELINE_OUTPUT_SANDBOX_PREFIX + "_"
                    ),
                    table_id=address.table_id,
                )
                fixture_path = fixture_path_for_address(
                    self.state_code(), fixture_address, test_name
                )

                try:
                    self.compare_table_to_fixture(
                        address,
                        expected_output_fixture_path=fixture_path,
                        columns_to_ignore=[],
                        create_expected=create_expected,
                        expect_missing_fixtures_on_empty_results=True,
                        expect_unique_output_rows=True,
                    )
                except FileNotFoundError as e:
                    raise ValueError(
                        f"No fixture file found corresponding to results for "
                        f"[{address.to_str()}]. Expected to find fixture at path "
                        f"[{fixture_path}]. If you expect the test to produce "
                        f"results for [{address.to_str()}], then rerun this test "
                        f"with create_expected=True to generate the fixture."
                    ) from e
