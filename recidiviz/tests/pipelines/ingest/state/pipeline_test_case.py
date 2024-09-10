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
"""Base TestCase class for tests that run ingest pipelines."""
from typing import Optional

import apache_beam
from mock import patch
from more_itertools import one

from recidiviz.big_query.big_query_address import BigQueryAddress
from recidiviz.ingest.direct.dataset_config import raw_tables_dataset_for_region
from recidiviz.ingest.direct.types.direct_ingest_constants import (
    MATERIALIZATION_TIME_COL_NAME,
)
from recidiviz.ingest.direct.types.direct_ingest_instance import DirectIngestInstance
from recidiviz.pipelines.ingest.dataset_config import (
    ingest_view_materialization_results_dataset,
    state_dataset_for_state_code,
)
from recidiviz.pipelines.ingest.state.pipeline import StateIngestPipeline
from recidiviz.pipelines.normalization.dataset_config import (
    normalized_state_dataset_for_state_code_ingest_pipeline_output,
)
from recidiviz.source_tables.collect_all_source_table_configs import (
    build_raw_data_source_table_collections_for_state_and_instance,
)
from recidiviz.source_tables.ingest_pipeline_output_table_collector import (
    build_ingest_view_source_table_configs,
    build_normalized_state_output_source_table_collection,
    build_state_output_source_table_collection,
)
from recidiviz.source_tables.source_table_config import SourceTableCollection
from recidiviz.source_tables.source_table_update_manager import SourceTableUpdateManager
from recidiviz.tests.big_query.big_query_emulator_test_case import (
    BigQueryEmulatorTestCase,
)
from recidiviz.tests.ingest.direct.direct_ingest_raw_fixture_loader import (
    DirectIngestRawDataFixtureLoader,
)
from recidiviz.tests.ingest.direct.fixture_util import DirectIngestTestFixturePath
from recidiviz.tests.pipelines.fake_bigquery import (
    FakeReadFromBigQueryWithEmulator,
    FakeWriteToBigQueryEmulator,
)
from recidiviz.tests.pipelines.ingest.state.ingest_region_test_mixin import (
    IngestRegionTestMixin,
)
from recidiviz.tests.pipelines.utils.run_pipeline_test_utils import (
    DEFAULT_TEST_PIPELINE_OUTPUT_SANDBOX_PREFIX,
    run_test_pipeline,
)
from recidiviz.tests.test_setup_utils import BQ_EMULATOR_PROJECT_ID


class StateIngestPipelineTestCase(BigQueryEmulatorTestCase, IngestRegionTestMixin):
    """Base TestCase class for tests that run ingest pipelines."""

    wipe_emulator_data_on_teardown = False

    @classmethod
    def _expected_output_collections(cls) -> list[SourceTableCollection]:
        collections = [
            build_state_output_source_table_collection(cls.state_code()),
            build_normalized_state_output_source_table_collection(cls.state_code()),
        ]

        return [
            c.as_sandbox_collection(DEFAULT_TEST_PIPELINE_OUTPUT_SANDBOX_PREFIX)
            for c in collections
        ]

    # TODO(#30495): We should be able to include the ingest view source tables in the
    #  _expected_output_collections() class method once we can derive the schemas from
    #  the ingest mappings files.
    def expected_output_collections(self) -> list[SourceTableCollection]:
        return [
            *self._expected_output_collections(),
            self.ingest_views_source_table_collection,
        ]

    @classmethod
    def get_source_tables(cls) -> list[SourceTableCollection]:
        raw_data_collections = (
            build_raw_data_source_table_collections_for_state_and_instance(
                cls.state_code(),
                DirectIngestInstance.PRIMARY,
                region_module_override=cls.region_module_override(),
            )
        )

        # For performance reasons, only load the schemas for the actual tables we'll
        # need in this test.

        # Filter down to just tables in the us_xx_raw_data dataset
        raw_tables_dataset = raw_tables_dataset_for_region(
            state_code=cls.state_code(), instance=DirectIngestInstance.PRIMARY
        )
        us_xx_raw_data_collection = one(
            c for c in raw_data_collections if c.dataset_id == raw_tables_dataset
        )
        return [
            *cls._expected_output_collections(),
            us_xx_raw_data_collection,
        ]

    def setUp(self) -> None:
        super().setUp()
        self.region_patcher = patch(
            "recidiviz.ingest.direct.direct_ingest_regions.get_direct_ingest_region"
        )
        self.region_patcher.start().return_value = self.region()
        self.raw_fixture_loader = DirectIngestRawDataFixtureLoader(
            state_code=self.state_code(),
            emulator_test=self,
            region_module=self.region_module_override(),
        )

        self.ingest_views_source_table_collection = one(
            build_ingest_view_source_table_configs(
                self.bq_client, state_codes=[self.state_code()]
            )
        ).as_sandbox_collection(DEFAULT_TEST_PIPELINE_OUTPUT_SANDBOX_PREFIX)

        self._load_ingest_view_source_tables()

    # TODO(#30495): We should be able to include the ingest view source tables in
    #  get_source_tables() once we can derive the schemas from the ingest mappings
    #  files.
    def _load_ingest_view_source_tables(self) -> None:
        update_manager = SourceTableUpdateManager()
        update_manager.update(self.ingest_views_source_table_collection)

    def tearDown(self) -> None:
        self.region_patcher.stop()
        self._clear_emulator_table_data()
        super().tearDown()

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
        return normalized_state_dataset_for_state_code_ingest_pipeline_output(
            cls.state_code(), DEFAULT_TEST_PIPELINE_OUTPUT_SANDBOX_PREFIX
        )

    def setup_region_raw_data_bq_tables(self, test_name: str) -> None:
        self.raw_fixture_loader.load_raw_fixtures_to_emulator(
            self.ingest_view_collector().get_query_builders(),
            ingest_test_identifier=f"{test_name}.csv",
            file_update_dt=None,
        )

    def create_fake_bq_read_source_constructor(
        self,
        query: str,
        # pylint: disable=unused-argument
        use_standard_sql: bool,
        validate: bool,
    ) -> FakeReadFromBigQueryWithEmulator:
        return FakeReadFromBigQueryWithEmulator(query=query, test_case=self)

    def create_fake_bq_write_sink_constructor(
        self,
        # pylint: disable=unused-argument
        output_table: str,
        output_dataset: str,
        write_disposition: apache_beam.io.BigQueryDisposition,
    ) -> FakeWriteToBigQueryEmulator:
        return FakeWriteToBigQueryEmulator(
            output_dataset=output_dataset,
            output_table=output_table,
            write_disposition=write_disposition,
            test_case=self,
        )

    def _fixture_path_for_pipeline_output_address(
        self, address: BigQueryAddress, test_name: str
    ) -> str:
        """Returns the file path that contains expected results for test |test_name|
        for the table at |address|.
        """
        file_name = f"{test_name}.csv"
        if address.dataset_id == self.expected_ingest_view_dataset():
            fixture_path = (
                DirectIngestTestFixturePath.for_ingest_view_test_results_fixture(
                    region_code=self.state_code().value,
                    ingest_view_name=address.table_id,
                    file_name=file_name,
                )
            )
        elif address.dataset_id == self.expected_state_dataset():
            fixture_path = DirectIngestTestFixturePath.for_state_data_fixture(
                region_code=self.state_code().value,
                table_id=address.table_id,
                file_name=file_name,
            )
        elif address.dataset_id == self.expected_normalized_state_dataset():
            fixture_path = (
                DirectIngestTestFixturePath.for_normalized_state_data_fixture(
                    region_code=self.state_code().value,
                    table_id=address.table_id,
                    file_name=file_name,
                )
            )
        else:
            raise ValueError(
                f"Unexpected dataset [{address.dataset_id}] for pipeline output "
                f"address [{address.to_str()}]"
            )

        return fixture_path.full_path()

    def run_test_ingest_pipeline(
        self,
        test_name: str,
        create_expected: bool = False,
        ingest_view_results_only: bool = False,
        pre_normalization_only: bool = False,
        ingest_views_to_run: Optional[str] = None,
        raw_data_upper_bound_dates_json_override: Optional[str] = None,
        run_normalization_override: bool | None = None,
    ) -> None:
        """Runs an ingest pipeline, writing output the the BQ emulator and comparing
        that output against a set of expected fixture files.

        If you are updating ingest logic and expect logic to change, set
        create_expected=True to have this test output the pipeline results to the
        fixture files.
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
            run_normalization_override=run_normalization_override,
        )

        for collection in self.expected_output_collections():
            for address, source_table in collection.source_tables_by_address.items():
                fixture_path = self._fixture_path_for_pipeline_output_address(
                    address, test_name
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
                    )
                except FileNotFoundError as e:
                    raise ValueError(
                        f"No fixture file found corresponding to results for "
                        f"[{address.to_str()}]. Expected to find fixture at path "
                        f"[{fixture_path}]. If you expect the test to produce results "
                        f"for [{address.to_str()}], then rerun this test with "
                        f"create_expected=True to generate the fixture."
                    ) from e
