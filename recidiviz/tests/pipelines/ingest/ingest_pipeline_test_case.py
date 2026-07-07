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
"""Shared base for activity and identity ingest pipeline tests."""

import abc

import apache_beam
from mock import patch
from mock.mock import _patch
from more_itertools import one

from recidiviz.ingest.direct.dataset_config import raw_tables_dataset_for_region
from recidiviz.ingest.direct.direct_ingest_regions import get_direct_ingest_region
from recidiviz.ingest.direct.types.direct_ingest_instance import DirectIngestInstance
from recidiviz.source_tables import activity_pipeline_output_table_collector
from recidiviz.source_tables.collect_all_source_table_configs import (
    build_raw_data_source_table_collections_for_state_and_instance,
)
from recidiviz.source_tables.source_table_config import SourceTableCollection
from recidiviz.tests.big_query.big_query_emulator_test_case import (
    BigQueryEmulatorTestCase,
)
from recidiviz.tests.ingest.direct.direct_ingest_raw_fixture_loader import (
    DirectIngestRawDataFixtureLoader,
)
from recidiviz.tests.pipelines.fake_bigquery import (
    FakeReadFromBigQueryWithEmulator,
    FakeWriteToBigQueryEmulator,
)
from recidiviz.tests.pipelines.ingest.ingest_region_test_mixin import (
    IngestRegionTestMixin,
)


class IngestPipelineTestCase(BigQueryEmulatorTestCase, IngestRegionTestMixin):
    """Shared base for activity and identity ingest pipeline tests."""

    wipe_emulator_data_on_teardown = False
    direct_ingest_regions_patcher: _patch | None

    @classmethod
    @abc.abstractmethod
    def expected_output_collections(cls) -> list[SourceTableCollection]:
        """Must be implemented by subclasses to return the pipeline-specific
        output table collections (already wrapped as sandbox collections)."""

    @classmethod
    def get_source_tables(cls) -> list[SourceTableCollection]:
        raw_data_collections = (
            build_raw_data_source_table_collections_for_state_and_instance(
                cls.state_code(),
                DirectIngestInstance.PRIMARY,
                region_module_override=cls.region_module_override(),
            )
        )

        # For performance reasons, only load the schemas for the raw_data
        # dataset our ingest views actually depend on.
        raw_tables_dataset = raw_tables_dataset_for_region(
            state_code=cls.state_code(), instance=DirectIngestInstance.PRIMARY
        )
        us_xx_raw_data_collection = one(
            c for c in raw_data_collections if c.dataset_id == raw_tables_dataset
        )
        return [
            *cls.expected_output_collections(),
            us_xx_raw_data_collection,
        ]

    @classmethod
    def setUpClass(cls) -> None:
        cls.direct_ingest_regions_patcher = None
        if cls.region_module_override():
            cls.direct_ingest_regions_patcher = patch(
                f"{activity_pipeline_output_table_collector.__name__}.direct_ingest_regions",
                autospec=True,
            )
            mock_direct_ingest_regions = cls.direct_ingest_regions_patcher.start()
            mock_direct_ingest_regions.get_direct_ingest_region.side_effect = (
                lambda region_code: get_direct_ingest_region(
                    region_code, region_module_override=cls.region_module_override()
                )
            )
        super().setUpClass()

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

    def tearDown(self) -> None:
        self.region_patcher.stop()
        self._clear_emulator_table_data()
        super().tearDown()

    @classmethod
    def tearDownClass(cls) -> None:
        super().tearDownClass()
        if cls.direct_ingest_regions_patcher:
            cls.direct_ingest_regions_patcher.stop()

    def setup_region_raw_data_bq_tables(self, test_name: str) -> None:
        self.raw_fixture_loader.load_raw_fixtures_to_emulator(
            self.ingest_view_collector().get_query_builders(),
            ingest_test_identifier=test_name,
            create_tables=False,
        )

    def create_fake_bq_read_source_constructor(
        self,
        query: str,
        # pylint: disable=unused-argument
        use_standard_sql: bool,
        validate: bool,
        bigquery_job_labels: dict[str, str],
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
