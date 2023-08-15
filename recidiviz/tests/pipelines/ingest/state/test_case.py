# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2023 Recidiviz, Inc.
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
"""A class that represents a test case to be used for testing the ingest pipeline."""
from collections import defaultdict
from copy import deepcopy
from datetime import datetime
from typing import Any, Callable, Dict, List, Tuple

from apache_beam.testing.util import BeamAssertException
from dateutil import parser
from mock import patch
from more_itertools import one

from recidiviz.big_query.big_query_address import BigQueryAddress
from recidiviz.common.constants.states import StateCode
from recidiviz.ingest.direct.dataset_config import raw_tables_dataset_for_region
from recidiviz.ingest.direct.ingest_mappings.ingest_view_manifest_collector import (
    IngestViewManifestCollector,
)
from recidiviz.ingest.direct.ingest_mappings.ingest_view_results_parser_delegate import (
    IngestViewResultsParserDelegateImpl,
)
from recidiviz.ingest.direct.raw_data.direct_ingest_raw_file_import_manager import (
    DirectIngestRawFileImportManager,
)
from recidiviz.ingest.direct.raw_data.raw_file_configs import (
    DirectIngestRegionRawFileConfig,
)
from recidiviz.ingest.direct.types.direct_ingest_instance import DirectIngestInstance
from recidiviz.ingest.direct.views.direct_ingest_view_query_builder import (
    DirectIngestViewRawFileDependency,
    RawFileHistoricalRowsFilterType,
)
from recidiviz.ingest.direct.views.direct_ingest_view_query_builder_collector import (
    DirectIngestViewQueryBuilderCollector,
)
from recidiviz.persistence.database.schema_type import SchemaType
from recidiviz.persistence.entity.base_entity import Entity
from recidiviz.pipelines.ingest.state.generate_ingest_view_results import (
    ADDITIONAL_SCHEMA_COLUMNS,
    LOWER_BOUND_DATETIME_COL_NAME,
    MATERIALIZATION_TIME_COL_NAME,
    UPPER_BOUND_DATETIME_COL_NAME,
)
from recidiviz.tests.big_query.big_query_emulator_test_case import (
    BigQueryEmulatorTestCase,
)
from recidiviz.tests.ingest.direct import fake_regions
from recidiviz.tests.ingest.direct.fixture_util import (
    ingest_view_raw_table_dependency_fixture_path,
    ingest_view_results_fixture_path,
    load_dataframe_from_path,
)
from recidiviz.tests.pipelines.fake_bigquery import (
    FakeReadAllFromBigQueryWithEmulator,
    FakeReadFromBigQueryWithEmulator,
)
from recidiviz.tests.utils.fake_region import fake_region


class StateIngestPipelineTestCase(BigQueryEmulatorTestCase):
    """A test case class to test the ingest pipeline."""

    def setUp(self) -> None:
        super().setUp()
        self.region_code = StateCode.US_DD.value.lower()
        self.raw_data_tables_dataset = raw_tables_dataset_for_region(
            StateCode.US_DD, DirectIngestInstance.PRIMARY
        )
        self.region = fake_region(
            region_code=self.region_code,
            environment="staging",
            region_module=fake_regions,
        )
        self.direct_ingest_raw_file_config = DirectIngestRegionRawFileConfig(
            region_code=StateCode.US_DD.value, region_module=fake_regions
        )

        self.region_patcher = patch(
            "recidiviz.ingest.direct.direct_ingest_regions.get_direct_ingest_region"
        )
        self.region_patcher.start().return_value = self.region

        self.raw_file_config_patcher = patch(
            "recidiviz.ingest.direct.views.direct_ingest_view_query_builder"
            ".get_region_raw_file_config"
        )
        self.raw_file_config_patcher.start().return_value = (
            self.direct_ingest_raw_file_config
        )

        self.ingest_view_manifest_collector = IngestViewManifestCollector(
            self.region,
            IngestViewResultsParserDelegateImpl(
                self.region,
                schema_type=SchemaType.STATE,
                ingest_instance=DirectIngestInstance.PRIMARY,
                results_update_datetime=datetime.now(),
            ),
        )
        self.view_collector = DirectIngestViewQueryBuilderCollector(
            self.region,
            self.ingest_view_manifest_collector.launchable_ingest_views(),
        )

    def tearDown(self) -> None:
        super().tearDown()
        self.region_patcher.stop()
        self.raw_file_config_patcher.stop()

    def setup_single_ingest_view_raw_data_bq_tables(
        self, ingest_view_name: str, test_name: str
    ) -> None:
        ingest_view_builder = self.view_collector.get_query_builder_by_view_name(
            ingest_view_name
        )
        for (
            raw_table_dependency_config
        ) in ingest_view_builder.raw_table_dependency_configs:
            self._load_bq_table_for_raw_dependency(
                raw_table_dependency_config, test_name=test_name
            )

    def setup_region_raw_data_bq_tables(self, test_name: str) -> None:
        # Deduplicate raw table dependencies where multiple tables are read from
        # same order.
        configs_by_name_and_filter_type: Dict[
            str,
            Dict[RawFileHistoricalRowsFilterType, DirectIngestViewRawFileDependency],
        ] = defaultdict(dict)
        for (
            ingest_view
        ) in self.ingest_view_manifest_collector.launchable_ingest_views():
            ingest_view_builder = self.view_collector.get_query_builder_by_view_name(
                ingest_view
            )
            for (
                raw_table_dependency_config
            ) in ingest_view_builder.raw_table_dependency_configs:
                configs_by_name_and_filter_type[
                    raw_table_dependency_config.raw_file_config.file_tag
                ][raw_table_dependency_config.filter_type] = raw_table_dependency_config

        for config_by_type in configs_by_name_and_filter_type.values():
            for raw_table_dependency_config in config_by_type.values():
                self._load_bq_table_for_raw_dependency(
                    raw_table_dependency_config, test_name=test_name
                )

    def _load_bq_table_for_raw_dependency(
        self,
        raw_table_dependency_config: DirectIngestViewRawFileDependency,
        test_name: str,
    ) -> None:
        """Sets up the BQ emulator with appropriate raw data tables for the test region."""
        table_address = BigQueryAddress(
            dataset_id=self.raw_data_tables_dataset,
            table_id=raw_table_dependency_config.raw_file_config.file_tag,
        )

        schema = DirectIngestRawFileImportManager.create_raw_table_schema(
            raw_file_config=raw_table_dependency_config.raw_file_config
        )
        self.create_mock_table(table_address, schema)

        self.load_rows_into_table(
            table_address,
            data=load_dataframe_from_path(
                raw_fixture_path=ingest_view_raw_table_dependency_fixture_path(
                    self.region.region_code,
                    raw_file_dependency_config=raw_table_dependency_config,
                    file_name=f"{test_name}.csv",
                ),
                fixture_columns=[c.name for c in schema],
            ).to_dict("records"),
        )

    def get_expected_ingest_view_results(
        self, *, ingest_view_name: str, test_name: str
    ) -> List[Dict[str, Any]]:
        return load_dataframe_from_path(
            raw_fixture_path=ingest_view_results_fixture_path(
                region_code=self.region.region_code,
                ingest_view_name=ingest_view_name,
                file_name=f"{test_name}.csv",
            ),
            fixture_columns=[
                *self.ingest_view_manifest_collector.ingest_view_to_manifest[
                    ingest_view_name
                ].input_columns,
                MATERIALIZATION_TIME_COL_NAME,
                UPPER_BOUND_DATETIME_COL_NAME,
                LOWER_BOUND_DATETIME_COL_NAME,
            ],
        ).to_dict("records")

    def get_expected_root_entities(
        self, *, ingest_view_name: str, test_name: str
    ) -> List[Entity]:
        rows = list(
            self.get_expected_ingest_view_results(
                ingest_view_name=ingest_view_name, test_name=test_name
            )
        )
        for row in rows:
            for column in ADDITIONAL_SCHEMA_COLUMNS:
                row.pop(column.name)
        return self.ingest_view_manifest_collector.manifest_parser.parse(
            ingest_view_name=ingest_view_name,
            contents_iterator=iter(rows),
        )

    def get_expected_root_entities_with_upperbound_dates(
        self, *, ingest_view_name: str, test_name: str
    ) -> List[Tuple[datetime, Entity]]:
        rows = list(
            self.get_expected_ingest_view_results(
                ingest_view_name=ingest_view_name, test_name=test_name
            )
        )
        results: List[Tuple[datetime, Entity]] = []
        for row in rows:
            upper_bound_date = parser.isoparse(row[UPPER_BOUND_DATETIME_COL_NAME])
            for column in ADDITIONAL_SCHEMA_COLUMNS:
                row.pop(column.name)
            results.append(
                (
                    upper_bound_date,
                    one(
                        self.ingest_view_manifest_collector.manifest_parser.parse(
                            ingest_view_name=ingest_view_name,
                            contents_iterator=iter([row]),
                        )
                    ),
                )
            )
        return results

    def create_fake_bq_read_source_constructor(
        self,
        query: str,
    ) -> FakeReadFromBigQueryWithEmulator:
        return FakeReadFromBigQueryWithEmulator(query=query, test_case=self)

    def create_fake_bq_read_all_source_constructor(
        self,
    ) -> FakeReadAllFromBigQueryWithEmulator:
        return FakeReadAllFromBigQueryWithEmulator()

    @staticmethod
    def validate_ingest_view_results(
        expected_output: List[Dict[str, Any]]
    ) -> Callable[[List[Dict[str, Any]]], None]:
        """Allows for validating the output of ingest view results without worrying about
        the output of the materialization time."""

        def _validate_ingest_view_results_output(output: List[Dict[str, Any]]) -> None:
            copy_of_expected_output = deepcopy(expected_output)
            for record in copy_of_expected_output:
                record.pop(MATERIALIZATION_TIME_COL_NAME)
            copy_of_output = deepcopy(output)
            for record in copy_of_output:
                if not MATERIALIZATION_TIME_COL_NAME in record:
                    raise BeamAssertException("Missing materialization time column")
                record.pop(MATERIALIZATION_TIME_COL_NAME)
                if record[LOWER_BOUND_DATETIME_COL_NAME]:
                    record[LOWER_BOUND_DATETIME_COL_NAME] = datetime.isoformat(
                        record[LOWER_BOUND_DATETIME_COL_NAME]
                    )
                record[UPPER_BOUND_DATETIME_COL_NAME] = datetime.isoformat(
                    record[UPPER_BOUND_DATETIME_COL_NAME]
                )
            if copy_of_output != copy_of_expected_output:
                raise BeamAssertException(
                    f"Output does not match expected output: output is {copy_of_output}, expected is {copy_of_expected_output}"
                )

        return _validate_ingest_view_results_output
