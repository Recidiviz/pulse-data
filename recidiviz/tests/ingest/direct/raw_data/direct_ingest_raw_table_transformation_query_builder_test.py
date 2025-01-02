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
"""Unit tests for DirectIngestTempRawTablePreMigrationTransformationQueryBuilder"""
import datetime
import os
from enum import Enum
from typing import Tuple

import pandas as pd

from recidiviz.big_query.big_query_address import BigQueryAddress
from recidiviz.common.constants.states import StateCode
from recidiviz.ingest.direct.raw_data.direct_ingest_raw_table_schema_builder import (
    RawDataTableBigQuerySchemaBuilder,
)
from recidiviz.ingest.direct.raw_data.direct_ingest_raw_table_transformation_query_builder import (
    DirectIngestTempRawTablePreMigrationTransformationQueryBuilder,
)
from recidiviz.ingest.direct.raw_data.raw_file_configs import (
    DirectIngestRawFileConfig,
    get_region_raw_file_config,
)
from recidiviz.ingest.direct.types.direct_ingest_instance import DirectIngestInstance
from recidiviz.tests.big_query.big_query_emulator_test_case import (
    BigQueryEmulatorTestCase,
)
from recidiviz.tests.ingest.direct import fake_regions
from recidiviz.tests.ingest.direct.fixture_util import load_dataframe_from_path
from recidiviz.tests.ingest.direct.raw_data import transformation_fixtures


class TransformationFixtureType(Enum):
    input = "input"
    output = "output"


class TestDirectIngestTempRawTablePreMigrationTransformation(BigQueryEmulatorTestCase):
    """Unit tests for DirectIngestTempRawTablePreMigrationTransformationQueryBuilder"""

    def setUp(self) -> None:
        super().setUp()
        self.region_raw_file_config = get_region_raw_file_config(
            region_code=StateCode.US_XX.value, region_module=fake_regions
        )

        self.query_builder = (
            DirectIngestTempRawTablePreMigrationTransformationQueryBuilder(
                region_raw_file_config=self.region_raw_file_config,
                raw_data_instance=DirectIngestInstance.PRIMARY,
            )
        )

    def _get_raw_data_from_fixture(
        self,
        fixture_directory_name: str,
        file_tag: str,
        ftype: TransformationFixtureType,
    ) -> pd.DataFrame:
        raw_fixture_path = os.path.join(
            os.path.dirname(transformation_fixtures.__file__),
            fixture_directory_name,
            f"{file_tag}_{ftype.value}.csv",
        )
        return load_dataframe_from_path(raw_fixture_path, fixture_columns=None)

    def _load_temp_table(
        self,
        address: BigQueryAddress,
        raw_file_config: DirectIngestRawFileConfig,
        data: pd.DataFrame,
    ) -> None:
        self.create_mock_table(
            address=address,
            schema=RawDataTableBigQuerySchemaBuilder.build_bq_schema_for_config(
                raw_file_config=raw_file_config, include_recidiviz_managed_fields=False
            ),
        )
        self.load_rows_into_table(address=address, data=data.to_dict("records"))

    def load_raw_data_for_directory(
        self, fixture_directory_name: str, file_tag: str
    ) -> Tuple[BigQueryAddress, pd.DataFrame]:

        raw_file_config = self.region_raw_file_config.raw_file_configs[file_tag]

        temp_raw_data = self._get_raw_data_from_fixture(
            fixture_directory_name, file_tag, TransformationFixtureType.input
        )

        address = BigQueryAddress(dataset_id="temp_dataset", table_id=f"{file_tag}__1")

        self._load_temp_table(
            address,
            raw_file_config,
            temp_raw_data,
        )

        exepcted_output = self._get_raw_data_from_fixture(
            fixture_directory_name, file_tag, TransformationFixtureType.output
        )

        return address, exepcted_output

    def run_test(self, test_name: str, file_tag: str) -> None:
        source_table, expected_output = self.load_raw_data_for_directory(
            test_name, file_tag
        )
        update_datetime = (
            datetime.datetime(2024, 1, 1, 1, 1, 1, 1111, tzinfo=datetime.UTC)
            if test_name == "update_datetime_microseconds"
            else datetime.datetime(2024, 1, 1, 1, 1, 1, tzinfo=datetime.UTC)
        )
        query = self.query_builder.build_pre_migration_transformations_query(
            project_id=self.project_id,
            file_tag=file_tag,
            source_table=source_table,
            file_id=1,
            update_datetime=update_datetime,
            is_deleted=False,
        )

        results = self.query(query)

        self.compare_expected_and_result_dfs(expected=expected_output, results=results)

    def test_no_rows(self) -> None:
        self.run_test("no_rows", "singlePrimaryKey")

    def test_no_changes(self) -> None:
        self.run_test("no_changes", "singlePrimaryKey")

    def test_trimming_needed(self) -> None:
        self.run_test("trimming_needed", "singlePrimaryKey")

    def test_trim_and_nulls(self) -> None:
        self.run_test("trim_and_nulls", "singlePrimaryKey")

    def test_update_datetime_microseconds(self) -> None:
        self.run_test("update_datetime_microseconds", "singlePrimaryKey")
