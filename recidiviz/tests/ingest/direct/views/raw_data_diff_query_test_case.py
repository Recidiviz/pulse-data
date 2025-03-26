#  Recidiviz - a data platform for criminal justice reform
#  Copyright (C) 2023 Recidiviz, Inc.
#
#  This program is free software: you can redistribute it and/or modify
#  it under the terms of the GNU General Public License as published by
#  the Free Software Foundation, either version 3 of the License, or
#  (at your option) any later version.
#
#  This program is distributed in the hope that it will be useful,
#  but WITHOUT ANY WARRANTY; without even the implied warranty of
#  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#  GNU General Public License for more details.
#
#  You should have received a copy of the GNU General Public License
#  along with this program.  If not, see <https://www.gnu.org/licenses/>.
#  =============================================================================
"""An implementation of BigQueryEmulatorTestCase with functionality specific to raw data diff queries."""
import datetime
import os
from enum import Enum
from typing import Optional

import pandas as pd
import pytest

from recidiviz.big_query.big_query_address import BigQueryAddress
from recidiviz.common.constants.states import StateCode
from recidiviz.ingest.direct.dataset_config import (
    raw_data_pruning_new_raw_data_dataset,
    raw_tables_dataset_for_region,
)
from recidiviz.ingest.direct.raw_data.direct_ingest_raw_table_schema_builder import (
    RawDataTableBigQuerySchemaBuilder,
)
from recidiviz.ingest.direct.raw_data.raw_file_configs import get_region_raw_file_config
from recidiviz.ingest.direct.types.direct_ingest_instance import DirectIngestInstance
from recidiviz.ingest.direct.views.raw_data_diff_query_builder import (
    RawDataDiffQueryBuilder,
)
from recidiviz.tests.big_query.big_query_emulator_test_case import (
    BigQueryEmulatorTestCase,
)
from recidiviz.tests.ingest.direct import fake_regions
from recidiviz.tests.ingest.direct.fixture_util import load_dataframe_from_path
from recidiviz.tests.ingest.direct.raw_data import raw_data_diff_fixtures
from recidiviz.utils import csv


class RawDataDiffFixtureType(Enum):
    # Fixtures meant to mimic existing raw data tables in `us_xx_raw_data` dataset.
    EXISTING = "EXISTING"

    # Fixtures meant to mimic temporary raw data tables to be diffed against.
    NEW = "NEW"

    # Fixtures used to validate diff query results.
    EXPECTED_RESULTS = "EXPECTED_RESULTS"

    def get_absolute_directory_path_from_fixture_type(self, file_tag: str) -> str:
        parent_directory = os.path.dirname(raw_data_diff_fixtures.__file__)
        return os.path.join(
            parent_directory, file_tag, self.get_file_name_from_fixture_type()
        )

    def get_file_name_from_fixture_type(self) -> str:
        if self is RawDataDiffFixtureType.EXISTING:
            file = "existing_raw_data.csv"
        elif self is RawDataDiffFixtureType.NEW:
            file = "new_raw_data.csv"
        elif self is RawDataDiffFixtureType.EXPECTED_RESULTS:
            file = "expected_diff.csv"
        else:
            raise ValueError(f"Unexpected fixture file type [{self}]")

        return file


@pytest.mark.uses_bq_emulator
class RawDataDiffEmulatorQueryTestCase(BigQueryEmulatorTestCase):
    """An extension of BigQueryEmulatorTestCase with functionality specific to testing
    raw data diff queries.
    """

    def setUp(self) -> None:
        super().setUp()
        self.state_code = StateCode.US_XX
        self.raw_data_instance = DirectIngestInstance.PRIMARY

    def _create_mock_raw_bq_table_from_fixture(
        self,
        fixture_directory_name: str,
        file_tag: str,
        fixture_type: RawDataDiffFixtureType,
        new_file_id: Optional[int] = None,
    ) -> None:
        """Create a mock raw data BigQuery table from a raw data fixture."""
        raw_data_df = self._get_raw_data_from_fixture(
            fixture_directory_name, fixture_type
        )
        self._load_mock_raw_table_to_bq(
            file_tag,
            fixture_type,
            raw_data_df,
            new_file_id,
        )

    def _get_raw_data_from_fixture(
        self, fixture_directory_name: str, fixture_type: RawDataDiffFixtureType
    ) -> pd.DataFrame:
        """Pull data from a raw data fixture."""
        raw_fixture_path = fixture_type.get_absolute_directory_path_from_fixture_type(
            fixture_directory_name
        )
        fixture_columns = csv.get_csv_columns(raw_fixture_path)
        raw_data_df = load_dataframe_from_path(raw_fixture_path, fixture_columns)
        return raw_data_df

    def _load_mock_raw_table_to_bq(
        self,
        file_tag: str,
        fixture_type: RawDataDiffFixtureType,
        mock_data: pd.DataFrame,
        new_file_id: Optional[int] = None,
        include_recidiviz_managed_fields: bool = True,
    ) -> None:
        """Load mock raw table to associated dataset on BQ."""
        dataset_id = (
            raw_tables_dataset_for_region(
                state_code=self.state_code, instance=self.raw_data_instance
            )
            if fixture_type == RawDataDiffFixtureType.EXISTING
            else raw_data_pruning_new_raw_data_dataset(
                self.state_code, self.raw_data_instance
            )
        )

        if fixture_type == RawDataDiffFixtureType.NEW and not new_file_id:
            raise ValueError(
                f"Expected an associated new_file_id for fixture_type={fixture_type}"
            )

        address = BigQueryAddress(
            dataset_id=dataset_id,
            table_id=(
                file_tag
                if fixture_type == RawDataDiffFixtureType.EXISTING
                else file_tag + f"__{new_file_id}"
            ),
        )
        region_config = get_region_raw_file_config(
            region_code=self.state_code.value, region_module=fake_regions
        )
        self.create_mock_table(
            address=address,
            schema=RawDataTableBigQuerySchemaBuilder.build_bq_schema_for_config(
                raw_file_config=region_config.raw_file_configs[file_tag],
                include_recidiviz_managed_fields=include_recidiviz_managed_fields,
            ),
        )
        self.load_rows_into_table(address, mock_data.to_dict("records"))

    def _load_existing_and_temporary_fixtures_to_bq(
        self,
        fixture_directory_name: str,
        file_tag: str,
        new_file_id: int,
    ) -> None:
        """Load existing and temporary raw data fixtures to BigQuery."""
        self._create_mock_raw_bq_table_from_fixture(
            fixture_directory_name,
            file_tag,
            RawDataDiffFixtureType.EXISTING,
        )
        self._create_mock_raw_bq_table_from_fixture(
            fixture_directory_name,
            file_tag,
            RawDataDiffFixtureType.NEW,
            new_file_id,
        )

    def run_diff_query_and_validate_output(
        self,
        file_tag: str,
        fixture_directory_name: str,
        new_file_id: int,
        new_update_datetime: datetime.datetime,
    ) -> None:
        """For a given fixture file tag, run a raw data diff query and validate output matches expected output."""
        self._load_existing_and_temporary_fixtures_to_bq(
            fixture_directory_name, file_tag, new_file_id
        )

        raw_file_config = get_region_raw_file_config(
            region_code=self.state_code.value, region_module=fake_regions
        ).raw_file_configs[file_tag]

        diff_query = RawDataDiffQueryBuilder(
            project_id=self.project_id,
            state_code=self.state_code,
            raw_data_instance=self.raw_data_instance,
            file_id=new_file_id,
            update_datetime=new_update_datetime,
            new_raw_data_table_id=file_tag + f"__{new_file_id}",
            raw_file_config=raw_file_config,
            new_raw_data_dataset=raw_data_pruning_new_raw_data_dataset(
                self.state_code, self.raw_data_instance
            ),
            region_module=fake_regions,
        ).build_query()

        results = self.query(diff_query)

        expected_results = self._get_raw_data_from_fixture(
            fixture_directory_name, RawDataDiffFixtureType.EXPECTED_RESULTS
        )
        self.compare_expected_and_result_dfs(expected=expected_results, results=results)
