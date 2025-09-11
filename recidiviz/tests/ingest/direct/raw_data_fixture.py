# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2025 Recidiviz, Inc.
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
"""This module consolidates operations on raw data fixtures for direct ingest."""

import os

import pandas as pd
from google.cloud.bigquery import SchemaField

from recidiviz.big_query.big_query_address import BigQueryAddress
from recidiviz.ingest.direct.dataset_config import raw_tables_dataset_for_region
from recidiviz.ingest.direct.raw_data.direct_ingest_raw_table_schema_builder import (
    RawDataTableBigQuerySchemaBuilder,
)
from recidiviz.ingest.direct.raw_data.raw_file_configs import DirectIngestRawFileConfig
from recidiviz.ingest.direct.types.direct_ingest_constants import (
    RAW_DATA_METADATA_COLUMNS,
)
from recidiviz.ingest.direct.types.direct_ingest_instance import DirectIngestInstance
from recidiviz.tests.ingest.direct.fixture_util import (
    fixture_path_for_raw_file_config,
    load_dataframe_from_path,
)


class RawDataFixture:
    """
    Encapsulates data and related operations on raw data fixtures for direct ingest testing.

    When a RawDataConfig is used for an ingest view, it becomes apart of a DirectIngestViewRawFileDependency.
    When that raw_file_config is used in a test, this class is instaniated from that raw_file_config,
    and is used to read and write expected data to fixture files and the BigQueryEmulator.
    """

    def __init__(self, raw_file_config: DirectIngestRawFileConfig) -> None:
        self.raw_file_config = raw_file_config
        self.state_code = raw_file_config.state_code
        self.bq_dataset_id = raw_tables_dataset_for_region(
            state_code=self.state_code,
            instance=DirectIngestInstance.PRIMARY,
        )

    @property
    def address(self) -> BigQueryAddress:
        return BigQueryAddress(
            dataset_id=self.bq_dataset_id,
            table_id=self.raw_file_config.file_tag,
        )

    @property
    def schema(self) -> list[SchemaField]:
        return RawDataTableBigQuerySchemaBuilder.build_bq_schema_for_config(
            raw_file_config=self.raw_file_config
        )

    def _check_dataframe_schema_against_raw_file_config(
        self, df: pd.DataFrame, raw_fixture_path: str
    ) -> None:
        expected_columns = (
            set(c.name.lower() for c in self.raw_file_config.current_columns)
            | RAW_DATA_METADATA_COLUMNS
        )
        fixture_file_columns = set(df.columns.str.lower())
        if extra_columns := fixture_file_columns - expected_columns:
            raise ValueError(
                f"Found extra columns [{extra_columns}] for fixture {raw_fixture_path}"
            )
        # TODO(#36159): Enforce that ALL columns of a raw file config are in a fixture file, not
        # just a subset.
        # Historically, we allowed fixture file schemas to be a subset of raw raw_file_config schemas.
        # When all fixtures are generated however, we can enforce that a fixture file has the same
        # schema as the raw data raw_file_config it is representing.
        if missing_metadata := RAW_DATA_METADATA_COLUMNS - fixture_file_columns:
            raise ValueError(
                f"Missing columns [{missing_metadata}] for fixture {raw_fixture_path}"
            )

    def read_fixture_file_into_dataframe(self, test_identifier: str) -> pd.DataFrame:
        try:
            raw_fixture_path = fixture_path_for_raw_file_config(
                self.state_code, self.raw_file_config, test_identifier
            )
            df = load_dataframe_from_path(raw_fixture_path, None)
        except Exception as e:
            raise ValueError(
                f"Failed to load fixture for {self.raw_file_config.file_tag} for test "
                f"{test_identifier}"
            ) from e

        self._check_dataframe_schema_against_raw_file_config(df, raw_fixture_path)
        file_ids_per_update_dt = set(
            df.groupby("update_datetime").file_id.nunique().values
        )
        if file_ids_per_update_dt and file_ids_per_update_dt != {1}:
            raise ValueError(
                f"{raw_fixture_path} has multiple file_id values per update_datetime"
            )
        return df

    def write_dataframe_into_fixture_file(
        self, df: pd.DataFrame, test_identifier: str
    ) -> None:
        raw_fixture_path = fixture_path_for_raw_file_config(
            self.state_code, self.raw_file_config, test_identifier
        )
        self._check_dataframe_schema_against_raw_file_config(df, raw_fixture_path)
        os.makedirs(os.path.dirname(raw_fixture_path), exist_ok=True)
        df.to_csv(raw_fixture_path, index=False)

    def write_empty_fixture(self, test_identifier: str) -> None:
        """Writes an empty fixture file for the given test identifier."""
        df = pd.DataFrame(columns=[col.name for col in self.schema])
        self.write_dataframe_into_fixture_file(df, test_identifier)
