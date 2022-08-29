# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2022 Recidiviz, Inc.
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
"""An implementation of BigQueryViewTestCase with functionality specific to testing
ingest view queries.
"""
from datetime import datetime, timedelta
from typing import Iterable, Optional, Tuple

import numpy as np
import pandas as pd
import pytest
from sqlalchemy.sql import sqltypes

from recidiviz.big_query.big_query_view import BigQueryView
from recidiviz.ingest.direct.direct_ingest_regions import get_direct_ingest_region
from recidiviz.ingest.direct.ingest_view_materialization.ingest_view_materializer import (
    IngestViewMaterializerImpl,
)
from recidiviz.ingest.direct.raw_data.direct_ingest_raw_file_import_manager import (
    DirectIngestRawFileConfig,
    DirectIngestRawFileImportManager,
    augment_raw_data_df_with_metadata_columns,
)
from recidiviz.ingest.direct.types.cloud_task_args import IngestViewMaterializationArgs
from recidiviz.ingest.direct.types.direct_ingest_instance import DirectIngestInstance
from recidiviz.ingest.direct.views.direct_ingest_big_query_view_types import (
    DirectIngestPreProcessedIngestViewBuilder,
)
from recidiviz.ingest.direct.views.direct_ingest_view_collector import (
    DirectIngestPreProcessedIngestViewCollector,
)
from recidiviz.tests.big_query.big_query_view_test_case import BigQueryViewTestCase
from recidiviz.tests.big_query.fakes.fake_table_schema import PostgresTableSchema
from recidiviz.tests.ingest.direct.fixture_util import (
    DirectIngestFixtureDataFileType,
    direct_ingest_fixture_path,
)
from recidiviz.utils import csv

DEFAULT_FILE_UPDATE_DATETIME = datetime(2021, 4, 14, 0, 0, 0)
DEFAULT_QUERY_RUN_DATETIME = datetime(2021, 4, 15, 0, 0, 0)


@pytest.mark.uses_db
class IngestViewQueryTestCase(BigQueryViewTestCase):
    """An implementation of BigQueryViewTestCase with functionality specific to testing
    ingest view queries.
    """

    def setUp(self) -> None:
        super().setUp()
        self.region_code: str
        self.view_builder: DirectIngestPreProcessedIngestViewBuilder
        self.query_run_dt = DEFAULT_QUERY_RUN_DATETIME

    @staticmethod
    def view_builder_for_tag(
        region_code: str, ingest_view_name: str
    ) -> DirectIngestPreProcessedIngestViewBuilder:
        return DirectIngestPreProcessedIngestViewCollector(
            get_direct_ingest_region(region_code), []
        ).get_view_builder_by_view_name(ingest_view_name)

    def run_ingest_view_test(self, fixtures_files_name: str) -> None:
        """Reads in the expected output CSV file from the ingest view fixture path and
        asserts that the results from the raw data ingest view query are equal. Prints
        out the dataframes for both expected rows and results.
        """
        self.create_mock_raw_bq_tables_from_fixtures(
            region_code=self.region_code,
            ingest_view_builder=self.view_builder,
            raw_fixtures_name=fixtures_files_name,
        )

        expected_output_fixture_path = direct_ingest_fixture_path(
            region_code=self.region_code,
            fixture_file_type=DirectIngestFixtureDataFileType.INGEST_VIEW_RESULTS,
            file_tag=self.view_builder.ingest_view_name,
            file_name=fixtures_files_name,
        )
        results = self.query_ingest_view_for_builder(self.view_builder)

        self.compare_results_to_fixture(results, expected_output_fixture_path)

    @staticmethod
    def _check_valid_fixture_columns(
        raw_file_config: DirectIngestRawFileConfig, fixture_file: str
    ) -> None:
        fixture_columns = csv.get_csv_columns(fixture_file)

        DirectIngestRawFileImportManager.check_found_columns_are_subset_of_config(
            raw_file_config=raw_file_config, found_columns=fixture_columns
        )

    def create_mock_raw_bq_tables_from_fixtures(
        self,
        region_code: str,
        ingest_view_builder: DirectIngestPreProcessedIngestViewBuilder,
        raw_fixtures_name: str,
    ) -> None:
        """Loads mock raw data tables from fixture files used by the given ingest view.
        All raw fixture files must have names matching |raw_fixtures_name|.
        """
        ingest_view = ingest_view_builder.build()
        for raw_file_config in ingest_view.raw_table_dependency_configs:
            raw_fixture_path = direct_ingest_fixture_path(
                region_code=region_code,
                fixture_file_type=DirectIngestFixtureDataFileType.RAW,
                file_tag=raw_file_config.file_tag,
                file_name=raw_fixtures_name,
            )
            print(
                f"Loading fixture data for raw file [{raw_file_config.file_tag}] from file path [{raw_fixture_path}]."
            )

            self._check_valid_fixture_columns(raw_file_config, raw_fixture_path)

            self.create_mock_raw_file(
                region_code=region_code,
                file_config=raw_file_config,
                mock_data=csv.get_rows_as_tuples(raw_fixture_path),
            )

    @staticmethod
    def schema_from_raw_file_config(
        config: DirectIngestRawFileConfig,
    ) -> PostgresTableSchema:
        return PostgresTableSchema(
            {
                # Postgres does case-sensitive lowercase search on all non-quoted
                # column (and table) names. We lowercase all the column names so that
                # a query like "SELECT MyCol FROM table;" finds the column "mycol".
                column.name.lower(): sqltypes.String
                for column in config.available_columns
            }
        )

    def create_mock_raw_file(
        self,
        region_code: str,
        file_config: DirectIngestRawFileConfig,
        mock_data: Iterable[Tuple[Optional[str], ...]],
        update_datetime: datetime = DEFAULT_FILE_UPDATE_DATETIME,
    ) -> None:
        mock_schema = self.schema_from_raw_file_config(file_config)
        raw_data_df = augment_raw_data_df_with_metadata_columns(
            raw_data_df=pd.DataFrame(mock_data, columns=mock_schema.data_types.keys()),
            file_id=0,
            utc_upload_datetime=update_datetime,
        )
        # Adds empty strings as NULL to the PG test database
        raw_data_df.replace("", np.nan, inplace=True)
        # For the raw data tables we make the table name `us_xx_file_tag`. It would be
        # closer to the actual produced query to make it something like
        # `us_xx_raw_data_file_tag`, but that more easily gets us closer to the 63
        # character hard limit imposed by Postgres.
        self.create_mock_bq_table(
            dataset_id=region_code.lower(),
            # Postgres does case-sensitive lowercase search on all non-quoted
            # table (and column) names. We lowercase all the table names so that
            # a query like "SELECT my_col FROM MyTable;" finds the table "mytable".
            table_id=file_config.file_tag.lower(),
            mock_schema=mock_schema,
            mock_data=raw_data_df,
        )

    def query_ingest_view_for_builder(
        self, view_builder: DirectIngestPreProcessedIngestViewBuilder
    ) -> pd.DataFrame:
        """Uses the ingest view diff query from DirectIngestIngestViewExportManager.debug_query_for_args to query
        raw data for ingest view tests."""
        view: BigQueryView = view_builder.build()
        lower_bound_datetime_exclusive_: datetime = (
            DEFAULT_FILE_UPDATE_DATETIME - timedelta(days=1)
        )
        upper_bound_datetime_inclusive_: datetime = self.query_run_dt
        view_query = str(
            IngestViewMaterializerImpl.debug_query_for_args(
                ingest_views_by_name={view_builder.ingest_view_name: view},
                ingest_view_materialization_args=IngestViewMaterializationArgs(
                    ingest_view_name=view_builder.ingest_view_name,
                    lower_bound_datetime_exclusive=lower_bound_datetime_exclusive_,
                    upper_bound_datetime_inclusive=upper_bound_datetime_inclusive_,
                    ingest_instance=DirectIngestInstance.PRIMARY,
                ),
            )
        )

        return self.query_view(view.table_for_query, view_query)
