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
"""Utility class for testing BQ views against Postgres"""
import logging
import re
import unittest
from datetime import datetime, timedelta
from typing import Dict, Iterable, List, Optional, Sequence, Tuple, Type, Union

import numpy as np
import pandas as pd
import pytest
from pandas._testing import assert_frame_equal
from sqlalchemy.sql import sqltypes

from recidiviz.big_query.big_query_view import (
    BigQueryAddress,
    BigQueryView,
    BigQueryViewBuilder,
)
from recidiviz.ingest.direct.gcs.file_type import GcsfsDirectIngestFileType
from recidiviz.ingest.direct.ingest_view_materialization.ingest_view_materializer import (
    IngestViewMaterializerImpl,
)
from recidiviz.ingest.direct.raw_data.direct_ingest_raw_file_import_manager import (
    DirectIngestRawFileConfig,
    augment_raw_data_df_with_metadata_columns,
)
from recidiviz.ingest.direct.types.cloud_task_args import GcsfsIngestViewExportArgs
from recidiviz.ingest.direct.views.direct_ingest_big_query_view_types import (
    DirectIngestPreProcessedIngestViewBuilder,
)
from recidiviz.ingest.direct.views.direct_ingest_view_collector import (
    DirectIngestPreProcessedIngestViewCollector,
)
from recidiviz.tests.big_query.fakes.fake_big_query_database import FakeBigQueryDatabase
from recidiviz.tests.big_query.fakes.fake_table_schema import MockTableSchema
from recidiviz.tests.ingest.direct.fixture_util import direct_ingest_fixture_path
from recidiviz.tools.postgres import local_postgres_helpers
from recidiviz.utils import csv
from recidiviz.utils.regions import get_region

DEBUG = True


DEFAULT_FILE_UPDATE_DATETIME = datetime(2021, 4, 14, 0, 0, 0)
DEFAULT_QUERY_RUN_DATETIME = datetime(2021, 4, 15, 0, 0, 0)


@pytest.mark.uses_db
class BaseViewTest(unittest.TestCase):
    """This is a utility class that allows BQ views to be tested using Postgres instead.

    This is NOT fully featured and has some shortcomings, most notably:
    1. It uses naive regexes to rewrite parts of the query. This works for the most part but may produce invalid
       queries in some cases. For instance, the lazy capture groups may capture the wrong tokens in nested function
       calls.
    2. Postgres can only use ORDINALS when unnesting and indexing into arrays, while BigQuery uses OFFSETS (or both).
       This does not translate the results (add or subtract one). So long as the query consistently uses one or the
       other, it should produce correct results.
    3. This does not (yet) support chaining of views. To test a view query, any tables or views that it queries from
       must be created and seeded with data using `create_table`.
    4. Not all BigQuery SQL syntax has been translated, and it is possible that some features may not have equivalent
       functionality in Postgres and therefore can't be translated.

    Given these, it may not make sense to use this for all of our views. If it prevents you from using BQ features that
    would be helpful, or creates more headaches than value it provides, it may not be necessary.
    """

    temp_db_dir: str

    @classmethod
    def setUpClass(cls) -> None:
        cls.temp_db_dir = local_postgres_helpers.start_on_disk_postgresql_database()

    def setUp(self) -> None:
        self.data_types: Optional[Union[Type, Dict[str, Type]]] = {}
        self.region_code: str
        # View specific regex patterns to replace in the BigQuery SQL for the Postgres server. These are applied before
        # the rest of the SQL rewrites.
        self.sql_regex_replacements: Dict[str, str] = {}
        self.view_builder: DirectIngestPreProcessedIngestViewBuilder
        self.query_run_dt = DEFAULT_QUERY_RUN_DATETIME

        self.fake_bq_db = FakeBigQueryDatabase()

    def tearDown(self) -> None:
        self.fake_bq_db.teardown_databases()

    @classmethod
    def tearDownClass(cls) -> None:
        local_postgres_helpers.stop_and_clear_on_disk_postgresql_database(
            cls.temp_db_dir
        )

    # ~~~~~~~~ INGEST-SPECIFIC TEST FUNCTIONALITY ~~~~~~~~#
    # TODO(#9717): Refactor all ingest-specific code out of this class and into an
    #  ingest view specific TestCase implementation.
    @staticmethod
    def view_builder_for_tag(
        region_code: str, ingest_view_name: str
    ) -> DirectIngestPreProcessedIngestViewBuilder:
        return DirectIngestPreProcessedIngestViewCollector(
            get_region(region_code, is_direct_ingest=True), []
        ).get_view_builder_by_view_name(ingest_view_name)

    def run_ingest_view_test(self, fixtures_files_name: str) -> None:
        self.create_mock_raw_bq_tables_from_fixtures(
            region_code=self.region_code,
            ingest_view_builder=self.view_builder,
            raw_fixtures_name=fixtures_files_name,
        )

        self.compare_results_to_expected_output(
            region_code=self.region_code,
            view_builder=self.view_builder,
            expected_output_fixture_file_name=fixtures_files_name,
            data_types=self.data_types,
        )

    def compare_results_to_expected_output(
        self,
        region_code: str,
        view_builder: DirectIngestPreProcessedIngestViewBuilder,
        expected_output_fixture_file_name: str,
        data_types: Optional[Union[Type, Dict[str, Type]]] = None,
    ) -> None:
        """Reads in the expected output CSV file from the ingest view fixture path and asserts that the results
        from the raw data ingest view query are equal. Prints out the dataframes for both expected rows and results."""
        expected_output_fixture_path = direct_ingest_fixture_path(
            region_code=region_code,
            file_type=GcsfsDirectIngestFileType.INGEST_VIEW,
            file_tag=view_builder.ingest_view_name,
            file_name=expected_output_fixture_file_name,
        )
        print(
            f"Loading expected results for ingest view "
            f"[{view_builder.ingest_view_name}] from path "
            f"[{expected_output_fixture_path}]"
        )
        expected_output = list(
            csv.get_rows_as_tuples(expected_output_fixture_path, skip_header_row=False)
        )
        expected_columns = [column.lower() for column in expected_output.pop(0)]

        results = self.query_ingest_view_for_builder(
            view_builder,
            dimensions=expected_columns,
            data_types=data_types,
        )
        expected = pd.DataFrame(expected_output, columns=expected_columns)
        expected = expected.astype(self.data_types)
        expected = expected.astype(self.data_types)
        expected = expected.set_index(expected_columns)
        print("**** EXPECTED ****")
        print(expected)
        print("**** ACTUAL ****")
        print(results)
        assert_frame_equal(expected, results)

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
                file_type=GcsfsDirectIngestFileType.RAW_DATA,
                file_tag=raw_file_config.file_tag,
                file_name=raw_fixtures_name,
            )
            print(
                f"Loading fixture data for raw file [{raw_file_config.file_tag}] from file path [{raw_fixture_path}]."
            )

            self.create_mock_raw_file(
                region_code=region_code,
                file_config=raw_file_config,
                mock_data=csv.get_rows_as_tuples(raw_fixture_path),
            )

    @staticmethod
    def schema_from_raw_file_config(
        config: DirectIngestRawFileConfig,
    ) -> MockTableSchema:
        return MockTableSchema(
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
        self,
        view_builder: DirectIngestPreProcessedIngestViewBuilder,
        dimensions: List[str],
        data_types: Optional[Union[Type, Dict[str, Type]]] = None,
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
                ingest_view_export_args=GcsfsIngestViewExportArgs(
                    ingest_view_name=view_builder.ingest_view_name,
                    lower_bound_datetime_exclusive=lower_bound_datetime_exclusive_,
                    upper_bound_datetime_inclusive=upper_bound_datetime_inclusive_,
                    output_bucket_name="any_bucket",
                ),
            )
        )
        if self.sql_regex_replacements:
            for bq_sql_regex_pattern, pg_sql in self.sql_regex_replacements.items():
                view_query = re.sub(bq_sql_regex_pattern, pg_sql, view_query)

        return self.query_view(
            view.table_for_query,
            view_query,
            data_types=data_types,
            dimensions=dimensions,
        )

    # ~~~~~~~~ GENERAL VIEW TEST FUNCTIONALITY ~~~~~~~~#

    def create_mock_bq_table(
        self,
        dataset_id: str,
        table_id: str,
        mock_schema: MockTableSchema,
        mock_data: pd.DataFrame,
    ) -> None:
        self.fake_bq_db.create_mock_bq_table(
            dataset_id=dataset_id,
            table_id=table_id,
            mock_schema=mock_schema,
            mock_data=mock_data,
        )

    def create_view(self, view_builder: BigQueryViewBuilder) -> None:
        self.fake_bq_db.create_view(view_builder)

    def query_view(
        self,
        table_address: BigQueryAddress,
        view_query: str,
        data_types: Optional[Union[Type, Dict[str, Type]]],
        dimensions: List[str],
    ) -> pd.DataFrame:
        results = self.fake_bq_db.run_query(view_query, data_types, dimensions)

        # Log results to debug log level, to see them pass --log-level DEBUG to pytest
        logging.debug("Results for `%s`:\n%s", table_address, results.to_string())
        return results

    def query_view_for_builder(
        self,
        view_builder: BigQueryViewBuilder,
        data_types: Optional[Union[Type, Dict[str, Type]]],
        dimensions: List[str],
    ) -> pd.DataFrame:
        if isinstance(view_builder, DirectIngestPreProcessedIngestViewBuilder):
            raise ValueError(
                f"Found view builder type [{type(view_builder)}] - use "
                f"query_ingest_view_for_builder() for this type instead."
            )

        view: BigQueryView = view_builder.build()
        return self.query_view(
            view.table_for_query, view.view_query, data_types, dimensions
        )

    def query_view_chain(
        self,
        view_builders: Sequence[BigQueryViewBuilder],
        data_types: Dict[str, Type],
        dimensions: List[str],
    ) -> pd.DataFrame:
        for view_builder in view_builders[:-1]:
            self.create_view(view_builder)
        return self.query_view_for_builder(view_builders[-1], data_types, dimensions)
