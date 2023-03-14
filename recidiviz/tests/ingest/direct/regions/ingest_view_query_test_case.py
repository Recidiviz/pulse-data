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
import abc
import datetime
import os.path
from typing import Iterable, Iterator, Optional, Tuple, Type, Union

import pandas as pd
import pytest
import pytz
from google.cloud import bigquery
from more_itertools import one
from pandas.testing import assert_frame_equal

from recidiviz.big_query.big_query_address import BigQueryAddress
from recidiviz.common.constants.states import StateCode
from recidiviz.ingest.direct.direct_ingest_regions import get_direct_ingest_region
from recidiviz.ingest.direct.ingest_view_materialization.ingest_view_materializer import (
    IngestViewMaterializerImpl,
)
from recidiviz.ingest.direct.raw_data.dataset_config import (
    raw_tables_dataset_for_region,
)
from recidiviz.ingest.direct.raw_data.direct_ingest_raw_file_import_manager import (
    DirectIngestRawFileConfig,
    DirectIngestRawFileImportManager,
    augment_raw_data_df_with_metadata_columns,
    check_found_columns_are_subset_of_config,
)
from recidiviz.ingest.direct.types.cloud_task_args import IngestViewMaterializationArgs
from recidiviz.ingest.direct.types.direct_ingest_instance import DirectIngestInstance
from recidiviz.ingest.direct.views.direct_ingest_view_query_builder import (
    DirectIngestViewQueryBuilder,
)
from recidiviz.ingest.direct.views.direct_ingest_view_query_builder_collector import (
    DirectIngestViewQueryBuilderCollector,
)
from recidiviz.tests.big_query.big_query_emulator_test_case import (
    BigQueryEmulatorTestCase,
)
from recidiviz.tests.big_query.big_query_test_helper import (
    BigQueryTestHelper,
    query_view,
)
from recidiviz.tests.big_query.big_query_view_test_case import BigQueryViewTestCase
from recidiviz.tests.big_query.fakes.fake_table_schema import PostgresTableSchema
from recidiviz.tests.ingest.direct.fixture_util import (
    DirectIngestFixtureDataFileType,
    direct_ingest_fixture_path,
)
from recidiviz.utils import csv, environment

DEFAULT_FILE_UPDATE_DATETIME = datetime.datetime(2021, 4, 14, 0, 0, 0, tzinfo=pytz.UTC)
DEFAULT_QUERY_RUN_DATETIME = datetime.datetime(2021, 4, 15, 0, 0, 0)


def _replace_empty_with_null(
    values: Iterable[Tuple[str, ...]]
) -> Iterator[Tuple[Optional[str], ...]]:
    for row in values:
        yield tuple(value or None for value in row)


class IngestViewTestBigQueryDatabaseDelegate(BigQueryTestHelper):
    """An interface for ingest view testing logic that is specific to the underlying
    database used for the test."""

    @staticmethod
    def ingest_view_for_tag(
        region_code: str, ingest_view_name: str
    ) -> DirectIngestViewQueryBuilder:
        return DirectIngestViewQueryBuilderCollector(
            get_direct_ingest_region(region_code), []
        ).get_query_builder_by_view_name(ingest_view_name)

    @abc.abstractmethod
    def load_mock_raw_table(
        self,
        region_code: str,
        file_tag: str,
        mock_data: pd.DataFrame,
    ) -> None:
        """Insert data for this raw file into the test database."""

    @abc.abstractmethod
    def normalize_expected_columns(self, columns: Tuple[str, ...]) -> Tuple[str, ...]:
        """Normalize column names of the expected data to match output from the database."""


# TODO(#15020): Merge this with the test case once all view tests are migrated
class IngestViewQueryTester:
    """Ingest view test code independent of database"""

    def __init__(self, helper: IngestViewTestBigQueryDatabaseDelegate) -> None:
        self.helper = helper

    def run_ingest_view_test(
        self,
        fixtures_files_name: str,
        region_code: str,
        ingest_view: DirectIngestViewQueryBuilder,
        query_run_dt: datetime.datetime,
        create_expected: bool,
    ) -> None:
        """Reads in the expected output CSV file from the ingest view fixture path and
        asserts that the results from the raw data ingest view query are equal. Prints
        out the dataframes for both expected rows and results.

        It will read raw files that match the following (for any `file_tag`):
        `recidiviz/tests/ingest/direct/direct_ingest_fixtures/ux_xx/raw/{file_tag}/{fixtures_files_name}.csv`

        And compare output against the following file:
        `recidiviz/tests/ingest/direct/direct_ingest_fixtures/ux_xx/ingest_view/{ingest_view_name}/{fixtures_files_name}.csv`

        Passing `create_expected=True` will first create the expected output csv before
        the comparison check.
        """
        self._create_mock_raw_bq_tables_from_fixtures(
            region_code=region_code,
            ingest_view=ingest_view,
            raw_fixtures_name=fixtures_files_name,
        )

        expected_output_fixture_path = direct_ingest_fixture_path(
            region_code=region_code,
            fixture_file_type=DirectIngestFixtureDataFileType.INGEST_VIEW_RESULTS,
            file_tag=ingest_view.ingest_view_name,
            file_name=fixtures_files_name,
        )
        results = self._query_ingest_view_for_builder(ingest_view, query_run_dt)

        if create_expected:
            if environment.in_ci():
                raise AssertionError(
                    "`create_expected` should only be used when writing or updating the test."
                )
            # Make output directory if it doesn't yet exist
            output_directory = os.path.dirname(expected_output_fixture_path)
            os.makedirs(output_directory, exist_ok=True)

            results.to_csv(expected_output_fixture_path, index=False)

        self.compare_results_to_fixture(results, expected_output_fixture_path)

    @staticmethod
    def _check_valid_fixture_columns(
        raw_file_config: DirectIngestRawFileConfig, fixture_file: str
    ) -> None:
        fixture_columns = csv.get_csv_columns(fixture_file)

        check_found_columns_are_subset_of_config(
            raw_file_config=raw_file_config, found_columns=fixture_columns
        )

    def _create_mock_raw_bq_tables_from_fixtures(
        self,
        region_code: str,
        ingest_view: DirectIngestViewQueryBuilder,
        raw_fixtures_name: str,
    ) -> None:
        """Loads mock raw data tables from fixture files used by the given ingest view.
        All raw fixture files must have names matching |raw_fixtures_name|.
        """
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
            raw_data_df = self._get_raw_data(raw_file_config, raw_fixture_path)

            self.helper.load_mock_raw_table(
                region_code=region_code,
                file_tag=raw_file_config.file_tag,
                mock_data=raw_data_df,
            )

    @staticmethod
    def columns_from_raw_file_config(
        config: DirectIngestRawFileConfig, headers: Tuple[str, ...]
    ) -> Tuple[str, ...]:
        column_names = tuple(column.name for column in config.available_columns)
        if headers != column_names:
            raise ValueError(
                f"Columns in file do not match file config:\nheaders: {headers}\nconfig: {column_names}"
            )
        return column_names

    def _get_raw_data(
        self, raw_file_config: DirectIngestRawFileConfig, raw_fixture_path: str
    ) -> pd.DataFrame:
        mock_data = csv.get_rows_as_tuples(raw_fixture_path, skip_header_row=False)
        header_row = next(mock_data)
        columns = self.columns_from_raw_file_config(raw_file_config, header_row)
        values = _replace_empty_with_null(mock_data)
        return augment_raw_data_df_with_metadata_columns(
            raw_data_df=pd.DataFrame(values, columns=columns),
            file_id=0,
            utc_upload_datetime=DEFAULT_FILE_UPDATE_DATETIME,
        )

    def _query_ingest_view_for_builder(
        self,
        ingest_view: DirectIngestViewQueryBuilder,
        query_run_dt: datetime.datetime,
    ) -> pd.DataFrame:
        """Uses the ingest view diff query from DirectIngestIngestViewExportManager.debug_query_for_args to query
        raw data for ingest view tests."""
        lower_bound_datetime_exclusive = (
            DEFAULT_FILE_UPDATE_DATETIME - datetime.timedelta(days=1)
        )
        upper_bound_datetime_inclusive = query_run_dt
        view_query = str(
            IngestViewMaterializerImpl.debug_query_for_args(
                ingest_views_by_name={ingest_view.ingest_view_name: ingest_view},
                raw_data_source_instance=DirectIngestInstance.PRIMARY,
                ingest_view_materialization_args=IngestViewMaterializationArgs(
                    ingest_view_name=ingest_view.ingest_view_name,
                    lower_bound_datetime_exclusive=lower_bound_datetime_exclusive,
                    upper_bound_datetime_inclusive=upper_bound_datetime_inclusive,
                    ingest_instance=DirectIngestInstance.PRIMARY,
                ),
            )
        )

        return query_view(self.helper, ingest_view.ingest_view_name, view_query)

    @staticmethod
    def fixture_comparison_data_type_for_column(
        df: pd.DataFrame, column: str
    ) -> Union[Type, pd.StringDtype]:
        """Inspects data in the provided |column| in |df| to determine the column data
        type (dtype) that should be used to compare this column in |df| to a column in
        a DataFrame read from a fixture file.
        """
        column_value_types = {
            type(val) for val in df[column].tolist() if val is not None
        }

        if len(column_value_types) == 0:
            # There are no values in this column so we can't conclude what the type
            # is.
            return object

        if len(column_value_types) > 1:
            raise ValueError(
                f"Found multiple conflicting types [{column_value_types}] in column "
                f"[{column}] with values: {df[column].tolist()}"
            )

        python_type = one(column_value_types)

        if python_type == int:
            return int

        if python_type == float:
            return float

        if python_type in (
            str,
            # Boolean values are represented in fixtures as 'True' and 'False' -
            # we convert these back to strings in the result for comparison
            # to fixture data.
            bool,
            # Pandas doesn't support dates like 9999-12-31 which are often
            # present in query results (and valid in BQ). We convert these types
            # back to strings for comparison.
            datetime.datetime,
            datetime.date,
            pd.Timestamp,
            # Collection types are not hashable so there are issues with setting
            # collection type columns as index columns.
            list,
            set,
            dict,
        ):
            return pd.StringDtype()

        raise ValueError(
            f"Found unhandled data type [{python_type}] for column [{column}] "
            f"with values [{df[column].tolist()}]"
        )

    def compare_results_to_fixture(
        self, results: pd.DataFrame, expected_output_fixture_path: str
    ) -> None:
        """Compares the results in the given Dataframe that have been presumably read
        out of a local, fake BQ instance (i.e. backed by Postgres) and compares them
        to the data in the fixture file at the provided path.
        """

        print(f"Loading expected results from path [{expected_output_fixture_path}]")
        expected_output = list(
            csv.get_rows_as_tuples(expected_output_fixture_path, skip_header_row=False)
        )

        columns = self.helper.normalize_expected_columns(expected_output.pop(0))
        expected = pd.DataFrame(
            _replace_empty_with_null(expected_output), columns=columns
        )

        if sorted(results.columns) != sorted(expected.columns):
            raise ValueError(
                f"Columns in expected and actual results do not match (order agnostic). "
                f"Expected: {expected.columns}. Actual: {results.columns}."
            )

        data_types = {
            column: self.fixture_comparison_data_type_for_column(results, column)
            for column in results.columns.tolist()
        }
        dimensions = results.columns.tolist()

        # Reorder the columns in the expected results to match the actual result column
        # order.
        expected = expected[results.columns]

        expected = self.helper.apply_types_and_sort(expected, data_types, dimensions)

        results = self.helper.apply_types_and_sort(results, data_types, dimensions)

        print("**** EXPECTED ****")
        print(expected.info())
        print(expected)
        print("**** ACTUAL ****")
        print(results.info())
        print(results)
        assert_frame_equal(expected, results)


# TODO(#15020): Delete this once all view tests are migrated
@pytest.mark.uses_db
class IngestViewQueryTestCase(
    BigQueryViewTestCase, IngestViewTestBigQueryDatabaseDelegate
):
    """An extension of BigQueryViewTestCase with functionality specific to testing
    ingest view queries.
    """

    def setUp(self) -> None:
        super().setUp()
        self.region_code: str
        # TODO(#19137): Get the view builder directly instead of requiring the test to
        # do that.
        self.ingest_view: DirectIngestViewQueryBuilder
        self.query_run_dt = DEFAULT_QUERY_RUN_DATETIME

        self.tester = IngestViewQueryTester(self)

    def run_ingest_view_test(
        self, fixtures_files_name: str, create_expected: bool = False
    ) -> None:
        self.tester.run_ingest_view_test(
            fixtures_files_name=fixtures_files_name,
            region_code=self.region_code,
            ingest_view=self.ingest_view,
            query_run_dt=self.query_run_dt,
            create_expected=create_expected,
        )

    def load_mock_raw_table(
        self,
        region_code: str,
        file_tag: str,
        mock_data: pd.DataFrame,
    ) -> None:
        bq_schema = (
            DirectIngestRawFileImportManager.create_raw_table_schema_from_columns(
                mock_data.columns.values
            )
        )
        mock_schema = PostgresTableSchema.from_big_query_schema_fields(
            # Postgres does case-sensitive lowercase search on all non-quoted
            # column (and table) names. We lowercase all the column names so that
            # a query like "SELECT MyCol FROM table;" finds the column "mycol".
            [
                bigquery.SchemaField(
                    name=schema_field.name.lower(),
                    field_type=schema_field.field_type,
                    mode=schema_field.mode,
                )
                for schema_field in bq_schema
            ]
        )
        mock_data.columns = list(mock_schema.data_types.keys())
        # For the raw data tables we make the table name `us_xx_file_tag`. It would be
        # closer to the actual produced query to make it something like
        # `us_xx_raw_data_file_tag`, but that more easily gets us closer to the 63
        # character hard limit imposed by Postgres.
        self.create_mock_bq_table(
            dataset_id=region_code.lower(),
            # Postgres does case-sensitive lowercase search on all non-quoted
            # table (and column) names. We lowercase all the table names so that
            # a query like "SELECT my_col FROM MyTable;" finds the table "mytable".
            table_id=file_tag.lower(),
            mock_schema=mock_schema,
            mock_data=mock_data,
        )

    def normalize_expected_columns(self, columns: Tuple[str, ...]) -> Tuple[str, ...]:
        return tuple(column.lower() for column in columns)


@pytest.mark.uses_bq_emulator
class IngestViewEmulatorQueryTestCase(
    BigQueryEmulatorTestCase, IngestViewTestBigQueryDatabaseDelegate
):
    """An extension of BigQueryEmulatorTestCase with functionality specific to testing
    ingest view queries.
    """

    def setUp(self) -> None:
        super().setUp()
        self.region_code: str
        # TODO(#19137): Get the view builder directly instead of requiring the test to
        # do that.
        self.ingest_view: DirectIngestViewQueryBuilder
        self.query_run_dt = DEFAULT_QUERY_RUN_DATETIME

        self.tester = IngestViewQueryTester(self)

    def run_ingest_view_test(
        self, fixtures_files_name: str, create_expected: bool = False
    ) -> None:
        self.tester.run_ingest_view_test(
            fixtures_files_name=fixtures_files_name,
            region_code=self.region_code,
            ingest_view=self.ingest_view,
            query_run_dt=self.query_run_dt,
            create_expected=create_expected,
        )

    def load_mock_raw_table(
        self,
        region_code: str,
        file_tag: str,
        mock_data: pd.DataFrame,
    ) -> None:
        address = BigQueryAddress(
            dataset_id=raw_tables_dataset_for_region(
                state_code=StateCode(region_code), instance=DirectIngestInstance.PRIMARY
            ),
            table_id=file_tag,
        )
        self.create_mock_table(
            address=address,
            schema=DirectIngestRawFileImportManager.create_raw_table_schema_from_columns(
                mock_data.columns.values
            ),
        )
        self.load_rows_into_table(address, mock_data.to_dict("records"))

    def normalize_expected_columns(self, columns: Tuple[str, ...]) -> Tuple[str, ...]:
        return columns
