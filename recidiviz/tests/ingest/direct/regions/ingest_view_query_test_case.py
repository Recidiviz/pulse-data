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
from typing import List, Tuple

import pandas as pd
import pytest
import pytz
from google.cloud import bigquery

from recidiviz.big_query.big_query_address import BigQueryAddress
from recidiviz.common.constants.states import StateCode
from recidiviz.ingest.direct.dataset_config import raw_tables_dataset_for_region
from recidiviz.ingest.direct.direct_ingest_regions import get_direct_ingest_region
from recidiviz.ingest.direct.raw_data.direct_ingest_raw_file_import_manager import (
    DirectIngestRawFileConfig,
    DirectIngestRawFileImportManager,
    augment_raw_data_df_with_metadata_columns,
    check_found_columns_are_subset_of_config,
    get_region_raw_file_config,
)
from recidiviz.ingest.direct.types.direct_ingest_constants import (
    UPDATE_DATETIME_COL_NAME,
)
from recidiviz.ingest.direct.types.direct_ingest_instance import DirectIngestInstance
from recidiviz.ingest.direct.views.direct_ingest_view_query_builder import (
    DirectIngestViewQueryBuilder,
    DirectIngestViewRawFileDependency,
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
    DirectIngestTestFixturePath,
    load_dataframe_from_path,
    replace_empty_with_null,
)
from recidiviz.utils import csv, environment

# we need file_update_dt to have a pytz.UTC timezone, but the query_run_dt to be
# timzone naive for bq
DEFAULT_FILE_UPDATE_DATETIME = datetime.datetime.now(tz=pytz.UTC) - datetime.timedelta(
    days=1
)
DEFAULT_QUERY_RUN_DATETIME = datetime.datetime.utcnow()


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
        test_method_name: str,
        fixtures_files_name: str,
        region_code: str,
        ingest_view: DirectIngestViewQueryBuilder,
        query_run_dt: datetime.datetime,
        file_update_dt: datetime.datetime,
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

        fixture_name = os.path.splitext(fixtures_files_name)[0]
        expected_test_name = f"test_{fixture_name}"
        if test_method_name != expected_test_name:
            raise ValueError(
                f"Expected test name [{expected_test_name}] for fixture file name "
                f"[{fixtures_files_name}]. Found [{test_method_name}]"
            )

        self._create_mock_raw_bq_tables_from_fixtures(
            region_code=region_code,
            ingest_view=ingest_view,
            raw_fixtures_name=fixtures_files_name,
            file_update_dt=file_update_dt,
        )

        expected_output_fixture_path = (
            DirectIngestTestFixturePath.for_ingest_view_test_results_fixture(
                region_code=region_code,
                ingest_view_name=ingest_view.ingest_view_name,
                file_name=fixtures_files_name,
            ).full_path()
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

        pd.options.display.width = 9999
        pd.options.display.max_columns = 999
        pd.options.display.max_rows = 999
        pd.options.display.max_colwidth = 999
        self.compare_results_to_fixture(results, expected_output_fixture_path)

    @staticmethod
    def _check_valid_fixture_columns(
        raw_file_dependency_config: DirectIngestViewRawFileDependency, fixture_file: str
    ) -> List[str]:
        fixture_columns = csv.get_csv_columns(fixture_file)

        if raw_file_dependency_config.filter_to_latest:
            expected_extra_columns = []
        else:
            # We expect all fixtures for {myTable@ALL} type tags to have an
            # update_datetime column.
            expected_extra_columns = [UPDATE_DATETIME_COL_NAME]
        columns_to_check = [
            col for col in fixture_columns if col not in expected_extra_columns
        ]
        check_found_columns_are_subset_of_config(
            raw_file_config=raw_file_dependency_config.raw_file_config,
            found_columns=columns_to_check,
        )
        for expected_extra_column in expected_extra_columns:
            if expected_extra_column not in fixture_columns:
                raise ValueError(
                    f"Fixture [{fixture_file}] does not have expected column "
                    f"[{expected_extra_column}]"
                )

        return fixture_columns

    def _create_mock_raw_bq_tables_from_fixtures(
        self,
        region_code: str,
        ingest_view: DirectIngestViewQueryBuilder,
        raw_fixtures_name: str,
        file_update_dt: datetime.datetime,
    ) -> None:
        """Loads mock raw data tables from fixture files used by the given ingest view.
        All raw fixture files must have names matching |raw_fixtures_name|.
        """
        for raw_table_dependency_config in ingest_view.raw_table_dependency_configs:
            raw_data_df = self._get_raw_data(
                region_code,
                raw_table_dependency_config,
                raw_fixtures_name,
                file_update_dt,
            )
            self.helper.load_mock_raw_table(
                region_code=region_code,
                file_tag=raw_table_dependency_config.file_tag,
                mock_data=raw_data_df,
            )

    @staticmethod
    def columns_from_raw_file_config(
        config: DirectIngestRawFileConfig, headers: Tuple[str, ...]
    ) -> Tuple[str, ...]:
        column_names = tuple(column.name for column in config.documented_columns)
        if headers != column_names:
            raise ValueError(
                "Columns in file do not match file config:\n"
                f"From file:\n{headers}\n"
                f"From config:\n{column_names}\n"
                f"File is missing:\n{set(column_names) - set(headers)}\n"
                f"Config is missing:\n{set(headers) - set(column_names)}\n"
            )
        return column_names

    def _get_raw_data(
        self,
        region_code: str,
        raw_file_dependency_config: DirectIngestViewRawFileDependency,
        raw_fixtures_name: str,
        file_update_dt: datetime.datetime,
    ) -> pd.DataFrame:
        raw_fixture_path = DirectIngestTestFixturePath.for_raw_file_fixture(
            region_code=region_code,
            raw_file_dependency_config=raw_file_dependency_config,
            file_name=raw_fixtures_name,
        ).full_path()
        print(
            f"Loading fixture data for raw file [{raw_file_dependency_config.file_tag}] "
            f"from file path [{raw_fixture_path}]."
        )

        fixture_columns = self._check_valid_fixture_columns(
            raw_file_dependency_config, raw_fixture_path
        )

        raw_data_df = load_dataframe_from_path(raw_fixture_path, fixture_columns)

        if not raw_file_dependency_config.filter_to_latest:
            # We don't add metadata columns since this fixture file should already
            # have an update_datetime column and the file_id will never be referenced.
            return raw_data_df

        return augment_raw_data_df_with_metadata_columns(
            raw_data_df=raw_data_df,
            file_id=0,
            utc_upload_datetime=file_update_dt,
        )

    def _query_ingest_view_for_builder(
        self,
        ingest_view: DirectIngestViewQueryBuilder,
        query_run_dt: datetime.datetime,
    ) -> pd.DataFrame:
        """Uses the ingest view query run by Dataflow pipelines to query raw data for
        ingest view tests.
        """
        view_query = ingest_view.build_query(
            config=DirectIngestViewQueryBuilder.QueryStructureConfig(
                raw_data_source_instance=DirectIngestInstance.PRIMARY,
                raw_data_datetime_upper_bound=query_run_dt,
                # Only use the ORDER BY clause here to test that the columns exist in
                # the query output. We should be able to remove order_by_cols entirely
                # now that we're not using them in the Dataflow pipeline. This is not
                # needed for test determinism since we always sort the results by all
                # columns before comparing.
                use_order_by=True,
            )
        )
        return query_view(self.helper, ingest_view.ingest_view_name, view_query)

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
            replace_empty_with_null(expected_output), columns=columns
        )

        self.helper.compare_expected_and_result_dfs(expected=expected, results=results)


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
        self.file_update_dt = DEFAULT_FILE_UPDATE_DATETIME

        self.tester = IngestViewQueryTester(self)

    def run_ingest_view_test(
        self, fixtures_files_name: str, create_expected: bool = False
    ) -> None:
        self.tester.run_ingest_view_test(
            test_method_name=self._testMethodName,
            fixtures_files_name=fixtures_files_name,
            region_code=self.region_code,
            ingest_view=self.ingest_view,
            query_run_dt=self.query_run_dt,
            file_update_dt=self.file_update_dt,
            create_expected=create_expected,
        )

    def load_mock_raw_table(
        self,
        region_code: str,
        file_tag: str,
        mock_data: pd.DataFrame,
    ) -> None:
        region_config = get_region_raw_file_config(region_code)
        bq_schema = (
            DirectIngestRawFileImportManager.create_raw_table_schema_from_columns(
                raw_file_config=region_config.raw_file_configs[file_tag],
                columns=mock_data.columns.values,
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
        self.file_update_dt = DEFAULT_FILE_UPDATE_DATETIME

        self.tester = IngestViewQueryTester(self)

    def run_ingest_view_test(
        self, fixtures_files_name: str, create_expected: bool = False
    ) -> None:
        self.tester.run_ingest_view_test(
            test_method_name=self._testMethodName,
            fixtures_files_name=fixtures_files_name,
            region_code=self.region_code,
            ingest_view=self.ingest_view,
            query_run_dt=self.query_run_dt,
            file_update_dt=self.file_update_dt,
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
        region_config = get_region_raw_file_config(region_code)
        self.create_mock_table(
            address=address,
            schema=DirectIngestRawFileImportManager.create_raw_table_schema_from_columns(
                raw_file_config=region_config.raw_file_configs[file_tag],
                columns=mock_data.columns.values,
            ),
        )
        self.load_rows_into_table(address, mock_data.to_dict("records"))

    def normalize_expected_columns(self, columns: Tuple[str, ...]) -> Tuple[str, ...]:
        return columns
