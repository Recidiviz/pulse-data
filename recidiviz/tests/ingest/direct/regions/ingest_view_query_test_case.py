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
import datetime
import os.path
from collections import defaultdict
from concurrent import futures
from typing import Dict, List, Tuple

import pandas as pd
import pytz
from google.cloud.bigquery import DatasetReference
from more_itertools import one

from recidiviz.big_query.big_query_address import BigQueryAddress
from recidiviz.big_query.big_query_client import BQ_CLIENT_MAX_POOL_SIZE
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
    IS_DELETED_COL_NAME,
    UPDATE_DATETIME_COL_NAME,
)
from recidiviz.ingest.direct.types.direct_ingest_instance import DirectIngestInstance
from recidiviz.ingest.direct.views.direct_ingest_view_query_builder import (
    DirectIngestViewQueryBuilder,
    DirectIngestViewRawFileDependency,
    RawFileHistoricalRowsFilterType,
)
from recidiviz.ingest.direct.views.direct_ingest_view_query_builder_collector import (
    DirectIngestViewQueryBuilderCollector,
)
from recidiviz.tests.big_query.big_query_emulator_test_case import (
    BigQueryEmulatorTestCase,
)
from recidiviz.tests.big_query.big_query_test_helper import (
    check_for_ctes_with_no_comments,
    query_view,
)
from recidiviz.tests.ingest.direct.fixture_util import (
    DirectIngestTestFixturePath,
    load_dataframe_from_path,
    replace_empty_with_null,
)
from recidiviz.tests.ingest.direct.regions.ingest_view_cte_comment_exemptions import (
    THESE_INGEST_VIEWS_HAVE_UNDOCUMENTED_CTES,
)
from recidiviz.utils import csv, environment

# we need file_update_dt to have a pytz.UTC timezone, but the query_run_dt to be
# timzone naive for bq
DEFAULT_FILE_UPDATE_DATETIME = datetime.datetime.now(tz=pytz.UTC) - datetime.timedelta(
    days=1
)
DEFAULT_QUERY_RUN_DATETIME = datetime.datetime.utcnow()


class IngestViewEmulatorQueryTestCase(BigQueryEmulatorTestCase):
    """An extension of BigQueryEmulatorTestCase with functionality specific to testing
    ingest view queries.
    """

    @staticmethod
    def ingest_view_for_tag(
        region_code: str, ingest_view_name: str
    ) -> DirectIngestViewQueryBuilder:
        return DirectIngestViewQueryBuilderCollector(
            get_direct_ingest_region(region_code), []
        ).get_query_builder_by_view_name(ingest_view_name)

    def setUp(self) -> None:
        super().setUp()
        # TODO(#5508) Have region/state code be enum
        self.region_code: str
        # TODO(#19137): Get the view builder directly instead of requiring the test to
        # do that.
        self.ingest_view: DirectIngestViewQueryBuilder
        self.query_run_dt = DEFAULT_QUERY_RUN_DATETIME
        self.file_update_dt = DEFAULT_FILE_UPDATE_DATETIME

    def run_ingest_view_test(
        self, fixtures_files_name: str, create_expected: bool = False
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
        self.bq_client.create_dataset_if_necessary(
            dataset_ref=DatasetReference(
                project=self.project_id,
                dataset_id=raw_tables_dataset_for_region(
                    state_code=StateCode(self.region_code),
                    instance=DirectIngestInstance.PRIMARY,
                ),
            )
        )

        fixture_name = os.path.splitext(fixtures_files_name)[0]
        expected_test_name = f"test_{fixture_name}"
        if self._testMethodName != expected_test_name:
            raise ValueError(
                f"Expected test name [{expected_test_name}] for fixture file name "
                f"[{fixtures_files_name}]. Found [{self._testMethodName}]"
            )

        self._create_mock_raw_bq_tables_from_fixtures(
            region_code=self.region_code,
            ingest_view=self.ingest_view,
            raw_fixtures_name=fixtures_files_name,
            file_update_dt=self.file_update_dt,
        )

        expected_output_fixture_path = (
            DirectIngestTestFixturePath.for_ingest_view_test_results_fixture(
                region_code=self.region_code,
                ingest_view_name=self.ingest_view.ingest_view_name,
                file_name=fixtures_files_name,
            ).full_path()
        )
        results = self._query_ingest_view_for_builder(
            self.ingest_view, self.query_run_dt
        )

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
        self.check_ingest_view_ctes_are_documented(
            self.ingest_view, self.query_run_dt, self.region_code
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
            check_exists=False,
            create_dataset=False,
        )
        self.load_rows_into_table(address, mock_data.to_dict("records"))

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
            expected_extra_columns = [IS_DELETED_COL_NAME, UPDATE_DATETIME_COL_NAME]
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
        with futures.ThreadPoolExecutor(
            # Conservatively allow only half as many workers as allowed connections.
            # Lower this number if we see "urllib3.connectionpool:Connection pool is
            # full, discarding connection" errors.
            max_workers=int(BQ_CLIENT_MAX_POOL_SIZE / 2)
        ) as executor:
            to_create = []

            configs_by_file_tag: Dict[
                str, List[DirectIngestViewRawFileDependency]
            ] = defaultdict(list)
            for raw_table_dependency_config in ingest_view.raw_table_dependency_configs:
                configs_by_file_tag[raw_table_dependency_config.file_tag].append(
                    raw_table_dependency_config
                )

            for file_tag, dependency_configs in configs_by_file_tag.items():
                if len(dependency_configs) == 1:
                    raw_table_dependency_config = dependency_configs[0]
                else:
                    # When we have more than one dependency to the same raw file tag,
                    # we are dealing with a view that references both LATEST ({myTag})
                    # and ALL ({myTag@ALL}) versions of the data. In this case we only
                    # load the ALL data since the LATEST data can be derived from it.
                    raw_table_dependency_config = one(
                        c
                        for c in dependency_configs
                        if c.filter_type == RawFileHistoricalRowsFilterType.ALL
                    )

                raw_data_df = self._get_raw_data(
                    region_code,
                    raw_table_dependency_config,
                    raw_fixtures_name,
                    file_update_dt,
                )
                to_create.append((raw_table_dependency_config.file_tag, raw_data_df))

            create_table_futures = {
                executor.submit(
                    self.load_mock_raw_table,
                    region_code=region_code,
                    file_tag=file_tag,
                    mock_data=raw_data_df,
                )
                for (file_tag, raw_data_df) in to_create
            }

        for future in futures.as_completed(create_table_futures):
            future.result()

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
        """Loads the raw data fixture file for the provided dependency into a Dataframe,
        augmenting with extra metadata columns as appropriate.
        """
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
            # The fixture files for @ALL files have update_datetime, but not file_id.
            # We derive a file_id from the update_datetime, assuming that all data with
            # the same update_datetime came from the same file.
            raw_data_df["file_id"] = (
                raw_data_df["update_datetime"]
                .rank(
                    # The "dense" method assigns the same value where update_datetime
                    # values are the same.
                    method="dense"
                )
                .astype(int)
            )
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
            )
        )
        return query_view(self, ingest_view.ingest_view_name, view_query)

    def check_ingest_view_ctes_are_documented(
        self,
        ingest_view: DirectIngestViewQueryBuilder,
        query_run_dt: datetime.datetime,
        region_code: str,
    ) -> None:
        state_code = StateCode(region_code.upper())
        if (
            ingest_view.ingest_view_name
            # TODO(#5508) Have region/state code be enum
            in THESE_INGEST_VIEWS_HAVE_UNDOCUMENTED_CTES.get(state_code, {})
        ):
            return None
        query = ingest_view.build_query(
            config=DirectIngestViewQueryBuilder.QueryStructureConfig(
                raw_data_source_instance=DirectIngestInstance.PRIMARY,
                raw_data_datetime_upper_bound=query_run_dt,
            )
        )
        return check_for_ctes_with_no_comments(query, ingest_view.ingest_view_name)

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

        columns = expected_output.pop(0)
        expected = pd.DataFrame(
            replace_empty_with_null(expected_output), columns=columns
        )

        self.compare_expected_and_result_dfs(expected=expected, results=results)
