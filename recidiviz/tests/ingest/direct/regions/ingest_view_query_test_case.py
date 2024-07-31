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
from typing import Dict, Iterable, List, Tuple

import attr
import pandas as pd
import pytz
from google.cloud.bigquery import SchemaField
from more_itertools import one

from recidiviz.big_query.big_query_address import BigQueryAddress
from recidiviz.big_query.big_query_client import BQ_CLIENT_MAX_POOL_SIZE
from recidiviz.common.constants.states import StateCode
from recidiviz.ingest.direct.dataset_config import raw_tables_dataset_for_region
from recidiviz.ingest.direct.raw_data.direct_ingest_raw_file_import_manager import (
    DirectIngestRawFileConfig,
    augment_raw_data_df_with_metadata_columns,
    check_found_columns_are_subset_of_config,
)
from recidiviz.ingest.direct.raw_data.direct_ingest_raw_table_schema_builder import (
    RawDataTableBigQuerySchemaBuilder,
)
from recidiviz.ingest.direct.raw_data.raw_file_configs import get_region_raw_file_config
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
    get_undocumented_ctes,
    query_view,
)
from recidiviz.tests.ingest.direct.fixture_util import (
    DirectIngestTestFixturePath,
    load_dataframe_from_path,
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


@attr.define
class RawDataFixture:
    config: DirectIngestViewRawFileDependency
    fixture_data_df: pd.DataFrame
    bq_dataset_id: str

    @property
    def address(self) -> BigQueryAddress:
        return BigQueryAddress(
            dataset_id=self.bq_dataset_id,
            table_id=self.config.file_tag,
        )

    @property
    def schema(self) -> List[SchemaField]:
        return RawDataTableBigQuerySchemaBuilder.build_bq_schmea_for_config(
            raw_file_config=self.config.raw_file_config
        )


class DirectIngestRawDataFixtureLoader:
    """Finds relevant raw data fixtures for ingest tests and loads them to the BigQueryEmulator."""

    def __init__(self, state_code: StateCode, emulator_test: BigQueryEmulatorTestCase):
        self.state_code = state_code
        self.bq_client = emulator_test.bq_client
        self.raw_tables_dataset_id = raw_tables_dataset_for_region(
            state_code=self.state_code, instance=DirectIngestInstance.PRIMARY
        )
        self.raw_file_config = get_region_raw_file_config(self.state_code.value)

    def load_raw_fixtures_to_emulator(
        self,
        ingest_views: List[DirectIngestViewQueryBuilder],
        ingest_test_identifier: str,
        file_update_dt: datetime.datetime,
    ) -> None:
        """
        Loads raw data tables to the emulator for the given ingest views.
        All raw fixture files must have names matching the ingest_test_identifier.
        For example, an ingest test named "test_person_001" will look for fixture
        files called "person_001.csv".
        """
        self.bq_client.create_dataset_if_necessary(
            dataset_id=self.raw_tables_dataset_id
        )
        with futures.ThreadPoolExecutor(
            # Conservatively allow only half as many workers as allowed connections.
            # Lower this number if we see "urllib3.connectionpool:Connection pool is
            # full, discarding connection" errors.
            max_workers=int(BQ_CLIENT_MAX_POOL_SIZE / 2)
        ) as executor:
            create_table_futures = [
                executor.submit(self._load_fixture_to_emulator, fixture=fixture)
                for fixture in self._generate_raw_data_fixtures(
                    ingest_views, ingest_test_identifier, file_update_dt
                )
            ]
        for future in futures.as_completed(create_table_futures):
            future.result()

    def _load_fixture_to_emulator(self, fixture: RawDataFixture) -> None:
        self.bq_client.create_table_with_schema(
            dataset_id=fixture.address.dataset_id,
            table_id=fixture.address.table_id,
            schema_fields=fixture.schema,
        )
        self.bq_client.stream_into_table(
            fixture.address.dataset_id,
            fixture.address.table_id,
            rows=fixture.fixture_data_df.to_dict("records"),
        )

    def _generate_raw_data_fixtures(
        self,
        ingest_views: List[DirectIngestViewQueryBuilder],
        ingest_test_identifier: str,
        file_update_dt: datetime.datetime,
    ) -> Iterable[RawDataFixture]:
        """
        Generates the unique set of RawDataFixture objects for the given
        ingest views, assumed file update datetime, and raw fixture filename.
        All raw fixture files must have names matching the ingest_test_identifier.
        """
        for config in self._raw_dependencies_for_ingest_views(ingest_views):
            yield RawDataFixture(
                config=config,
                bq_dataset_id=self.raw_tables_dataset_id,
                fixture_data_df=self._read_raw_data_fixture(
                    config,
                    ingest_test_identifier,
                    file_update_dt,
                ),
            )

    def _raw_dependencies_for_ingest_views(
        self,
        ingest_views: List[DirectIngestViewQueryBuilder],
    ) -> Iterable[DirectIngestViewRawFileDependency]:
        """
        Generates the minimum set of DirectIngestViewRawFileDependency objects.
        If we use both LATEST and ALL for a data source, we'll only yield the ALL.
        """
        configs_by_file_tag: Dict[
            str, List[DirectIngestViewRawFileDependency]
        ] = defaultdict(list)
        for ingest_view in ingest_views:
            for config in ingest_view.raw_table_dependency_configs:
                configs_by_file_tag[config.file_tag].append(config)

        for dependency_configs in configs_by_file_tag.values():
            if len(dependency_configs) == 1:
                yield dependency_configs[0]
            else:
                # When we have more than one dependency to the same raw file tag,
                # we are dealing with a view that references both LATEST ({myTag})
                # and ALL ({myTag@ALL}) versions of the data. In this case we only
                # load the ALL data since the LATEST data can be derived from it.
                yield one(
                    c
                    for c in dependency_configs
                    if c.filter_type == RawFileHistoricalRowsFilterType.ALL
                )

    def _check_valid_fixture_columns(
        self,
        raw_file_dependency_config: DirectIngestViewRawFileDependency,
        fixture_file: str,
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

    def _read_raw_data_fixture(
        self,
        raw_file_dependency_config: DirectIngestViewRawFileDependency,
        csv_fixture_file_name: str,
        file_update_dt: datetime.datetime,
    ) -> pd.DataFrame:
        """
        Reads the raw data fixture file for the provided dependency into a Dataframe.
        If the fixture is the @ALL version, we add a file_id based on the fixture update_datetime.
        If the fixture is instead LATEST, we add a dummy file_id and use file_update_dt
        as the update_datetime.
        """
        raw_fixture_path = DirectIngestTestFixturePath.for_raw_file_fixture(
            region_code=self.state_code.value,
            raw_file_dependency_config=raw_file_dependency_config,
            file_name=csv_fixture_file_name,
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


class IngestViewEmulatorQueryTestCase(BigQueryEmulatorTestCase):
    """An extension of BigQueryEmulatorTestCase with functionality specific to testing
    ingest view queries.
    """

    @property
    def state_code(self) -> StateCode:
        raise NotImplementedError(
            "Set the state_code property on the state specific subclass."
        )

    @property
    def ingest_view_name(self) -> str:
        raise NotImplementedError(
            "Set the ingest_view_name property on the test subclass."
        )

    def setUp(self) -> None:
        super().setUp()
        self.ingest_view: DirectIngestViewQueryBuilder = (
            DirectIngestViewQueryBuilderCollector.from_state_code(
                self.state_code
            ).get_query_builder_by_view_name(self.ingest_view_name)
        )

        # Is replaced in some downstream tests
        self.file_update_dt = DEFAULT_FILE_UPDATE_DATETIME
        self.query_run_dt = DEFAULT_QUERY_RUN_DATETIME
        self.raw_fixture_delegate = DirectIngestRawDataFixtureLoader(
            self.state_code, emulator_test=self
        )

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
        fixture_name = os.path.splitext(fixtures_files_name)[0]
        expected_test_name = f"test_{fixture_name}"
        if self._testMethodName != expected_test_name:
            raise ValueError(
                f"Expected test name [{expected_test_name}] for fixture file name "
                f"[{fixtures_files_name}]. Found [{self._testMethodName}]"
            )
        self.raw_fixture_delegate.load_raw_fixtures_to_emulator(
            [self.ingest_view], fixtures_files_name, self.file_update_dt
        )

        expected_output_fixture_path = (
            DirectIngestTestFixturePath.for_ingest_view_test_results_fixture(
                region_code=self.state_code.value,
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
            self.ingest_view, self.query_run_dt, self.state_code
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
        state_code: StateCode,
    ) -> None:
        """Throws if the view has CTEs that are not properly documented with a comment,
        or if CTEs that are now documented are still listed in the exemptions list.
        """
        query = ingest_view.build_query(
            config=DirectIngestViewQueryBuilder.QueryStructureConfig(
                raw_data_source_instance=DirectIngestInstance.PRIMARY,
                raw_data_datetime_upper_bound=query_run_dt,
            )
        )
        undocumented_ctes = get_undocumented_ctes(query)
        ingest_view_name = ingest_view.ingest_view_name

        view_exemptions = THESE_INGEST_VIEWS_HAVE_UNDOCUMENTED_CTES.get(state_code, {})
        allowed_undocumented_ctes = set(view_exemptions.get(ingest_view_name, []))

        unexpected_undocumented_ctes = undocumented_ctes - allowed_undocumented_ctes
        if unexpected_undocumented_ctes:
            undocumented_ctes_str = ", ".join(unexpected_undocumented_ctes)
            raise ValueError(
                f"Query {ingest_view_name} has undocumented CTEs: "
                f"{undocumented_ctes_str}"
            )
        exempt_ctes_that_are_documented = allowed_undocumented_ctes - undocumented_ctes
        if exempt_ctes_that_are_documented:
            raise ValueError(
                f"Found CTEs for query {ingest_view_name} that are now "
                f"documented: {sorted(exempt_ctes_that_are_documented)}. Please remove "
                f"these from the THESE_INGEST_VIEWS_HAVE_UNDOCUMENTED_CTES list."
            )
        if ingest_view_name in view_exemptions:
            if not view_exemptions[ingest_view_name]:
                raise ValueError(
                    f"Query {ingest_view_name} has all CTEs documented - please remove "
                    f"its empty entry from THESE_INGEST_VIEWS_HAVE_UNDOCUMENTED_CTES."
                )

    def compare_results_to_fixture(
        self, results: pd.DataFrame, expected_output_fixture_path: str
    ) -> None:
        """Compares the results in the given Dataframe that have been presumably read
        out of a local, fake BQ instance (i.e. backed by Postgres) and compares them
        to the data in the fixture file at the provided path.
        """

        print(f"Loading expected results from path [{expected_output_fixture_path}]")
        expected = load_dataframe_from_path(
            expected_output_fixture_path, fixture_columns=None, allow_comments=True
        )
        self.compare_expected_and_result_dfs(expected=expected, results=results)
