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
import logging
import os.path
from functools import cache
from types import ModuleType
from typing import Optional, Tuple

import pandas as pd
from more_itertools import one

from recidiviz.big_query.big_query_address import BigQueryAddress
from recidiviz.big_query.big_query_sqlglot_helpers import get_undocumented_ctes
from recidiviz.common.constants.states import StateCode
from recidiviz.ingest.direct.dataset_config import raw_tables_dataset_for_region
from recidiviz.ingest.direct.raw_data.raw_file_configs import DirectIngestRawFileConfig
from recidiviz.ingest.direct.types.direct_ingest_instance import DirectIngestInstance
from recidiviz.ingest.direct.views.direct_ingest_view_query_builder import (
    DirectIngestViewQueryBuilder,
)
from recidiviz.source_tables.collect_all_source_table_configs import (
    build_raw_data_source_table_collections_for_state_and_instance,
)
from recidiviz.source_tables.source_table_config import SourceTableCollection
from recidiviz.tests.big_query.big_query_emulator_test_case import (
    BigQueryEmulatorTestCase,
)
from recidiviz.tests.big_query.sqlglot_helpers import (
    check_query_is_not_ordered_outside_of_windows,
)
from recidiviz.tests.ingest.direct.direct_ingest_raw_fixture_loader import (
    DirectIngestRawDataFixtureLoader,
)
from recidiviz.tests.ingest.direct.fixture_util import fixture_path_for_address
from recidiviz.tests.ingest.direct.legacy_fixture_path import (
    DirectIngestTestFixturePath,
)
from recidiviz.tests.ingest.direct.regions.base_ingest_test_cases import (
    BaseStateIngestTestCase,
)
from recidiviz.tests.ingest.direct.regions.ingest_view_cte_comment_exemptions import (
    THESE_INGEST_VIEWS_HAVE_UNDOCUMENTED_CTES,
)
from recidiviz.tests.pipelines.ingest.state.ingest_region_test_mixin import (
    IngestRegionTestMixin,
)
from recidiviz.utils.environment import in_ci

DEFAULT_QUERY_RUN_DATETIME = datetime.datetime.utcnow()


# TODO(#38322): Migrate all states and remove this class
class LegacyIngestViewEmulatorQueryTestCase(
    BigQueryEmulatorTestCase, IngestRegionTestMixin
):
    """An extension of BigQueryEmulatorTestCase with functionality specific to testing
    ingest view queries.

    TODO(#22059): Standardize ingest view result fixtures for pipeline and ingest view
                  tests, and update this test case to load those fixtures using the same
                  common code as the pipeline test.
    """

    wipe_emulator_data_on_teardown = False

    @classmethod
    def get_source_tables(cls) -> list[SourceTableCollection]:
        collections = build_raw_data_source_table_collections_for_state_and_instance(
            cls.state_code(),
            DirectIngestInstance.PRIMARY,
            region_module_override=cls.region_module_override(),
        )

        # For performance reasons, only load the schemas for the actual tables we'll
        # need in this test.

        # Filter down to just tables in the us_xx_raw_data dataset
        raw_tables_dataset = raw_tables_dataset_for_region(
            state_code=cls.state_code(), instance=DirectIngestInstance.PRIMARY
        )
        collection = one(c for c in collections if c.dataset_id == raw_tables_dataset)

        # Next, filter down to just the tables that are dependencies of this ingest view
        filtered_collection = SourceTableCollection(
            dataset_id=collection.dataset_id,
            update_config=collection.update_config,
            labels=collection.labels,
            description=collection.description,
        )
        for file_tag in cls.ingest_view().raw_data_table_dependency_file_tags:
            address = BigQueryAddress(
                dataset_id=collection.dataset_id, table_id=file_tag
            )
            source_table = collection.source_tables_by_address[address]
            filtered_collection.source_tables_by_address[address] = source_table
        return [filtered_collection]

    @classmethod
    def region_module_override(cls) -> Optional[ModuleType]:
        return None

    @classmethod
    @abc.abstractmethod
    def ingest_view_name(cls) -> str:
        """Subclasses must implement this method to return the ingest view name"""

    @classmethod
    @cache
    def ingest_view(cls) -> DirectIngestViewQueryBuilder:
        return cls.ingest_view_collector().get_query_builder_by_view_name(
            cls.ingest_view_name()
        )

    def setUp(self) -> None:
        super().setUp()
        # Is replaced in some downstream tests
        self.query_run_dt = DEFAULT_QUERY_RUN_DATETIME
        self.raw_fixture_delegate = DirectIngestRawDataFixtureLoader(
            self.state_code(), emulator_test=self
        )

    def tearDown(self) -> None:
        self._clear_emulator_table_data()
        super().tearDown()

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
            [self.ingest_view()],
            fixtures_files_name,
            create_tables=False,
        )

        expected_output_fixture_path = (
            DirectIngestTestFixturePath.for_ingest_view_test_results_fixture(
                region_code=self.state_code().value,
                ingest_view_name=self.ingest_view_name(),
                file_name=fixtures_files_name,
            ).full_path()
        )
        results = self._query_ingest_view_for_builder(
            self.ingest_view(), self.query_run_dt
        )

        self.compare_results_to_fixture(
            results,
            expected_output_fixture_path,
            create_expected=create_expected,
            expect_missing_fixtures_on_empty_results=False,
        )
        lint_ingest_view_query(self.ingest_view(), self.query_run_dt, self.state_code())

    @staticmethod
    def columns_from_raw_file_config(
        config: DirectIngestRawFileConfig, headers: Tuple[str, ...]
    ) -> Tuple[str, ...]:
        column_names = tuple(
            column.name for column in config.current_documented_columns
        )
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
            query_structure_config=DirectIngestViewQueryBuilder.QueryStructureConfig(
                raw_data_source_instance=DirectIngestInstance.PRIMARY,
                raw_data_datetime_upper_bound=query_run_dt,
            )
        )
        query_job = self.bq_client.run_query_async(
            query_str=view_query, use_query_cache=True
        )

        query_job.result()

        # TODO(#26259): Once the emulator properly returns the schemas
        #  for views and query results, verify that the query schema matches the schema
        #  defined in the manifest input_columns. Also enforce that ingest views do not
        #  produce REPEATED mode columns (i.e. array type columns) because we don't have
        #  a way to represent that in YAMLs.

        results = query_job.to_dataframe()
        # Log results to debug log level, to see them pass --log-level DEBUG to pytest
        logging.debug(
            "Results for `%s`:\n%s", ingest_view.ingest_view_name, results.to_string()
        )
        return results


def lint_ingest_view_query(
    ingest_view: DirectIngestViewQueryBuilder,
    query_run_dt: datetime.datetime,
    state_code: StateCode,
) -> None:
    """Does advanced checks of ingest view queries for efficiency and documentation."""
    query = ingest_view.build_query(
        query_structure_config=DirectIngestViewQueryBuilder.QueryStructureConfig(
            raw_data_source_instance=DirectIngestInstance.PRIMARY,
            raw_data_datetime_upper_bound=query_run_dt,
        )
    )
    check_ingest_view_ctes_are_documented(query, ingest_view, state_code)
    try:
        check_query_is_not_ordered_outside_of_windows(query)
    except ValueError as ve:
        msg = (
            f"Found unnecessary ORDER BY statement in ingest view '{ingest_view.ingest_view_name}'\n"
            "Ingest view queries with ordered results are unnecessarily inefficient."
        )
        raise ValueError(msg) from ve


def check_ingest_view_ctes_are_documented(
    query: str,
    ingest_view: DirectIngestViewQueryBuilder,
    state_code: StateCode,
) -> None:
    """Throws if the view has CTEs that are not properly documented with a comment,
    or if CTEs that are now documented are still listed in the exemptions list.
    """
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


class StateIngestViewTestCase(BigQueryEmulatorTestCase, BaseStateIngestTestCase):
    """
    Base class for ingest view tests, where we test a query of
    raw data against expected results.
    """

    @classmethod
    def state_code(cls) -> StateCode:
        raise NotImplementedError("Ingest tests must have a StateCode!")

    @classmethod
    def ingest_view_builder(cls) -> DirectIngestViewQueryBuilder:
        raise NotImplementedError(
            "Ingest view tests must declare their DirectIngestViewQueryBuilder!"
        )

    def setUp(self) -> None:
        super().setUp()
        # Is replaced in some downstream tests
        self.query_run_dt = DEFAULT_QUERY_RUN_DATETIME
        self.raw_fixture_delegate = DirectIngestRawDataFixtureLoader(
            self.state_code(), emulator_test=self
        )

    def run_ingest_view_test(
        self,
        create_expected_output: bool = False,
    ) -> None:
        """Runs the ingest view query for this test's IngestViewBuilder and compares against expected results."""
        if in_ci() and create_expected_output:
            raise ValueError("Cannot have create_expected_output=True in the CI.")
        expected_ingest_view_name = self.ingest_view_builder().ingest_view_name

        # self.id() is from unittest.TestCase
        # https://docs.python.org/3/library/unittest.html#unittest.TestCase.id
        *_, file_name, _class_name, test_name = self.id().split(".")

        if file_name != f"view_{expected_ingest_view_name}_test":
            raise ValueError(
                f"Ingest view test is in an unexpected file. Expected 'view_{expected_ingest_view_name}_test.py'"
            )

        (
            ingest_view_name,
            characteristic,
        ) = self.get_ingest_view_name_and_characteristic(test_name)

        if ingest_view_name != expected_ingest_view_name:
            self.fail(f"Test must begin with `test_{expected_ingest_view_name}`")

        lint_ingest_view_query(
            self.ingest_view_builder(), self.query_run_dt, self.state_code()
        )

        self.raw_fixture_delegate.load_raw_fixtures_to_emulator(
            [self.ingest_view_builder()],
            f"{characteristic}.csv",
            create_tables=True,
        )
        results = self._run_ingest_view_query()

        expected_output_fixture_path = fixture_path_for_address(
            self.state_code(),
            BigQueryAddress(
                dataset_id=f"{self.state_code().value.lower()}_ingest_view_results",
                table_id=ingest_view_name,
            ),
            characteristic,
        )
        if create_expected_output:
            self.create_directory_for_characteristic_level_fixtures(
                expected_output_fixture_path, characteristic
            )
        self.compare_results_to_fixture(
            results,
            expected_output_fixture_path,
            create_expected=create_expected_output,
            expect_missing_fixtures_on_empty_results=False,
        )

    def _run_ingest_view_query(self) -> pd.DataFrame:
        """Uses the ingest view query run by Dataflow pipelines to query raw data."""
        view_query = self.ingest_view_builder().build_query(
            query_structure_config=DirectIngestViewQueryBuilder.QueryStructureConfig(
                raw_data_source_instance=DirectIngestInstance.PRIMARY,
                raw_data_datetime_upper_bound=self.query_run_dt,
            )
        )
        query_job = self.bq_client.run_query_async(
            query_str=view_query, use_query_cache=False
        )

        # TODO(#26259): Once the emulator properly returns the schemas
        #  for views and query results, verify that the query schema matches the schema
        #  defined in the manifest input_columns. Also enforce that ingest views do not
        #  produce REPEATED mode columns (i.e. array type columns) because we don't have
        #  a way to represent that in YAMLs.
        results = query_job.result().to_dataframe()

        # Log results to debug log level, to see them pass --log-level DEBUG to pytest
        logging.debug(
            "Results for `%s`:\n%s",
            self.ingest_view_builder().ingest_view_name,
            results.to_string(),
        )
        return results
