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
import csv
import datetime
import io
import logging
import os.path
import re
from functools import cache
from types import ModuleType
from typing import Any, Optional, Tuple

import pandas as pd
from more_itertools import one

from recidiviz.big_query.big_query_address import BigQueryAddress
from recidiviz.big_query.big_query_sqlglot_helpers import get_undocumented_ctes
from recidiviz.common.constants.states import StateCode
from recidiviz.common.io.local_file_contents_handle import LocalFileContentsHandle
from recidiviz.ingest.direct.dataset_config import raw_tables_dataset_for_region
from recidiviz.ingest.direct.direct_ingest_regions import get_direct_ingest_region
from recidiviz.ingest.direct.external_id_type_helpers import (
    external_id_types_by_state_code,
)
from recidiviz.ingest.direct.ingest_mappings.ingest_view_contents_context import (
    IngestViewContentsContext,
)
from recidiviz.ingest.direct.ingest_mappings.ingest_view_manifest_compiler import (
    IngestViewManifestCompiler,
)
from recidiviz.ingest.direct.ingest_mappings.ingest_view_manifest_compiler_delegate import (
    StateSchemaIngestViewManifestCompilerDelegate,
)
from recidiviz.ingest.direct.raw_data.direct_ingest_raw_table_schema_builder import (
    RawDataTableBigQuerySchemaBuilder,
)
from recidiviz.ingest.direct.raw_data.raw_file_configs import DirectIngestRawFileConfig
from recidiviz.ingest.direct.types.direct_ingest_instance import DirectIngestInstance
from recidiviz.ingest.direct.views.direct_ingest_view_query_builder import (
    DirectIngestViewQueryBuilder,
)
from recidiviz.persistence.entity.entities_module_context_factory import (
    entities_module_context_for_module,
)
from recidiviz.persistence.entity.entity_utils import print_entity_trees
from recidiviz.persistence.entity.state import entities as state_entities
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
from recidiviz.tests.ingest.direct.fixture_util import (
    fixture_path_for_address,
    ingest_mapping_output_fixture_path,
)
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


# TODO(#38322): Move this logic back into
#  StateIngestViewTestCase::test_validate_view_output_schema once
#  LegacyIngestViewEmulatorQueryTestCase is deleted.
def _test_validate_view_output_schema(
    *,
    test_case: BigQueryEmulatorTestCase,
    state_code: StateCode,
    ingest_view_builder: DirectIngestViewQueryBuilder,
    query_run_dt: datetime.datetime,
    manifest_compiler: IngestViewManifestCompiler,
    create_tables: bool,
) -> None:
    """This test runs for all subclasses of StateIngestViewTestCase and enforces:
    * The view compiles and runs against empty raw data tables
    * The output schema of the view query matches the column names / types defined
      in the `input_columns` dictionary in this view's ingest mappings YAML file.
    * The output schema of this view query does not have any REPEATED fields (not
      supported by ingest mappings).
    """
    ingest_view_name = ingest_view_builder.ingest_view_name
    raw_table_dataset = raw_tables_dataset_for_region(
        state_code=state_code,
        instance=DirectIngestInstance.PRIMARY,
    )
    if create_tables:
        test_case.bq_client.create_dataset_if_necessary(dataset_id=raw_table_dataset)

        raw_table_file_tags_to_load = {
            d.file_tag: d.raw_file_config
            for d in ingest_view_builder.raw_table_dependency_configs
        }

        for file_tag, raw_config in raw_table_file_tags_to_load.items():
            schema = RawDataTableBigQuerySchemaBuilder.build_bq_schema_for_config(
                raw_file_config=raw_config
            )
            address = BigQueryAddress(dataset_id=raw_table_dataset, table_id=file_tag)

            test_case.create_mock_table(address, schema)

    view_query = ingest_view_builder.build_query(
        query_structure_config=DirectIngestViewQueryBuilder.QueryStructureConfig(
            raw_data_source_instance=DirectIngestInstance.PRIMARY,
            raw_data_datetime_upper_bound=query_run_dt,
        )
    )
    query_job = test_case.bq_client.run_query_async(
        query_str=view_query, use_query_cache=True
    )
    view_output_schema = query_job.result().schema

    for field in view_output_schema:
        if field.mode == "REPEATED":
            raise ValueError(
                f"Found field {field.name} in view {ingest_view_name} with "
                f"REPEATED mode. REPEATED mode fields not supported in ingest "
                f"views. If you need to map a list of items, serialize to a "
                f"comma-separated string and use $iterable to iterate over the "
                f"items. OR you can serialize to JSON and use "
                f"$split_json/$iterable to iterate over the items."
            )

    expected_mappings_input_column_to_type = {
        field.name: field.field_type for field in view_output_schema
    }
    manifest = manifest_compiler.compile_manifest(ingest_view_name=ingest_view_name)
    actual_mappings_input_column_to_type = manifest.input_column_to_type

    test_case.assertEqual(
        expected_mappings_input_column_to_type,
        actual_mappings_input_column_to_type,
        f"The input_columns configuration in the mappings YAML for view "
        f"{ingest_view_name} does not match the actual output of the view query.",
    )


class StateIngestViewAndMappingTestCaseMeta(abc.ABCMeta):
    def __new__(
        mcs, cls_name: str, bases: tuple[type], attributes: dict[str, Any]
    ) -> Any:
        # Don't enforce the rule on the StateIngestViewTestCase itself
        if cls_name not in (
            "StateIngestViewAndMappingTestCase",
            # TODO(#38322): Delete this line when LegacyIngestViewEmulatorQueryTestCase
            #  is deleted
            "LegacyIngestViewEmulatorQueryTestCase",
        ):
            if attributes.get("__test__", False) is not True:
                raise TypeError(
                    f"Class {cls_name} must explicitly set `__test__ = True`."
                )
        return super().__new__(mcs, cls_name, bases, attributes)


# TODO(#38322): Migrate all states and remove this class
class LegacyIngestViewEmulatorQueryTestCase(
    BigQueryEmulatorTestCase,
    IngestRegionTestMixin,
    metaclass=StateIngestViewAndMappingTestCaseMeta,
):
    """An extension of BigQueryEmulatorTestCase with functionality specific to testing
    ingest view queries.

    TODO(#22059): Standardize ingest view result fixtures for pipeline and ingest view
                  tests, and update this test case to load those fixtures using the same
                  common code as the pipeline test.
    """

    wipe_emulator_data_on_teardown = False

    # Prevent test discovery so we only run
    # test_validate_view_output_schema for subclasses. All subclasses
    # are required to set __test__ = True
    __test__ = False

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

    def test_validate_view_output_schema(self) -> None:
        """This test runs for all subclasses of LegacyIngestViewEmulatorQueryTestCase
          and enforces:
        * The view compiles and runs against empty raw data tables
        * The output schema of the view query matches the column names / types defined
          in the `input_columns` dictionary in this view's ingest mappings YAML file.
        * The output schema of this view query does not have any REPEATED fields (not
          supported by ingest mappings).
        """
        _test_validate_view_output_schema(
            test_case=self,
            state_code=self.state_code(),
            ingest_view_builder=self.ingest_view(),
            query_run_dt=self.query_run_dt,
            manifest_compiler=IngestViewManifestCompiler(
                delegate=StateSchemaIngestViewManifestCompilerDelegate(
                    region=get_direct_ingest_region(self.state_code().value)
                )
            ),
            create_tables=False,
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
            [self.ingest_view()],
            fixture_name,
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
            expect_unique_output_rows=True,
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


def quote_json_values(json_str: str) -> str:
    """Quotes unquoted values in JSON strings."""
    # Matches patterns like: "key": value — where value is unquoted string or date
    updated = re.sub(r'(".*?"):\s*([^",\]\}]+)', r'\1: "\2"', json_str)
    # We want null values from within JSON to be empty strings
    # (the behavior we had before quoting json values here)
    return updated.replace('"NULL"', '""').replace('"null"', '""')


class StateIngestViewAndMappingTestCase(
    BigQueryEmulatorTestCase,
    BaseStateIngestTestCase,
    # Enforce at class collection time that all subclasses of StateIngestViewTestCase
    # set __test__ back to True.
    metaclass=StateIngestViewAndMappingTestCaseMeta,
):
    """
    This is the base test class for ingest testing, where we
      - test a query of raw data against expected results
      - test the parsing of those results against an expected entity tree

    To use this test, subclass it and add a state code and ingest view builder.
    """

    # Prevent test discovery so we only run
    # test_validate_view_output_schema for subclasses. All subclasses
    # are required to set __test__ = True
    __test__ = False

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
        self.ingest_view_manifest_compiler = self.build_ingest_view_manifest_compiler()

    def test_validate_view_output_schema(self) -> None:
        """This test runs for all subclasses of StateIngestViewTestCase and enforces:
        * The view compiles and runs against empty raw data tables
        * The output schema of the view query matches the column names / types defined
          in the `input_columns` dictionary in this view's ingest mappings YAML file.
        * The output schema of this view query does not have any REPEATED fields (not
          supported by ingest mappings).
        """
        _test_validate_view_output_schema(
            test_case=self,
            state_code=self.state_code(),
            ingest_view_builder=self.ingest_view_builder(),
            query_run_dt=self.query_run_dt,
            manifest_compiler=self.build_ingest_view_manifest_compiler(),
            create_tables=True,
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
            characteristic,
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
            expect_unique_output_rows=True,
        )
        # After ingest view results are compared, run the ingest mapping test
        self._run_ingest_mapping_test(
            create_expected_output=create_expected_output,
            ingest_view_name=ingest_view_name,
            characteristic=characteristic,
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

        results = query_job.result().to_dataframe()

        # Log results to debug log level, to see them pass --log-level DEBUG to pytest
        logging.debug(
            "Results for `%s`:\n%s",
            self.ingest_view_builder().ingest_view_name,
            results.to_string(),
        )
        return results

    def _read_from_ingest_view_results_fixture(
        self, ingest_view_name: str, characteristic: str
    ) -> list:
        """Reads a fixture file containing ingest view results."""
        input_fixture = fixture_path_for_address(
            self.state_code(),
            BigQueryAddress(
                dataset_id=f"{self.state_code().value.lower()}_ingest_view_results",
                table_id=ingest_view_name,
            ),
            characteristic,
        )
        return list(
            csv.DictReader(
                LocalFileContentsHandle(
                    input_fixture, cleanup_file=False
                ).get_contents_iterator()
            )
        )

    def _run_ingest_mapping_test(
        self,
        ingest_view_name: str,
        characteristic: str,
        create_expected_output: bool = False,
    ) -> None:
        """
        This tests that an ingest mapping produces an expected output (entity tree)
        from particular ingest view results.

        Args:
            create_expected_output:
                When True, this argument will write the expected entity tree representation
                to a file. The file name is <ingest view name>.fixture and is simply a text file.
                The path to this file arises from `ingest_view_results_parsing_fixture_path`

        Note that empty (either None or empty list) fields on entities are not written out to the file
        or string buffer for this test.
        """
        if in_ci() and create_expected_output:
            raise ValueError("Cannot have create_expected_output=True in the CI.")

        # Build input by reading in fixture, compiling the mapping, and parsing fixture data.
        ingest_view_results = self._read_from_ingest_view_results_fixture(
            ingest_view_name, characteristic
        )
        manifest = self.ingest_view_manifest_compiler.compile_manifest(
            ingest_view_name=ingest_view_name
        )

        # Check that our mapping has a defined RootEntity with
        # defined external ID types.
        if manifest.root_entity_cls not in {
            state_entities.StatePerson,
            state_entities.StateStaff,
        }:
            raise ValueError(f"Unexpected RootEntity type: {manifest.root_entity_cls}")
        allowed_id_types = external_id_types_by_state_code()[self.state_code()]
        found_id_types = manifest.root_entity_external_id_types
        if unexpected_id_types := found_id_types - allowed_id_types:
            raise ValueError(
                f"Unexpected external ID types for {manifest.root_entity_cls}: {unexpected_id_types}"
            )

        # TODO(#39819) Remove this workaround when the emulator quotes date values
        # in JSON correctly. big_query_emulator_test.py has examples
        for row in ingest_view_results:
            # Quote unquoted values in JSON strings
            for key, value in row.items():
                if isinstance(value, str) and value.upper().strip() != "NULL":
                    row[key] = quote_json_values(value)

        parsed_ingest_view_results = manifest.parse_contents(
            contents_iterator=ingest_view_results,
            context=IngestViewContentsContext.build_for_tests(),
        )

        # Build expected entity trees by loading the representation from a fixture
        expected_fixture_path = ingest_mapping_output_fixture_path(
            self.state_code(), ingest_view_name, characteristic
        )
        state_entity_context = entities_module_context_for_module(state_entities)
        if create_expected_output:
            self.create_directory_for_characteristic_level_fixtures(
                expected_fixture_path, characteristic
            )
            with open(expected_fixture_path, "w", encoding="utf-8") as _output_fixture:
                print_entity_trees(
                    parsed_ingest_view_results,
                    state_entity_context,
                    file_or_buffer=_output_fixture,
                )
        with open(expected_fixture_path, "r", encoding="utf-8") as _output_fixture:
            expected_trees = _output_fixture.read()

        # Actually test the ingest mapping!
        # We write the parsed entity tree representation to a string buffer
        # so that we can compare against the representation in the fixture file.
        with io.StringIO() as tree_buffer:
            print_entity_trees(
                parsed_ingest_view_results,
                state_entity_context,
                file_or_buffer=tree_buffer,
            )
            actual_trees = tree_buffer.getvalue()
        self.assertEqual(actual_trees, expected_trees)
