# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2020 Recidiviz, Inc.
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
"""Helper classes for mocking reading / writing from BigQuery in tests."""
import abc
import datetime
import re
from typing import (
    Any,
    Callable,
    Collection,
    Dict,
    Generic,
    Iterable,
    List,
    Optional,
    Set,
    Type,
    TypeVar,
    Union,
)

import apache_beam
from apache_beam.io.gcp.internal.clients import bigquery as beam_bigquery
from apache_beam.pvalue import PBegin, PCollection
from apache_beam.testing.util import BeamAssertException, assert_that, equal_to
from more_itertools import one

from recidiviz.big_query.big_query_address import BigQueryAddress
from recidiviz.big_query.big_query_client import BigQueryClientImpl
from recidiviz.big_query.big_query_query_provider import (
    BigQueryQueryProvider,
    StateFilteredQueryProvider,
)
from recidiviz.big_query.big_query_results_contents_handle import (
    BigQueryResultsContentsHandle,
)
from recidiviz.common.constants.states import StateCode
from recidiviz.persistence.entity.entities_bq_schema import (
    get_bq_schema_for_entities_module,
)
from recidiviz.persistence.entity.state import entities as state_entities
from recidiviz.pipelines.dataflow_config import DATAFLOW_METRICS_TO_TABLES
from recidiviz.pipelines.metrics.utils.metric_utils import RecidivizMetricType
from recidiviz.pipelines.utils.beam_utils.extractor_utils import ROOT_ENTITY_ID_KEY
from recidiviz.pipelines.utils.execution_utils import TableRow
from recidiviz.pipelines.utils.pipeline_run_utils import collect_all_pipeline_classes
from recidiviz.tests.big_query.big_query_emulator_test_case import (
    BigQueryEmulatorTestCase,
)
from recidiviz.tests.pipelines.calculator_test_utils import NormalizedDatabaseDict
from recidiviz.utils.types import assert_type

DatasetStr = str
QueryStr = str
DataTablesDict = Dict[str, Iterable[NormalizedDatabaseDict]]

# Regex matching queries used by calc pipelines to hydrate database entities.
# Example query:
# SELECT * FROM `recidiviz-staging.state.state_person` WHERE state_code IN ('US_XX') AND person_id IN (123, 456)
ENTITY_TABLE_QUERY_REGEX = re.compile(
    r"SELECT ([A-Za-z_\ \,\*]+) FROM `[a-z\d\-]+\.([a-z_]*state[a-z_]*)\.([a-z_]+)` "
    r"WHERE state_code IN \(\'([\w\d]+)\'\)( AND ([a-z_]+) IN \(([\'\w\d ,]+)\))?"
)

# Regex matching queries used by calc pipelines to hydrate reference data.
# Example query:
# SELECT * FROM `recidiviz-staging.reference_views.state_person_to_state_staff` WHERE state_code IN ('US_XX') AND person_id IN (123, 456)
REFERENCE_VIEWS_QUERY_REGEX = re.compile(
    r"SELECT ([A-Za-z_\ \,\*]+) FROM `[a-z\d\-]+\.([a-z_]*reference[a-z_]*)\.([a-z_]+)` "
    r"WHERE state_code IN \(\'([\w\d]+)\'\)( AND ([a-z_]+) IN \(([\'\w\d ,]+)\))?"
)

# Regex matching queries used by calc pipelines to hydrate association table rows
# that include the root_entity_id (usually person_id).
# Example query (with newlines added for readability):
# SELECT state_incarceration_incident.person_id as root_entity_id,
#   state_incarceration_incident_outcome.incarceration_incident_outcome_id,
#   state_incarceration_incident_outcome.incarceration_incident_id
# FROM `recidiviz-123.state.state_incarceration_incident_outcome` state_incarceration_incident_outcome
# JOIN (SELECT * FROM `recidiviz-123.state.state_incarceration_incident`
#       WHERE state_code IN ('US_XX') AND person_id IN (12345)) state_incarceration_incident
# ON state_incarceration_incident.incarceration_incident_id
#   = state_incarceration_incident_outcome.incarceration_incident_id
ASSOCIATION_VALUES_QUERY_REGEX = re.compile(
    r"SELECT ([a-z_]+\.[a-z_]+) as root_entity_id, ([a-z_]+\.[a-z_]+), ([a-z_]+\.[a-z_]+) "
    r"FROM `[a-z\d\-]+\.([a-z_]+)\.([a-z_]+)` ([a-z_]+) "
    r"JOIN \(SELECT \* FROM `[a-z\d\-]+\.([a-z_]+)\.([a-z_]+)` "
    r"WHERE state_code IN \(\'([\w\d]+)\'\)( AND ([a-z_]+) IN \(([\'\w\d ,]+)\))?\) ([a-z_]+) "
    r"ON ([a-z_]+\.[a-z_]+) = ([a-z_]+\.[a-z_]+)"
)


TEST_REFERENCE_QUERY_NAME = "test_reference_query_name"
TEST_REFERENCE_QUERY_PROVIDER = StateFilteredQueryProvider(
    state_code_filter=StateCode.US_XX,
    original_query="SELECT state_code, person_id, a, b FROM UNNEST([])",
)


class FakeBigQueryAssertMatchers:
    """Functions to be used by Apache Beam testing `assert_that` functions to
    validate pipeline outputs."""

    @staticmethod
    def validate_metric_output(
        expected_metric_type: RecidivizMetricType,
    ) -> Callable[[Iterable[Dict[str, Any]]], None]:
        """Asserts that the pipeline produced the expected types of metrics."""

        def _validate_metric_output(output: Iterable[Any]) -> None:
            if not output:
                raise BeamAssertException(
                    f"Failed assert. Output is empty for expected type [{expected_metric_type}]."
                )

            for metric in output:
                if not metric["metric_type"] == expected_metric_type.value:
                    raise BeamAssertException(
                        f"Failed assert. Output metric is not of type [{expected_metric_type}]."
                    )

        return _validate_metric_output


class FakeReadFromBigQuery(apache_beam.PTransform):
    """Creates a PCollection from the provided |table_values|."""

    def __init__(self, table_values: Iterable[NormalizedDatabaseDict]):
        super().__init__()
        self._table_values = table_values

    def expand(self, input_or_inputs: PBegin) -> Any:
        return input_or_inputs | "Loading table values" >> apache_beam.Create(
            self._table_values
        )


class FakeReadFromBigQueryWithEmulator(apache_beam.PTransform):
    """Must be used from within a test that extends BigQueryEmulatorTestCase, returns a
    PCollection of query results. Mocks ReadFromBigQuery PTransform.
    """

    def __init__(
        self,
        query: str,
        test_case: BigQueryEmulatorTestCase,
    ) -> None:
        super().__init__()
        self._query = query
        self._test_case = test_case

    def expand(self, input_or_inputs: PBegin) -> PCollection[Dict[str, Any]]:
        return (
            input_or_inputs
            | "Querying table against BQ Emulator"
            >> apache_beam.Create(
                values=list(
                    BigQueryResultsContentsHandle(
                        self._test_case.bq_client.run_query_async(
                            query_str=self._query, use_query_cache=True
                        )
                    ).get_contents_iterator()
                )
            )
            # The Dataflow BQ IO connectors return datetime objects as strings, but not
            # the BQ Python client, so to match expected behavior, we convert to strings
            # here.
            | "Cleaning values" >> apache_beam.Map(change_datetime_to_str)
        )


# TODO(#25244): Update all pipeline tests to just load data into a BQ emulator and mock
#  the pipelines to read from the emulator instead of real BQ.
class FakeReadFromBigQueryFactory:
    """Factory class that vends fake constructors that can be used to mock the ReadFromBigQuery object."""

    def create_fake_bq_source_constructor(
        self,
        *,
        expected_entities_dataset: str,
        data_dict: DataTablesDict,
        root_entity_id_field: str = "person_id",
        state_code: StateCode = StateCode.US_XX,
    ) -> Callable[[QueryStr, bool, bool, dict[str, str]], FakeReadFromBigQuery]:
        """Returns a constructors function that can mock the ReadFromBigQuery class and will return a
        FakeReadFromBigQuery instead.
        """

        def _fake_bq_source_constructor(
            query: QueryStr,
            # pylint: disable=unused-argument
            use_standard_sql: bool,
            validate: bool,
            bigquery_job_labels: dict[str, str],
        ) -> FakeReadFromBigQuery:
            table_values = self._extractor_utils_data_dict_query_fn(
                expected_entities_dataset=expected_entities_dataset,
                query=query,
                data_dict=data_dict,
                root_entity_id_field=root_entity_id_field,
                state_code=state_code,
            )
            return FakeReadFromBigQuery(table_values=table_values)

        return _fake_bq_source_constructor

    @staticmethod
    def _extractor_utils_data_dict_query_fn(
        *,
        expected_entities_dataset: str,
        query: QueryStr,
        data_dict: DataTablesDict,
        root_entity_id_field: str,
        state_code: StateCode,
    ) -> Iterable[NormalizedDatabaseDict]:
        """Default implementation of the fake query function, which parses, validates,
        and replicates the behavior of the provided query string, returning data out
        of the data_dict object."""
        if re.match(ASSOCIATION_VALUES_QUERY_REGEX, query):
            return FakeReadFromBigQueryFactory._do_fake_association_values_query(
                data_dict,
                query,
                expected_entities_dataset,
                root_entity_id_field,
            )

        if re.match(ENTITY_TABLE_QUERY_REGEX, query):
            return FakeReadFromBigQueryFactory._do_fake_entity_table_query(
                data_dict,
                query,
                expected_entities_dataset,
                root_entity_id_field,
            )

        for pipeline in collect_all_pipeline_classes():
            for (
                provider_name,
                state_filtered_provider,
            ) in pipeline.all_input_reference_query_providers(
                state_code, address_overrides=None
            ).items():
                query_no_filter = state_filtered_provider.original_query
                if isinstance(query_no_filter, str):
                    query_no_filter_str = query_no_filter
                else:
                    query_no_filter_str = query_no_filter.get_query()

                view_query = BigQueryQueryProvider.strip_semicolon(query_no_filter_str)
                if view_query in query:
                    return FakeReadFromBigQueryFactory._get_data_from_table(
                        data_dict=data_dict,
                        table_name=provider_name,
                        state_code_value=state_code.value,
                        root_entity_id_field=root_entity_id_field,
                        root_entity_filter_ids=None,
                        selected_column_name_to_alias=None,
                    )

        if assert_type(TEST_REFERENCE_QUERY_PROVIDER.original_query, QueryStr) in query:
            return FakeReadFromBigQueryFactory._get_data_from_table(
                data_dict=data_dict,
                table_name=TEST_REFERENCE_QUERY_NAME,
                state_code_value=state_code.value,
                root_entity_id_field=root_entity_id_field,
                root_entity_filter_ids=None,
                selected_column_name_to_alias=None,
            )

        raise ValueError(f"Query string does not match known query format: {query}")

    @staticmethod
    def _do_fake_entity_table_query(
        data_dict: DataTablesDict,
        query: str,
        expected_entities_dataset: str,
        expected_root_entity_id_field: str,
    ) -> Iterable[NormalizedDatabaseDict]:
        """Parses, validates, and replicates the behavior of the provided entity table
        query string, returning data out of the data_dict object.
        """
        if not (match := re.match(ENTITY_TABLE_QUERY_REGEX, query)):
            raise ValueError(f"Query does not match regex: {query}")
        dataset = match.group(2)
        table_name = match.group(3)
        if table_name not in data_dict:
            raise ValueError(f"Table {table_name} not in data dict")

        if dataset != expected_entities_dataset:
            raise ValueError(
                f"Found dataset [{dataset}] does not match expected "
                f"dataset [{expected_entities_dataset}]"
            )

        state_code_value = match.group(4)
        if not state_code_value:
            raise ValueError(f"Found no state_code in query [{query}]")

        root_entity_id_field = match.group(6)
        root_entity_id_field_filter_list_str = match.group(7)
        if root_entity_id_field and root_entity_id_field_filter_list_str:
            if root_entity_id_field != expected_root_entity_id_field:
                raise ValueError(
                    f"Expected value [{expected_root_entity_id_field}] "
                    f"for root_entity_id_field does not match: [{root_entity_id_field}"
                )
        elif root_entity_id_field or root_entity_id_field_filter_list_str:
            raise ValueError(
                "Found one of root_entity_id_field, root_entity_id_field_filter_list_str is"
                " None, but not both."
            )

        root_entity_filter_ids = (
            id_list_str_to_set(root_entity_id_field_filter_list_str)
            if root_entity_id_field_filter_list_str
            else None
        )

        selected_columns_str = match.group(1)

        if selected_columns_str != "*":
            # This query selected for certain columns from the entity table. Filter
            # output to just those columns.
            col_entity_name_to_query_name_pairs = [
                (
                    (col, col)
                    if " as " not in col
                    else (col.split(" as ")[0], col.split(" as ")[1])
                )
                for col in selected_columns_str.split(",")
            ]

            selected_column_name_to_alias = {
                col_name.strip(): query_name.strip()
                for col_name, query_name in col_entity_name_to_query_name_pairs
            }
        else:
            selected_column_name_to_alias = None

        return FakeReadFromBigQueryFactory._get_data_from_table(
            data_dict=data_dict,
            table_name=table_name,
            state_code_value=state_code_value,
            root_entity_id_field=root_entity_id_field,
            root_entity_filter_ids=root_entity_filter_ids,
            selected_column_name_to_alias=selected_column_name_to_alias,
        )

    @staticmethod
    def _get_data_from_table(
        *,
        data_dict: DataTablesDict,
        table_name: str,
        state_code_value: str,
        root_entity_id_field: str,
        root_entity_filter_ids: Optional[Set[int]],
        selected_column_name_to_alias: Optional[Dict[str, str]],
    ) -> Iterable[NormalizedDatabaseDict]:
        """Returns data from the provided |data_dict| that corresponds to the
        |table_name| and other provided filters.
        """
        all_table_rows = data_dict[table_name]

        filtered_rows = filter_results(
            table_name,
            all_table_rows,
            "state_code",
            {state_code_value},
            allow_none_values=False,
        )

        if root_entity_filter_ids:
            filtered_rows = filter_results(
                table_name,
                filtered_rows,
                root_entity_id_field,
                root_entity_filter_ids,
                allow_none_values=False,
            )

        if selected_column_name_to_alias:
            filtered_rows_limited_columns: List[NormalizedDatabaseDict] = []
            for row in filtered_rows:
                limited_columns_row = {
                    alias_name: row[col_name]
                    for col_name, alias_name in selected_column_name_to_alias.items()
                }
                filtered_rows_limited_columns.append(limited_columns_row)
            filtered_rows = filtered_rows_limited_columns

        return filtered_rows

    @staticmethod
    def _do_fake_association_values_query(
        data_dict: DataTablesDict,
        query: str,
        expected_entities_dataset: str,
        expected_root_entity_id_field: str,
    ) -> Iterable[NormalizedDatabaseDict]:
        """Parses, validates, and replicates the behavior of the provided association
        value query string, returning data out of the data_dict object.
        """
        match = re.match(ASSOCIATION_VALUES_QUERY_REGEX, query)
        if not match:
            raise ValueError(f"Query does not match regex: {query}")

        entity_table_alias_1, root_entity_id_field = match.group(1).split(".")
        association_table_alias_1, root_id = match.group(2).split(".")
        association_table_alias_2, related_id = match.group(3).split(".")

        dataset_1 = match.group(4)
        association_table_name = match.group(5)
        association_table_alias_3 = match.group(6)
        dataset_2 = match.group(7)
        entity_table_name = match.group(8)
        entity_table_alias_2 = match.group(13)
        entity_table_alias_3, entity_table_join_column = match.group(14).split(".")
        association_table_alias_4, association_table_join_column = match.group(
            15
        ).split(".")

        all_association_table_aliases = {
            association_table_alias_1,
            association_table_alias_2,
            association_table_alias_3,
            association_table_alias_4,
        }
        if len(all_association_table_aliases) != 1:
            raise ValueError(
                f"Unexpected multiple association table aliases: {all_association_table_aliases}"
            )

        all_entity_table_aliases = {
            entity_table_alias_1,
            entity_table_alias_2,
            entity_table_alias_3,
        }
        if len(all_association_table_aliases) != 1:
            raise ValueError(
                f"Unexpected multiple entity table aliases: {all_entity_table_aliases}"
            )

        dataset = one({dataset_1, dataset_2})
        if dataset != expected_entities_dataset:
            raise ValueError(
                f"Found dataset {dataset} does not match expected dataset {expected_entities_dataset}"
            )

        if association_table_name not in data_dict:
            raise ValueError(f"Table {association_table_name} not in data dict")

        _check_field_exists_in_table(association_table_name, root_id)
        _check_field_exists_in_table(association_table_name, related_id)
        _check_field_exists_in_table(
            association_table_name, association_table_join_column
        )
        _check_field_exists_in_table(entity_table_name, entity_table_join_column)

        all_entity_table_rows = data_dict[entity_table_name]
        state_code_value = match.group(9)
        if not state_code_value:
            raise ValueError(f"Found no state_code in query [{query}]")

        filtered_entity_rows = filter_results(
            entity_table_name,
            all_entity_table_rows,
            "state_code",
            {state_code_value},
            allow_none_values=False,
        )
        root_entity_id_field_for_filter = match.group(11)
        root_entity_id_field_filter_list_str = match.group(12)

        if root_entity_id_field_for_filter and root_entity_id_field_filter_list_str:
            if root_entity_id_field_for_filter != expected_root_entity_id_field:
                raise ValueError(
                    f"Expected value [{expected_root_entity_id_field}] for root_entity_id_field does not match: "
                    f"[{root_entity_id_field_for_filter}"
                )

            filtered_entity_rows = filter_results(
                entity_table_name,
                filtered_entity_rows,
                root_entity_id_field_for_filter,
                id_list_str_to_set(root_entity_id_field_filter_list_str),
                allow_none_values=False,
            )
        elif root_entity_id_field_for_filter or root_entity_id_field_filter_list_str:
            raise ValueError(
                "Found one of root_entity_id_field_for_filter, root_entity_id_field_filter_list_str is None, "
                "but not both."
            )

        valid_entity_join_ids = {
            row[entity_table_join_column] for row in filtered_entity_rows
        }

        all_association_table_rows = data_dict[association_table_name]

        joined_filtered_rows = [
            {
                ROOT_ENTITY_ID_KEY: entity_row[root_entity_id_field],
                root_id: association_table_row[root_id],
                related_id: association_table_row[related_id],
            }
            for entity_row in filtered_entity_rows
            for association_table_row in all_association_table_rows
            if entity_row[root_id] == association_table_row[root_id]
        ]

        return filter_results(
            table_name=association_table_name,
            unfiltered_table_rows=joined_filtered_rows,
            filter_id_name=association_table_join_column,
            filter_id_set=valid_entity_join_ids,
            allow_none_values=True,
        )


# TODO(#25244): Mock the pipelines to write back to the emulator instead of real BQ and
#  then just verify that the correct data got written to BQ at the end of the test.
class FakeWriteToBigQuery(apache_beam.PTransform):
    """Fake PTransform that no-ops instead of writing to BQ."""

    def __init__(
        self,
        output_table: str,
    ):
        super().__init__()
        self._output_table = output_table

    @abc.abstractmethod
    def expand(self, input_or_inputs: PCollection) -> Any:
        """Validates the output. Must be implemented by subclasses."""


class FakeWriteToBigQueryEmulator(apache_beam.PTransform):
    """Fake PTransform that writes rows to the BigQuery emulator."""

    def __init__(
        self,
        output_dataset: str,
        output_table: str,
        write_disposition: apache_beam.io.BigQueryDisposition,
        test_case: BigQueryEmulatorTestCase,
    ):
        super().__init__()
        self._output_dataset = output_dataset
        self._output_table = output_table
        self._write_disposition = write_disposition
        # This is unused but here so that we're forced to use this class in the context
        # of a BigQueryEmulatorTestCase.
        self._test_case = test_case

    def expand(self, input_or_inputs: PCollection[TableRow]) -> None:
        # Store thes values as local variables so `self` is not captured by the
        # _write_rows function. This class stores a BigQueryEmulatorTestCase value
        # which cannot be pickled, so cannot be referenced in a `Map` transform.
        output_dataset = self._output_dataset
        output_table = self._output_table
        output_address = BigQueryAddress(
            dataset_id=output_dataset, table_id=output_table
        )
        write_disposition = self._write_disposition

        def _write_rows(_: None, rows: list[TableRow]) -> None:
            # Create a new client so that we don't have to referenced `self._test_case`
            # from inside the mapped function. Because we're running in the context of
            # a BigQueryEmulatorTestCase, this client will talk to the emulator.
            bq_client = BigQueryClientImpl()

            if write_disposition == apache_beam.io.BigQueryDisposition.WRITE_TRUNCATE:
                query_job = bq_client.run_query_async(
                    query_str=f"DELETE FROM `{bq_client.project_id}.{output_address.to_str()}` WHERE TRUE",
                    use_query_cache=False,
                )
                query_job.result()

            bq_client.stream_into_table(address=output_address, rows=rows)

        _ = (
            input_or_inputs.pipeline
            # Create a PCollection with a single value so the _write_rows function gets
            # called exactly once.
            | apache_beam.Create([None])
            | apache_beam.Map(
                _write_rows,
                # Convert the rows to a list - this is only a valid thing to do in tests
                # where we are confident that the rows will all fit into memory at once
                rows=apache_beam.pvalue.AsList(input_or_inputs),
            )
        )

    @classmethod
    def get_mock_write_to_big_query_constructor(
        cls,
        test_case: BigQueryEmulatorTestCase,
    ) -> Callable[
        [str, str, apache_beam.io.BigQueryDisposition], "FakeWriteToBigQueryEmulator"
    ]:
        """Returns a function that is a mock version of the `WriteToBigQuery`
        constructor and will return a `FakeWriteToBigQueryEmulator` instance.
        """

        def mock_write_to_big_query_constructor(
            output_dataset: str,
            output_table: str,
            write_disposition: apache_beam.io.BigQueryDisposition,
        ) -> "FakeWriteToBigQueryEmulator":
            return FakeWriteToBigQueryEmulator(
                output_dataset=output_dataset,
                output_table=output_table,
                write_disposition=write_disposition,
                test_case=test_case,
            )

        return mock_write_to_big_query_constructor


class FakeWriteMetricsToBigQuery(FakeWriteToBigQuery):
    """Fake PTransform for metric pipelines that no-ops instead of writing metrics to
    BQ."""

    def __init__(
        self,
        output_table: str,
        expected_output_tags: Collection[str],
    ):
        super().__init__(output_table)
        metric_types_for_table = {
            metric_class(job_id="xxx", state_code="xxx").metric_type  # type: ignore[call-arg]
            for metric_class, table_id in DATAFLOW_METRICS_TO_TABLES.items()
            if table_id == output_table
        }

        self._expected_output_metric_types = expected_output_tags
        self._expected_metric_type = one(metric_types_for_table)

    def expand(self, input_or_inputs: PCollection) -> Any:
        if self._expected_metric_type.value in self._expected_output_metric_types:
            assert_that(
                input_or_inputs,
                FakeBigQueryAssertMatchers.validate_metric_output(
                    self._expected_metric_type
                ),
            )
        else:
            assert_that(input_or_inputs, equal_to([]))

        return []


class FakeWriteExactOutputToBigQuery(FakeWriteToBigQuery):
    """Fake PTransform for pipelines that no-ops instead of writing given rows to BQ."""

    def __init__(
        self,
        output_table: str,
        expected_output: List[Dict[str, Any]],
    ) -> None:
        super().__init__(output_table)
        self._expected_output = expected_output

    def expand(self, input_or_inputs: PCollection) -> Any:
        assert_that(input_or_inputs, equal_to(self._expected_output))


FakeWriteToBigQueryType = TypeVar("FakeWriteToBigQueryType", bound=FakeWriteToBigQuery)


class FakeWriteToBigQueryFactory(Generic[FakeWriteToBigQueryType]):
    """Factory class that vends fake constructors that can be used to mock the
    FakeWriteToBigQuery object."""

    def __init__(self, fake_write_to_big_query_cls: Type[FakeWriteToBigQueryType]):
        self._fake_write_to_big_query_cls = fake_write_to_big_query_cls

    def create_fake_bq_sink_constructor(
        self,
        *,
        expected_dataset: str,
        **kwargs: Any,
    ) -> Callable[
        [
            str,
            str,
            apache_beam.io.BigQueryDisposition,
        ],
        FakeWriteToBigQueryType,
    ]:
        """Constructor for writing to BQ"""

        def write_constructor(
            output_table: str,
            output_dataset: str,
            write_disposition: apache_beam.io.BigQueryDisposition,
            schema: Optional[  # pylint: disable=unused-argument,redefined-outer-name
                beam_bigquery.TableSchema
            ] = None,
            validator_fn_generator: Optional[  # pylint: disable=unused-argument
                Callable[
                    [List[Dict[str, Any]], str],
                    Callable[[List[Dict[str, Any]]], None],
                ]
            ] = None,
        ) -> FakeWriteToBigQueryType:
            if output_dataset != expected_dataset:
                raise ValueError(
                    f"Output dataset [{output_dataset}] does not match expected_dataset [{expected_dataset}] "
                    f"writing to table [{output_table}]"
                )

            if write_disposition not in (
                apache_beam.io.BigQueryDisposition.WRITE_APPEND,
                apache_beam.io.BigQueryDisposition.WRITE_TRUNCATE,
            ):
                raise ValueError(f"Unexpected write_disposition: {write_disposition}.")

            return self._fake_write_to_big_query_cls(  # type: ignore[call-arg]
                output_table, **kwargs
            )

        return write_constructor


def change_datetime_to_str(output: Dict[str, Any]) -> Dict[str, Any]:
    """Converts datetime objects to strings in the output."""
    for key, value in output.items():
        if isinstance(value, datetime.datetime):
            output[key] = value.strftime("%Y-%m-%d %H:%M:%S")
    return output


def id_list_str_to_set(id_list_str: str) -> Set[int]:
    return {int(filter_id) for filter_id in id_list_str.split(", ")}


def filter_results(
    table_name: str,
    unfiltered_table_rows: Iterable[NormalizedDatabaseDict],
    filter_id_name: str,
    filter_id_set: Union[Set[int], Set[str]],
    allow_none_values: bool,
) -> Iterable[NormalizedDatabaseDict]:
    _check_field_exists_in_table(table_name, filter_id_name)

    for row in unfiltered_table_rows:
        if filter_id_name not in row:
            raise ValueError(
                f"Field [{filter_id_name}] not in row for table [{table_name}]: {row}"
            )
        if not allow_none_values and row[filter_id_name] is None:
            raise ValueError(
                f"Field [{filter_id_name}] is None for row in [{table_name}]: {row}"
            )

    return [
        row for row in unfiltered_table_rows if row[filter_id_name] in filter_id_set
    ]


def _check_field_exists_in_table(table_name: str, field_name: str) -> None:
    """Given the name of a table our fake BQ utilities might be reading from, throws if
    that field does not exist in the table.
    """
    if table_name in {
        # This is the name of a fake reference table used in some tests
        TEST_REFERENCE_QUERY_NAME,
        # These are the names of reference view queries read by pipelines
        # TODO(#25244): Even though the dataflow pipelines now run ingest view queries
        #  directly, our test setup is still very hacky and instead of setting up source
        #  tables that feed into each reference query, we just seed the tests with query
        #  results that we return when the query matches an expected query. We should
        #  instead update our pipeline tests to actually read from the BQ emulator and
        #  set up the necessary source table info there so we can avoid calling this
        #  function at all.
        "us_ix_case_update_info",
        "us_me_snoozed_opportunity_notes",
        "state_person_to_state_staff",
    }:
        return

    bq_schema = get_bq_schema_for_entities_module(state_entities)

    if table_name not in bq_schema:
        raise ValueError(f"No valid table with name: [{table_name}]")

    table_schema = bq_schema[table_name]
    column_names = {f.name for f in table_schema}

    if field_name not in column_names:
        raise ValueError(f"Column {field_name} does not exist in table {table_name}.")
