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
from recidiviz.big_query.big_query_results_contents_handle import (
    BigQueryResultsContentsHandle,
)
from recidiviz.persistence.database.schema.state import schema
from recidiviz.persistence.database.schema_type import SchemaType
from recidiviz.persistence.database.schema_utils import (
    get_all_table_classes_in_schema,
    get_database_entities_by_association_table,
    get_database_entity_by_table_name,
    is_association_table,
)
from recidiviz.persistence.entity.normalized_entities_utils import (
    normalized_entity_class_with_base_class_name,
)
from recidiviz.pipelines.dataflow_config import DATAFLOW_METRICS_TO_TABLES
from recidiviz.pipelines.metrics.utils.metric_utils import RecidivizMetricType
from recidiviz.pipelines.normalization.utils.normalized_entity_conversion_utils import (
    bq_schema_for_normalized_state_association_table,
    column_names_on_bq_schema_for_normalized_state_entity,
)
from recidiviz.pipelines.utils.beam_utils.extractor_utils import UNIFYING_ID_KEY
from recidiviz.tests.big_query.big_query_emulator_test_case import (
    BigQueryEmulatorTestCase,
)
from recidiviz.tests.pipelines.calculator_test_utils import NormalizedDatabaseDict

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
# SELECT * FROM `recidiviz-staging.reference_views.persons_to_recent_county_of_residence` WHERE state_code IN ('US_XX') AND person_id IN (123, 456)
REFERENCE_VIEWS_QUERY_REGEX = re.compile(
    r"SELECT ([A-Za-z_\ \,\*]+) FROM `[a-z\d\-]+\.([a-z_]*reference[a-z_]*)\.([a-z_]+)` "
    r"WHERE state_code IN \(\'([\w\d]+)\'\)( AND ([a-z_]+) IN \(([\'\w\d ,]+)\))?"
)

# Regex matching queries used by calc pipelines to hydrate association table rows
# that include the unifying_id (usually person_id).
# Example query (with newlines added for readability):
# SELECT state_incarceration_incident.person_id as unifying_id,
#   state_incarceration_incident_outcome.incarceration_incident_outcome_id,
#   state_incarceration_incident_outcome.incarceration_incident_id
# FROM `recidiviz-123.state.state_incarceration_incident_outcome` state_incarceration_incident_outcome
# JOIN (SELECT * FROM `recidiviz-123.state.state_incarceration_incident`
#       WHERE state_code IN ('US_XX') AND person_id IN (12345)) state_incarceration_incident
# ON state_incarceration_incident.incarceration_incident_id
#   = state_incarceration_incident_outcome.incarceration_incident_id
ASSOCIATION_VALUES_QUERY_REGEX = re.compile(
    r"SELECT ([a-z_]+\.[a-z_]+) as unifying_id, ([a-z_]+\.[a-z_]+), ([a-z_]+\.[a-z_]+) "
    r"FROM `[a-z\d\-]+\.([a-z_]+)\.([a-z_]+)` ([a-z_]+) "
    r"JOIN \(SELECT \* FROM `[a-z\d\-]+\.([a-z_]+)\.([a-z_]+)` "
    r"WHERE state_code IN \(\'([\w\d]+)\'\)( AND ([a-z_]+) IN \(([\'\w\d ,]+)\))?\) ([a-z_]+) "
    r"ON ([a-z_]+\.[a-z_]+) = ([a-z_]+\.[a-z_]+)"
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

    @staticmethod
    def validate_normalized_entity_output(
        base_class_name: str,
    ) -> Callable[[Iterable[Dict[str, Any]]], None]:
        """Asserts that the pipeline produces dictionaries with the expected keys
        corresponding to the column names in the table into which the output will be
        written."""

        def _validate_normalized_entity_output(
            output: Iterable[Dict[str, Any]]
        ) -> None:
            normalized_entity_class = normalized_entity_class_with_base_class_name(
                base_class_name=base_class_name
            )
            expected_column_names = set(
                column_names_on_bq_schema_for_normalized_state_entity(
                    normalized_entity_class
                )
            )

            if not output:
                raise BeamAssertException(
                    f"Failed assert. Output is empty for entity {base_class_name}."
                )
            for output_dict in output:
                if set(output_dict.keys()) != expected_column_names:
                    raise BeamAssertException(
                        "Output dictionary does not have "
                        f"the expected keys. Expected: [{expected_column_names}], "
                        f"found: [{list(output_dict.keys())}]."
                    )

        return _validate_normalized_entity_output

    @staticmethod
    def validate_normalized_association_entity_output(
        child_class_name: str, parent_class_name: str
    ) -> Callable[[Iterable[Dict[str, Any]]], None]:
        """Asserts that the pipeline produces dictionaries with the expected keys
        corresponding to the column names in the association table which the output
        will be written."""

        def _validate_normalized_association_entity_output(
            output: Iterable[Dict[str, Any]]
        ) -> None:
            schema_for_association = bq_schema_for_normalized_state_association_table(
                child_class_name, parent_class_name
            )

            expected_column_names = {field.name for field in schema_for_association}

            if not output:
                raise BeamAssertException(
                    f"Failed assert. Output is empty for entity association {child_class_name}, {parent_class_name}"
                )
            for output_dict in output:
                if set(output_dict.keys()) != expected_column_names:
                    raise BeamAssertException(
                        "Output dictionary does not have "
                        f"the expected keys. Expected: [{expected_column_names}],"
                        f"found: [{list(output_dict.keys())}]"
                    )

        return _validate_normalized_association_entity_output


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

    def __init__(self, query: str, test_case: BigQueryEmulatorTestCase) -> None:
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


class FakeReadAllFromBigQueryWithEmulator(apache_beam.PTransform):
    """Must be used from within a test that extends BigQueryEmulatorTestCase, returns a
    PCollection of query results. Mocks ReadAllFromBigQuery PTransform.
    """

    def __init__(self) -> None:
        super().__init__()

    def expand(
        self, input_or_inputs: PCollection[apache_beam.io.ReadFromBigQueryRequest]
    ) -> PCollection[Dict[str, Any]]:
        return (
            input_or_inputs
            | "Querying table against BQ Emulator"
            >> apache_beam.FlatMap(self._query_against_emulator)
            | "Cleaning values" >> apache_beam.Map(change_datetime_to_str)
        )

    def _query_against_emulator(
        self, request: apache_beam.io.ReadFromBigQueryRequest
    ) -> List[Dict[str, Any]]:
        # NOTE: Ideally this class would mimic FakeReadFromBigQueryWithEmulator and
        # get bq_client from the TestCase, however for some reason this class must
        # be pickleable (we don't know why this one and not
        # FakeReadFromBigQueryWithEmulator), so cannot store the TestCase on the class.
        bq_client = BigQueryClientImpl()
        return list(
            BigQueryResultsContentsHandle(
                bq_client.run_query_async(query_str=request.query, use_query_cache=True)
            ).get_contents_iterator()
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
        expected_reference_views_dataset: str = "reference_views",
        unifying_id_field: str = "person_id",
    ) -> Callable[[QueryStr, bool, bool], FakeReadFromBigQuery]:
        """Returns a constructors function that can mock the ReadFromBigQuery class and will return a
        FakeReadFromBigQuery instead.
        """

        def _fake_bq_source_constructor(
            query: QueryStr,
            # pylint: disable=unused-argument
            use_standard_sql: bool,
            validate: bool,
        ) -> FakeReadFromBigQuery:
            table_values = self._extractor_utils_data_dict_query_fn(
                expected_entities_dataset=expected_entities_dataset,
                expected_reference_views_dataset=expected_reference_views_dataset,
                query=query,
                data_dict=data_dict,
                unifying_id_field=unifying_id_field,
            )
            return FakeReadFromBigQuery(table_values=table_values)

        return _fake_bq_source_constructor

    @staticmethod
    def _extractor_utils_data_dict_query_fn(
        *,
        expected_entities_dataset: str,
        expected_reference_views_dataset: str,
        query: QueryStr,
        data_dict: DataTablesDict,
        unifying_id_field: str,
    ) -> Iterable[NormalizedDatabaseDict]:
        """Default implementation of the fake query function, which parses, validates,
        and replicates the behavior of the provided query string, returning data out
        of the data_dict object."""
        if re.match(ASSOCIATION_VALUES_QUERY_REGEX, query):
            return FakeReadFromBigQueryFactory._do_fake_association_values_query(
                data_dict,
                query,
                expected_entities_dataset,
                unifying_id_field,
            )

        if re.match(ENTITY_TABLE_QUERY_REGEX, query):
            return FakeReadFromBigQueryFactory._do_fake_entity_table_query(
                data_dict,
                query,
                expected_entities_dataset,
                unifying_id_field,
            )

        if re.match(REFERENCE_VIEWS_QUERY_REGEX, query):
            return FakeReadFromBigQueryFactory._do_fake_entity_table_query(
                data_dict,
                query,
                expected_reference_views_dataset,
                unifying_id_field,
            )

        raise ValueError(f"Query string does not match known query format: {query}")

    @staticmethod
    def _do_fake_entity_table_query(
        data_dict: DataTablesDict,
        query: str,
        expected_entities_dataset: str,
        expected_unifying_id_field: str,
    ) -> Iterable[NormalizedDatabaseDict]:
        """Parses, validates, and replicates the behavior of the provided entity table
        query string, returning data out of the data_dict object.
        """
        if match := re.match(ENTITY_TABLE_QUERY_REGEX, query):
            dataset = match.group(2)
        elif match := re.match(REFERENCE_VIEWS_QUERY_REGEX, query):
            dataset = match.group(2)
        else:
            raise ValueError(f"Query does not match regex: {query}")

        table_name = match.group(3)
        if table_name not in data_dict:
            raise ValueError(f"Table {table_name} not in data dict")

        if dataset != expected_entities_dataset:
            raise ValueError(
                f"Found dataset [{dataset}] does not match expected "
                f"dataset [{expected_entities_dataset}]"
            )

        all_table_rows = data_dict[table_name]

        state_code_value = match.group(4)
        if not state_code_value:
            raise ValueError(f"Found no state_code in query [{query}]")

        filtered_rows = filter_results(
            table_name,
            all_table_rows,
            "state_code",
            {state_code_value},
            allow_none_values=False,
        )

        unifying_id_field = match.group(6)
        unifying_id_field_filter_list_str = match.group(7)

        if unifying_id_field and unifying_id_field_filter_list_str:
            if unifying_id_field != expected_unifying_id_field:
                raise ValueError(
                    f"Expected value [{expected_unifying_id_field}] "
                    f"for unifying_id_field does not match: [{unifying_id_field}"
                )

            filtered_rows = filter_results(
                table_name,
                filtered_rows,
                unifying_id_field,
                id_list_str_to_set(unifying_id_field_filter_list_str),
                allow_none_values=False,
            )
        elif unifying_id_field or unifying_id_field_filter_list_str:
            raise ValueError(
                "Found one of unifying_id_field, unifying_id_field_filter_list_str is"
                " None, but not both."
            )

        selected_columns = match.group(1)

        if selected_columns != "*":
            # This query selected for certain columns from the entity table. Filter
            # output to just those columns.
            col_entity_name_to_query_name_pairs = [
                (col, col)
                if " as " not in col
                else (col.split(" as ")[0], col.split(" as ")[1])
                for col in selected_columns.split(",")
            ]

            query_names_to_col_entity_names = {
                query_name.strip(): col_name.strip()
                for col_name, query_name in col_entity_name_to_query_name_pairs
            }

            filtered_rows_limited_columns: List[NormalizedDatabaseDict] = []
            for row in filtered_rows:
                limited_columns_row = {
                    query_name: row[col_name]
                    for query_name, col_name in query_names_to_col_entity_names.items()
                }
                filtered_rows_limited_columns.append(limited_columns_row)
            filtered_rows = filtered_rows_limited_columns

        return filtered_rows

    @staticmethod
    def _do_fake_association_values_query(
        data_dict: DataTablesDict,
        query: str,
        expected_entities_dataset: str,
        expected_unifying_id_field: str,
    ) -> Iterable[NormalizedDatabaseDict]:
        """Parses, validates, and replicates the behavior of the provided association
        value query string, returning data out of the data_dict object.
        """
        match = re.match(ASSOCIATION_VALUES_QUERY_REGEX, query)
        if not match:
            raise ValueError(f"Query does not match regex: {query}")

        entity_table_alias_1, unifying_id_field = match.group(1).split(".")
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

        check_field_exists_in_table(association_table_name, root_id)
        check_field_exists_in_table(association_table_name, related_id)
        check_field_exists_in_table(
            association_table_name, association_table_join_column
        )
        check_field_exists_in_table(entity_table_name, entity_table_join_column)

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
        unifying_id_field_for_filter = match.group(11)
        unifying_id_field_filter_list_str = match.group(12)

        if unifying_id_field_for_filter and unifying_id_field_filter_list_str:
            if unifying_id_field_for_filter != expected_unifying_id_field:
                raise ValueError(
                    f"Expected value [{expected_unifying_id_field}] for unifying_id_field does not match: "
                    f"[{unifying_id_field_for_filter}"
                )

            filtered_entity_rows = filter_results(
                entity_table_name,
                filtered_entity_rows,
                unifying_id_field_for_filter,
                id_list_str_to_set(unifying_id_field_filter_list_str),
                allow_none_values=False,
            )
        elif unifying_id_field_for_filter or unifying_id_field_filter_list_str:
            raise ValueError(
                "Found one of unifying_id_field, unifying_id_field_filter_list_str is None, but not both."
            )

        valid_entity_join_ids = {
            row[entity_table_join_column] for row in filtered_entity_rows
        }

        all_association_table_rows = data_dict[association_table_name]

        joined_filtered_rows = [
            {
                UNIFYING_ID_KEY: entity_row[unifying_id_field],
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


class FakeWriteNormalizedEntitiesToBigQuery(FakeWriteToBigQuery):
    """Fake PTransform for normalization pipelines that no-ops instead of writing
    normalized entities to BQ."""

    def __init__(
        self,
        output_table: str,
        expected_output_tags: Collection[str],
    ):
        super().__init__(output_table)
        self._expected_output_tags = expected_output_tags

    def expand(self, input_or_inputs: PCollection) -> Any:
        if is_association_table(self._output_table):
            child_entity, parent_entity = get_database_entities_by_association_table(
                schema, self._output_table
            )

            if (
                f"{child_entity.__name__}_{parent_entity.__name__}"
                in self._expected_output_tags
            ):
                assert_that(
                    input_or_inputs,
                    FakeBigQueryAssertMatchers.validate_normalized_association_entity_output(
                        child_entity.__name__, parent_entity.__name__
                    ),
                )
            else:
                assert_that(input_or_inputs, equal_to([]))
        else:
            db_entity = get_database_entity_by_table_name(schema, self._output_table)

            if db_entity.__name__ in self._expected_output_tags:
                assert_that(
                    input_or_inputs,
                    FakeBigQueryAssertMatchers.validate_normalized_entity_output(
                        db_entity.__name__
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


ExpectedOutput = Iterable[Dict[str, Any]]
ActualOutput = List[Dict[str, Any]]


class FakeWriteOutputToBigQueryWithValidator(FakeWriteToBigQuery):
    """Allows for the write results of BigQuery to be validated in a custom way."""

    def __init__(
        self,
        output_address: BigQueryAddress,
        expected_output: ExpectedOutput,
        validator_fn_generator: Callable[
            [ExpectedOutput, str], Callable[[ActualOutput], None]
        ],
    ) -> None:
        super().__init__(output_table=output_address.table_id)
        self._expected_output = expected_output
        # This function takes in a list of expected results and returns a function that will verify
        # that the pipeline output matches the expected results.
        self._validator_fn_generator = validator_fn_generator

    def expand(self, input_or_inputs: PCollection) -> Any:
        assert_that(
            input_or_inputs,
            self._validator_fn_generator(self._expected_output, self._output_table),
        )


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
    check_field_exists_in_table(table_name, filter_id_name)

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


def check_field_exists_in_table(table_name: str, field_name: str) -> None:
    if table_name in {
        # These are tables read by pipelines that are not in the sqlalchemy schema - skip this check
        # TODO(#22528): We should be able to remove these exemptions once dataflow pipelines run
        #  these reference queries directly.
        "us_mo_sentence_statuses",
        "persons_to_recent_county_of_residence",
        "state_race_ethnicity_population_counts",
        "us_id_case_update_info",
        "us_ix_case_update_info",
        "state_charge_offense_description_to_labels",
        "state_person_to_state_staff",
    }:
        return

    matching_tables = {
        table
        for table in get_all_table_classes_in_schema(SchemaType.STATE)
        if table.name == table_name
    }
    if not matching_tables:
        raise ValueError(f"No valid table with name: [{table_name}]")

    table = one(matching_tables)

    column_names = {sqlalchemy_column.name for sqlalchemy_column in table.columns}

    if field_name not in column_names:
        raise ValueError(f"Column {field_name} does not exist in table {table_name}.")
