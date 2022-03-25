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
import re
from typing import (
    Any,
    Callable,
    Collection,
    Dict,
    Generic,
    List,
    Optional,
    Set,
    Type,
    TypeVar,
    Union,
)

import apache_beam
from apache_beam.pvalue import PCollection
from apache_beam.testing.util import BeamAssertException, assert_that, equal_to
from more_itertools import one

from recidiviz.calculator.dataflow_config import DATAFLOW_METRICS_TO_TABLES
from recidiviz.calculator.pipeline.metrics.utils.metric_utils import RecidivizMetricType
from recidiviz.calculator.pipeline.utils.beam_utils.extractor_utils import (
    UNIFYING_ID_KEY,
)
from recidiviz.calculator.pipeline.utils.entity_normalization.normalized_entities_utils import (
    normalized_entity_class_exists_for_base_class_with_name,
    normalized_entity_class_with_base_class_name,
)
from recidiviz.calculator.pipeline.utils.entity_normalization.normalized_entity_conversion_utils import (
    column_names_on_bq_schema_for_normalized_state_entity,
)
from recidiviz.persistence.database.schema.state import schema
from recidiviz.persistence.database.schema_utils import (
    get_database_entity_by_table_name,
    get_state_table_classes,
)
from recidiviz.tests.calculator.calculator_test_utils import NormalizedDatabaseDict

DatasetStr = str
QueryStr = str
DataTablesDict = Dict[str, List[NormalizedDatabaseDict]]
DataDictQueryFn = Callable[
    [DatasetStr, QueryStr, DataTablesDict, str, Optional[DatasetStr]],
    List[NormalizedDatabaseDict],
]

# Regex matching queries used by calc pipelines to hydrate database entities.
# Example query:
# SELECT * FROM `recidiviz-staging.state.state_person` WHERE state_code IN ('US_XX') AND person_id IN (123, 456)
ENTITY_TABLE_QUERY_REGEX = re.compile(
    r"SELECT ([A-Za-z_\ \,\*]+) FROM `[a-z\d\-]+\.([a-z_]+)\.([a-z_]+)` "
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
    ) -> Callable[[List[Dict[str, Any]]], None]:
        """Asserts that the pipeline produced the expected types of metrics."""

        def _validate_metric_output(output: List[Dict[str, Any]]) -> None:
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
    ) -> Callable[[List[Dict[str, Any]]], None]:
        """Asserts that the pipeline produces dictionaries with the expected keys
        corresponding to the column names in the table into which the output will be
        written."""

        def _validate_normalized_entity_output(output: List[Dict[str, Any]]) -> None:
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


class FakeReadFromBigQuery(apache_beam.PTransform):
    """Creates a PCollection from the provided |table_values|."""

    def __init__(self, table_values: List[NormalizedDatabaseDict]):
        super().__init__()
        self._table_values = table_values

    def expand(self, input_or_inputs: PCollection) -> Any:
        return input_or_inputs | "Loading table values" >> apache_beam.Create(
            self._table_values
        )


class FakeReadFromBigQueryFactory:
    """Factory class that vends fake constructors that can be used to mock the ReadFromBigQuery object."""

    def __init__(self, data_dict_query_fn: DataDictQueryFn = None):
        self._data_dict_query_fn: DataDictQueryFn = (
            data_dict_query_fn
            if data_dict_query_fn
            else self._extractor_utils_data_dict_query_fn
        )

    def create_fake_bq_source_constructor(
        self,
        expected_dataset: str,
        data_dict: DataTablesDict,
        unifying_id_field: str = "person_id",
        expected_normalized_dataset: Optional[str] = None,
    ) -> Callable[[QueryStr], FakeReadFromBigQuery]:
        """Returns a constructors function that can mock the ReadFromBigQuery class and will return a
        FakeReadFromBigQuery instead.
        """

        data_dict_query_fn = self._data_dict_query_fn

        def _fake_bq_source_constructor(query: QueryStr) -> FakeReadFromBigQuery:
            table_values = data_dict_query_fn(
                expected_dataset,
                query,
                data_dict,
                unifying_id_field,
                expected_normalized_dataset,
            )
            return FakeReadFromBigQuery(table_values=table_values)

        return _fake_bq_source_constructor

    @staticmethod
    def _extractor_utils_data_dict_query_fn(
        expected_dataset: str,
        query: QueryStr,
        data_dict: DataTablesDict,
        unifying_id_field: str,
        expected_normalized_dataset: Optional[str],
    ) -> List[NormalizedDatabaseDict]:
        """Default implementation of the fake query function, which parses, validates,
        and replicates the behavior of the provided query string, returning data out
        of the data_dict object."""
        expected_normalized_dataset = expected_normalized_dataset or None

        if re.match(ASSOCIATION_VALUES_QUERY_REGEX, query):
            return FakeReadFromBigQueryFactory._do_fake_association_values_query(
                data_dict,
                query,
                expected_dataset,
                unifying_id_field,
                expected_normalized_dataset,
            )

        if re.match(ENTITY_TABLE_QUERY_REGEX, query):
            return FakeReadFromBigQueryFactory._do_fake_entity_table_query(
                data_dict,
                query,
                expected_dataset,
                unifying_id_field,
                expected_normalized_dataset,
            )

        raise ValueError(f"Query string does not match known query format: {query}")

    @staticmethod
    def _do_fake_entity_table_query(
        data_dict: DataTablesDict,
        query: str,
        expected_dataset: str,
        expected_unifying_id_field: str,
        expected_normalized_dataset: Optional[str] = None,
    ) -> List[NormalizedDatabaseDict]:
        """Parses, validates, and replicates the behavior of the provided entity table
        query string, returning data out of the data_dict object.
        """
        match = re.match(ENTITY_TABLE_QUERY_REGEX, query)

        if not match:
            raise ValueError(f"Query does not match regex: {query}")

        dataset = match.group(2)

        table_name = match.group(3)
        if table_name not in data_dict:
            raise ValueError(f"Table {table_name} not in data dict")

        try:
            db_entity = get_database_entity_by_table_name(schema, table_name)
        except ValueError:
            db_entity = None

        if (
            db_entity
            and expected_normalized_dataset
            and normalized_entity_class_exists_for_base_class_with_name(
                db_entity.__name__
            )
        ):
            if dataset != expected_normalized_dataset:
                raise ValueError(
                    f"Found dataset [{dataset}] does not match expected "
                    f"dataset [{expected_normalized_dataset}]"
                )
        elif dataset != expected_dataset:
            raise ValueError(
                f"Found dataset [{dataset}] does not match expected "
                f"dataset [{expected_dataset}]"
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
        expected_dataset: str,
        expected_unifying_id_field: str,
        # TODO(#11734): Implement support for association queries from normalized
        #  datasets
        _expected_normalized_dataset: Optional[str] = None,
    ) -> List[NormalizedDatabaseDict]:
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
        if dataset != expected_dataset:
            raise ValueError(
                f"Found dataset {dataset} does not match expected dataset {expected_dataset}"
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


class FakeWriteToBigQuery(apache_beam.PTransform):
    """Fake PTransform that no-ops instead of writing to BQ."""

    def __init__(
        self,
        output_table: str,
        expected_output_tags: Collection[str],
    ):
        super().__init__()
        self._output_table = output_table
        self._expected_output_tags = expected_output_tags

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
        super().__init__(output_table, expected_output_tags)
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
        super().__init__(output_table, expected_output_tags)

    def expand(self, input_or_inputs: PCollection) -> Any:
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
        expected_output_tags: Collection[str],
        expected_output: List[Dict[str, Any]],
    ) -> None:
        super().__init__(output_table, expected_output_tags)
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
        expected_dataset: str,
        expected_output_tags: Collection[str],
        **kwargs: Any,
    ) -> Callable[
        [str, str, apache_beam.io.BigQueryDisposition], FakeWriteToBigQueryType
    ]:
        def write_constructor(
            output_table: str,
            output_dataset: str,
            write_disposition: apache_beam.io.BigQueryDisposition,
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
                output_table, expected_output_tags, **kwargs
            )

        return write_constructor


def id_list_str_to_set(id_list_str: str) -> Set[int]:
    return {int(filter_id) for filter_id in id_list_str.split(", ")}


def filter_results(
    table_name: str,
    unfiltered_table_rows: List[NormalizedDatabaseDict],
    filter_id_name: str,
    filter_id_set: Union[Set[int], Set[str]],
    allow_none_values: bool,
) -> List[NormalizedDatabaseDict]:

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
        "supervision_period_to_agent_association",
        "supervision_period_judicial_district_association",
        "state_race_ethnicity_population_counts",
        "us_mo_sentence_statuses",
        "persons_to_recent_county_of_residence",
        "incarceration_period_judicial_district_association",
        "state_race_ethnicity_population_counts",
        "us_id_case_update_info",
    }:
        return

    matching_tables = {
        table for table in get_state_table_classes() if table.name == table_name
    }
    if not matching_tables:
        raise ValueError(f"No valid table with name: [{table_name}]")

    table = one(matching_tables)

    column_names = {sqlalchemy_column.name for sqlalchemy_column in table.columns}

    if field_name not in column_names:
        raise ValueError(f"Column {field_name} does not exist in table {table_name}.")
