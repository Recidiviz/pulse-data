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
import re
from typing import Dict, Callable, List, Set, Union, Any, TypeVar, Generic, Type, Collection

from apache_beam.testing.util import assert_that, BeamAssertException, equal_to

import apache_beam
from more_itertools import one

from recidiviz.calculator.dataflow_output_storage_config import DATAFLOW_METRICS_TO_TABLES
from recidiviz.calculator.pipeline.utils.metric_utils import RecidivizMetricType
from recidiviz.persistence.database.schema_utils import get_state_table_classes
from recidiviz.tests.calculator.calculator_test_utils import NormalizedDatabaseDict

DatasetStr = str
QueryStr = str
DataTablesDict = Dict[str, List[NormalizedDatabaseDict]]
DataDictQueryFn = Callable[[DatasetStr, QueryStr, DataTablesDict, str], List[NormalizedDatabaseDict]]

# Regex matching queries used by calc pipelines to hydrate database entities.
# Example query:
# SELECT * FROM `recidiviz-staging.state.state_person` WHERE state_code IN ('US_XX') AND person_id IN (123, 456)
ENTITY_TABLE_QUERY_REGEX = re.compile(
    r'SELECT \* FROM `([a-z\d\-]+\.[a-z_]+)\.([a-z_]+)` '
    r'WHERE state_code IN \(\'([\w\d]+)\'\)( AND ([a-z_]+) IN \(([\'\w\d ,]+)\))?'
)

# Regex matching queries used by calc pipelines to hydrate association table rows.
# Example query (with newlines added for readability):
# SELECT state_incarceration_period.incarceration_period_id,
#   state_incarceration_period.source_supervision_violation_response_id
# FROM `recidiviz-123.state.state_incarceration_period` state_incarceration_period
# JOIN (SELECT * FROM `recidiviz-123.state.state_supervision_violation_response`
#       WHERE state_code IN ('US_XX') AND person_id IN (12345)) state_supervision_violation_response
# ON state_supervision_violation_response.supervision_violation_response_id
#   = state_incarceration_period.source_supervision_violation_response_id
ASSOCIATION_TABLE_QUERY_REGEX = re.compile(
    r'SELECT ([a-z_]+\.[a-z_]+), ([a-z_]+\.[a-z_]+) '
    r'FROM `([a-z\d\-]+\.[a-z_]+)\.([a-z_]+)` ([a-z_]+) '
    r'JOIN \(SELECT \* FROM `([a-z\d\-]+\.[a-z_]+)\.([a-z_]+)` '
    r'WHERE state_code IN \(\'([\w\d]+)\'\)( AND ([a-z_]+) IN \(([\'\w\d ,]+)\))?\) ([a-z_]+) '
    r'ON ([a-z_]+\.[a-z_]+) = ([a-z_]+\.[a-z_]+)'
)


class FakeBigQueryAssertMatchers:
    """Functions to be used by Apache Beam testing `assert_that` functions to
    validate pipeline outputs."""

    @staticmethod
    def validate_metric_output(expected_metric_type: RecidivizMetricType):
        """Asserts that the pipeline produced the expected types of metrics, and that it produced aggregate metrics
        only for the expected aggregate metric types."""
        def _validate_metric_output(output) -> None:
            if not output:
                raise BeamAssertException(
                    f'Failed assert. Output is empty for expected type [{expected_metric_type}].')

            for metric in output:
                if not metric['metric_type'] == expected_metric_type.value:
                    raise BeamAssertException(
                        f'Failed assert. Output metric is not of type [{expected_metric_type}].')

        return _validate_metric_output


class FakeReadFromBigQuery(apache_beam.PTransform):
    """Creates a PCollection from the provided |table_values|."""

    def __init__(self, table_values: List[NormalizedDatabaseDict]):
        super().__init__()
        self._table_values = table_values

    def expand(self, input_or_inputs):
        return (input_or_inputs
                | "Loading table values" >>
                apache_beam.Create(self._table_values))


class FakeReadFromBigQueryFactory:
    """Factory class that vends fake constructors that can be used to mock the ReadFromBigQuery object."""
    def __init__(self,
                 data_dict_query_fn: DataDictQueryFn = None):
        self._data_dict_query_fn: DataDictQueryFn = \
            data_dict_query_fn if data_dict_query_fn else self._extractor_utils_data_dict_query_fn

    def create_fake_bq_source_constructor(
            self,
            expected_dataset: str,
            data_dict: DataTablesDict,
            unifying_id_field: str = 'person_id'
    ) -> Callable[[QueryStr], FakeReadFromBigQuery]:
        """Returns a constructors function that can mock the ReadFromBigQuery class and will return a
        FakeReadFromBigQuery instead.
        """

        data_dict_query_fn = self._data_dict_query_fn

        def _fake_bq_source_constructor(query: QueryStr) -> FakeReadFromBigQuery:
            table_values = data_dict_query_fn(expected_dataset, query, data_dict, unifying_id_field)
            return FakeReadFromBigQuery(table_values=table_values)
        return _fake_bq_source_constructor

    @staticmethod
    def _extractor_utils_data_dict_query_fn(expected_dataset: str,
                                            query: QueryStr,
                                            data_dict: DataTablesDict,
                                            unifying_id_field: str) -> List[NormalizedDatabaseDict]:
        """Default implementation of the fake query function, which parses, validates, and replicates the behavior of
        the provided query string, returning data out of the data_dict object."""
        if re.match(ASSOCIATION_TABLE_QUERY_REGEX, query):
            return FakeReadFromBigQueryFactory._do_fake_association_tables_query(
                data_dict, query, expected_dataset, unifying_id_field)

        if re.match(ENTITY_TABLE_QUERY_REGEX, query):
            return FakeReadFromBigQueryFactory._do_fake_entity_table_query(
                data_dict, query, expected_dataset, unifying_id_field)

        raise ValueError(f'Query string does not match known query format: {query}')

    @staticmethod
    def _do_fake_entity_table_query(data_dict: DataTablesDict,
                                    query: str,
                                    expected_dataset: str,
                                    expected_unifying_id_field: str) -> List[NormalizedDatabaseDict]:
        """Parses, validates, and replicates the behavior of the provided entity table query string, returning
        data out of the data_dict object.
        """
        match = re.match(ENTITY_TABLE_QUERY_REGEX, query)

        if not match:
            raise ValueError(f'Query does not match regex: {query}')

        dataset = match.group(1)

        if dataset != expected_dataset:
            raise ValueError(f'Found dataset {dataset} does not match expected dataset {expected_dataset}')

        table_name = match.group(2)
        if table_name not in data_dict:
            raise ValueError(f'Table {table_name} not in data dict')

        all_table_rows = data_dict[table_name]

        state_code_value = match.group(3)
        if not state_code_value:
            raise ValueError(f'Found no state_code in query [{query}]')

        filtered_rows = filter_results(
            table_name, all_table_rows, 'state_code', {state_code_value}, allow_none_values=False)

        unifying_id_field = match.group(5)
        unifying_id_field_filter_list_str = match.group(6)

        if unifying_id_field and unifying_id_field_filter_list_str:
            if unifying_id_field != expected_unifying_id_field:
                raise ValueError(
                    f'Expected value [{expected_unifying_id_field}] for unifying_id_field does not match: '
                    f'[{unifying_id_field}')

            filtered_rows = filter_results(table_name,
                                           filtered_rows,
                                           unifying_id_field,
                                           id_list_str_to_set(unifying_id_field_filter_list_str),
                                           allow_none_values=False)
        elif unifying_id_field or unifying_id_field_filter_list_str:
            raise ValueError('Found one of unifying_id_field, unifying_id_field_filter_list_str is None, but not both.')

        return filtered_rows

    @staticmethod
    def _do_fake_association_tables_query(data_dict: DataTablesDict,
                                          query: str,
                                          expected_dataset: str,
                                          expected_unifying_id_field: str) -> List[NormalizedDatabaseDict]:
        """Parses, validates, and replicates the behavior of the provided association table query string, returning
        data out of the data_dict object.
        """
        match = re.match(ASSOCIATION_TABLE_QUERY_REGEX, query)
        if not match:
            raise ValueError(f'Query does not match regex: {query}')

        association_table_alias_1, association_table_parent_id = match.group(1).split('.')
        association_table_alias_2, association_table_entity_id = match.group(2).split('.')
        dataset_1 = match.group(3)
        association_table_name = match.group(4)
        association_table_alias_3 = match.group(5)
        dataset_2 = match.group(6)
        entity_table_name = match.group(7)
        entity_table_alias_1 = match.group(12)
        entity_table_alias_2, entity_table_join_column = match.group(13).split('.')
        association_table_alias_4, association_table_join_column = match.group(14).split('.')

        all_association_table_aliases = {association_table_alias_1,
                                         association_table_alias_2,
                                         association_table_alias_3,
                                         association_table_alias_4}
        if len(all_association_table_aliases) != 1:
            raise ValueError(f'Unexpected multiple association table aliases: {all_association_table_aliases}')

        all_entity_table_aliases = {entity_table_alias_1,
                                    entity_table_alias_2}
        if len(all_association_table_aliases) != 1:
            raise ValueError(f'Unexpected multiple entity table aliases: {all_entity_table_aliases}')

        dataset = one({dataset_1, dataset_2})
        if dataset != expected_dataset:
            raise ValueError(f'Found dataset {dataset} does not match expected dataset {expected_dataset}')

        if association_table_name not in data_dict:
            raise ValueError(f'Table {association_table_name} not in data dict')

        check_field_exists_in_table(association_table_name, association_table_parent_id)
        check_field_exists_in_table(association_table_name, association_table_entity_id)
        check_field_exists_in_table(association_table_name, association_table_join_column)
        check_field_exists_in_table(entity_table_name, entity_table_join_column)

        all_entity_table_rows = data_dict[entity_table_name]
        state_code_value = match.group(8)
        if not state_code_value:
            raise ValueError(f'Found no state_code in query [{query}]')

        filtered_enity_rows = filter_results(
            entity_table_name, all_entity_table_rows, 'state_code', {state_code_value}, allow_none_values=False)
        unifying_id_field = match.group(10)
        unifying_id_field_filter_list_str = match.group(11)

        if unifying_id_field and unifying_id_field_filter_list_str:
            if unifying_id_field != expected_unifying_id_field:
                raise ValueError(
                    f'Expected value [{expected_unifying_id_field}] for unifying_id_field does not match: '
                    f'[{unifying_id_field}')

            filtered_enity_rows = filter_results(entity_table_name,
                                                 filtered_enity_rows,
                                                 unifying_id_field,
                                                 id_list_str_to_set(unifying_id_field_filter_list_str),
                                                 allow_none_values=False)
        elif unifying_id_field or unifying_id_field_filter_list_str:
            raise ValueError('Found one of unifying_id_field, unifying_id_field_filter_list_str is None, but not both.')

        valid_entity_join_ids = {row[entity_table_join_column] for row in filtered_enity_rows}

        all_association_table_rows = data_dict[association_table_name]
        return filter_results(association_table_name,
                              all_association_table_rows,
                              association_table_join_column,
                              valid_entity_join_ids,
                              allow_none_values=True)


class FakeWriteToBigQuery(apache_beam.PTransform):
    """Fake PTransform that no-ops instead of writing to BQ."""
    def __init__(self, output_table: str, expected_output_metric_types: Collection[RecidivizMetricType]):
        super().__init__()
        metric_types_for_table = {metric_class(job_id='xxx', state_code='xxx').metric_type  # type: ignore[call-arg]
                                  for metric_class, table_id in DATAFLOW_METRICS_TO_TABLES.items()
                                  if table_id == output_table}

        self._expected_output_metric_types = expected_output_metric_types
        self._expected_metric_type = one(metric_types_for_table)

    def expand(self, input_or_inputs):
        if self._expected_metric_type in self._expected_output_metric_types:
            assert_that(input_or_inputs, FakeBigQueryAssertMatchers.validate_metric_output(self._expected_metric_type))
        else:
            assert_that(input_or_inputs, equal_to([]))

        return {}


FakeWriteToBigQueryType = TypeVar('FakeWriteToBigQueryType', bound=FakeWriteToBigQuery)


class FakeWriteToBigQueryFactory(Generic[FakeWriteToBigQueryType]):
    """Factory class that vends fake constructors that can be used to mock the FakeWriteToBigQuery object."""

    def __init__(self, fake_write_to_big_query_cls: Type[FakeWriteToBigQueryType]):
        self._fake_write_to_big_query_cls = fake_write_to_big_query_cls

    def create_fake_bq_sink_constructor(
            self,
            expected_dataset: str,
            expected_output_metric_types: Collection[RecidivizMetricType],
            **kwargs: Any
    ) -> Callable[[str, str], FakeWriteToBigQueryType]:
        def write_constructor(
            output_table: str,
            output_dataset: str,
        ) -> FakeWriteToBigQuery:
            if output_dataset != expected_dataset:
                raise ValueError(
                    f'Output dataset [{output_dataset}] does not match expected_dataset [{expected_dataset}] '
                    f'writing to table [{output_table}]')
            return self._fake_write_to_big_query_cls(  # type: ignore[call-arg]
                output_table, expected_output_metric_types, **kwargs)

        return write_constructor


def id_list_str_to_set(id_list_str: str) -> Set[int]:
    return {int(filter_id) for filter_id in id_list_str.split(', ')}


def filter_results(table_name: str,
                   unfiltered_table_rows: List[NormalizedDatabaseDict],
                   filter_id_name: str,
                   filter_id_set: Union[Set[int], Set[str]],
                   allow_none_values: bool) -> List[NormalizedDatabaseDict]:

    check_field_exists_in_table(table_name, filter_id_name)

    for row in unfiltered_table_rows:
        if filter_id_name not in row:
            raise ValueError(f'Field [{filter_id_name}] not in row for table [{table_name}]: {row}')
        if not allow_none_values and row[filter_id_name] is None:
            raise ValueError(f'Field [{filter_id_name}] is None for row in [{table_name}]: {row}')

    return [row for row in unfiltered_table_rows if row[filter_id_name] in filter_id_set]


def check_field_exists_in_table(table_name: str, field_name: str) -> None:
    if table_name in {
        # These are tables read by pipelines that are not in the sqlalchemy schema - skip this check
        'supervision_period_to_agent_association',
        'supervision_period_judicial_district_association',
        'state_race_ethnicity_population_counts',
        'us_mo_sentence_statuses',
        'persons_to_recent_county_of_residence',
        'incarceration_period_judicial_district_association',
        'state_race_ethnicity_population_counts',
    }:
        return

    matching_tables = {table for table in get_state_table_classes() if table.name == table_name}
    if not matching_tables:
        raise ValueError(f'No valid table with name: [{table_name}]')

    table = one(matching_tables)

    column_names = {sqlalchemy_column.name for sqlalchemy_column in table.columns}

    if field_name not in column_names:
        raise ValueError(f'Column {field_name} does not exist in table {table_name}.')
