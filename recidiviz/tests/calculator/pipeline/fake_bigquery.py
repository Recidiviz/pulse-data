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
from typing import Dict, Callable, List, Set

import apache_beam
from more_itertools import one

from recidiviz.persistence.database.schema_utils import get_state_table_classes
from recidiviz.tests.calculator.calculator_test_utils import NormalizedDatabaseDict

DatasetStr = str
QueryStr = str
DataTablesDict = Dict[str, List[NormalizedDatabaseDict]]
DataDictQueryFn = Callable[[DatasetStr, QueryStr, DataTablesDict, str], List[NormalizedDatabaseDict]]

ENTITY_TABLE_QUERY_REGEX = re.compile(
    r'SELECT \* FROM `([a-z\d\-.]+)\.([a-z_]+)`'
    r'( WHERE ([a-z_]+) IN \(([\'\w\d ,]+)\))?'
)

ASSOCIATION_TABLE_QUERY_REGEX = re.compile(
    r'SELECT ([a-z_]+\.[a-z_]+), ([a-z_]+\.[a-z_]+) '
    r'FROM `([a-z\d\-.]+)\.([a-z_]+)` ([a-z_]+) '
    r'JOIN \(SELECT \* FROM `([a-z\d\-.]+)\.([a-z_]+)`( WHERE ([a-z_]+) IN \(([\d ,]+)\))?\) ([a-z_]+) '
    r'ON ([a-z_]+\.[a-z_]+) = ([a-z_]+\.[a-z_]+)'
)


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
                                    unifying_id_field: str) -> List[NormalizedDatabaseDict]:
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

        filter_field = match.group(4)
        filter_field_list_str = match.group(5)
        if filter_field and filter_field_list_str:
            if filter_field == 'state_code':
                filter_field_list_value = filter_field_list_str.replace("\'", "")

                matching_entities: List[NormalizedDatabaseDict] = []
                for entity_dict in data_dict[table_name]:
                    if filter_field not in entity_dict.keys() or entity_dict.get(filter_field) is None:
                        raise ValueError(
                            f'Expected {filter_field} to be a set field in {table_name}.'
                        )
                    if entity_dict.get(filter_field) == filter_field_list_value:
                        matching_entities.append(entity_dict)

                return matching_entities
            if unifying_id_field != filter_field:
                raise ValueError(
                    f'Expected unifying_id_field {unifying_id_field} to equal the filter_id_name {filter_field}')

            return filter_results(data_dict, table_name, filter_field, id_list_str_to_set(filter_field_list_str))

        if filter_field or filter_field_list_str:
            raise ValueError('Found one of filter_id_name, filter_id_list_str is None, but not both.')

        return data_dict[table_name]

    @staticmethod
    def _do_fake_association_tables_query(data_dict: DataTablesDict,
                                          query: str,
                                          expected_dataset: str,
                                          unifying_id_field: str) -> List[NormalizedDatabaseDict]:
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
        entity_table_alias_1 = match.group(11)
        entity_table_alias_2, entity_table_join_column = match.group(12).split('.')
        association_table_alias_4, association_table_join_column = match.group(13).split('.')

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

        filter_id_name = match.group(9)
        filter_id_list_str = match.group(10)
        if filter_id_name and filter_id_list_str:
            if unifying_id_field != filter_id_name:
                raise ValueError(
                    f'Expected unifying_id_field {unifying_id_field} to equal the filter_id_name {filter_id_name}')

            valid_entities = \
                filter_results(data_dict, entity_table_name, filter_id_name,
                               id_list_str_to_set(filter_id_list_str))
            valid_entity_join_ids = {row[entity_table_join_column] for row in valid_entities}

            return filter_results(data_dict,
                                  association_table_name,
                                  association_table_join_column,
                                  valid_entity_join_ids)

        if filter_id_name or filter_id_list_str:
            raise ValueError('Found one of filter_id_name, filter_id_list_str is None, but not both.')

        return data_dict[association_table_name]


def id_list_str_to_set(id_list_str: str) -> Set[int]:
    return {int(filter_id) for filter_id in id_list_str.split(', ')}


def filter_results(data_dict: DataTablesDict,
                   table_name: str,
                   filter_id_name: str,
                   filter_id_set: Set[int]) -> List[NormalizedDatabaseDict]:
    unfiltered_data = data_dict[table_name]

    check_field_exists_in_table(table_name, filter_id_name)

    return [row for row in unfiltered_data if row[filter_id_name] in filter_id_set]


def check_field_exists_in_table(table_name: str, field_name: str) -> None:
    table = one({table for table in get_state_table_classes() if table.name == table_name})

    column_names = {sqlalchemy_column.name for sqlalchemy_column in table.columns}

    if field_name not in column_names:
        raise ValueError(f'Column {field_name} does not exist in table {table_name}.')
