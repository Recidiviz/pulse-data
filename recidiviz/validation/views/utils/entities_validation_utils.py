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
"""Utils for writing validations for the normalization pipelines."""
from typing import List, Type

from recidiviz.persistence.entity import entity_utils
from recidiviz.persistence.entity.base_entity import Entity
from recidiviz.persistence.entity.entities_bq_schema import (
    get_bq_schema_for_entity_table,
)
from recidiviz.persistence.entity.state import normalized_entities
from recidiviz.persistence.entity.state.normalized_state_entity import (
    NormalizedStateEntity,
)
from recidiviz.utils.string import StrictStringFormatter
from recidiviz.utils.types import assert_subclass

SELECT_FROM_NORMALIZED_ENTITY_TABLE_TEMPLATE = (
    "(SELECT state_code, state_code AS region_code, {id_column}, {columns} "
    "FROM `{{project_id}}.{{normalized_state_dataset}}.{table_id}` "
    "{invalid_rows_filter_clause})"
)

PRIMARY_KEYS_UNIQUE_ACROSS_ALL_STATES_QUERY_TEMPLATE = """
SELECT
    'ALL' AS state_code,
    'ALL' AS region_code,
    '{table_id}' AS entity_name,
    COUNT(*) as total_count,
    COUNT(DISTINCT({id_column})) as distinct_id_count
FROM `{{project_id}}.normalized_state.{table_id}`
GROUP BY 1, 2
"""


def unique_primary_keys_values_across_all_states_query() -> str:
    """Builds a query to identify when entities in both the state and normalized_state
    datasets have unique primary keys."""
    entity_sub_queries: List[str] = []

    # Sort classes by name to produce a deterministic query string
    for entity_cls in sorted(
        entity_utils.get_all_entity_classes_in_module(normalized_entities),
        key=lambda cls: cls.__name__,
    ):
        table_id = entity_cls.get_table_id()
        id_column = entity_cls.get_class_id_name()

        entity_sub_queries.append(
            StrictStringFormatter().format(
                PRIMARY_KEYS_UNIQUE_ACROSS_ALL_STATES_QUERY_TEMPLATE,
                table_id=table_id,
                id_column=id_column,
            )
        )

    return "\nUNION ALL\n".join(entity_sub_queries)


def _validate_normalized_entity_has_all_fields(
    normalized_entity: Type[NormalizedStateEntity], fields_to_validate: List[str]
) -> None:
    """Asserts that the given |normalized_entity| class contains all of the fields in
    |fields_to_validate|.

    Raises an error if the class does not contain all of the fields.
    """
    schema_field_names = [
        field.name
        for field in get_bq_schema_for_entity_table(
            normalized_entities,
            assert_subclass(normalized_entity, Entity).get_table_id(),
        )
    ]

    for field in fields_to_validate:
        if field not in schema_field_names:
            raise ValueError(
                f"The {normalized_entity.__name__} does not contain metric field: {field}."
            )


def validation_query_for_normalized_entity(
    normalized_entity_class: Type[NormalizedStateEntity],
    additional_columns_to_select: List[str],
    invalid_rows_filter_clause: str,
    validation_description: str,
) -> str:
    """Builds a validation query for the table associated with the given
    |normalized_entity_class|."""
    if not invalid_rows_filter_clause.startswith("WHERE"):
        raise ValueError(
            "Invalid filter clause. Must start with 'WHERE'. "
            f"Found: {invalid_rows_filter_clause}."
        )

    _validate_normalized_entity_has_all_fields(
        normalized_entity_class, additional_columns_to_select
    )

    table_id = assert_subclass(normalized_entity_class, Entity).get_table_id()
    id_column = assert_subclass(normalized_entity_class, Entity).get_class_id_name()

    query = StrictStringFormatter().format(
        SELECT_FROM_NORMALIZED_ENTITY_TABLE_TEMPLATE,
        id_column=id_column,
        columns=", ".join(additional_columns_to_select),
        table_id=table_id,
        invalid_rows_filter_clause=invalid_rows_filter_clause,
    )

    return f"/*{validation_description}*/\n\n{query}"
