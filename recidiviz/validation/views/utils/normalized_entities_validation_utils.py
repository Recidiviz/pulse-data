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
from typing import List, Tuple, Type

from recidiviz.calculator.pipeline.normalization.utils.normalized_entities import (
    NormalizedStateEntity,
)
from recidiviz.calculator.pipeline.normalization.utils.normalized_entities_utils import (
    NORMALIZED_ENTITY_CLASSES,
)
from recidiviz.calculator.pipeline.normalization.utils.normalized_entity_conversion_utils import (
    bq_schema_for_normalized_state_entity,
)
from recidiviz.persistence.database import schema_utils
from recidiviz.persistence.entity import entity_utils
from recidiviz.persistence.entity.state import entities as state_entities
from recidiviz.utils.string import StrictStringFormatter

UNIQUE_IDS_TEMPLATE = """
SELECT
    state_code as region_code,
    '{table_id}' as entity_name,
    COUNT(*) as total_count,
    COUNT(DISTINCT({id_column})) as distinct_id_count
FROM
    `{{project_id}}.{{normalized_state_dataset}}.{table_id}`
GROUP BY 1
"""

SELECT_FROM_ENTITY_TABLE_TEMPLATE = (
    "(SELECT state_code AS region_code, {id_column}, {columns} "
    "FROM `{{project_id}}.{{normalized_state_dataset}}.{table_id}` "
    "{invalid_rows_filter_clause})"
)


def _table_id_and_id_column_for_normalized_state_entity(
    normalized_entity_class: Type[NormalizedStateEntity],
) -> Tuple[str, str]:
    base_class_name = normalized_entity_class.base_class_name()
    base_schema_class = schema_utils.get_state_database_entity_with_name(
        base_class_name
    )
    base_entity_class = entity_utils.get_entity_class_in_module_with_name(
        entities_module=state_entities, class_name=base_class_name
    )
    return base_schema_class.__tablename__, base_entity_class.get_class_id_name()


def unique_entity_id_values_query() -> str:
    """Builds a query to identify when entity normalization pipelines are producing
    entities with duplicate ID values."""
    entity_sub_queries: List[str] = []

    for entity_cls in NORMALIZED_ENTITY_CLASSES:
        table_id, id_column = _table_id_and_id_column_for_normalized_state_entity(
            entity_cls
        )

        entity_sub_queries.append(
            StrictStringFormatter().format(
                UNIQUE_IDS_TEMPLATE,
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
        field.name for field in bq_schema_for_normalized_state_entity(normalized_entity)
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

    table_id, id_column = _table_id_and_id_column_for_normalized_state_entity(
        normalized_entity_class
    )

    query = StrictStringFormatter().format(
        SELECT_FROM_ENTITY_TABLE_TEMPLATE,
        id_column=id_column,
        columns=", ".join(additional_columns_to_select),
        table_id=table_id,
        invalid_rows_filter_clause=invalid_rows_filter_clause,
    )

    return f"/*{validation_description}*/\n\n{query}"
