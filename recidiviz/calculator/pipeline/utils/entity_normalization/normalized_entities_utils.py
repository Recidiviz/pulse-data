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
"""Utils for working with NormalizedStateEntity objects."""
from typing import List, Set, Type

import attr
from google.cloud import bigquery

from recidiviz.big_query.big_query_client import BigQueryClientImpl
from recidiviz.big_query.big_query_utils import schema_field_for_attribute
from recidiviz.calculator.pipeline.utils.entity_normalization.normalized_entities import (
    NormalizedStateEntity,
    NormalizedStateIncarcerationPeriod,
    NormalizedStateProgramAssignment,
    NormalizedStateSupervisionCaseTypeEntry,
    NormalizedStateSupervisionPeriod,
    NormalizedStateSupervisionViolatedConditionEntry,
    NormalizedStateSupervisionViolation,
    NormalizedStateSupervisionViolationResponse,
    NormalizedStateSupervisionViolationResponseDecisionEntry,
    NormalizedStateSupervisionViolationTypeEntry,
)
from recidiviz.common.attr_mixins import BuildableAttr
from recidiviz.common.attr_utils import is_flat_field
from recidiviz.persistence.database import schema_utils
from recidiviz.persistence.database.schema_utils import (
    get_state_table_classes,
    get_table_class_by_name,
)

# All entity classes that have Normalized versions
NORMALIZED_ENTITY_CLASSES: List[Type[NormalizedStateEntity]] = [
    NormalizedStateIncarcerationPeriod,
    NormalizedStateProgramAssignment,
    NormalizedStateSupervisionPeriod,
    NormalizedStateSupervisionCaseTypeEntry,
    NormalizedStateSupervisionViolationResponse,
    NormalizedStateSupervisionViolationResponseDecisionEntry,
    NormalizedStateSupervisionViolation,
    NormalizedStateSupervisionViolationTypeEntry,
    NormalizedStateSupervisionViolatedConditionEntry,
]


def fields_unique_to_normalized_class(
    entity_cls: Type[NormalizedStateEntity],
) -> Set[str]:
    """Returns the names of the fields that are unique to the NormalizedStateEntity
    and are not on the base state Entity class."""
    normalized_class_fields_dict = attr.fields_dict(entity_cls)
    base_class: Type[BuildableAttr] = entity_cls.__base__
    base_class_fields_dict = attr.fields_dict(base_class)

    return set(normalized_class_fields_dict.keys()).difference(
        set(base_class_fields_dict.keys())
    )


def bq_schema_for_normalized_state_entity(
    entity_cls: Type[NormalizedStateEntity],
) -> List[bigquery.SchemaField]:
    """Returns the necessary BigQuery schema for the NormalizedStateEntity, which is
    a list of SchemaField objects containing the column name and value type for
    each attribute on the NormalizedStateEntity."""
    unique_fields_on_normalized_class = fields_unique_to_normalized_class(entity_cls)

    schema_fields_for_additional_fields: List[bigquery.SchemaField] = []

    for field, attribute in attr.fields_dict(entity_cls).items():
        if field not in unique_fields_on_normalized_class:
            continue

        if not is_flat_field(attribute):
            raise ValueError(
                "Only flat fields are supported as additional fields on "
                f"NormalizedStateEntities. Found: {attribute} in field "
                f"{field}."
            )

        schema_fields_for_additional_fields.append(
            schema_field_for_attribute(field_name=field, attribute=attribute)
        )

    base_class: Type[BuildableAttr] = entity_cls.__base__
    base_schema_class = schema_utils.get_state_database_entity_with_name(
        base_class.__name__
    )

    return (
        BigQueryClientImpl.schema_for_sqlalchemy_table(
            get_table_class_by_name(
                base_schema_class.__tablename__, list(get_state_table_classes())
            )
        )
        + schema_fields_for_additional_fields
    )
