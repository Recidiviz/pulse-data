# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2026 Recidiviz, Inc.
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
"""A validation check that generates an error if extraction produced two
entities with the same values. These should have been merged into a single
entity.
"""
from collections import defaultdict
from typing import Any

from recidiviz.documents.extraction.models.llm_request_output_schema_field_names import (
    ENTITIES_FIELD_NAME,
)
from recidiviz.documents.extraction.models.llm_request_output_values import (
    LLMRequestOutputValues,
)
from recidiviz.documents.extraction.validation.llm_document_validation_result import (
    ValidationCheckType,
    ValidationIssue,
)


class DuplicateEntityCheck:
    """A validation check that generates an error if extraction produced two
    entities with the same values. These should have been merged into a single
    entity.
    """

    @classmethod
    def issues(cls, *, output: LLMRequestOutputValues) -> list[ValidationIssue]:
        """Returns one `ValidationIssue` per group of entities in |output|
        sharing identical canonical entity-field values, or an empty list when
        every emitted entity is distinct.
        """
        entity_field_names = output.output_schema.entities_field.primary_keys
        entity_indexes_by_values: dict[tuple[Any, ...], list[int]] = defaultdict(list)
        for index, entity in enumerate(output.resolved_entities()):
            values = tuple(entity[field_name] for field_name in entity_field_names)
            entity_indexes_by_values[values].append(index)
        return [
            cls._issue(
                entity_field_values=dict(zip(entity_field_names, values)),
                entity_indexes=indexes,
            )
            for values, indexes in entity_indexes_by_values.items()
            if len(indexes) > 1
        ]

    @staticmethod
    def _issue(
        *, entity_field_values: dict[str, Any], entity_indexes: list[int]
    ) -> ValidationIssue:
        """Returns a duplicate-entity finding for the entities at
        |entity_indexes|, all of which share |entity_field_values|.
        """
        duplicated_elements = ", ".join(
            f"{ENTITIES_FIELD_NAME}[{index}]" for index in entity_indexes
        )
        return ValidationIssue(
            check_type=ValidationCheckType.DUPLICATE_ENTITY,
            field_name=ENTITIES_FIELD_NAME,
            detail=(
                f"Entities {duplicated_elements} have identical entity-field "
                f"values {entity_field_values}; entities with identical "
                f"entity-field values are the same entity and must be emitted "
                f"as one element."
            ),
        )
