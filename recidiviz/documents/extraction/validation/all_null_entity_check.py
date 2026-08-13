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
"""Defines a validation check that generates an error for any entity that has
only null fields.
"""
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


class AllNullEntityCheck:
    """A validation check that generates an error for any entity that has only
    null fields.

    If every entity field is null, the entity is identified by nothing and
    describes nothing so there is no real-world entity behind it to enrich or
    trace mentions to.
    """

    @classmethod
    def issues(cls, *, output: LLMRequestOutputValues) -> list[ValidationIssue]:
        """Returns one `ValidationIssue` per entity in |output| whose every
        entity-field value is null, or an empty list when every entity carries
        at least one.
        """
        entity_field_names = output.output_schema.entities_field.primary_keys
        return [
            ValidationIssue(
                check_type=ValidationCheckType.ALL_NULL_ENTITY,
                field_name=f"{ENTITIES_FIELD_NAME}[{index}]",
                detail=(
                    f"Entity carries a null for every entity field "
                    f"({entity_field_names}); an entity identified by no field "
                    f"value describes nothing."
                ),
            )
            for index, entity in enumerate(output.resolved_entities())
            if all(entity[field_name] is None for field_name in entity_field_names)
        ]
