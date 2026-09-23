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
"""A validation check that generates an error for any ARRAY_OF_STRUCT entry
whose every user-defined field is null.
"""
from recidiviz.documents.extraction.models.llm_request_output_schema_field import (
    ArrayOfStructLLMRequestOutputSchemaField,
)
from recidiviz.documents.extraction.models.llm_request_output_schema_field_names import (
    ENTITY_ID_FIELD_NAME,
    ENTRY_NUMS_FIELD_NAME,
)
from recidiviz.documents.extraction.models.llm_request_output_values import (
    LLMRequestOutputValues,
)
from recidiviz.documents.extraction.validation.llm_document_validation_result import (
    ValidationCheckType,
    ValidationIssue,
)

_FRAMEWORK_SUB_FIELD_NAMES = frozenset({ENTITY_ID_FIELD_NAME, ENTRY_NUMS_FIELD_NAME})
"""Sub-fields the entity-resolution schema adds to every resolved entity. They
are always non-null, so they say nothing about whether the entity itself
carries a value.
"""


class AllNullArrayEntryCheck:
    """A validation check that generates an error for any ARRAY_OF_STRUCT entry
    whose every user-defined field is null.

    An entry identified by no value describes nothing, so nothing downstream can
    use it. An entry with any one non-null field passes: an employer entry that
    names only a pay rate still reports real information. For a resolved entity,
    the check reads only the entity fields, since entity_id and entry_nums are
    non-null on every entity.
    """

    @classmethod
    def issues(cls, *, output: LLMRequestOutputValues) -> list[ValidationIssue]:
        """Returns one `ValidationIssue` per entry of every ARRAY_OF_STRUCT field
        in |output| whose every user-defined sub-field is null, or an empty list
        when every entry carries at least one value.
        """
        return [
            issue
            for field in output.output_schema.array_of_struct_user_fields
            for issue in cls._field_issues(output=output, field=field)
        ]

    @classmethod
    def _field_issues(
        cls,
        *,
        output: LLMRequestOutputValues,
        field: ArrayOfStructLLMRequestOutputSchemaField,
    ) -> list[ValidationIssue]:
        """Returns one `ValidationIssue` per all-null entry of |field|."""
        value_field_names = [
            sub_field.name
            for sub_field in field.fields
            if sub_field.name not in _FRAMEWORK_SUB_FIELD_NAMES
        ]
        return [
            ValidationIssue(
                check_type=ValidationCheckType.ALL_NULL_ARRAY_ENTRY,
                field_name=f"{field.name}[{index}]",
                detail=(
                    f"Entry carries a null for every field ({value_field_names}); "
                    f"an entry identified by no field value describes nothing."
                ),
            )
            for index, entry in enumerate(output.array_elements(field=field))
            if all(entry[field_name] is None for field_name in value_field_names)
        ]
