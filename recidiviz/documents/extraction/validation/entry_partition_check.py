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
"""A validation check that generates an error if either:
a) An entry number shows up in multiple entities' `entry_nums` set
OR
b) An entry number is not present in any entity's `entry_nums` set

In other words, every entry must be associated with exactly one entity.
"""
from collections import Counter

from recidiviz.documents.extraction.models.llm_request_output_schema_field_names import (
    ENTITIES_FIELD_NAME,
    ENTRY_NUMS_FIELD_NAME,
)
from recidiviz.documents.extraction.models.llm_request_output_values import (
    LLMRequestOutputValues,
)
from recidiviz.documents.extraction.validation.llm_document_validation_result import (
    ValidationCheckType,
    ValidationIssue,
)


class EntryPartitionCheck:
    """A validation check that generates an error if either:
    a) An entry number shows up in multiple entities' `entry_nums` set
    OR
    b) An entry number is not present in any entity's `entry_nums` set

    In other words, every entry must be associated with exactly one entity.
    """

    @classmethod
    def issues(
        cls, *, output: LLMRequestOutputValues, expected_entry_nums: set[int]
    ) -> list[ValidationIssue]:
        """Returns one `ValidationIssue` per partition failure mode |output|'s
        entities exhibit against |expected_entry_nums| — dropped, invented, and
        duplicated entries — or an empty list for an exact partition.
        """
        # Count how many times each entry number is referenced by an entity
        assigned_entry_counts = Counter(
            entry_num
            for entity in output.resolved_entities()
            for entry_num in entity[ENTRY_NUMS_FIELD_NAME]
        )
        issues = []
        # Check for expected entries not resolved by an entity
        if dropped := sorted(expected_entry_nums - assigned_entry_counts.keys()):
            issues.append(
                cls._issue(
                    f"Entries {dropped} of the composite document are assigned "
                    f"to no entity; every entry must be assigned to exactly one."
                )
            )
        # Check for entities claiming to resolve entries that don't exist
        if invented := sorted(assigned_entry_counts.keys() - expected_entry_nums):
            issues.append(
                cls._issue(
                    f"Entities claim entry numbers {invented}, which match no "
                    f"entry of the composite document."
                )
            )
        # Check for entries assigned to more than one entity
        if duplicated := sorted(
            entry_num for entry_num, count in assigned_entry_counts.items() if count > 1
        ):
            issues.append(
                cls._issue(
                    f"Entries {duplicated} are each assigned more than once "
                    f"across the emitted entities; every entry must be assigned "
                    f"to exactly one."
                )
            )
        return issues

    @staticmethod
    def _issue(detail: str) -> ValidationIssue:
        """Returns a partition finding carrying |detail|, named against the
        `entities` field whose `entry_nums` collectively violate the partition.
        """
        return ValidationIssue(
            check_type=ValidationCheckType.ENTRY_PARTITION_VIOLATION,
            field_name=ENTITIES_FIELD_NAME,
            detail=detail,
        )
