# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2021 Recidiviz, Inc.
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
"""Custom parsers functions for US_ID. Can be referenced in an ingest view manifest
like this:

my_flat_field:
    $custom:
        $function: us_id_custom_parsers.<function name>
        $args:
            arg_1: <expression>
            arg_2: <expression>
"""
import functools
from typing import Optional, Set

from recidiviz.common.str_field_utils import (
    parse_date,
    safe_parse_days_from_duration_pieces,
)
from recidiviz.common.text_analysis import (
    TextAnalyzer,
    TextEntity,
    TextMatchingConfiguration,
)
from recidiviz.ingest.direct.regions.us_id.us_id_text_analysis_configuration import (
    UsIdTextEntity,
)

TEXT_ANALYZER = TextAnalyzer(
    configuration=TextMatchingConfiguration(
        stop_words_to_remove={"in", "out"},
        text_entities=list(UsIdTextEntity),
    )
)


@functools.lru_cache(maxsize=128)
def records_program_start(agnt_note_title: str) -> bool:
    """Returns whether a program is started by looking for treatment but not completion
    in the matched entities."""
    matched_entities = _match_note_title(agnt_note_title)
    return (
        UsIdTextEntity.ANY_TREATMENT in matched_entities
        and UsIdTextEntity.TREATMENT_COMPLETE not in matched_entities
    )


@functools.lru_cache(maxsize=128)
def records_program_discharge(agnt_note_title: str) -> bool:
    """Returns whether treatment completion is the matched entity."""
    matched_entities = _match_note_title(agnt_note_title)
    return (
        UsIdTextEntity.ANY_TREATMENT in matched_entities
        and UsIdTextEntity.TREATMENT_COMPLETE in matched_entities
    )


@functools.lru_cache(maxsize=128)
def records_violation(agnt_note_title: str, is_in_secondary: bool) -> bool:
    """Returns whether the agnt_note_title indicates a violation should be ingested."""
    matched_entities = _match_note_title(agnt_note_title)
    # TODO(#14891) Replace the overall logic once reruns complete
    if is_in_secondary:
        return (
            (
                UsIdTextEntity.VIOLATION in matched_entities
                or UsIdTextEntity.AGENTS_WARNING in matched_entities
                or UsIdTextEntity.REVOCATION in matched_entities
                or UsIdTextEntity.ABSCONSION_V2 in matched_entities
            )
            and UsIdTextEntity.REVOCATION_INCLUDE not in matched_entities
            and UsIdTextEntity.WARRANT_QUASHED not in matched_entities
        )
    return (
        UsIdTextEntity.VIOLATION in matched_entities
        or UsIdTextEntity.AGENTS_WARNING in matched_entities
        or UsIdTextEntity.REVOCATION in matched_entities
        or UsIdTextEntity.ABSCONSION in matched_entities
    ) and UsIdTextEntity.REVOCATION_INCLUDE not in matched_entities


@functools.lru_cache(maxsize=128)
def records_violation_response_decision(
    agnt_note_title: str, is_in_secondary: bool
) -> bool:
    """Returns whether the agnt_note_title indicates a violation response decision
    should be ingested."""
    matched_entities = _match_note_title(agnt_note_title)
    # TODO(#14891) Replace the overall logic once reruns complete
    if is_in_secondary:
        return (
            (
                UsIdTextEntity.AGENTS_WARNING in matched_entities
                or UsIdTextEntity.REVOCATION in matched_entities
            )
            and UsIdTextEntity.REVOCATION_INCLUDE not in matched_entities
            and UsIdTextEntity.WARRANT_QUASHED not in matched_entities
        )
    return (
        UsIdTextEntity.AGENTS_WARNING in matched_entities
        or UsIdTextEntity.REVOCATION in matched_entities
    ) and UsIdTextEntity.REVOCATION_INCLUDE not in matched_entities


@functools.lru_cache(maxsize=128)
def records_absconsion(agnt_note_title: str, is_in_secondary: bool) -> bool:
    """Returns whether the agnt_note_title indicates an absconsion."""
    matched_entities = _match_note_title(agnt_note_title)
    # TODO(#14891) Replace overall logic once reruns complete
    if is_in_secondary:
        return UsIdTextEntity.ABSCONSION_V2 in matched_entities
    return UsIdTextEntity.ABSCONSION in matched_entities


@functools.lru_cache(maxsize=128)
def records_temporary_custody_admission(agnt_note_title: str) -> bool:
    """Returns whether the agnt_note_title indicates an temporary custody admission should be
    ingested."""
    matched_entities = _match_note_title(agnt_note_title)
    return UsIdTextEntity.IN_CUSTODY in matched_entities


@functools.lru_cache(maxsize=128)
def records_new_investigation_period(agnt_note_title: str) -> bool:
    """Returns whether the agnt_note_title indicates a new investigation period should be
    ingested."""
    matched_entities = _match_note_title(agnt_note_title)
    return UsIdTextEntity.NEW_INVESTIGATION in matched_entities


@functools.lru_cache(maxsize=128)
def _match_note_title(agnt_note_title: str) -> Set[TextEntity]:
    """Returns the entities that the agnt_note_title matches to."""
    return TEXT_ANALYZER.extract_entities(agnt_note_title)


def parse_valid_offense_date(raw_date: str) -> Optional[str]:
    try:
        offense_date = parse_date(raw_date)
        if offense_date and 1900 <= offense_date.year <= 2100:
            return offense_date.isoformat()
        return None
    except ValueError:
        return None


def parse_duration_from_date_part_strings(
    years_str: str, months_str: str, days_str: str, start_dt_str: str
) -> Optional[str]:
    return str(
        safe_parse_days_from_duration_pieces(
            years_str, months_str, days_str, start_dt_str
        )
    )
