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

from recidiviz.ingest.direct.regions.us_id.us_id_custom_enum_parsers import (
    TEXT_ANALYZER,
    _match_note_title,
)
from recidiviz.ingest.direct.regions.us_id.us_id_text_analysis_configuration import (
    UsIdTextEntity,
)


@functools.lru_cache(maxsize=128)
def is_program_start(agnt_note_title: str) -> bool:
    """Returns whether a program is started by looking for treatment but not completion
    in the matched entities."""
    matched_entities = _match_note_title(agnt_note_title)
    return (
        UsIdTextEntity.ANY_TREATMENT in matched_entities
        and UsIdTextEntity.TREATMENT_COMPLETE not in matched_entities
    )


@functools.lru_cache(maxsize=128)
def is_program_discharge(agnt_note_title: str) -> bool:
    """Returns whether treatment completion is the matched entity."""
    matched_entities = _match_note_title(agnt_note_title)
    return (
        UsIdTextEntity.ANY_TREATMENT in matched_entities
        and UsIdTextEntity.TREATMENT_COMPLETE in matched_entities
    )


@functools.lru_cache(maxsize=128)
def cleaned_agnt_note_title(agnt_note_title: str) -> str:
    return TEXT_ANALYZER.normalize_text(agnt_note_title)
