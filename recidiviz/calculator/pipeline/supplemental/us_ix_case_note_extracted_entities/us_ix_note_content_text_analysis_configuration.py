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
"""Calc-level text matching utilities for US_IX for note content"""
from enum import auto

from recidiviz.common.text_analysis import (
    TextAnalyzer,
    TextEntity,
    TextMatchingConfiguration,
)


class UsIxNoteContentTextEntity(TextEntity):
    """Flags for indicators based on free note content matching for US_IX."""

    VIOLATION = auto()
    SANCTION = auto()
    EXTEND = auto()
    ABSCONSION = auto()
    IN_CUSTODY = auto()
    AGENTS_WARNING = auto()
    REVOCATION = auto()
    REVOCATION_INCLUDE = auto()
    OTHER = auto()
    NEW_INVESTIGATION = auto()
    PSI = auto()
    NEW_CRIME = auto()
    ANY_TREATMENT = auto()
    TREATMENT_COMPLETE = auto()
    INTERLOCK = auto()
    CASE_PLAN = auto()
    NCIC_ILETS_NCO_CHECK = auto()
    COMMUNITY_SERVICE = auto()
    NOT_CS = auto()
    TRANSFER_CHRONO = auto()
    LSU = auto()
    DUI = auto()
    NOT_M_DUI = auto()
    SPECIALTY_COURT = auto()
    COURT = auto()
    SSDI_SSI = auto()
    PENDING = auto()
    WAIVER = auto()
    UA = auto()


NOTE_CONTENT_TEXT_ANALYZER = TextAnalyzer(
    TextMatchingConfiguration(text_entities=list(UsIxNoteContentTextEntity))
)

if __name__ == "__main__":
    NOTE_CONTENT_TEXT_ANALYZER.run_and_print()
