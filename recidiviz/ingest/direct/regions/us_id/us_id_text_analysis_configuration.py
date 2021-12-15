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
"""Ingest-level text matching utilities for US_ID.

Note: If new flags or new fuzzy matchers are added, you must do an ingest rerun."""
from recidiviz.common.text_analysis import (
    RegexFuzzyMatcher,
    ScoringFuzzyMatcher,
    TextEntity,
)


class UsIdTextEntity(TextEntity):
    """Flags for indicators based on free text matching for US_ID."""

    # WARNING: IF YOU EDIT THESE VALUES, YOU MUST DO AN INGEST RERUN FOR AFFECTED REGIONS
    ANY_TREATMENT = [
        ScoringFuzzyMatcher(search_term="tx"),
        ScoringFuzzyMatcher(search_term="treatment"),
    ]
    TREATMENT_COMPLETE = [RegexFuzzyMatcher(search_regex=".*complet.*")]
