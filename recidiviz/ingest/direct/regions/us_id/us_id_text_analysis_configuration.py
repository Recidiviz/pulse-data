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
from thefuzz import fuzz

from recidiviz.common.text_analysis import (
    RegexFuzzyMatcher,
    ScoringFuzzyMatcher,
    TextEntity,
)


class UsIdTextEntity(TextEntity):
    """Flags for indicators based on free text matching for US_ID."""

    # WARNING: IF YOU EDIT THESE VALUES, YOU MUST DO AN INGEST RERUN FOR AFFECTED REGIONS
    VIOLATION = [
        RegexFuzzyMatcher(search_regex=".*violat.*"),
        RegexFuzzyMatcher(search_regex=".*voilat.*"),
        ScoringFuzzyMatcher(search_term="pv"),
        ScoringFuzzyMatcher(search_term="rov"),
        ScoringFuzzyMatcher(search_term="report of violation"),
    ]
    ABSCONSION = [
        ScoringFuzzyMatcher(
            search_term="abscond", matching_function=fuzz.partial_ratio
        ),
        ScoringFuzzyMatcher(search_term="absconsion"),
    ]
    IN_CUSTODY = [
        ScoringFuzzyMatcher(search_term="in custody"),
        ScoringFuzzyMatcher(search_term="arrest", matching_function=fuzz.partial_ratio),
    ]
    AGENTS_WARNING = [
        ScoringFuzzyMatcher(search_term="aw"),
        ScoringFuzzyMatcher(search_term="agents warrant"),
        ScoringFuzzyMatcher(search_term="cw"),
        ScoringFuzzyMatcher(search_term="bw"),
        ScoringFuzzyMatcher(search_term="commission warrant"),
        ScoringFuzzyMatcher(search_term="bench warrant"),
        ScoringFuzzyMatcher(search_term="warrant"),
    ]
    REVOCATION = [
        RegexFuzzyMatcher(search_regex=".*revok.*"),
        RegexFuzzyMatcher(search_regex=".*revoc.*"),
        ScoringFuzzyMatcher(search_term="rx"),
    ]
    REVOCATION_INCLUDE = [
        ScoringFuzzyMatcher(search_term="internet"),
        ScoringFuzzyMatcher(search_term="minor consent form"),
        ScoringFuzzyMatcher(search_term="relationship app"),
    ]
    NEW_INVESTIGATION = [
        ScoringFuzzyMatcher(search_term="psi"),
        ScoringFuzzyMatcher(search_term="file_review"),
        ScoringFuzzyMatcher(search_term="activation"),
    ]
    ANY_TREATMENT = [
        ScoringFuzzyMatcher(search_term="tx"),
        ScoringFuzzyMatcher(search_term="treatment"),
    ]
    TREATMENT_COMPLETE = [RegexFuzzyMatcher(search_regex=".*complet.*")]
