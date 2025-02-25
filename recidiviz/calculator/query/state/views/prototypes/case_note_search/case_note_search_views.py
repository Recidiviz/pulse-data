# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2024 Recidiviz, Inc.
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
"""Reference views used by other views."""
from typing import List

from recidiviz.big_query.big_query_view import BigQueryViewBuilder
from recidiviz.calculator.query.state.views.prototypes.case_note_search.case_notes import (
    CASE_NOTES_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.prototypes.case_note_search.us_ix.us_ix_case_notes import (
    US_IX_CASE_NOTES_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.prototypes.case_note_search.us_me.us_me_case_notes import (
    US_ME_CASE_NOTES_VIEW_BUILDER,
)

CASE_NOTE_SEARCH_VIEW_BUILDERS: List[BigQueryViewBuilder] = [
    CASE_NOTES_VIEW_BUILDER,
    US_IX_CASE_NOTES_VIEW_BUILDER,
    US_ME_CASE_NOTES_VIEW_BUILDER,
]
