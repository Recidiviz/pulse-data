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
"""All views needed for sentence_sessions"""

from typing import List

from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.state.views.sentence_sessions.sentence_to_consecutive_parent_sentence import (
    CONSECUTIVE_SENTENCES_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.sentence_sessions.sentences_and_charges import (
    SENTENCES_AND_CHARGES_VIEW_BUILDER,
)

SENTENCE_SESSIONS_VIEW_BUILDERS: List[SimpleBigQueryViewBuilder] = [
    CONSECUTIVE_SENTENCES_VIEW_BUILDER,
    SENTENCES_AND_CHARGES_VIEW_BUILDER,
]
