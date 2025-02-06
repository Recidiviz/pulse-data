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
from recidiviz.calculator.query.state.views.sentence_sessions.inferred_group_aggregated_sentence_group_projected_dates import (
    INFERRED_GROUP_AGGREGATED_SENTENCE_GROUP_PROJECTED_DATES_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.sentence_sessions.inferred_group_aggregated_sentence_projected_dates import (
    INFERRED_GROUP_AGGREGATED_SENTENCE_PROJECTED_DATES_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.sentence_sessions.overlapping_sentence_serving_periods import (
    OVERLAPPING_SENTENCE_SERVING_PERIODS_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.sentence_sessions.person_projected_date_sessions import (
    PERSON_PROJECTED_DATE_SESSIONS_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.sentence_sessions.sentence_imposed_group_summary import (
    SENTENCE_IMPOSED_GROUP_SUMMARY_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.sentence_sessions.sentence_inferred_group_projected_date_sessions import (
    SENTENCE_INFERRED_GROUP_PROJECTED_DATE_SESSIONS_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.sentence_sessions.sentence_projected_date_sessions import (
    SENTENCE_PROJECTED_DATE_SESSIONS_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.sentence_sessions.sentence_serving_period import (
    SENTENCE_SERVING_PERIOD_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.sentence_sessions.sentence_status_raw_text_sessions import (
    SENTENCE_STATUS_RAW_TEXT_SESSIONS_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.sentence_sessions.sentence_to_consecutive_parent_sentence import (
    CONSECUTIVE_SENTENCES_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.sentence_sessions.sentences_and_charges import (
    SENTENCES_AND_CHARGES_VIEW_BUILDER,
)

SENTENCE_SESSIONS_VIEW_BUILDERS: List[SimpleBigQueryViewBuilder] = [
    CONSECUTIVE_SENTENCES_VIEW_BUILDER,
    # NormalizedStateSentenceGroupLength aggregated to inferred groups
    INFERRED_GROUP_AGGREGATED_SENTENCE_GROUP_PROJECTED_DATES_VIEW_BUILDER,
    # NormalizedStateSentenceLength aggregated to inferred groups
    INFERRED_GROUP_AGGREGATED_SENTENCE_PROJECTED_DATES_VIEW_BUILDER,
    # The set of projected dates for every sentence inferred group
    # to be used in analysis and product.
    SENTENCE_INFERRED_GROUP_PROJECTED_DATE_SESSIONS_VIEW_BUILDER,
    # Combined sentence and charge metadata
    SENTENCES_AND_CHARGES_VIEW_BUILDER,
    # Periods of time when a sentence is being served
    SENTENCE_SERVING_PERIOD_VIEW_BUILDER,
    # Spans of time with overlapping serving sentences
    OVERLAPPING_SENTENCE_SERVING_PERIODS_VIEW_BUILDER,
    # Normalized sentence status raw texts sessionized across sentences
    SENTENCE_STATUS_RAW_TEXT_SESSIONS_VIEW_BUILDER,
    # Sentence group serving periods with projected dates
    PERSON_PROJECTED_DATE_SESSIONS_VIEW_BUILDER,
    # Sentence serving periods with projected dates
    SENTENCE_PROJECTED_DATE_SESSIONS_VIEW_BUILDER,
    # Characteristics of sentence imposed groups and their most severe charge
    SENTENCE_IMPOSED_GROUP_SUMMARY_VIEW_BUILDER,
]
