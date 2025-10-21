# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2025 Recidiviz, Inc.
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
"""US_ME exemptions for deprecated sentence v1 view references in product views."""

from recidiviz.big_query.big_query_address import BigQueryAddress
from recidiviz.calculator.query.state.views.sessions.incarceration_projected_completion_date_spans import (
    INCARCERATION_PROJECTED_COMPLETION_DATE_SPANS_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.sessions.sentence_spans import (
    SENTENCE_SPANS_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.sessions.sentences_preprocessed import (
    SENTENCES_PREPROCESSED_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.sessions.supervision_projected_completion_date_spans import (
    SUPERVISION_PROJECTED_COMPLETION_DATE_SPANS_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.workflows.firestore.client_record import (
    CLIENT_RECORD_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.workflows.firestore.resident_record import (
    RESIDENT_RECORD_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.workflows.firestore.us_me_complete_early_termination_record import (
    US_ME_COMPLETE_EARLY_TERMINATION_RECORD_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.workflows.firestore.us_me_custody_reclassification_review_form_record import (
    US_ME_RECLASSIFICATION_REVIEW_FORM_RECORD_VIEW_BUILDER,
)
from recidiviz.common.constants.states import StateCode
from recidiviz.persistence.entity.state.entities import (
    StateCharge,
    StateIncarcerationSentence,
    StateSupervisionSentence,
)
from recidiviz.pipelines.ingest.dataset_config import (
    normalized_state_dataset_for_state_code,
)
from recidiviz.task_eligibility.criteria.state_specific.us_me.supervision_past_half_full_term_release_date_from_probation_start import (
    VIEW_BUILDER as US_ME_SUPERVISION_PAST_HALF_FULL_TERM_RELEASE_DATE_FROM_PROBATION_START_VIEW_BUILDER,
)

# For each US_ME metric export, for each product view in that export, a mapping of
#   deprecated v1 sentences views that are a) referenced directly without going through
#   sentence_sessions/sentence_sessions_v2_all and are b) part of the ancestor graph of
#   this product view, with all places they are referenced.
US_ME_SENTENCE_V1_PRODUCT_USAGE_EXEMPTIONS: dict[
    str, dict[BigQueryAddress, dict[BigQueryAddress, set[BigQueryAddress]]]
] = {
    "WORKFLOWS_FIRESTORE": {
        CLIENT_RECORD_VIEW_BUILDER.address: {
            SUPERVISION_PROJECTED_COMPLETION_DATE_SPANS_VIEW_BUILDER.address: {
                CLIENT_RECORD_VIEW_BUILDER.address,
            },
        },
        RESIDENT_RECORD_VIEW_BUILDER.address: {
            INCARCERATION_PROJECTED_COMPLETION_DATE_SPANS_VIEW_BUILDER.address: {
                RESIDENT_RECORD_VIEW_BUILDER.address,
            },
            BigQueryAddress(
                dataset_id=normalized_state_dataset_for_state_code(StateCode.US_ME),
                table_id=StateIncarcerationSentence.get_table_id(),
            ): {
                RESIDENT_RECORD_VIEW_BUILDER.address,
            },
        },
        US_ME_RECLASSIFICATION_REVIEW_FORM_RECORD_VIEW_BUILDER.address: {
            SENTENCE_SPANS_VIEW_BUILDER.address: {
                US_ME_RECLASSIFICATION_REVIEW_FORM_RECORD_VIEW_BUILDER.address,
            },
            SENTENCES_PREPROCESSED_VIEW_BUILDER.address: {
                US_ME_RECLASSIFICATION_REVIEW_FORM_RECORD_VIEW_BUILDER.address,
            },
            BigQueryAddress(
                dataset_id=normalized_state_dataset_for_state_code(StateCode.US_ME),
                table_id=StateCharge.get_table_id(),
            ): {
                US_ME_RECLASSIFICATION_REVIEW_FORM_RECORD_VIEW_BUILDER.address,
            },
            BigQueryAddress(
                dataset_id=normalized_state_dataset_for_state_code(StateCode.US_ME),
                table_id=StateSupervisionSentence.get_table_id(),
            ): {
                US_ME_RECLASSIFICATION_REVIEW_FORM_RECORD_VIEW_BUILDER.address,
            },
        },
        US_ME_COMPLETE_EARLY_TERMINATION_RECORD_VIEW_BUILDER.address: {
            SUPERVISION_PROJECTED_COMPLETION_DATE_SPANS_VIEW_BUILDER.address: {
                US_ME_SUPERVISION_PAST_HALF_FULL_TERM_RELEASE_DATE_FROM_PROBATION_START_VIEW_BUILDER.address,
            },
        },
    },
}
