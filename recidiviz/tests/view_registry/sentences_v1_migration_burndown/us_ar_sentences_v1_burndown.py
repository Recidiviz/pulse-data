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
"""US_AR exemptions for deprecated sentence v1 view references in product views."""

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
from recidiviz.calculator.query.state.views.workflows.firestore.us_ar_institutional_worker_status_record import (
    US_AR_INSTITUTIONAL_WORKER_STATUS_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.workflows.firestore.us_ar_work_release_record import (
    US_AR_WORK_RELEASE_RECORD_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.workflows.us_ar.resident_metadata import (
    US_AR_RESIDENT_METADATA_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.workflows.us_ar.resident_record_incarceration_cases_with_dates import (
    US_AR_RESIDENT_RECORD_INCARCERATION_CASES_WITH_DATES_VIEW_BUILDER,
)
from recidiviz.task_eligibility.criteria.state_specific.us_ar.eligible_criminal_history_309 import (
    VIEW_BUILDER as US_AR_ELIGIBLE_CRIMINAL_HISTORY_309_VIEW_BUILDER,
)
from recidiviz.task_eligibility.criteria.state_specific.us_ar.eligible_criminal_history_work_release import (
    VIEW_BUILDER as US_AR_ELIGIBLE_CRIMINAL_HISTORY_WORK_RELEASE_VIEW_BUILDER,
)

# For each US_AR metric export, for each product view in that export, a mapping of
#   deprecated v1 sentences views that are a) referenced directly without going through
#   sentence_sessions/sentence_sessions_v2_all and are b) part of the ancestor graph of
#   this product view, with all places they are referenced.
US_AR_SENTENCE_V1_PRODUCT_USAGE_EXEMPTIONS: dict[
    str, dict[BigQueryAddress, dict[BigQueryAddress, set[BigQueryAddress]]]
] = {
    "WORKFLOWS_FIRESTORE": {
        US_AR_INSTITUTIONAL_WORKER_STATUS_VIEW_BUILDER.address: {
            SENTENCE_SPANS_VIEW_BUILDER.address: {
                US_AR_RESIDENT_METADATA_VIEW_BUILDER.address,
            },
            SENTENCES_PREPROCESSED_VIEW_BUILDER.address: {
                US_AR_ELIGIBLE_CRIMINAL_HISTORY_309_VIEW_BUILDER.address,
                US_AR_RESIDENT_METADATA_VIEW_BUILDER.address,
            },
        },
        US_AR_WORK_RELEASE_RECORD_VIEW_BUILDER.address: {
            SENTENCE_SPANS_VIEW_BUILDER.address: {
                US_AR_RESIDENT_METADATA_VIEW_BUILDER.address,
            },
            SENTENCES_PREPROCESSED_VIEW_BUILDER.address: {
                US_AR_ELIGIBLE_CRIMINAL_HISTORY_WORK_RELEASE_VIEW_BUILDER.address,
                US_AR_RESIDENT_METADATA_VIEW_BUILDER.address,
            },
        },
        CLIENT_RECORD_VIEW_BUILDER.address: {
            SUPERVISION_PROJECTED_COMPLETION_DATE_SPANS_VIEW_BUILDER.address: {
                CLIENT_RECORD_VIEW_BUILDER.address,
            },
        },
        RESIDENT_RECORD_VIEW_BUILDER.address: {
            INCARCERATION_PROJECTED_COMPLETION_DATE_SPANS_VIEW_BUILDER.address: {
                US_AR_RESIDENT_RECORD_INCARCERATION_CASES_WITH_DATES_VIEW_BUILDER.address,
            },
            SENTENCE_SPANS_VIEW_BUILDER.address: {
                US_AR_RESIDENT_METADATA_VIEW_BUILDER.address,
            },
            SENTENCES_PREPROCESSED_VIEW_BUILDER.address: {
                US_AR_RESIDENT_METADATA_VIEW_BUILDER.address,
            },
        },
    },
}
