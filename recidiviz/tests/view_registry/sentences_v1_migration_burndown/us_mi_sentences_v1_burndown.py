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
"""US_MI exemptions for deprecated sentence v1 view references in product views."""

from recidiviz.big_query.big_query_address import BigQueryAddress
from recidiviz.calculator.query.state.views.outliers.metric_benchmarks import (
    METRIC_BENCHMARKS_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.outliers.supervision_client_events import (
    SUPERVISION_CLIENT_EVENTS_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.outliers.supervision_officer_metrics import (
    SUPERVISION_OFFICER_METRICS_VIEW_BUILDER,
)
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
from recidiviz.calculator.query.state.views.workflows.firestore.us_mi_complete_discharge_early_from_supervision_request_record import (
    US_MI_COMPLETE_DISCHARGE_EARLY_FROM_SUPERVISION_REQUEST_RECORD_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.workflows.resident_record_incarceration_cases_with_dates import (
    RESIDENT_RECORD_INCARCERATION_CASES_WITH_DATES_VIEW_BUILDER,
)
from recidiviz.task_eligibility.criteria.general.serving_at_least_one_year_on_parole_supervision_or_supervision_out_of_state import (
    VIEW_BUILDER as SERVING_AT_LEAST_ONE_YEAR_ON_PAROLE_SUPERVISION_OR_SUPERVISION_OUT_OF_STATE_VIEW_BUILDER,
)

# For each US_MI metric export, for each product view in that export, a mapping of
#   deprecated v1 sentences views that are a) referenced directly without going through
#   sentence_sessions/sentence_sessions_v2_all and are b) part of the ancestor graph of
#   this product view, with all places they are referenced.
US_MI_SENTENCE_V1_PRODUCT_USAGE_EXEMPTIONS: dict[
    str, dict[BigQueryAddress, dict[BigQueryAddress, set[BigQueryAddress]]]
] = {
    "INSIGHTS": {
        SUPERVISION_CLIENT_EVENTS_VIEW_BUILDER.address: {
            SUPERVISION_PROJECTED_COMPLETION_DATE_SPANS_VIEW_BUILDER.address: {
                CLIENT_RECORD_VIEW_BUILDER.address,
            },
            INCARCERATION_PROJECTED_COMPLETION_DATE_SPANS_VIEW_BUILDER.address: {
                RESIDENT_RECORD_INCARCERATION_CASES_WITH_DATES_VIEW_BUILDER.address,
            },
            SENTENCE_SPANS_VIEW_BUILDER.address: {
                SERVING_AT_LEAST_ONE_YEAR_ON_PAROLE_SUPERVISION_OR_SUPERVISION_OUT_OF_STATE_VIEW_BUILDER.address,
            },
            SENTENCES_PREPROCESSED_VIEW_BUILDER.address: {
                SERVING_AT_LEAST_ONE_YEAR_ON_PAROLE_SUPERVISION_OR_SUPERVISION_OUT_OF_STATE_VIEW_BUILDER.address,
            },
        },
        SUPERVISION_OFFICER_METRICS_VIEW_BUILDER.address: {
            SENTENCE_SPANS_VIEW_BUILDER.address: {
                SERVING_AT_LEAST_ONE_YEAR_ON_PAROLE_SUPERVISION_OR_SUPERVISION_OUT_OF_STATE_VIEW_BUILDER.address,
            },
            SENTENCES_PREPROCESSED_VIEW_BUILDER.address: {
                SERVING_AT_LEAST_ONE_YEAR_ON_PAROLE_SUPERVISION_OR_SUPERVISION_OUT_OF_STATE_VIEW_BUILDER.address,
            },
            SUPERVISION_PROJECTED_COMPLETION_DATE_SPANS_VIEW_BUILDER.address: {
                CLIENT_RECORD_VIEW_BUILDER.address,
            },
            INCARCERATION_PROJECTED_COMPLETION_DATE_SPANS_VIEW_BUILDER.address: {
                RESIDENT_RECORD_INCARCERATION_CASES_WITH_DATES_VIEW_BUILDER.address,
            },
        },
        METRIC_BENCHMARKS_VIEW_BUILDER.address: {
            SENTENCE_SPANS_VIEW_BUILDER.address: {
                SERVING_AT_LEAST_ONE_YEAR_ON_PAROLE_SUPERVISION_OR_SUPERVISION_OUT_OF_STATE_VIEW_BUILDER.address,
            },
            SENTENCES_PREPROCESSED_VIEW_BUILDER.address: {
                SERVING_AT_LEAST_ONE_YEAR_ON_PAROLE_SUPERVISION_OR_SUPERVISION_OUT_OF_STATE_VIEW_BUILDER.address,
            },
            SUPERVISION_PROJECTED_COMPLETION_DATE_SPANS_VIEW_BUILDER.address: {
                CLIENT_RECORD_VIEW_BUILDER.address,
            },
            INCARCERATION_PROJECTED_COMPLETION_DATE_SPANS_VIEW_BUILDER.address: {
                RESIDENT_RECORD_INCARCERATION_CASES_WITH_DATES_VIEW_BUILDER.address,
            },
        },
    },
    "WORKFLOWS_FIRESTORE": {
        CLIENT_RECORD_VIEW_BUILDER.address: {
            SUPERVISION_PROJECTED_COMPLETION_DATE_SPANS_VIEW_BUILDER.address: {
                CLIENT_RECORD_VIEW_BUILDER.address,
            },
        },
        RESIDENT_RECORD_VIEW_BUILDER.address: {
            INCARCERATION_PROJECTED_COMPLETION_DATE_SPANS_VIEW_BUILDER.address: {
                RESIDENT_RECORD_INCARCERATION_CASES_WITH_DATES_VIEW_BUILDER.address,
            },
        },
        US_MI_COMPLETE_DISCHARGE_EARLY_FROM_SUPERVISION_REQUEST_RECORD_VIEW_BUILDER.address: {
            SENTENCE_SPANS_VIEW_BUILDER.address: {
                SERVING_AT_LEAST_ONE_YEAR_ON_PAROLE_SUPERVISION_OR_SUPERVISION_OUT_OF_STATE_VIEW_BUILDER.address,
            },
            SENTENCES_PREPROCESSED_VIEW_BUILDER.address: {
                SERVING_AT_LEAST_ONE_YEAR_ON_PAROLE_SUPERVISION_OR_SUPERVISION_OUT_OF_STATE_VIEW_BUILDER.address,
            },
        },
    },
}
