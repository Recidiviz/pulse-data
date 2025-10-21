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
"""US_IX exemptions for deprecated sentence v1 view references in product views."""

from recidiviz.big_query.big_query_address import BigQueryAddress
from recidiviz.calculator.query.state.views.analyst_data.us_ix.us_ix_early_discharge_sessions_preprocessing import (
    US_IX_EARLY_DISCHARGE_SESSIONS_PREPROCESSING_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.analyst_data.us_ix.us_ix_sls_q1 import (
    US_IX_SLS_Q1_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.analyst_data.us_ix.us_ix_sls_q3 import (
    US_IX_SLS_Q3_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.dashboard.pathways.event_level.liberty_to_prison_transitions import (
    LIBERTY_TO_PRISON_TRANSITIONS_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.dashboard.vitals_summaries.vitals_summaries import (
    VITALS_SUMMARIES_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.dashboard.vitals_summaries.vitals_time_series import (
    VITALS_TIME_SERIES_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.jii_texting.jii_to_text import (
    JII_TO_TEXT_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.outliers.metric_benchmarks import (
    METRIC_BENCHMARKS_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.outliers.supervision_client_events import (
    SUPERVISION_CLIENT_EVENTS_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.outliers.supervision_officer_metrics import (
    SUPERVISION_OFFICER_METRICS_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.sentencing.case_disposition import (
    SENTENCING_CASE_DISPOSITION_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.sentencing.charge_record import (
    SENTENCING_CHARGE_RECORD_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.sessions.compartment_sessions_closest_sentence_imposed_group import (
    COMPARTMENT_SESSIONS_CLOSEST_SENTENCE_IMPOSED_GROUP_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.sessions.incarceration_projected_completion_date_spans import (
    INCARCERATION_PROJECTED_COMPLETION_DATE_SPANS_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.sessions.sentence_imposed_group_summary import (
    SENTENCE_IMPOSED_GROUP_SUMMARY_VIEW_BUILDER,
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
from recidiviz.calculator.query.state.views.sessions.us_ix.us_ix_consecutive_sentences_preprocessed import (
    US_IX_CONSECUTIVE_SENTENCES_PREPROCESSED_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.workflows.firestore.client_record import (
    CLIENT_RECORD_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.workflows.firestore.resident_record import (
    RESIDENT_RECORD_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.workflows.firestore.us_ix_complete_discharge_early_from_supervision_request_record import (
    US_IX_COMPLETE_DISCHARGE_EARLY_FROM_SUPERVISION_REQUEST_RECORD_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.workflows.firestore.us_ix_complete_full_term_discharge_from_supervision_request_record import (
    US_IX_COMPLETE_FULL_TERM_DISCHARGE_FROM_SUPERVISION_REQUEST_RECORD_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.workflows.firestore.us_ix_complete_transfer_to_limited_supervision_form_record import (
    US_IX_COMPLETE_TRANSFER_TO_LIMITED_SUPERVISION_FORM_RECORD_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.workflows.firestore.us_ix_custody_level_downgrade_record import (
    US_IX_CUSTODY_LEVEL_DOWNGRADE_RECORD_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.workflows.firestore.us_ix_transfer_to_crc_resident_worker_request_record import (
    US_IX_TRANSFER_TO_CRC_RESIDENT_WORKER_REQUEST_RECORD_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.workflows.firestore.us_ix_transfer_to_crc_work_release_request_record import (
    US_IX_TRANSFER_TO_CRC_WORK_RELEASE_REQUEST_RECORD_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.workflows.firestore.us_ix_transfer_to_xcrc_request_record import (
    US_IX_TRANSFER_TO_XCRC_REQUEST_RECORD_VIEW_BUILDER,
)
from recidiviz.common.constants.states import StateCode
from recidiviz.persistence.entity.state.entities import StateCharge
from recidiviz.pipelines.ingest.dataset_config import (
    normalized_state_dataset_for_state_code,
)

# For each US_IX metric export, for each product view in that export, a mapping of
#   deprecated v1 sentences views that are a) referenced directly without going through
#   sentence_sessions/sentence_sessions_v2_all and are b) part of the ancestor graph of
#   this product view, with all places they are referenced.
US_IX_SENTENCE_V1_PRODUCT_USAGE_EXEMPTIONS: dict[
    str, dict[BigQueryAddress, dict[BigQueryAddress, set[BigQueryAddress]]]
] = {
    "INSIGHTS": {
        SUPERVISION_CLIENT_EVENTS_VIEW_BUILDER.address: {
            SUPERVISION_PROJECTED_COMPLETION_DATE_SPANS_VIEW_BUILDER.address: {
                CLIENT_RECORD_VIEW_BUILDER.address,
                US_IX_EARLY_DISCHARGE_SESSIONS_PREPROCESSING_VIEW_BUILDER.address,
                US_IX_COMPLETE_DISCHARGE_EARLY_FROM_SUPERVISION_REQUEST_RECORD_VIEW_BUILDER.address,
                US_IX_COMPLETE_TRANSFER_TO_LIMITED_SUPERVISION_FORM_RECORD_VIEW_BUILDER.address,
            },
            INCARCERATION_PROJECTED_COMPLETION_DATE_SPANS_VIEW_BUILDER.address: {
                RESIDENT_RECORD_VIEW_BUILDER.address,
            },
            SENTENCE_SPANS_VIEW_BUILDER.address: {
                US_IX_COMPLETE_DISCHARGE_EARLY_FROM_SUPERVISION_REQUEST_RECORD_VIEW_BUILDER.address,
                US_IX_COMPLETE_TRANSFER_TO_LIMITED_SUPERVISION_FORM_RECORD_VIEW_BUILDER.address,
            },
            SENTENCES_PREPROCESSED_VIEW_BUILDER.address: {
                US_IX_COMPLETE_DISCHARGE_EARLY_FROM_SUPERVISION_REQUEST_RECORD_VIEW_BUILDER.address,
                US_IX_COMPLETE_TRANSFER_TO_LIMITED_SUPERVISION_FORM_RECORD_VIEW_BUILDER.address,
            },
            US_IX_CONSECUTIVE_SENTENCES_PREPROCESSED_VIEW_BUILDER.address: {
                US_IX_COMPLETE_DISCHARGE_EARLY_FROM_SUPERVISION_REQUEST_RECORD_VIEW_BUILDER.address,
            },
            BigQueryAddress(
                dataset_id=normalized_state_dataset_for_state_code(StateCode.US_IX),
                table_id=StateCharge.get_table_id(),
            ): {
                US_IX_COMPLETE_DISCHARGE_EARLY_FROM_SUPERVISION_REQUEST_RECORD_VIEW_BUILDER.address,
                US_IX_COMPLETE_TRANSFER_TO_LIMITED_SUPERVISION_FORM_RECORD_VIEW_BUILDER.address,
                US_IX_TRANSFER_TO_XCRC_REQUEST_RECORD_VIEW_BUILDER.address,
                US_IX_TRANSFER_TO_CRC_WORK_RELEASE_REQUEST_RECORD_VIEW_BUILDER.address,
                US_IX_TRANSFER_TO_CRC_RESIDENT_WORKER_REQUEST_RECORD_VIEW_BUILDER.address,
                US_IX_SLS_Q1_VIEW_BUILDER.address,
                US_IX_SLS_Q3_VIEW_BUILDER.address,
            },
        },
        SUPERVISION_OFFICER_METRICS_VIEW_BUILDER.address: {
            SENTENCE_SPANS_VIEW_BUILDER.address: {
                US_IX_COMPLETE_DISCHARGE_EARLY_FROM_SUPERVISION_REQUEST_RECORD_VIEW_BUILDER.address,
                US_IX_COMPLETE_TRANSFER_TO_LIMITED_SUPERVISION_FORM_RECORD_VIEW_BUILDER.address,
            },
            SENTENCES_PREPROCESSED_VIEW_BUILDER.address: {
                US_IX_COMPLETE_DISCHARGE_EARLY_FROM_SUPERVISION_REQUEST_RECORD_VIEW_BUILDER.address,
                US_IX_COMPLETE_TRANSFER_TO_LIMITED_SUPERVISION_FORM_RECORD_VIEW_BUILDER.address,
            },
            SUPERVISION_PROJECTED_COMPLETION_DATE_SPANS_VIEW_BUILDER.address: {
                CLIENT_RECORD_VIEW_BUILDER.address,
                US_IX_EARLY_DISCHARGE_SESSIONS_PREPROCESSING_VIEW_BUILDER.address,
                US_IX_COMPLETE_DISCHARGE_EARLY_FROM_SUPERVISION_REQUEST_RECORD_VIEW_BUILDER.address,
                US_IX_COMPLETE_TRANSFER_TO_LIMITED_SUPERVISION_FORM_RECORD_VIEW_BUILDER.address,
            },
            INCARCERATION_PROJECTED_COMPLETION_DATE_SPANS_VIEW_BUILDER.address: {
                RESIDENT_RECORD_VIEW_BUILDER.address,
            },
            US_IX_CONSECUTIVE_SENTENCES_PREPROCESSED_VIEW_BUILDER.address: {
                US_IX_COMPLETE_DISCHARGE_EARLY_FROM_SUPERVISION_REQUEST_RECORD_VIEW_BUILDER.address,
            },
            BigQueryAddress(
                dataset_id=normalized_state_dataset_for_state_code(StateCode.US_IX),
                table_id=StateCharge.get_table_id(),
            ): {
                US_IX_COMPLETE_DISCHARGE_EARLY_FROM_SUPERVISION_REQUEST_RECORD_VIEW_BUILDER.address,
                US_IX_COMPLETE_TRANSFER_TO_LIMITED_SUPERVISION_FORM_RECORD_VIEW_BUILDER.address,
                US_IX_TRANSFER_TO_XCRC_REQUEST_RECORD_VIEW_BUILDER.address,
                US_IX_TRANSFER_TO_CRC_WORK_RELEASE_REQUEST_RECORD_VIEW_BUILDER.address,
                US_IX_TRANSFER_TO_CRC_RESIDENT_WORKER_REQUEST_RECORD_VIEW_BUILDER.address,
                US_IX_SLS_Q1_VIEW_BUILDER.address,
                US_IX_SLS_Q3_VIEW_BUILDER.address,
            },
        },
        METRIC_BENCHMARKS_VIEW_BUILDER.address: {
            SENTENCE_SPANS_VIEW_BUILDER.address: {
                US_IX_COMPLETE_DISCHARGE_EARLY_FROM_SUPERVISION_REQUEST_RECORD_VIEW_BUILDER.address,
                US_IX_COMPLETE_TRANSFER_TO_LIMITED_SUPERVISION_FORM_RECORD_VIEW_BUILDER.address,
            },
            SENTENCES_PREPROCESSED_VIEW_BUILDER.address: {
                US_IX_COMPLETE_DISCHARGE_EARLY_FROM_SUPERVISION_REQUEST_RECORD_VIEW_BUILDER.address,
                US_IX_COMPLETE_TRANSFER_TO_LIMITED_SUPERVISION_FORM_RECORD_VIEW_BUILDER.address,
            },
            SUPERVISION_PROJECTED_COMPLETION_DATE_SPANS_VIEW_BUILDER.address: {
                CLIENT_RECORD_VIEW_BUILDER.address,
                US_IX_EARLY_DISCHARGE_SESSIONS_PREPROCESSING_VIEW_BUILDER.address,
                US_IX_COMPLETE_DISCHARGE_EARLY_FROM_SUPERVISION_REQUEST_RECORD_VIEW_BUILDER.address,
                US_IX_COMPLETE_TRANSFER_TO_LIMITED_SUPERVISION_FORM_RECORD_VIEW_BUILDER.address,
            },
            INCARCERATION_PROJECTED_COMPLETION_DATE_SPANS_VIEW_BUILDER.address: {
                RESIDENT_RECORD_VIEW_BUILDER.address,
            },
            US_IX_CONSECUTIVE_SENTENCES_PREPROCESSED_VIEW_BUILDER.address: {
                US_IX_COMPLETE_DISCHARGE_EARLY_FROM_SUPERVISION_REQUEST_RECORD_VIEW_BUILDER.address,
            },
            BigQueryAddress(
                dataset_id=normalized_state_dataset_for_state_code(StateCode.US_IX),
                table_id=StateCharge.get_table_id(),
            ): {
                US_IX_COMPLETE_DISCHARGE_EARLY_FROM_SUPERVISION_REQUEST_RECORD_VIEW_BUILDER.address,
                US_IX_COMPLETE_TRANSFER_TO_LIMITED_SUPERVISION_FORM_RECORD_VIEW_BUILDER.address,
                US_IX_TRANSFER_TO_XCRC_REQUEST_RECORD_VIEW_BUILDER.address,
                US_IX_TRANSFER_TO_CRC_WORK_RELEASE_REQUEST_RECORD_VIEW_BUILDER.address,
                US_IX_TRANSFER_TO_CRC_RESIDENT_WORKER_REQUEST_RECORD_VIEW_BUILDER.address,
                US_IX_SLS_Q1_VIEW_BUILDER.address,
                US_IX_SLS_Q3_VIEW_BUILDER.address,
            },
        },
    },
    "PATHWAYS_EVENT_LEVEL": {
        LIBERTY_TO_PRISON_TRANSITIONS_VIEW_BUILDER.address: {
            COMPARTMENT_SESSIONS_CLOSEST_SENTENCE_IMPOSED_GROUP_VIEW_BUILDER.address: {
                LIBERTY_TO_PRISON_TRANSITIONS_VIEW_BUILDER.address,
            },
            SENTENCE_IMPOSED_GROUP_SUMMARY_VIEW_BUILDER.address: {
                LIBERTY_TO_PRISON_TRANSITIONS_VIEW_BUILDER.address,
            },
        },
    },
    "JII_TEXTING": {
        JII_TO_TEXT_VIEW_BUILDER.address: {
            SUPERVISION_PROJECTED_COMPLETION_DATE_SPANS_VIEW_BUILDER.address: {
                CLIENT_RECORD_VIEW_BUILDER.address,
                US_IX_EARLY_DISCHARGE_SESSIONS_PREPROCESSING_VIEW_BUILDER.address,
                US_IX_COMPLETE_DISCHARGE_EARLY_FROM_SUPERVISION_REQUEST_RECORD_VIEW_BUILDER.address,
                US_IX_COMPLETE_TRANSFER_TO_LIMITED_SUPERVISION_FORM_RECORD_VIEW_BUILDER.address,
            },
            SENTENCE_SPANS_VIEW_BUILDER.address: {
                US_IX_COMPLETE_DISCHARGE_EARLY_FROM_SUPERVISION_REQUEST_RECORD_VIEW_BUILDER.address,
                US_IX_COMPLETE_TRANSFER_TO_LIMITED_SUPERVISION_FORM_RECORD_VIEW_BUILDER.address,
            },
            SENTENCES_PREPROCESSED_VIEW_BUILDER.address: {
                US_IX_COMPLETE_DISCHARGE_EARLY_FROM_SUPERVISION_REQUEST_RECORD_VIEW_BUILDER.address,
                US_IX_COMPLETE_TRANSFER_TO_LIMITED_SUPERVISION_FORM_RECORD_VIEW_BUILDER.address,
            },
            US_IX_CONSECUTIVE_SENTENCES_PREPROCESSED_VIEW_BUILDER.address: {
                US_IX_COMPLETE_DISCHARGE_EARLY_FROM_SUPERVISION_REQUEST_RECORD_VIEW_BUILDER.address,
            },
            BigQueryAddress(
                dataset_id=normalized_state_dataset_for_state_code(StateCode.US_IX),
                table_id=StateCharge.get_table_id(),
            ): {
                US_IX_COMPLETE_DISCHARGE_EARLY_FROM_SUPERVISION_REQUEST_RECORD_VIEW_BUILDER.address,
                US_IX_COMPLETE_TRANSFER_TO_LIMITED_SUPERVISION_FORM_RECORD_VIEW_BUILDER.address,
            },
        },
    },
    "SENTENCING": {
        SENTENCING_CHARGE_RECORD_VIEW_BUILDER.address: {
            BigQueryAddress(
                dataset_id=normalized_state_dataset_for_state_code(StateCode.US_IX),
                table_id=StateCharge.get_table_id(),
            ): {
                SENTENCING_CHARGE_RECORD_VIEW_BUILDER.address,
            },
        },
        SENTENCING_CASE_DISPOSITION_VIEW_BUILDER.address: {
            COMPARTMENT_SESSIONS_CLOSEST_SENTENCE_IMPOSED_GROUP_VIEW_BUILDER.address: {
                SENTENCING_CASE_DISPOSITION_VIEW_BUILDER.address,
            },
            SENTENCE_IMPOSED_GROUP_SUMMARY_VIEW_BUILDER.address: {
                SENTENCING_CASE_DISPOSITION_VIEW_BUILDER.address,
            },
            SENTENCES_PREPROCESSED_VIEW_BUILDER.address: {
                SENTENCING_CASE_DISPOSITION_VIEW_BUILDER.address,
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
                RESIDENT_RECORD_VIEW_BUILDER.address,
            },
        },
        US_IX_COMPLETE_FULL_TERM_DISCHARGE_FROM_SUPERVISION_REQUEST_RECORD_VIEW_BUILDER.address: {
            SUPERVISION_PROJECTED_COMPLETION_DATE_SPANS_VIEW_BUILDER.address: {
                US_IX_EARLY_DISCHARGE_SESSIONS_PREPROCESSING_VIEW_BUILDER.address,
            },
        },
        US_IX_COMPLETE_TRANSFER_TO_LIMITED_SUPERVISION_FORM_RECORD_VIEW_BUILDER.address: {
            SENTENCE_SPANS_VIEW_BUILDER.address: {
                US_IX_COMPLETE_TRANSFER_TO_LIMITED_SUPERVISION_FORM_RECORD_VIEW_BUILDER.address,
            },
            SENTENCES_PREPROCESSED_VIEW_BUILDER.address: {
                US_IX_COMPLETE_TRANSFER_TO_LIMITED_SUPERVISION_FORM_RECORD_VIEW_BUILDER.address,
            },
            SUPERVISION_PROJECTED_COMPLETION_DATE_SPANS_VIEW_BUILDER.address: {
                US_IX_COMPLETE_TRANSFER_TO_LIMITED_SUPERVISION_FORM_RECORD_VIEW_BUILDER.address,
            },
            BigQueryAddress(
                dataset_id=normalized_state_dataset_for_state_code(StateCode.US_IX),
                table_id=StateCharge.get_table_id(),
            ): {
                US_IX_COMPLETE_TRANSFER_TO_LIMITED_SUPERVISION_FORM_RECORD_VIEW_BUILDER.address,
            },
        },
        US_IX_TRANSFER_TO_XCRC_REQUEST_RECORD_VIEW_BUILDER.address: {
            BigQueryAddress(
                dataset_id=normalized_state_dataset_for_state_code(StateCode.US_IX),
                table_id=StateCharge.get_table_id(),
            ): {
                US_IX_TRANSFER_TO_XCRC_REQUEST_RECORD_VIEW_BUILDER.address,
            },
        },
        US_IX_COMPLETE_DISCHARGE_EARLY_FROM_SUPERVISION_REQUEST_RECORD_VIEW_BUILDER.address: {
            US_IX_CONSECUTIVE_SENTENCES_PREPROCESSED_VIEW_BUILDER.address: {
                US_IX_COMPLETE_DISCHARGE_EARLY_FROM_SUPERVISION_REQUEST_RECORD_VIEW_BUILDER.address,
            },
            SENTENCE_SPANS_VIEW_BUILDER.address: {
                US_IX_COMPLETE_DISCHARGE_EARLY_FROM_SUPERVISION_REQUEST_RECORD_VIEW_BUILDER.address,
            },
            SENTENCES_PREPROCESSED_VIEW_BUILDER.address: {
                US_IX_COMPLETE_DISCHARGE_EARLY_FROM_SUPERVISION_REQUEST_RECORD_VIEW_BUILDER.address,
            },
            SUPERVISION_PROJECTED_COMPLETION_DATE_SPANS_VIEW_BUILDER.address: {
                US_IX_EARLY_DISCHARGE_SESSIONS_PREPROCESSING_VIEW_BUILDER.address,
                US_IX_COMPLETE_DISCHARGE_EARLY_FROM_SUPERVISION_REQUEST_RECORD_VIEW_BUILDER.address,
            },
            BigQueryAddress(
                dataset_id=normalized_state_dataset_for_state_code(StateCode.US_IX),
                table_id=StateCharge.get_table_id(),
            ): {
                US_IX_COMPLETE_DISCHARGE_EARLY_FROM_SUPERVISION_REQUEST_RECORD_VIEW_BUILDER.address,
            },
        },
        US_IX_TRANSFER_TO_CRC_WORK_RELEASE_REQUEST_RECORD_VIEW_BUILDER.address: {
            BigQueryAddress(
                dataset_id=normalized_state_dataset_for_state_code(StateCode.US_IX),
                table_id=StateCharge.get_table_id(),
            ): {
                US_IX_TRANSFER_TO_CRC_WORK_RELEASE_REQUEST_RECORD_VIEW_BUILDER.address,
            },
        },
        US_IX_TRANSFER_TO_CRC_RESIDENT_WORKER_REQUEST_RECORD_VIEW_BUILDER.address: {
            BigQueryAddress(
                dataset_id=normalized_state_dataset_for_state_code(StateCode.US_IX),
                table_id=StateCharge.get_table_id(),
            ): {
                US_IX_TRANSFER_TO_CRC_RESIDENT_WORKER_REQUEST_RECORD_VIEW_BUILDER.address,
            },
        },
        US_IX_CUSTODY_LEVEL_DOWNGRADE_RECORD_VIEW_BUILDER.address: {
            BigQueryAddress(
                dataset_id=normalized_state_dataset_for_state_code(StateCode.US_IX),
                table_id=StateCharge.get_table_id(),
            ): {
                US_IX_SLS_Q1_VIEW_BUILDER.address,
                US_IX_SLS_Q3_VIEW_BUILDER.address,
            },
        },
    },
    "VITALS": {
        VITALS_SUMMARIES_VIEW_BUILDER.address: {
            BigQueryAddress(
                dataset_id=normalized_state_dataset_for_state_code(StateCode.US_IX),
                table_id=StateCharge.get_table_id(),
            ): {
                US_IX_SLS_Q1_VIEW_BUILDER.address,
                US_IX_SLS_Q3_VIEW_BUILDER.address,
            },
            SUPERVISION_PROJECTED_COMPLETION_DATE_SPANS_VIEW_BUILDER.address: {
                US_IX_EARLY_DISCHARGE_SESSIONS_PREPROCESSING_VIEW_BUILDER.address,
            },
        },
        VITALS_TIME_SERIES_VIEW_BUILDER.address: {
            BigQueryAddress(
                dataset_id=normalized_state_dataset_for_state_code(StateCode.US_IX),
                table_id=StateCharge.get_table_id(),
            ): {
                US_IX_SLS_Q1_VIEW_BUILDER.address,
                US_IX_SLS_Q3_VIEW_BUILDER.address,
            },
            SUPERVISION_PROJECTED_COMPLETION_DATE_SPANS_VIEW_BUILDER.address: {
                US_IX_EARLY_DISCHARGE_SESSIONS_PREPROCESSING_VIEW_BUILDER.address,
            },
        },
    },
}
