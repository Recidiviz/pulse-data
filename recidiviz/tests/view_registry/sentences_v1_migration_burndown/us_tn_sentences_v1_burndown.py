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
"""US_TN exemptions for deprecated sentence v1 view references in product views."""

from recidiviz.big_query.big_query_address import BigQueryAddress
from recidiviz.calculator.query.state.views.dashboard.pathways.event_level.liberty_to_prison_transitions import (
    LIBERTY_TO_PRISON_TRANSITIONS_VIEW_BUILDER,
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
from recidiviz.calculator.query.state.views.sessions.compartment_sessions_closest_sentence_imposed_group import (
    COMPARTMENT_SESSIONS_CLOSEST_SENTENCE_IMPOSED_GROUP_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.sessions.incarceration_projected_completion_date_spans import (
    INCARCERATION_PROJECTED_COMPLETION_DATE_SPANS_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.sessions.sentence_deadline_spans import (
    SENTENCE_DEADLINE_SPANS_VIEW_BUILDER,
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
from recidiviz.calculator.query.state.views.workflows.firestore.client_record import (
    CLIENT_RECORD_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.workflows.firestore.resident_record import (
    RESIDENT_RECORD_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.workflows.firestore.us_tn_annual_reclassification_review_record import (
    US_TN_ANNUAL_RECLASSIFICATION_REVIEW_RECORD_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.workflows.firestore.us_tn_custody_level_downgrade_record import (
    US_TN_CUSTODY_LEVEL_DOWNGRADE_RECORD_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.workflows.firestore.us_tn_full_term_supervision_discharge_record import (
    US_TN_FULL_TERM_SUPERVISION_DISCHARGE_RECORD_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.workflows.firestore.us_tn_initial_classification_review_record import (
    US_TN_INITIAL_CLASSIFICATION_REVIEW_RECORD_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.workflows.firestore.us_tn_suspension_of_direct_supervision_record import (
    US_TN_SUSPENSION_OF_DIRECT_SUPERVISION_RECORD_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.workflows.firestore.us_tn_transfer_to_compliant_reporting_2025_policy_record import (
    US_TN_TRANSFER_TO_COMPLIANT_REPORTING_2025_POLICY_RECORD_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.workflows.firestore.us_tn_transfer_to_compliant_reporting_record import (
    US_TN_TRANSFER_TO_COMPLIANT_REPORTING_RECORD_VIEW_BUILDER,
)
from recidiviz.common.constants.states import StateCode
from recidiviz.observations.views.events.person.incarceration_release import (
    VIEW_BUILDER as INCARCERATION_RELEASE_VIEW_BUILDER,
)
from recidiviz.observations.views.events.person.supervision_release import (
    VIEW_BUILDER as SUPERVISION_RELEASE_VIEW_BUILDER,
)
from recidiviz.observations.views.spans.person.sentence_span import (
    VIEW_BUILDER as SENTENCE_SPAN_VIEW_BUILDER,
)
from recidiviz.persistence.entity.state.entities import (
    StateIncarcerationSentence,
    StateSupervisionSentence,
)
from recidiviz.pipelines.ingest.dataset_config import (
    normalized_state_dataset_for_state_code,
)
from recidiviz.task_eligibility.criteria.state_specific.us_tn.not_on_community_supervision_for_life import (
    VIEW_BUILDER as US_TN_NOT_ON_COMMUNITY_SUPERVISION_FOR_LIFE_VIEW_BUILDER,
)
from recidiviz.task_eligibility.criteria.state_specific.us_tn.not_on_life_sentence_or_lifetime_supervision import (
    VIEW_BUILDER as US_TN_NOT_ON_LIFE_SENTENCE_OR_LIFETIME_SUPERVISION_VIEW_BUILDER,
)
from recidiviz.task_eligibility.criteria.state_specific.us_tn.not_serving_ineligible_cr_offense import (
    VIEW_BUILDER as US_TN_NOT_SERVING_INELIGIBLE_CR_OFFENSE_VIEW_BUILDER,
)
from recidiviz.task_eligibility.criteria.state_specific.us_tn.not_serving_ineligible_cr_offense_policy_b import (
    VIEW_BUILDER as US_TN_NOT_SERVING_INELIGIBLE_CR_OFFENSE_POLICY_B_VIEW_BUILDER,
)
from recidiviz.task_eligibility.criteria.state_specific.us_tn.not_serving_unknown_cr_offense import (
    VIEW_BUILDER as US_TN_NOT_SERVING_UNKNOWN_CR_OFFENSE_VIEW_BUILDER,
)

# For each US_TN metric export, for each product view in that export, a mapping of
#   deprecated v1 sentences views that are a) referenced directly without going through
#   sentence_sessions/sentence_sessions_v2_all and are b) part of the ancestor graph of
#   this product view, with all places they are referenced.
US_TN_SENTENCE_V1_PRODUCT_USAGE_EXEMPTIONS: dict[
    str, dict[BigQueryAddress, dict[BigQueryAddress, set[BigQueryAddress]]]
] = {
    "INSIGHTS": {
        SUPERVISION_CLIENT_EVENTS_VIEW_BUILDER.address: {
            INCARCERATION_PROJECTED_COMPLETION_DATE_SPANS_VIEW_BUILDER.address: {
                RESIDENT_RECORD_VIEW_BUILDER.address,
            },
            SENTENCE_SPANS_VIEW_BUILDER.address: {
                US_TN_NOT_SERVING_INELIGIBLE_CR_OFFENSE_VIEW_BUILDER.address,
                US_TN_NOT_SERVING_INELIGIBLE_CR_OFFENSE_POLICY_B_VIEW_BUILDER.address,
                US_TN_NOT_SERVING_UNKNOWN_CR_OFFENSE_VIEW_BUILDER.address,
                US_TN_FULL_TERM_SUPERVISION_DISCHARGE_RECORD_VIEW_BUILDER.address,
                US_TN_SUSPENSION_OF_DIRECT_SUPERVISION_RECORD_VIEW_BUILDER.address,
                US_TN_TRANSFER_TO_COMPLIANT_REPORTING_2025_POLICY_RECORD_VIEW_BUILDER.address,
                US_TN_TRANSFER_TO_COMPLIANT_REPORTING_RECORD_VIEW_BUILDER.address,
            },
            SENTENCES_PREPROCESSED_VIEW_BUILDER.address: {
                US_TN_NOT_ON_COMMUNITY_SUPERVISION_FOR_LIFE_VIEW_BUILDER.address,
                US_TN_NOT_ON_LIFE_SENTENCE_OR_LIFETIME_SUPERVISION_VIEW_BUILDER.address,
                US_TN_NOT_SERVING_INELIGIBLE_CR_OFFENSE_VIEW_BUILDER.address,
                US_TN_NOT_SERVING_INELIGIBLE_CR_OFFENSE_POLICY_B_VIEW_BUILDER.address,
                US_TN_NOT_SERVING_UNKNOWN_CR_OFFENSE_VIEW_BUILDER.address,
                US_TN_ANNUAL_RECLASSIFICATION_REVIEW_RECORD_VIEW_BUILDER.address,
                US_TN_CUSTODY_LEVEL_DOWNGRADE_RECORD_VIEW_BUILDER.address,
                US_TN_FULL_TERM_SUPERVISION_DISCHARGE_RECORD_VIEW_BUILDER.address,
                US_TN_INITIAL_CLASSIFICATION_REVIEW_RECORD_VIEW_BUILDER.address,
                US_TN_SUSPENSION_OF_DIRECT_SUPERVISION_RECORD_VIEW_BUILDER.address,
                US_TN_TRANSFER_TO_COMPLIANT_REPORTING_2025_POLICY_RECORD_VIEW_BUILDER.address,
                US_TN_TRANSFER_TO_COMPLIANT_REPORTING_RECORD_VIEW_BUILDER.address,
            },
            SUPERVISION_PROJECTED_COMPLETION_DATE_SPANS_VIEW_BUILDER.address: {
                CLIENT_RECORD_VIEW_BUILDER.address,
            },
            BigQueryAddress(
                dataset_id=normalized_state_dataset_for_state_code(StateCode.US_TN),
                table_id=StateIncarcerationSentence.get_table_id(),
            ): {
                CLIENT_RECORD_VIEW_BUILDER.address,
            },
            BigQueryAddress(
                dataset_id=normalized_state_dataset_for_state_code(StateCode.US_TN),
                table_id=StateSupervisionSentence.get_table_id(),
            ): {
                CLIENT_RECORD_VIEW_BUILDER.address,
            },
        },
        SUPERVISION_OFFICER_METRICS_VIEW_BUILDER.address: {
            SENTENCE_DEADLINE_SPANS_VIEW_BUILDER.address: {
                INCARCERATION_RELEASE_VIEW_BUILDER.address,
                SUPERVISION_RELEASE_VIEW_BUILDER.address,
                SENTENCE_SPAN_VIEW_BUILDER.address,
            },
            INCARCERATION_PROJECTED_COMPLETION_DATE_SPANS_VIEW_BUILDER.address: {
                RESIDENT_RECORD_VIEW_BUILDER.address,
            },
            SENTENCE_SPANS_VIEW_BUILDER.address: {
                INCARCERATION_RELEASE_VIEW_BUILDER.address,
                SUPERVISION_RELEASE_VIEW_BUILDER.address,
                SENTENCE_SPAN_VIEW_BUILDER.address,
                US_TN_NOT_SERVING_INELIGIBLE_CR_OFFENSE_VIEW_BUILDER.address,
                US_TN_NOT_SERVING_INELIGIBLE_CR_OFFENSE_POLICY_B_VIEW_BUILDER.address,
                US_TN_NOT_SERVING_UNKNOWN_CR_OFFENSE_VIEW_BUILDER.address,
                US_TN_FULL_TERM_SUPERVISION_DISCHARGE_RECORD_VIEW_BUILDER.address,
                US_TN_SUSPENSION_OF_DIRECT_SUPERVISION_RECORD_VIEW_BUILDER.address,
                US_TN_TRANSFER_TO_COMPLIANT_REPORTING_2025_POLICY_RECORD_VIEW_BUILDER.address,
                US_TN_TRANSFER_TO_COMPLIANT_REPORTING_RECORD_VIEW_BUILDER.address,
            },
            SENTENCES_PREPROCESSED_VIEW_BUILDER.address: {
                SENTENCE_SPAN_VIEW_BUILDER.address,
                US_TN_NOT_ON_COMMUNITY_SUPERVISION_FOR_LIFE_VIEW_BUILDER.address,
                US_TN_NOT_ON_LIFE_SENTENCE_OR_LIFETIME_SUPERVISION_VIEW_BUILDER.address,
                US_TN_NOT_SERVING_INELIGIBLE_CR_OFFENSE_VIEW_BUILDER.address,
                US_TN_NOT_SERVING_INELIGIBLE_CR_OFFENSE_POLICY_B_VIEW_BUILDER.address,
                US_TN_NOT_SERVING_UNKNOWN_CR_OFFENSE_VIEW_BUILDER.address,
                US_TN_ANNUAL_RECLASSIFICATION_REVIEW_RECORD_VIEW_BUILDER.address,
                US_TN_CUSTODY_LEVEL_DOWNGRADE_RECORD_VIEW_BUILDER.address,
                US_TN_FULL_TERM_SUPERVISION_DISCHARGE_RECORD_VIEW_BUILDER.address,
                US_TN_INITIAL_CLASSIFICATION_REVIEW_RECORD_VIEW_BUILDER.address,
                US_TN_SUSPENSION_OF_DIRECT_SUPERVISION_RECORD_VIEW_BUILDER.address,
                US_TN_TRANSFER_TO_COMPLIANT_REPORTING_2025_POLICY_RECORD_VIEW_BUILDER.address,
                US_TN_TRANSFER_TO_COMPLIANT_REPORTING_RECORD_VIEW_BUILDER.address,
            },
            SUPERVISION_PROJECTED_COMPLETION_DATE_SPANS_VIEW_BUILDER.address: {
                CLIENT_RECORD_VIEW_BUILDER.address,
            },
            BigQueryAddress(
                dataset_id=normalized_state_dataset_for_state_code(StateCode.US_TN),
                table_id=StateIncarcerationSentence.get_table_id(),
            ): {
                CLIENT_RECORD_VIEW_BUILDER.address,
            },
            BigQueryAddress(
                dataset_id=normalized_state_dataset_for_state_code(StateCode.US_TN),
                table_id=StateSupervisionSentence.get_table_id(),
            ): {
                CLIENT_RECORD_VIEW_BUILDER.address,
            },
        },
        METRIC_BENCHMARKS_VIEW_BUILDER.address: {
            SENTENCE_DEADLINE_SPANS_VIEW_BUILDER.address: {
                INCARCERATION_RELEASE_VIEW_BUILDER.address,
                SUPERVISION_RELEASE_VIEW_BUILDER.address,
                SENTENCE_SPAN_VIEW_BUILDER.address,
            },
            INCARCERATION_PROJECTED_COMPLETION_DATE_SPANS_VIEW_BUILDER.address: {
                RESIDENT_RECORD_VIEW_BUILDER.address,
            },
            SENTENCE_SPANS_VIEW_BUILDER.address: {
                INCARCERATION_RELEASE_VIEW_BUILDER.address,
                SUPERVISION_RELEASE_VIEW_BUILDER.address,
                SENTENCE_SPAN_VIEW_BUILDER.address,
                US_TN_NOT_SERVING_INELIGIBLE_CR_OFFENSE_VIEW_BUILDER.address,
                US_TN_NOT_SERVING_INELIGIBLE_CR_OFFENSE_POLICY_B_VIEW_BUILDER.address,
                US_TN_NOT_SERVING_UNKNOWN_CR_OFFENSE_VIEW_BUILDER.address,
                US_TN_FULL_TERM_SUPERVISION_DISCHARGE_RECORD_VIEW_BUILDER.address,
                US_TN_SUSPENSION_OF_DIRECT_SUPERVISION_RECORD_VIEW_BUILDER.address,
                US_TN_TRANSFER_TO_COMPLIANT_REPORTING_2025_POLICY_RECORD_VIEW_BUILDER.address,
                US_TN_TRANSFER_TO_COMPLIANT_REPORTING_RECORD_VIEW_BUILDER.address,
            },
            SENTENCES_PREPROCESSED_VIEW_BUILDER.address: {
                SENTENCE_SPAN_VIEW_BUILDER.address,
                US_TN_NOT_ON_COMMUNITY_SUPERVISION_FOR_LIFE_VIEW_BUILDER.address,
                US_TN_NOT_ON_LIFE_SENTENCE_OR_LIFETIME_SUPERVISION_VIEW_BUILDER.address,
                US_TN_NOT_SERVING_INELIGIBLE_CR_OFFENSE_VIEW_BUILDER.address,
                US_TN_NOT_SERVING_INELIGIBLE_CR_OFFENSE_POLICY_B_VIEW_BUILDER.address,
                US_TN_NOT_SERVING_UNKNOWN_CR_OFFENSE_VIEW_BUILDER.address,
                US_TN_ANNUAL_RECLASSIFICATION_REVIEW_RECORD_VIEW_BUILDER.address,
                US_TN_CUSTODY_LEVEL_DOWNGRADE_RECORD_VIEW_BUILDER.address,
                US_TN_FULL_TERM_SUPERVISION_DISCHARGE_RECORD_VIEW_BUILDER.address,
                US_TN_INITIAL_CLASSIFICATION_REVIEW_RECORD_VIEW_BUILDER.address,
                US_TN_SUSPENSION_OF_DIRECT_SUPERVISION_RECORD_VIEW_BUILDER.address,
                US_TN_TRANSFER_TO_COMPLIANT_REPORTING_2025_POLICY_RECORD_VIEW_BUILDER.address,
                US_TN_TRANSFER_TO_COMPLIANT_REPORTING_RECORD_VIEW_BUILDER.address,
            },
            SUPERVISION_PROJECTED_COMPLETION_DATE_SPANS_VIEW_BUILDER.address: {
                CLIENT_RECORD_VIEW_BUILDER.address,
            },
            BigQueryAddress(
                dataset_id=normalized_state_dataset_for_state_code(StateCode.US_TN),
                table_id=StateIncarcerationSentence.get_table_id(),
            ): {
                CLIENT_RECORD_VIEW_BUILDER.address,
            },
            BigQueryAddress(
                dataset_id=normalized_state_dataset_for_state_code(StateCode.US_TN),
                table_id=StateSupervisionSentence.get_table_id(),
            ): {
                CLIENT_RECORD_VIEW_BUILDER.address,
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
    "WORKFLOWS_FIRESTORE": {
        CLIENT_RECORD_VIEW_BUILDER.address: {
            SUPERVISION_PROJECTED_COMPLETION_DATE_SPANS_VIEW_BUILDER.address: {
                CLIENT_RECORD_VIEW_BUILDER.address,
            },
            BigQueryAddress(
                dataset_id=normalized_state_dataset_for_state_code(StateCode.US_TN),
                table_id=StateIncarcerationSentence.get_table_id(),
            ): {
                CLIENT_RECORD_VIEW_BUILDER.address,
            },
            BigQueryAddress(
                dataset_id=normalized_state_dataset_for_state_code(StateCode.US_TN),
                table_id=StateSupervisionSentence.get_table_id(),
            ): {
                CLIENT_RECORD_VIEW_BUILDER.address,
            },
        },
        RESIDENT_RECORD_VIEW_BUILDER.address: {
            INCARCERATION_PROJECTED_COMPLETION_DATE_SPANS_VIEW_BUILDER.address: {
                RESIDENT_RECORD_VIEW_BUILDER.address,
            },
        },
        US_TN_SUSPENSION_OF_DIRECT_SUPERVISION_RECORD_VIEW_BUILDER.address: {
            SENTENCE_SPANS_VIEW_BUILDER.address: {
                US_TN_SUSPENSION_OF_DIRECT_SUPERVISION_RECORD_VIEW_BUILDER.address,
            },
            SENTENCES_PREPROCESSED_VIEW_BUILDER.address: {
                US_TN_NOT_ON_COMMUNITY_SUPERVISION_FOR_LIFE_VIEW_BUILDER.address,
                US_TN_SUSPENSION_OF_DIRECT_SUPERVISION_RECORD_VIEW_BUILDER.address,
            },
        },
        US_TN_TRANSFER_TO_COMPLIANT_REPORTING_2025_POLICY_RECORD_VIEW_BUILDER.address: {
            SENTENCE_SPANS_VIEW_BUILDER.address: {
                US_TN_NOT_SERVING_INELIGIBLE_CR_OFFENSE_POLICY_B_VIEW_BUILDER.address,
                US_TN_TRANSFER_TO_COMPLIANT_REPORTING_2025_POLICY_RECORD_VIEW_BUILDER.address,
            },
            SENTENCES_PREPROCESSED_VIEW_BUILDER.address: {
                US_TN_NOT_ON_COMMUNITY_SUPERVISION_FOR_LIFE_VIEW_BUILDER.address,
                US_TN_NOT_SERVING_INELIGIBLE_CR_OFFENSE_POLICY_B_VIEW_BUILDER.address,
                US_TN_TRANSFER_TO_COMPLIANT_REPORTING_2025_POLICY_RECORD_VIEW_BUILDER.address,
            },
        },
        US_TN_FULL_TERM_SUPERVISION_DISCHARGE_RECORD_VIEW_BUILDER.address: {
            SENTENCE_SPANS_VIEW_BUILDER.address: {
                US_TN_FULL_TERM_SUPERVISION_DISCHARGE_RECORD_VIEW_BUILDER.address,
            },
            SENTENCES_PREPROCESSED_VIEW_BUILDER.address: {
                US_TN_NOT_ON_LIFE_SENTENCE_OR_LIFETIME_SUPERVISION_VIEW_BUILDER.address,
                US_TN_FULL_TERM_SUPERVISION_DISCHARGE_RECORD_VIEW_BUILDER.address,
            },
        },
        US_TN_TRANSFER_TO_COMPLIANT_REPORTING_RECORD_VIEW_BUILDER.address: {
            SENTENCE_SPANS_VIEW_BUILDER.address: {
                US_TN_NOT_SERVING_INELIGIBLE_CR_OFFENSE_VIEW_BUILDER.address,
                US_TN_NOT_SERVING_UNKNOWN_CR_OFFENSE_VIEW_BUILDER.address,
                US_TN_TRANSFER_TO_COMPLIANT_REPORTING_RECORD_VIEW_BUILDER.address,
            },
            SENTENCES_PREPROCESSED_VIEW_BUILDER.address: {
                US_TN_NOT_ON_LIFE_SENTENCE_OR_LIFETIME_SUPERVISION_VIEW_BUILDER.address,
                US_TN_NOT_SERVING_INELIGIBLE_CR_OFFENSE_VIEW_BUILDER.address,
                US_TN_NOT_SERVING_UNKNOWN_CR_OFFENSE_VIEW_BUILDER.address,
                US_TN_TRANSFER_TO_COMPLIANT_REPORTING_RECORD_VIEW_BUILDER.address,
            },
        },
        US_TN_INITIAL_CLASSIFICATION_REVIEW_RECORD_VIEW_BUILDER.address: {
            SENTENCES_PREPROCESSED_VIEW_BUILDER.address: {
                US_TN_INITIAL_CLASSIFICATION_REVIEW_RECORD_VIEW_BUILDER.address,
            },
        },
        US_TN_ANNUAL_RECLASSIFICATION_REVIEW_RECORD_VIEW_BUILDER.address: {
            SENTENCES_PREPROCESSED_VIEW_BUILDER.address: {
                US_TN_ANNUAL_RECLASSIFICATION_REVIEW_RECORD_VIEW_BUILDER.address,
            },
        },
        US_TN_CUSTODY_LEVEL_DOWNGRADE_RECORD_VIEW_BUILDER.address: {
            SENTENCES_PREPROCESSED_VIEW_BUILDER.address: {
                US_TN_CUSTODY_LEVEL_DOWNGRADE_RECORD_VIEW_BUILDER.address,
            },
        },
    },
}
