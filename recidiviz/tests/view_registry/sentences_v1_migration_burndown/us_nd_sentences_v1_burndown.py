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
"""US_ND exemptions for deprecated sentence v1 view references in product views."""

from recidiviz.big_query.big_query_address import BigQueryAddress
from recidiviz.calculator.query.state.views.analyst_data.us_nd.us_nd_early_discharge_sessions_preprocessing import (
    US_ND_EARLY_DISCHARGE_SESSIONS_PREPROCESSING_VIEW_BUILDER,
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
from recidiviz.calculator.query.state.views.outliers.metric_benchmarks import (
    METRIC_BENCHMARKS_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.outliers.supervision_client_events import (
    SUPERVISION_CLIENT_EVENTS_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.outliers.supervision_officer_metrics import (
    SUPERVISION_OFFICER_METRICS_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.public_dashboard.incarceration.community_corrections_population_by_facility_by_demographics import (
    COMMUNITY_CORRECTIONS_POPULATION_BY_FACILITY_BY_DEMOGRAPHICS_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.public_dashboard.incarceration.incarceration_population_by_admission_reason import (
    INCARCERATION_POPULATION_BY_ADMISSION_REASON_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.public_dashboard.incarceration.incarceration_population_by_facility_by_demographics import (
    INCARCERATION_POPULATION_BY_FACILITY_BY_DEMOGRAPHICS_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.public_dashboard.sentencing.sentence_type_by_district_by_demographics import (
    SENTENCE_TYPE_BY_DISTRICT_BY_DEMOGRAPHICS_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.public_dashboard.supervision.supervision_population_by_district_by_demographics import (
    SUPERVISION_POPULATION_BY_DISTRICT_BY_DEMOGRAPHICS_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.public_dashboard.supervision.supervision_terminations_by_month import (
    SUPERVISION_TERMINATIONS_BY_MONTH_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.public_dashboard.supervision.supervision_terminations_by_period_by_demographics import (
    SUPERVISION_TERMINATIONS_BY_PERIOD_BY_DEMOGRAPHICS_VIEW_BUILDER,
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
from recidiviz.calculator.query.state.views.shared_metric.single_day_incarceration_population_for_spotlight import (
    SINGLE_DAY_INCARCERATION_POPULATION_FOR_SPOTLIGHT_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.shared_metric.single_day_supervision_population_for_spotlight import (
    SINGLE_DAY_SUPERVISION_POPULATION_FOR_SPOTLIGHT_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.shared_metric.supervision_terminations_for_spotlight import (
    SUPERVISION_TERMINATIONS_FOR_SPOTLIGHT_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.workflows.firestore.client_record import (
    CLIENT_RECORD_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.workflows.firestore.resident_record import (
    RESIDENT_RECORD_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.workflows.firestore.us_nd_complete_discharge_early_from_supervision_record import (
    US_ND_COMPLETE_DISCHARGE_EARLY_FROM_SUPERVISION_RECORD_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.workflows.firestore.us_nd_transfer_to_atp_form_record import (
    US_ND_TRANSFER_TO_ATP_FORM_RECORD_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.workflows.firestore.us_nd_transfer_to_minimum_facility_form_record import (
    US_ND_TRANSFER_TO_MINIMUM_FACILITY_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.workflows.us_nd.resident_record_incarceration_cases_with_dates import (
    US_ND_RESIDENT_RECORD_INCARCERATION_CASES_WITH_DATES_VIEW_BUILDER,
)
from recidiviz.common.constants.states import StateCode
from recidiviz.persistence.entity.state.entities import (
    StateCharge,
    StateSupervisionSentence,
)
from recidiviz.pipelines.ingest.dataset_config import (
    normalized_state_dataset_for_state_code,
)
from recidiviz.task_eligibility.criteria.general.incarceration_past_half_full_term_release_date import (
    VIEW_BUILDER as INCARCERATION_PAST_HALF_FULL_TERM_RELEASE_DATE_VIEW_BUILDER,
)
from recidiviz.task_eligibility.criteria.general.incarceration_within_1_year_of_full_term_completion_date import (
    VIEW_BUILDER as INCARCERATION_WITHIN_1_YEAR_OF_FULL_TERM_COMPLETION_DATE_VIEW_BUILDER,
)
from recidiviz.task_eligibility.criteria.general.incarceration_within_3_months_of_full_term_completion_date import (
    VIEW_BUILDER as INCARCERATION_WITHIN_3_MONTHS_OF_FULL_TERM_COMPLETION_DATE_VIEW_BUILDER,
)
from recidiviz.task_eligibility.criteria.general.incarceration_within_42_months_of_full_term_completion_date import (
    VIEW_BUILDER as INCARCERATION_WITHIN_42_MONTHS_OF_FULL_TERM_COMPLETION_DATE_VIEW_BUILDER,
)
from recidiviz.task_eligibility.criteria.general.supervision_early_discharge_before_full_term_completion_date import (
    VIEW_BUILDER as SUPERVISION_EARLY_DISCHARGE_BEFORE_FULL_TERM_COMPLETION_DATE_VIEW_BUILDER,
)

# For each US_ND metric export, for each product view in that export, a mapping of
#   deprecated v1 sentences views that are a) referenced directly without going through
#   sentence_sessions/sentence_sessions_v2_all and are b) part of the ancestor graph of
#   this product view, with all places they are referenced.
US_ND_SENTENCE_V1_PRODUCT_USAGE_EXEMPTIONS: dict[
    str, dict[BigQueryAddress, dict[BigQueryAddress, set[BigQueryAddress]]]
] = {
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
    "INSIGHTS": {
        SUPERVISION_CLIENT_EVENTS_VIEW_BUILDER.address: {
            INCARCERATION_PROJECTED_COMPLETION_DATE_SPANS_VIEW_BUILDER.address: {
                INCARCERATION_WITHIN_1_YEAR_OF_FULL_TERM_COMPLETION_DATE_VIEW_BUILDER.address,
                INCARCERATION_WITHIN_3_MONTHS_OF_FULL_TERM_COMPLETION_DATE_VIEW_BUILDER.address,
                INCARCERATION_WITHIN_42_MONTHS_OF_FULL_TERM_COMPLETION_DATE_VIEW_BUILDER.address,
                US_ND_RESIDENT_RECORD_INCARCERATION_CASES_WITH_DATES_VIEW_BUILDER.address,
            },
            SENTENCE_SPANS_VIEW_BUILDER.address: {
                INCARCERATION_PAST_HALF_FULL_TERM_RELEASE_DATE_VIEW_BUILDER.address,
            },
            SENTENCES_PREPROCESSED_VIEW_BUILDER.address: {
                INCARCERATION_PAST_HALF_FULL_TERM_RELEASE_DATE_VIEW_BUILDER.address,
            },
            SUPERVISION_PROJECTED_COMPLETION_DATE_SPANS_VIEW_BUILDER.address: {
                SUPERVISION_EARLY_DISCHARGE_BEFORE_FULL_TERM_COMPLETION_DATE_VIEW_BUILDER.address,
                CLIENT_RECORD_VIEW_BUILDER.address,
            },
            BigQueryAddress(
                dataset_id=normalized_state_dataset_for_state_code(StateCode.US_ND),
                table_id=StateCharge.get_table_id(),
            ): {
                US_ND_COMPLETE_DISCHARGE_EARLY_FROM_SUPERVISION_RECORD_VIEW_BUILDER.address,
            },
            BigQueryAddress(
                dataset_id=normalized_state_dataset_for_state_code(StateCode.US_ND),
                table_id=StateSupervisionSentence.get_table_id(),
            ): {
                US_ND_EARLY_DISCHARGE_SESSIONS_PREPROCESSING_VIEW_BUILDER.address,
                US_ND_COMPLETE_DISCHARGE_EARLY_FROM_SUPERVISION_RECORD_VIEW_BUILDER.address,
            },
        },
        SUPERVISION_OFFICER_METRICS_VIEW_BUILDER.address: {
            INCARCERATION_PROJECTED_COMPLETION_DATE_SPANS_VIEW_BUILDER.address: {
                INCARCERATION_WITHIN_1_YEAR_OF_FULL_TERM_COMPLETION_DATE_VIEW_BUILDER.address,
                INCARCERATION_WITHIN_3_MONTHS_OF_FULL_TERM_COMPLETION_DATE_VIEW_BUILDER.address,
                INCARCERATION_WITHIN_42_MONTHS_OF_FULL_TERM_COMPLETION_DATE_VIEW_BUILDER.address,
                US_ND_RESIDENT_RECORD_INCARCERATION_CASES_WITH_DATES_VIEW_BUILDER.address,
            },
            SENTENCE_SPANS_VIEW_BUILDER.address: {
                INCARCERATION_PAST_HALF_FULL_TERM_RELEASE_DATE_VIEW_BUILDER.address,
            },
            SENTENCES_PREPROCESSED_VIEW_BUILDER.address: {
                INCARCERATION_PAST_HALF_FULL_TERM_RELEASE_DATE_VIEW_BUILDER.address,
            },
            SUPERVISION_PROJECTED_COMPLETION_DATE_SPANS_VIEW_BUILDER.address: {
                SUPERVISION_EARLY_DISCHARGE_BEFORE_FULL_TERM_COMPLETION_DATE_VIEW_BUILDER.address,
                CLIENT_RECORD_VIEW_BUILDER.address,
            },
            BigQueryAddress(
                dataset_id=normalized_state_dataset_for_state_code(StateCode.US_ND),
                table_id=StateCharge.get_table_id(),
            ): {
                US_ND_COMPLETE_DISCHARGE_EARLY_FROM_SUPERVISION_RECORD_VIEW_BUILDER.address,
            },
            BigQueryAddress(
                dataset_id=normalized_state_dataset_for_state_code(StateCode.US_ND),
                table_id=StateSupervisionSentence.get_table_id(),
            ): {
                US_ND_EARLY_DISCHARGE_SESSIONS_PREPROCESSING_VIEW_BUILDER.address,
                US_ND_COMPLETE_DISCHARGE_EARLY_FROM_SUPERVISION_RECORD_VIEW_BUILDER.address,
            },
        },
        METRIC_BENCHMARKS_VIEW_BUILDER.address: {
            INCARCERATION_PROJECTED_COMPLETION_DATE_SPANS_VIEW_BUILDER.address: {
                INCARCERATION_WITHIN_1_YEAR_OF_FULL_TERM_COMPLETION_DATE_VIEW_BUILDER.address,
                INCARCERATION_WITHIN_3_MONTHS_OF_FULL_TERM_COMPLETION_DATE_VIEW_BUILDER.address,
                INCARCERATION_WITHIN_42_MONTHS_OF_FULL_TERM_COMPLETION_DATE_VIEW_BUILDER.address,
                US_ND_RESIDENT_RECORD_INCARCERATION_CASES_WITH_DATES_VIEW_BUILDER.address,
            },
            SENTENCE_SPANS_VIEW_BUILDER.address: {
                INCARCERATION_PAST_HALF_FULL_TERM_RELEASE_DATE_VIEW_BUILDER.address,
            },
            SENTENCES_PREPROCESSED_VIEW_BUILDER.address: {
                INCARCERATION_PAST_HALF_FULL_TERM_RELEASE_DATE_VIEW_BUILDER.address,
            },
            SUPERVISION_PROJECTED_COMPLETION_DATE_SPANS_VIEW_BUILDER.address: {
                SUPERVISION_EARLY_DISCHARGE_BEFORE_FULL_TERM_COMPLETION_DATE_VIEW_BUILDER.address,
                CLIENT_RECORD_VIEW_BUILDER.address,
            },
            BigQueryAddress(
                dataset_id=normalized_state_dataset_for_state_code(StateCode.US_ND),
                table_id=StateCharge.get_table_id(),
            ): {
                US_ND_COMPLETE_DISCHARGE_EARLY_FROM_SUPERVISION_RECORD_VIEW_BUILDER.address,
            },
            BigQueryAddress(
                dataset_id=normalized_state_dataset_for_state_code(StateCode.US_ND),
                table_id=StateSupervisionSentence.get_table_id(),
            ): {
                US_ND_EARLY_DISCHARGE_SESSIONS_PREPROCESSING_VIEW_BUILDER.address,
                US_ND_COMPLETE_DISCHARGE_EARLY_FROM_SUPERVISION_RECORD_VIEW_BUILDER.address,
            },
        },
    },
    "PUBLIC_DASHBOARD": {
        SUPERVISION_TERMINATIONS_BY_MONTH_VIEW_BUILDER.address: {
            SENTENCE_IMPOSED_GROUP_SUMMARY_VIEW_BUILDER.address: {
                SUPERVISION_TERMINATIONS_FOR_SPOTLIGHT_VIEW_BUILDER.address,
            },
            SENTENCE_SPANS_VIEW_BUILDER.address: {
                SUPERVISION_TERMINATIONS_FOR_SPOTLIGHT_VIEW_BUILDER.address,
            },
        },
        SUPERVISION_TERMINATIONS_BY_PERIOD_BY_DEMOGRAPHICS_VIEW_BUILDER.address: {
            SENTENCE_IMPOSED_GROUP_SUMMARY_VIEW_BUILDER.address: {
                SUPERVISION_TERMINATIONS_FOR_SPOTLIGHT_VIEW_BUILDER.address,
            },
            SENTENCE_SPANS_VIEW_BUILDER.address: {
                SUPERVISION_TERMINATIONS_FOR_SPOTLIGHT_VIEW_BUILDER.address,
            },
        },
        COMMUNITY_CORRECTIONS_POPULATION_BY_FACILITY_BY_DEMOGRAPHICS_VIEW_BUILDER.address: {
            COMPARTMENT_SESSIONS_CLOSEST_SENTENCE_IMPOSED_GROUP_VIEW_BUILDER.address: {
                SINGLE_DAY_INCARCERATION_POPULATION_FOR_SPOTLIGHT_VIEW_BUILDER.address,
            },
            SENTENCE_IMPOSED_GROUP_SUMMARY_VIEW_BUILDER.address: {
                SINGLE_DAY_INCARCERATION_POPULATION_FOR_SPOTLIGHT_VIEW_BUILDER.address,
            },
        },
        INCARCERATION_POPULATION_BY_ADMISSION_REASON_VIEW_BUILDER.address: {
            COMPARTMENT_SESSIONS_CLOSEST_SENTENCE_IMPOSED_GROUP_VIEW_BUILDER.address: {
                SINGLE_DAY_INCARCERATION_POPULATION_FOR_SPOTLIGHT_VIEW_BUILDER.address,
            },
            SENTENCE_IMPOSED_GROUP_SUMMARY_VIEW_BUILDER.address: {
                SINGLE_DAY_INCARCERATION_POPULATION_FOR_SPOTLIGHT_VIEW_BUILDER.address,
            },
        },
        INCARCERATION_POPULATION_BY_FACILITY_BY_DEMOGRAPHICS_VIEW_BUILDER.address: {
            COMPARTMENT_SESSIONS_CLOSEST_SENTENCE_IMPOSED_GROUP_VIEW_BUILDER.address: {
                SINGLE_DAY_INCARCERATION_POPULATION_FOR_SPOTLIGHT_VIEW_BUILDER.address,
            },
            SENTENCE_IMPOSED_GROUP_SUMMARY_VIEW_BUILDER.address: {
                SINGLE_DAY_INCARCERATION_POPULATION_FOR_SPOTLIGHT_VIEW_BUILDER.address,
            },
        },
        SENTENCE_TYPE_BY_DISTRICT_BY_DEMOGRAPHICS_VIEW_BUILDER.address: {
            COMPARTMENT_SESSIONS_CLOSEST_SENTENCE_IMPOSED_GROUP_VIEW_BUILDER.address: {
                SINGLE_DAY_INCARCERATION_POPULATION_FOR_SPOTLIGHT_VIEW_BUILDER.address,
                SINGLE_DAY_SUPERVISION_POPULATION_FOR_SPOTLIGHT_VIEW_BUILDER.address,
            },
            SENTENCE_IMPOSED_GROUP_SUMMARY_VIEW_BUILDER.address: {
                SINGLE_DAY_INCARCERATION_POPULATION_FOR_SPOTLIGHT_VIEW_BUILDER.address,
                SINGLE_DAY_SUPERVISION_POPULATION_FOR_SPOTLIGHT_VIEW_BUILDER.address,
            },
            SUPERVISION_PROJECTED_COMPLETION_DATE_SPANS_VIEW_BUILDER.address: {
                SINGLE_DAY_SUPERVISION_POPULATION_FOR_SPOTLIGHT_VIEW_BUILDER.address,
            },
        },
        SUPERVISION_POPULATION_BY_DISTRICT_BY_DEMOGRAPHICS_VIEW_BUILDER.address: {
            COMPARTMENT_SESSIONS_CLOSEST_SENTENCE_IMPOSED_GROUP_VIEW_BUILDER.address: {
                SINGLE_DAY_SUPERVISION_POPULATION_FOR_SPOTLIGHT_VIEW_BUILDER.address,
            },
            SENTENCE_IMPOSED_GROUP_SUMMARY_VIEW_BUILDER.address: {
                SINGLE_DAY_SUPERVISION_POPULATION_FOR_SPOTLIGHT_VIEW_BUILDER.address,
            },
            SUPERVISION_PROJECTED_COMPLETION_DATE_SPANS_VIEW_BUILDER.address: {
                SINGLE_DAY_SUPERVISION_POPULATION_FOR_SPOTLIGHT_VIEW_BUILDER.address,
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
                US_ND_RESIDENT_RECORD_INCARCERATION_CASES_WITH_DATES_VIEW_BUILDER.address,
            },
        },
        US_ND_TRANSFER_TO_MINIMUM_FACILITY_VIEW_BUILDER.address: {
            INCARCERATION_PROJECTED_COMPLETION_DATE_SPANS_VIEW_BUILDER.address: {
                INCARCERATION_WITHIN_3_MONTHS_OF_FULL_TERM_COMPLETION_DATE_VIEW_BUILDER.address,
                INCARCERATION_WITHIN_42_MONTHS_OF_FULL_TERM_COMPLETION_DATE_VIEW_BUILDER.address,
            },
        },
        US_ND_COMPLETE_DISCHARGE_EARLY_FROM_SUPERVISION_RECORD_VIEW_BUILDER.address: {
            SUPERVISION_PROJECTED_COMPLETION_DATE_SPANS_VIEW_BUILDER.address: {
                SUPERVISION_EARLY_DISCHARGE_BEFORE_FULL_TERM_COMPLETION_DATE_VIEW_BUILDER.address,
            },
            BigQueryAddress(
                dataset_id=normalized_state_dataset_for_state_code(StateCode.US_ND),
                table_id=StateCharge.get_table_id(),
            ): {
                US_ND_COMPLETE_DISCHARGE_EARLY_FROM_SUPERVISION_RECORD_VIEW_BUILDER.address,
            },
            BigQueryAddress(
                dataset_id=normalized_state_dataset_for_state_code(StateCode.US_ND),
                table_id=StateSupervisionSentence.get_table_id(),
            ): {
                US_ND_EARLY_DISCHARGE_SESSIONS_PREPROCESSING_VIEW_BUILDER.address,
                US_ND_COMPLETE_DISCHARGE_EARLY_FROM_SUPERVISION_RECORD_VIEW_BUILDER.address,
            },
        },
        US_ND_TRANSFER_TO_ATP_FORM_RECORD_VIEW_BUILDER.address: {
            INCARCERATION_PROJECTED_COMPLETION_DATE_SPANS_VIEW_BUILDER.address: {
                INCARCERATION_WITHIN_1_YEAR_OF_FULL_TERM_COMPLETION_DATE_VIEW_BUILDER.address,
                INCARCERATION_WITHIN_3_MONTHS_OF_FULL_TERM_COMPLETION_DATE_VIEW_BUILDER.address,
            },
        },
    },
    "VITALS": {
        VITALS_SUMMARIES_VIEW_BUILDER.address: {
            INCARCERATION_PROJECTED_COMPLETION_DATE_SPANS_VIEW_BUILDER.address: {
                INCARCERATION_WITHIN_1_YEAR_OF_FULL_TERM_COMPLETION_DATE_VIEW_BUILDER.address,
                INCARCERATION_WITHIN_3_MONTHS_OF_FULL_TERM_COMPLETION_DATE_VIEW_BUILDER.address,
                INCARCERATION_WITHIN_42_MONTHS_OF_FULL_TERM_COMPLETION_DATE_VIEW_BUILDER.address,
            },
            SENTENCES_PREPROCESSED_VIEW_BUILDER.address: {
                INCARCERATION_PAST_HALF_FULL_TERM_RELEASE_DATE_VIEW_BUILDER.address,
            },
            SENTENCE_SPANS_VIEW_BUILDER.address: {
                INCARCERATION_PAST_HALF_FULL_TERM_RELEASE_DATE_VIEW_BUILDER.address
            },
            SUPERVISION_PROJECTED_COMPLETION_DATE_SPANS_VIEW_BUILDER.address: {
                SUPERVISION_EARLY_DISCHARGE_BEFORE_FULL_TERM_COMPLETION_DATE_VIEW_BUILDER.address
            },
            BigQueryAddress(
                dataset_id=normalized_state_dataset_for_state_code(StateCode.US_ND),
                table_id=StateSupervisionSentence.get_table_id(),
            ): {
                US_ND_EARLY_DISCHARGE_SESSIONS_PREPROCESSING_VIEW_BUILDER.address,
            },
        },
        VITALS_TIME_SERIES_VIEW_BUILDER.address: {
            INCARCERATION_PROJECTED_COMPLETION_DATE_SPANS_VIEW_BUILDER.address: {
                INCARCERATION_WITHIN_1_YEAR_OF_FULL_TERM_COMPLETION_DATE_VIEW_BUILDER.address,
                INCARCERATION_WITHIN_3_MONTHS_OF_FULL_TERM_COMPLETION_DATE_VIEW_BUILDER.address,
                INCARCERATION_WITHIN_42_MONTHS_OF_FULL_TERM_COMPLETION_DATE_VIEW_BUILDER.address,
            },
            SENTENCES_PREPROCESSED_VIEW_BUILDER.address: {
                INCARCERATION_PAST_HALF_FULL_TERM_RELEASE_DATE_VIEW_BUILDER.address,
            },
            SENTENCE_SPANS_VIEW_BUILDER.address: {
                INCARCERATION_PAST_HALF_FULL_TERM_RELEASE_DATE_VIEW_BUILDER.address
            },
            SUPERVISION_PROJECTED_COMPLETION_DATE_SPANS_VIEW_BUILDER.address: {
                SUPERVISION_EARLY_DISCHARGE_BEFORE_FULL_TERM_COMPLETION_DATE_VIEW_BUILDER.address
            },
            BigQueryAddress(
                dataset_id=normalized_state_dataset_for_state_code(StateCode.US_ND),
                table_id=StateSupervisionSentence.get_table_id(),
            ): {
                US_ND_EARLY_DISCHARGE_SESSIONS_PREPROCESSING_VIEW_BUILDER.address,
            },
        },
    },
}
