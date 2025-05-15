# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2023 Recidiviz, Inc.
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
"""All views needed for sessions"""
from typing import List

from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.state.views.sessions.absconsion_bench_warrant_sessions import (
    ABSCONSION_BENCH_WARRANT_SESSIONS_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.sessions.admission_start_reason_dedup_priority import (
    ADMISSION_START_REASON_DEDUP_PRIORITY_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.sessions.assessment_level_dedup_priority import (
    ASSESSMENT_LEVEL_DEDUP_PRIORITY_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.sessions.assessment_lsir_responses import (
    ASSESSMENT_LSIR_RESPONSES_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.sessions.assessment_lsir_scoring_key import (
    ASSESSMENT_LSIR_SCORING_KEY_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.sessions.assessment_score_sessions import (
    ASSESSMENT_SCORE_SESSIONS_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.sessions.charges_preprocessed import (
    CHARGES_PREPROCESSED_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.sessions.cohort_month_index import (
    COHORT_MONTH_INDEX_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.sessions.compartment_level_0_super_sessions import (
    COMPARTMENT_LEVEL_0_SUPER_SESSIONS_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.sessions.compartment_level_1_dedup_priority import (
    COMPARTMENT_LEVEL_1_DEDUP_PRIORITY_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.sessions.compartment_level_1_super_sessions import (
    COMPARTMENT_LEVEL_1_SUPER_SESSIONS_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.sessions.compartment_level_2_dedup_priority import (
    COMPARTMENT_LEVEL_2_DEDUP_PRIORITY_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.sessions.compartment_level_2_super_sessions import (
    COMPARTMENT_LEVEL_2_SUPER_SESSIONS_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.sessions.compartment_session_end_reasons import (
    COMPARTMENT_SESSION_END_REASONS_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.sessions.compartment_session_start_reasons import (
    COMPARTMENT_SESSION_START_REASONS_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.sessions.compartment_sessions import (
    COMPARTMENT_SESSIONS_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.sessions.compartment_sessions_closest_sentence_imposed_group import (
    COMPARTMENT_SESSIONS_CLOSEST_SENTENCE_IMPOSED_GROUP_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.sessions.compartment_sub_sessions import (
    COMPARTMENT_SUB_SESSIONS_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.sessions.compartment_sub_sessions_preprocessed import (
    COMPARTMENT_SUB_SESSIONS_PREPROCESSED_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.sessions.consecutive_sentences_preprocessed import (
    CONSECUTIVE_SENTENCES_PREPROCESSED_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.sessions.custodial_authority_sessions import (
    CUSTODIAL_AUTHORITY_SESSIONS_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.sessions.custody_level_dedup_priority import (
    CUSTODY_LEVEL_DEDUP_PRIORITY_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.sessions.custody_level_raw_text_sessions import (
    CUSTODY_LEVEL_RAW_TEXT_SESSIONS_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.sessions.custody_level_sessions import (
    CUSTODY_LEVEL_SESSIONS_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.sessions.dataflow_sessions import (
    DATAFLOW_SESSIONS_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.sessions.dataflow_sessions_deduped_by_system_type import (
    DATAFLOW_SESSIONS_DEDUPED_BY_SYSTEM_TYPE_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.sessions.drug_screens_preprocessed import (
    DRUG_SCREENS_PREPROCESSED_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.sessions.housing_unit_raw_text_sessions import (
    HOUSING_UNIT_SESSIONS_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.sessions.housing_unit_type_collapsed_solitary_sessions import (
    HOUSING_UNIT_TYPE_COLLAPSED_SOLITARY_SESSIONS_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.sessions.housing_unit_type_non_protective_custody_solitary_sessions import (
    HOUSING_UNIT_TYPE_NON_PROTECTIVE_CUSTODY_SOLITARY_SESSIONS_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.sessions.housing_unit_type_sessions import (
    HOUSING_UNIT_TYPE_SESSIONS_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.sessions.incarceration_projected_completion_date_spans import (
    INCARCERATION_PROJECTED_COMPLETION_DATE_SPANS_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.sessions.incarceration_staff_assignment_sessions_preprocessed import (
    INCARCERATION_STAFF_ASSIGNMENT_SESSIONS_PREPROCESSED_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.sessions.incarceration_staff_attribute_sessions import (
    INCARCERATION_STAFF_ATTRIBUTE_SESSIONS_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.sessions.incarceration_staff_caseload_count_spans import (
    INCARCERATION_STAFF_CASELOAD_COUNT_SPANS_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.sessions.incarceration_staff_inferred_location_sessions import (
    INCARCERATION_STAFF_INFERRED_LOCATION_SESSIONS_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.sessions.incarceration_super_sessions import (
    INCARCERATION_SUPER_SESSIONS_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.sessions.justice_impact_sessions import (
    JUSTICE_IMPACT_SESSIONS_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.sessions.location_sessions import (
    LOCATION_SESSIONS_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.sessions.location_type_sessions import (
    LOCATION_TYPE_SESSIONS_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.sessions.parole_board_hearing_decisions import (
    PAROLE_BOARD_HEARING_DECISIONS_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.sessions.parole_board_hearing_sessions import (
    PAROLE_BOARD_HEARING_SESSIONS_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.sessions.person_age_sessions import (
    PERSON_AGE_SESSIONS_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.sessions.person_caseload_location_sessions import (
    PERSON_CASELOAD_LOCATION_SESSIONS_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.sessions.person_demographics import (
    PERSON_DEMOGRAPHICS_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.sessions.prioritized_supervision_sessions import (
    PRIORITIZED_SUPERVISION_SESSIONS_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.sessions.prioritized_supervision_super_sessions import (
    PRIORITIZED_SUPERVISION_SUPER_SESSIONS_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.sessions.reincarceration_sessions_from_sessions import (
    REINCARCERATION_SESSIONS_FROM_SESSIONS_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.sessions.release_termination_reason_dedup_priority import (
    RELEASE_TERMINATION_REASON_DEDUP_PRIORITY_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.sessions.revocation_sessions import (
    REVOCATION_SESSIONS_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.sessions.risk_assessment_score_sessions import (
    RISK_ASSESSMENT_SCORE_SESSIONS_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.sessions.sentence_deadline_spans import (
    SENTENCE_DEADLINE_SPANS_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.sessions.sentence_imposed_group_summary import (
    SENTENCE_IMPOSED_GROUP_SUMMARY_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.sessions.sentence_relationship import (
    SENTENCE_RELATIONSHIP_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.sessions.sentence_spans import (
    SENTENCE_SPANS_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.sessions.sentences_preprocessed import (
    SENTENCES_PREPROCESSED_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.sessions.session_location_names import (
    SESSION_LOCATION_NAMES_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.sessions.state_staff_id_to_legacy_supervising_officer_external_id import (
    STATE_STAFF_ID_TO_LEGACY_SUPERVISING_OFFICER_EXTERNAL_ID_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.sessions.state_staff_role_subtype_dedup_priority import (
    STATE_STAFF_ROLE_SUBTYPE_PRIORITY_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.sessions.supervision_downgrade_sessions import (
    SUPERVISION_DOWNGRADE_SESSIONS_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.sessions.supervision_employment_status_sessions import (
    SUPERVISION_EMPLOYMENT_STATUS_SESSIONS_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.sessions.supervision_level_dedup_priority import (
    SUPERVISION_LEVEL_DEDUP_PRIORITY_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.sessions.supervision_level_raw_text_sessions import (
    SUPERVISION_LEVEL_RAW_TEXT_SESSIONS_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.sessions.supervision_level_sessions import (
    SUPERVISION_LEVEL_SESSIONS_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.sessions.supervision_officer_caseload_count_spans import (
    SUPERVISION_OFFICER_CASELOAD_COUNT_SPANS_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.sessions.supervision_officer_inferred_location_sessions import (
    SUPERVISION_OFFICER_INFERRED_LOCATION_SESSIONS_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.sessions.supervision_officer_or_previous_if_transitional_sessions import (
    SUPERVISION_OFFICER_OR_PREVIOUS_IF_TRANSITIONAL_SESSIONS_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.sessions.supervision_officer_sessions import (
    SUPERVISION_OFFICER_SESSIONS_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.sessions.supervision_projected_completion_date_spans import (
    SUPERVISION_PROJECTED_COMPLETION_DATE_SPANS_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.sessions.supervision_staff_attribute_sessions import (
    SUPERVISION_STAFF_ATTRIBUTE_SESSIONS_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.sessions.supervision_staff_primary_specialized_caseload_type_sessions import (
    SUPERVISION_STAFF_PRIMARY_SPECIALIZED_CASELOAD_TYPE_SESSIONS_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.sessions.supervision_super_sessions import (
    SUPERVISION_SUPER_SESSIONS_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.sessions.supervision_tool_access_sessions import (
    SUPERVISION_TOOL_ACCESS_SESSIONS_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.sessions.supervision_unit_supervisor_sessions import (
    SUPERVISION_UNIT_SUPERVISOR_SESSIONS_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.sessions.supervisor_of_officer_sessions import (
    SUPERVISOR_OF_OFFICER_SESSIONS_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.sessions.system_sessions import (
    SYSTEM_SESSIONS_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.sessions.us_ar.us_ar_non_traditional_bed_sessions_preprocessed import (
    US_AR_NON_TRADITIONAL_BED_SESSIONS_PREPROCESSED_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.sessions.us_co.us_co_incarceration_population_metrics_preprocessed import (
    US_CO_INCARCERATION_POPULATION_METRICS_PREPROCESSED_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.sessions.us_co.us_co_incarceration_sentences_preprocessed import (
    US_CO_INCARCERATION_SENTENCES_PREPROCESSED_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.sessions.us_ix.us_ix_consecutive_sentences_preprocessed import (
    US_IX_CONSECUTIVE_SENTENCES_PREPROCESSED_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.sessions.us_ix.us_ix_drug_screens_preprocessed import (
    US_IX_DRUG_SCREENS_PREPROCESSED_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.sessions.us_ix.us_ix_incarceration_population_metrics_preprocessed import (
    US_IX_INCARCERATION_POPULATION_METRICS_PREPROCESSED_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.sessions.us_ix.us_ix_parole_board_hearing_sessions import (
    US_IX_PAROLE_BOARD_HEARING_SESSIONS_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.sessions.us_ix.us_ix_raw_lsir_assessments import (
    US_IX_RAW_LSIR_ASSESSMENTS_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.sessions.us_me.us_me_consecutive_sentences_preprocessed import (
    US_ME_CONSECUTIVE_SENTENCES_PREPROCESSED_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.sessions.us_me.us_me_incarceration_staff_assignment_sessions_preprocessed import (
    US_ME_INCARCERATION_STAFF_ASSIGNMENT_SESSIONS_PREPROCESSED_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.sessions.us_me.us_me_work_release_sessions_preprocessing import (
    US_ME_WORK_RELEASE_SESSIONS_PREPROCESSING_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.sessions.us_mi.us_mi_facility_housing_unit_type_collapsed_solitary_sessions import (
    US_MI_FACILITY_HOUSING_UNIT_TYPE_COLLAPSED_SOLITARY_SESSIONS_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.sessions.us_mi.us_mi_housing_unit_type_collapsed_solitary_sessions import (
    US_MI_HOUSING_UNIT_TYPE_COLLAPSED_SOLITARY_SESSIONS_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.sessions.us_mo.us_mo_charges_preprocessed import (
    US_MO_CHARGES_PREPROCESSED_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.sessions.us_mo.us_mo_confinement_type_sessions import (
    US_MO_CONFINEMENT_TYPE_SESSIONS_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.sessions.us_mo.us_mo_consecutive_sentences_preprocessed import (
    US_MO_CONSECUTIVE_SENTENCES_PREPROCESSED_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.sessions.us_mo.us_mo_housing_stay_sessions import (
    US_MO_HOUSING_STAY_SESSIONS_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.sessions.us_mo.us_mo_housing_stays_preprocessed import (
    US_MO_HOUSING_STAYS_PREPROCESSED_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.sessions.us_nd.us_nd_consecutive_sentences_preprocessed import (
    US_ND_CONSECUTIVE_SENTENCES_PREPROCESSED_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.sessions.us_nd.us_nd_incarceration_sentences_preprocessed import (
    US_ND_INCARCERATION_SENTENCES_PREPROCESSED_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.sessions.us_nd.us_nd_incarceration_staff_assignment_sessions_preprocessed import (
    US_ND_INCARCERATION_STAFF_ASSIGNMENT_SESSIONS_PREPROCESSED_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.sessions.us_nd.us_nd_raw_lsir_assessments import (
    US_ND_RAW_LSIR_ASSESSMENTS_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.sessions.us_nd.us_nd_work_release_sessions_preprocessing import (
    US_ND_WORK_RELEASE_SESSIONS_PREPROCESSING_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.sessions.us_ne.us_ne_special_condition_compliance_sessions import (
    US_NE_SPECIAL_CONDITION_COMPLIANCE_SESSIONS_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.sessions.us_pa.us_pa_supervision_population_metrics_preprocessed import (
    US_PA_SUPERVISION_POPULATION_METRICS_PREPROCESSED_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.sessions.us_tn.us_tn_consecutive_sentences_preprocessed import (
    US_TN_CONSECUTIVE_SENTENCES_PREPROCESSED_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.sessions.us_tn.us_tn_drug_screens_preprocessed import (
    US_TN_DRUG_SCREENS_PREPROCESSED_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.sessions.us_tn.us_tn_parole_board_hearing_decisions import (
    US_TN_PAROLE_BOARD_HEARING_DECISIONS_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.sessions.us_tn.us_tn_parole_board_hearing_sessions import (
    US_TN_PAROLE_BOARD_HEARING_SESSIONS_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.sessions.us_tn.us_tn_sentences_preprocessed import (
    US_TN_SENTENCES_PREPROCESSED_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.sessions.us_tn.us_tn_supervision_population_metrics_preprocessed import (
    US_TN_SUPERVISION_POPULATION_METRICS_PREPROCESSED_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.sessions.us_ut.us_ut_drug_screens_preprocessed import (
    US_UT_DRUG_SCREENS_PREPROCESSED_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.sessions.violation_responses import (
    VIOLATION_RESPONSES_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.sessions.work_release_sessions import (
    WORK_RELEASE_SESSIONS_VIEW_BUILDER,
)

SESSIONS_VIEW_BUILDERS: List[SimpleBigQueryViewBuilder] = [
    ABSCONSION_BENCH_WARRANT_SESSIONS_VIEW_BUILDER,
    ADMISSION_START_REASON_DEDUP_PRIORITY_VIEW_BUILDER,
    ASSESSMENT_LEVEL_DEDUP_PRIORITY_VIEW_BUILDER,
    ASSESSMENT_LSIR_RESPONSES_VIEW_BUILDER,
    ASSESSMENT_LSIR_SCORING_KEY_VIEW_BUILDER,
    ASSESSMENT_SCORE_SESSIONS_VIEW_BUILDER,
    CHARGES_PREPROCESSED_VIEW_BUILDER,
    COHORT_MONTH_INDEX_VIEW_BUILDER,
    COMPARTMENT_LEVEL_0_SUPER_SESSIONS_VIEW_BUILDER,
    COMPARTMENT_LEVEL_1_DEDUP_PRIORITY_VIEW_BUILDER,
    COMPARTMENT_LEVEL_1_SUPER_SESSIONS_VIEW_BUILDER,
    COMPARTMENT_LEVEL_2_DEDUP_PRIORITY_VIEW_BUILDER,
    COMPARTMENT_LEVEL_2_SUPER_SESSIONS_VIEW_BUILDER,
    COMPARTMENT_SESSIONS_CLOSEST_SENTENCE_IMPOSED_GROUP_VIEW_BUILDER,
    COMPARTMENT_SESSIONS_VIEW_BUILDER,
    COMPARTMENT_SESSION_END_REASONS_VIEW_BUILDER,
    COMPARTMENT_SESSION_START_REASONS_VIEW_BUILDER,
    COMPARTMENT_SUB_SESSIONS_PREPROCESSED_VIEW_BUILDER,
    COMPARTMENT_SUB_SESSIONS_VIEW_BUILDER,
    CONSECUTIVE_SENTENCES_PREPROCESSED_VIEW_BUILDER,
    CUSTODIAL_AUTHORITY_SESSIONS_VIEW_BUILDER,
    CUSTODY_LEVEL_DEDUP_PRIORITY_VIEW_BUILDER,
    CUSTODY_LEVEL_RAW_TEXT_SESSIONS_VIEW_BUILDER,
    CUSTODY_LEVEL_SESSIONS_VIEW_BUILDER,
    DATAFLOW_SESSIONS_VIEW_BUILDER,
    DATAFLOW_SESSIONS_DEDUPED_BY_SYSTEM_TYPE_VIEW_BUILDER,
    DRUG_SCREENS_PREPROCESSED_VIEW_BUILDER,
    HOUSING_UNIT_SESSIONS_VIEW_BUILDER,
    HOUSING_UNIT_TYPE_COLLAPSED_SOLITARY_SESSIONS_VIEW_BUILDER,
    HOUSING_UNIT_TYPE_NON_PROTECTIVE_CUSTODY_SOLITARY_SESSIONS_VIEW_BUILDER,
    HOUSING_UNIT_TYPE_SESSIONS_VIEW_BUILDER,
    INCARCERATION_PROJECTED_COMPLETION_DATE_SPANS_VIEW_BUILDER,
    INCARCERATION_STAFF_ASSIGNMENT_SESSIONS_PREPROCESSED_VIEW_BUILDER,
    INCARCERATION_STAFF_ATTRIBUTE_SESSIONS_VIEW_BUILDER,
    INCARCERATION_STAFF_CASELOAD_COUNT_SPANS_VIEW_BUILDER,
    INCARCERATION_STAFF_INFERRED_LOCATION_SESSIONS_VIEW_BUILDER,
    INCARCERATION_SUPER_SESSIONS_VIEW_BUILDER,
    JUSTICE_IMPACT_SESSIONS_VIEW_BUILDER,
    LOCATION_SESSIONS_VIEW_BUILDER,
    LOCATION_TYPE_SESSIONS_VIEW_BUILDER,
    PAROLE_BOARD_HEARING_DECISIONS_VIEW_BUILDER,
    PAROLE_BOARD_HEARING_SESSIONS_VIEW_BUILDER,
    PERSON_AGE_SESSIONS_VIEW_BUILDER,
    PERSON_CASELOAD_LOCATION_SESSIONS_VIEW_BUILDER,
    PERSON_DEMOGRAPHICS_VIEW_BUILDER,
    PRIORITIZED_SUPERVISION_SESSIONS_VIEW_BUILDER,
    PRIORITIZED_SUPERVISION_SUPER_SESSIONS_VIEW_BUILDER,
    REINCARCERATION_SESSIONS_FROM_SESSIONS_VIEW_BUILDER,
    RELEASE_TERMINATION_REASON_DEDUP_PRIORITY_VIEW_BUILDER,
    REVOCATION_SESSIONS_VIEW_BUILDER,
    RISK_ASSESSMENT_SCORE_SESSIONS_VIEW_BUILDER,
    SENTENCES_PREPROCESSED_VIEW_BUILDER,
    SENTENCE_DEADLINE_SPANS_VIEW_BUILDER,
    SENTENCE_IMPOSED_GROUP_SUMMARY_VIEW_BUILDER,
    SENTENCE_RELATIONSHIP_VIEW_BUILDER,
    SENTENCE_SPANS_VIEW_BUILDER,
    SESSION_LOCATION_NAMES_VIEW_BUILDER,
    STATE_STAFF_ID_TO_LEGACY_SUPERVISING_OFFICER_EXTERNAL_ID_VIEW_BUILDER,
    STATE_STAFF_ROLE_SUBTYPE_PRIORITY_VIEW_BUILDER,
    SUPERVISION_DOWNGRADE_SESSIONS_VIEW_BUILDER,
    SUPERVISION_EMPLOYMENT_STATUS_SESSIONS_VIEW_BUILDER,
    SUPERVISION_LEVEL_DEDUP_PRIORITY_VIEW_BUILDER,
    SUPERVISION_LEVEL_RAW_TEXT_SESSIONS_VIEW_BUILDER,
    SUPERVISION_LEVEL_SESSIONS_VIEW_BUILDER,
    SUPERVISION_OFFICER_CASELOAD_COUNT_SPANS_VIEW_BUILDER,
    SUPERVISION_OFFICER_INFERRED_LOCATION_SESSIONS_VIEW_BUILDER,
    SUPERVISION_OFFICER_SESSIONS_VIEW_BUILDER,
    SUPERVISION_OFFICER_OR_PREVIOUS_IF_TRANSITIONAL_SESSIONS_VIEW_BUILDER,
    SUPERVISION_PROJECTED_COMPLETION_DATE_SPANS_VIEW_BUILDER,
    SUPERVISION_STAFF_ATTRIBUTE_SESSIONS_VIEW_BUILDER,
    SUPERVISION_STAFF_PRIMARY_SPECIALIZED_CASELOAD_TYPE_SESSIONS_VIEW_BUILDER,
    SUPERVISION_SUPER_SESSIONS_VIEW_BUILDER,
    SUPERVISION_TOOL_ACCESS_SESSIONS_VIEW_BUILDER,
    SUPERVISION_UNIT_SUPERVISOR_SESSIONS_VIEW_BUILDER,
    SUPERVISOR_OF_OFFICER_SESSIONS_VIEW_BUILDER,
    SYSTEM_SESSIONS_VIEW_BUILDER,
    US_AR_NON_TRADITIONAL_BED_SESSIONS_PREPROCESSED_VIEW_BUILDER,
    US_CO_INCARCERATION_POPULATION_METRICS_PREPROCESSED_VIEW_BUILDER,
    US_CO_INCARCERATION_SENTENCES_PREPROCESSED_VIEW_BUILDER,
    US_IX_INCARCERATION_POPULATION_METRICS_PREPROCESSED_VIEW_BUILDER,
    US_IX_CONSECUTIVE_SENTENCES_PREPROCESSED_VIEW_BUILDER,
    US_IX_DRUG_SCREENS_PREPROCESSED_VIEW_BUILDER,
    US_IX_PAROLE_BOARD_HEARING_SESSIONS_VIEW_BUILDER,
    US_IX_RAW_LSIR_ASSESSMENTS_VIEW_BUILDER,
    US_ME_CONSECUTIVE_SENTENCES_PREPROCESSED_VIEW_BUILDER,
    US_ME_INCARCERATION_STAFF_ASSIGNMENT_SESSIONS_PREPROCESSED_VIEW_BUILDER,
    US_ME_WORK_RELEASE_SESSIONS_PREPROCESSING_VIEW_BUILDER,
    US_MI_FACILITY_HOUSING_UNIT_TYPE_COLLAPSED_SOLITARY_SESSIONS_VIEW_BUILDER,
    US_MI_HOUSING_UNIT_TYPE_COLLAPSED_SOLITARY_SESSIONS_VIEW_BUILDER,
    US_MO_CHARGES_PREPROCESSED_VIEW_BUILDER,
    US_MO_CONFINEMENT_TYPE_SESSIONS_VIEW_BUILDER,
    US_MO_CONSECUTIVE_SENTENCES_PREPROCESSED_VIEW_BUILDER,
    US_MO_HOUSING_STAYS_PREPROCESSED_VIEW_BUILDER,
    US_MO_HOUSING_STAY_SESSIONS_VIEW_BUILDER,
    US_NE_SPECIAL_CONDITION_COMPLIANCE_SESSIONS_VIEW_BUILDER,
    US_ND_CONSECUTIVE_SENTENCES_PREPROCESSED_VIEW_BUILDER,
    US_ND_INCARCERATION_SENTENCES_PREPROCESSED_VIEW_BUILDER,
    US_ND_INCARCERATION_STAFF_ASSIGNMENT_SESSIONS_PREPROCESSED_VIEW_BUILDER,
    US_ND_RAW_LSIR_ASSESSMENTS_VIEW_BUILDER,
    US_ND_WORK_RELEASE_SESSIONS_PREPROCESSING_VIEW_BUILDER,
    US_PA_SUPERVISION_POPULATION_METRICS_PREPROCESSED_VIEW_BUILDER,
    US_TN_CONSECUTIVE_SENTENCES_PREPROCESSED_VIEW_BUILDER,
    US_TN_DRUG_SCREENS_PREPROCESSED_VIEW_BUILDER,
    US_TN_PAROLE_BOARD_HEARING_DECISIONS_VIEW_BUILDER,
    US_TN_PAROLE_BOARD_HEARING_SESSIONS_VIEW_BUILDER,
    US_TN_SENTENCES_PREPROCESSED_VIEW_BUILDER,
    US_TN_SUPERVISION_POPULATION_METRICS_PREPROCESSED_VIEW_BUILDER,
    US_UT_DRUG_SCREENS_PREPROCESSED_VIEW_BUILDER,
    VIOLATION_RESPONSES_VIEW_BUILDER,
    WORK_RELEASE_SESSIONS_VIEW_BUILDER,
]
