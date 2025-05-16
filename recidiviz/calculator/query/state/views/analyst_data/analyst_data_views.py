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
"""All views needed for analyst data"""
from typing import List

from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.state.views.analyst_data.all_auth0_login_events import (
    ALL_AUTH0_LOGIN_EVENTS_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.analyst_data.all_task_type_eligibility_spans import (
    ALL_TASK_TYPE_ELIGIBILITY_SPANS_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.analyst_data.all_task_type_ineligible_criteria_sessions import (
    ALL_TASK_TYPE_INELIGIBLE_CRITERIA_SESSIONS_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.analyst_data.all_task_type_marked_ineligible_spans import (
    ALL_TASK_TYPE_MARKED_INELIGIBLE_SPANS_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.analyst_data.all_task_type_marked_submitted_spans import (
    ALL_TASK_TYPE_MARKED_SUBMITTED_SPANS_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.analyst_data.consecutive_payments_preprocessed import (
    CONSECUTIVE_PAYMENTS_PREPROCESSED_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.analyst_data.custody_classification_assessment_dates import (
    CUSTODY_CLASSIFICATION_ASSESSMENT_DATES_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.analyst_data.early_discharge_reports_per_officer import (
    EARLY_DISCHARGE_REPORTS_PER_OFFICER_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.analyst_data.early_discharge_sessions import (
    EARLY_DISCHARGE_SESSIONS_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.analyst_data.early_discharge_sessions_with_officer_and_supervisor import (
    EARLY_DISCHARGE_SESSIONS_WITH_OFFICER_AND_SUPERVISOR_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.analyst_data.earned_credit_activity import (
    EARNED_CREDIT_ACTIVITY_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.analyst_data.fines_fees_sessions import (
    FINES_FEES_SESSIONS_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.analyst_data.incarceration_incidents_preprocessed import (
    INCARCERATION_INCIDENTS_PREPROCESSED_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.analyst_data.insights_caseload_category_sessions import (
    INSIGHTS_CASELOAD_CATEGORY_SESSIONS_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.analyst_data.insights_officer_outlier_usage_cohort import (
    INSIGHTS_OFFICER_OUTLIER_USAGE_COHORT_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.analyst_data.insights_provisioned_user_registration_sessions import (
    INSIGHTS_PROVISIONED_USER_REGISTRATION_SESSIONS_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.analyst_data.insights_segment_events import (
    INSIGHTS_SEGMENT_EVENTS_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.analyst_data.insights_supervision_officer_caseload_category_sessions import (
    INSIGHTS_SUPERVISION_OFFICER_CASELOAD_CATEGORY_SESSIONS_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.analyst_data.insights_supervision_officer_outlier_status_archive_sessions import (
    INSIGHTS_SUPERVISION_OFFICER_OUTLIER_STATUS_ARCHIVE_SESSIONS_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.analyst_data.insights_supervisor_outlier_status_archive_sessions import (
    INSIGHTS_SUPERVISOR_OUTLIER_STATUS_ARCHIVE_SESSIONS_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.analyst_data.insights_user_auth0_registrations import (
    INSIGHTS_USER_AUTH0_REGISTRATIONS_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.analyst_data.insights_user_impact_funnel_status_sessions import (
    INSIGHTS_USER_IMPACT_FUNNEL_STATUS_SESSIONS_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.analyst_data.insights_user_person_assignment_sessions import (
    INSIGHTS_USER_PERSON_ASSIGNMENT_SESSIONS_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.analyst_data.invoices_preprocessed import (
    INVOICES_PREPROCESSED_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.analyst_data.number_months_downgrade_and_assessment_due import (
    NUMBER_MONTHS_BETWEEN_CUSTODY_DOWNGRADE_AND_ASSESSMENT_DUE_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.analyst_data.payments_preprocessed import (
    PAYMENTS_PREPROCESSED_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.analyst_data.permanent_exemptions_preprocessed import (
    PERMANENT_EXEMPTIONS_PREPROCESSED_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.analyst_data.population_density_by_supervision_office import (
    POPULATION_DENSITY_BY_SUPERVISION_OFFICE_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.analyst_data.product_roster_archive_sessions import (
    PRODUCT_ROSTER_ARCHIVE_SESSIONS_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.analyst_data.psa_risk_scores import (
    PSA_RISK_SCORES_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.analyst_data.recommended_custody_level_spans import (
    RECOMMENDED_CUSTODY_LEVEL_SPANS_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.analyst_data.session_cohort_reincarceration import (
    SESSION_COHORT_REINCARCERATION_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.analyst_data.supervision_clients_to_officers_ratio_quarterly import (
    SUPERVISION_CLIENTS_TO_OFFICERS_RATIO_QUARTERLY_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.analyst_data.supervision_staff_in_critically_understaffed_location_sessions_preprocessed import (
    SUPERVISION_STAFF_IN_CRITICALLY_UNDERSTAFFED_LOCATION_SESSIONS_PREPROCESSED_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.analyst_data.us_ar.us_ar_ovg_events_preprocessed import (
    US_AR_OVG_EVENTS_PREPROCESSED_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.analyst_data.us_ar.us_ar_ovg_timeline import (
    US_AR_OVG_TIMELINE_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.analyst_data.us_ar.us_ar_resident_approved_visitors_preprocessed import (
    US_AR_RESIDENT_APPROVED_VISITORS_PREPROCESSED_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.analyst_data.us_az.us_az_action_queue import (
    US_AZ_ACTION_QUEUE_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.analyst_data.us_az.us_az_agreement_form_signatures import (
    US_AZ_AGREEMENT_FORM_SIGNATURES_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.analyst_data.us_az.us_az_early_releases_from_incarceration import (
    US_AZ_EARLY_RELEASES_FROM_INCARCERATION_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.analyst_data.us_az.us_az_home_plan_preprocessed import (
    US_AZ_HOME_PLAN_PREPROCESSED_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.analyst_data.us_az.us_az_projected_dates import (
    US_AZ_PROJECTED_DATES_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.analyst_data.us_ca.us_ca_sustainable_housing_status_periods import (
    US_CA_SUSTAINABLE_HOUSING_STATUS_PERIODS_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.analyst_data.us_id.us_id_early_discharge_sessions_preprocessing import (
    US_ID_EARLY_DISCHARGE_SESSIONS_PREPROCESSING_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.analyst_data.us_ix.us_ix_detainer_spans import (
    US_IX_DETAINER_SPANS_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.analyst_data.us_ix.us_ix_early_discharge_sessions_preprocessing import (
    US_IX_EARLY_DISCHARGE_SESSIONS_PREPROCESSING_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.analyst_data.us_ix.us_ix_parole_dates_spans_preprocessing import (
    US_IX_PAROLE_DATES_SPANS_PREPROCESSING_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.analyst_data.us_ix.us_ix_recommended_custody_level_spans import (
    US_IX_RECOMMENDED_CUSTODY_LEVEL_SPANS_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.analyst_data.us_ix.us_ix_sls_q1 import (
    US_IX_SLS_Q1_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.analyst_data.us_ix.us_ix_sls_q2 import (
    US_IX_SLS_Q2_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.analyst_data.us_ix.us_ix_sls_q3 import (
    US_IX_SLS_Q3_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.analyst_data.us_ix.us_ix_sls_q4 import (
    US_IX_SLS_Q4_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.analyst_data.us_ix.us_ix_sls_q5 import (
    US_IX_SLS_Q5_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.analyst_data.us_ix.us_ix_sls_q6 import (
    US_IX_SLS_Q6_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.analyst_data.us_ma.us_ma_earned_credit_activity_preprocessed import (
    US_MA_EARNED_CREDIT_ACTIVITY_PREPROCESSED_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.analyst_data.us_ma.us_ma_person_projected_date_sessions_preprocessed import (
    US_MA_PERSON_PROJECTED_DATE_SESSIONS_PREPROCESSED_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.analyst_data.us_ma.us_ma_projected_dates import (
    US_MA_PROJECTED_DATES_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.analyst_data.us_me.us_me_early_discharge_sessions_preprocessing import (
    US_ME_EARLY_DISCHARGE_SESSIONS_PREPROCESSING_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.analyst_data.us_me.us_me_fines_fees_sessions_preprocessed import (
    US_ME_FINES_FEES_SESSIONS_PREPROCESSED_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.analyst_data.us_me.us_me_invoices_preprocessed import (
    US_ME_INVOICES_PREPROCESSED_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.analyst_data.us_me.us_me_payments_preprocessed import (
    US_ME_PAYMENTS_PREPROCESSED_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.analyst_data.us_me.us_me_sentence_term import (
    US_ME_SENTENCE_TERM_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.analyst_data.us_mi.us_mi_add_in_person_security_classification_committee_review import (
    US_MI_ADD_IN_PERSON_SECURITY_CLASSIFICATION_COMMITTEE_REVIEW_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.analyst_data.us_mi.us_mi_early_discharge_sessions_preprocessing import (
    US_MI_EARLY_DISCHARGE_SESSIONS_PREPROCESSING_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.analyst_data.us_mi.us_mi_security_classification_committee_review import (
    US_MI_SECURITY_CLASSIFICATION_COMMITTEE_REVIEW_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.analyst_data.us_mi.us_mi_supervision_classification_review import (
    US_MI_SUPERVISION_CLASSIFICATION_REVIEW_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.analyst_data.us_mi.us_mi_supervision_level_raw_text_mappings import (
    US_MI_SUPERVISION_LEVEL_RAW_TEXT_MAPPINGS_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.analyst_data.us_mi.us_mi_warden_in_person_security_classification_committee_review import (
    US_MI_WARDEN_IN_PERSON_SECURITY_CLASSIFICATION_COMMITTEE_REVIEW_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.analyst_data.us_mo.us_mo_classification_hearings_preprocessed import (
    US_MO_CLASSIFICATION_HEARINGS_PREPROCESSED_RECORD_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.analyst_data.us_mo.us_mo_mosop_prio_groups import (
    US_MO_MOSOP_PRIO_GROUPS_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.analyst_data.us_mo.us_mo_program_tracks import (
    US_MO_PROGRAM_TRACKS_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.analyst_data.us_mo.us_mo_restrictive_housing_record import (
    US_MO_RESTRICTIVE_HOUSING_RECORD_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.analyst_data.us_mo.us_mo_screeners_preprocessed import (
    US_MO_SCREENERS_PREPROCESSED_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.analyst_data.us_mo.us_mo_sentencing_dates_preprocessed import (
    US_MO_SENTENCING_DATES_PREPROCESSED_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.analyst_data.us_nd.us_nd_early_discharge_sessions_preprocessing import (
    US_ND_EARLY_DISCHARGE_SESSIONS_PREPROCESSING_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.analyst_data.us_or.us_or_earned_discharge_sentence_eligibility_spans import (
    US_OR_EARNED_DISCHARGE_SENTENCE_ELIGIBILITY_SPANS_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.analyst_data.us_or.us_or_funded_sentence import (
    US_OR_FUNDED_SENTENCE_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.analyst_data.us_or.us_or_in_state_sentence import (
    US_OR_IN_STATE_SENTENCE_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.analyst_data.us_or.us_or_no_convictions_since_sentence_start import (
    US_OR_NO_CONVICTIONS_SINCE_SENTENCE_START_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.analyst_data.us_or.us_or_not_conditional_discharge_or_diversion_sentence import (
    US_OR_NOT_CONDITIONAL_DISCHARGE_OR_DIVERSION_SENTENCE_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.analyst_data.us_or.us_or_sentence_imposition_date_eligible import (
    US_OR_SENTENCE_IMPOSITION_DATE_ELIGIBLE_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.analyst_data.us_or.us_or_served_6_months_supervision import (
    US_OR_SERVED_6_MONTHS_SUPERVISION_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.analyst_data.us_or.us_or_served_half_sentence import (
    US_OR_SERVED_HALF_SENTENCE_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.analyst_data.us_or.us_or_statute_eligible import (
    US_OR_STATUTE_ELIGIBLE_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.analyst_data.us_tn.us_tn_caf_q1 import (
    US_TN_CAF_Q1_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.analyst_data.us_tn.us_tn_caf_q2 import (
    US_TN_CAF_Q2_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.analyst_data.us_tn.us_tn_caf_q6 import (
    US_TN_CAF_Q6_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.analyst_data.us_tn.us_tn_caf_q7 import (
    US_TN_CAF_Q7_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.analyst_data.us_tn.us_tn_caf_q8 import (
    US_TN_CAF_Q8_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.analyst_data.us_tn.us_tn_contact_comments_preprocessed import (
    US_TN_CONTACT_COMMENTS_PREPROCESSED_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.analyst_data.us_tn.us_tn_custody_classification_assessment_dates_preprocessed import (
    US_TN_CUSTODY_CLASSIFICATION_ASSESSMENT_DATES_PREPROCESSED_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.analyst_data.us_tn.us_tn_custody_level_sessions_preprocessed import (
    US_TN_CUSTODY_LEVEL_SESSIONS_PREPROCESSED_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.analyst_data.us_tn.us_tn_exemptions_preprocessed import (
    US_TN_EXEMPTIONS_PREPROCESSED_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.analyst_data.us_tn.us_tn_fines_fees_sessions_preprocessed import (
    US_TN_FINES_FEES_SESSIONS_PREPROCESSED_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.analyst_data.us_tn.us_tn_incarceration_incidents_preprocessed import (
    US_TN_INCARCERATION_INCIDENTS_PREPROCESSED_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.analyst_data.us_tn.us_tn_invoices_preprocessed import (
    US_TN_INVOICES_PREPROCESSED_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.analyst_data.us_tn.us_tn_payments_preprocessed import (
    US_TN_PAYMENTS_PREPROCESSED_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.analyst_data.us_tn.us_tn_permanent_exemptions_preprocessed import (
    US_TN_PERMANENT_EXEMPTIONS_PREPROCESSED_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.analyst_data.us_tn.us_tn_prior_record_preprocessed import (
    US_TN_PRIOR_RECORD_PREPROCESSED_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.analyst_data.us_tn.us_tn_recommended_custody_level_spans import (
    US_TN_RECOMMENDED_CUSTODY_LEVEL_SPANS_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.analyst_data.us_tn.us_tn_relevant_contact_codes import (
    US_TN_RELEVANT_CONTACT_CODES_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.analyst_data.us_tn.us_tn_segregation_lists import (
    US_TN_SEGREGATION_LISTS_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.analyst_data.us_tn.us_tn_segregation_stays import (
    US_TN_SEGREGATION_STAYS_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.analyst_data.us_tn.us_tn_supervision_level_raw_text_sessions_inferred import (
    US_TN_SUPERVISION_LEVEL_RAW_TEXT_SESSIONS_INFERRED_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.analyst_data.us_tn.us_tn_zero_tolerance_codes import (
    US_TN_ZERO_TOLERANCE_CODES_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.analyst_data.us_tx.us_tx_contact_cadence_spans import (
    US_TX_CONTACT_CADENCE_SPANS_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.analyst_data.us_tx.us_tx_supervision_staff_in_critically_understaffed_location_sessions_preprocessed import (
    US_TX_SUPERVISION_STAFF_IN_CRITICALLY_UNDERSTAFFED_LOCATION_SESSIONS_PREPROCESSED_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.analyst_data.us_ut.us_ut_early_discharge_sessions_preprocessing import (
    US_UT_EARLY_DISCHARGE_SESSIONS_PREPROCESSING_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.analyst_data.workflows_live_completion_event_types_by_state import (
    WORKFLOWS_LIVE_COMPLETION_EVENT_TYPES_BY_STATE_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.analyst_data.workflows_officer_events import (
    WORKFLOWS_OFFICER_EVENTS_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.analyst_data.workflows_person_events import (
    WORKFLOWS_PERSON_EVENTS_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.analyst_data.workflows_person_impact_funnel_status_sessions import (
    WORKFLOWS_PERSON_IMPACT_FUNNEL_STATUS_SESSIONS_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.analyst_data.workflows_person_marked_ineligible_status_session_details import (
    WORKFLOWS_PERSON_MARKED_INELIGIBLE_STATUS_SESSION_DETAILS_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.analyst_data.workflows_provisioned_user_registration_sessions import (
    WORKFLOWS_PROVISIONED_USER_REGISTRATION_SESSIONS_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.analyst_data.workflows_record_archive_surfaceable_caseload_sessions import (
    WORKFLOWS_RECORD_ARCHIVE_SURFACEABLE_CASELOAD_SESSIONS_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.analyst_data.workflows_record_archive_surfaceable_person_sessions import (
    WORKFLOWS_RECORD_ARCHIVE_SURFACEABLE_PERSON_SESSIONS_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.analyst_data.workflows_user_auth0_registrations import (
    WORKFLOWS_USER_AUTH0_REGISTRATIONS_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.analyst_data.workflows_user_caseload_access_sessions import (
    WORKFLOWS_USER_CASELOAD_ACCESS_SESSIONS_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.analyst_data.workflows_user_person_assignment_sessions import (
    WORKFLOWS_USER_PERSON_ASSIGNMENT_SESSIONS_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.analyst_data.workflows_user_snooze_starts import (
    WORKFLOWS_USER_SNOOZE_STARTS_VIEW_BUILDER,
)

ANALYST_DATA_VIEW_BUILDERS: List[SimpleBigQueryViewBuilder] = [
    ALL_TASK_TYPE_ELIGIBILITY_SPANS_VIEW_BUILDER,
    ALL_TASK_TYPE_INELIGIBLE_CRITERIA_SESSIONS_VIEW_BUILDER,
    ALL_TASK_TYPE_MARKED_INELIGIBLE_SPANS_VIEW_BUILDER,
    ALL_TASK_TYPE_MARKED_SUBMITTED_SPANS_VIEW_BUILDER,
    EARLY_DISCHARGE_SESSIONS_VIEW_BUILDER,
    EARLY_DISCHARGE_SESSIONS_WITH_OFFICER_AND_SUPERVISOR_VIEW_BUILDER,
    EARLY_DISCHARGE_REPORTS_PER_OFFICER_VIEW_BUILDER,
    WORKFLOWS_OFFICER_EVENTS_VIEW_BUILDER,
    WORKFLOWS_PERSON_EVENTS_VIEW_BUILDER,
    POPULATION_DENSITY_BY_SUPERVISION_OFFICE_VIEW_BUILDER,
    PSA_RISK_SCORES_VIEW_BUILDER,
    SESSION_COHORT_REINCARCERATION_VIEW_BUILDER,
    SUPERVISION_CLIENTS_TO_OFFICERS_RATIO_QUARTERLY_VIEW_BUILDER,
    US_AZ_EARLY_RELEASES_FROM_INCARCERATION_VIEW_BUILDER,
    US_ID_EARLY_DISCHARGE_SESSIONS_PREPROCESSING_VIEW_BUILDER,
    US_IX_EARLY_DISCHARGE_SESSIONS_PREPROCESSING_VIEW_BUILDER,
    US_IX_PAROLE_DATES_SPANS_PREPROCESSING_VIEW_BUILDER,
    US_IX_DETAINER_SPANS_VIEW_BUILDER,
    US_IX_RECOMMENDED_CUSTODY_LEVEL_SPANS_VIEW_BUILDER,
    US_IX_SLS_Q1_VIEW_BUILDER,
    US_IX_SLS_Q2_VIEW_BUILDER,
    US_IX_SLS_Q3_VIEW_BUILDER,
    US_IX_SLS_Q4_VIEW_BUILDER,
    US_IX_SLS_Q5_VIEW_BUILDER,
    US_IX_SLS_Q6_VIEW_BUILDER,
    US_ME_EARLY_DISCHARGE_SESSIONS_PREPROCESSING_VIEW_BUILDER,
    US_ME_SENTENCE_TERM_VIEW_BUILDER,
    US_ME_INVOICES_PREPROCESSED_VIEW_BUILDER,
    US_ME_PAYMENTS_PREPROCESSED_VIEW_BUILDER,
    US_ME_FINES_FEES_SESSIONS_PREPROCESSED_VIEW_BUILDER,
    US_MI_EARLY_DISCHARGE_SESSIONS_PREPROCESSING_VIEW_BUILDER,
    US_MI_SUPERVISION_CLASSIFICATION_REVIEW_VIEW_BUILDER,
    US_MI_ADD_IN_PERSON_SECURITY_CLASSIFICATION_COMMITTEE_REVIEW_VIEW_BUILDER,
    US_MI_WARDEN_IN_PERSON_SECURITY_CLASSIFICATION_COMMITTEE_REVIEW_VIEW_BUILDER,
    US_MI_SUPERVISION_LEVEL_RAW_TEXT_MAPPINGS_VIEW_BUILDER,
    US_MI_SECURITY_CLASSIFICATION_COMMITTEE_REVIEW_VIEW_BUILDER,
    US_ND_EARLY_DISCHARGE_SESSIONS_PREPROCESSING_VIEW_BUILDER,
    US_OR_EARNED_DISCHARGE_SENTENCE_ELIGIBILITY_SPANS_VIEW_BUILDER,
    US_OR_FUNDED_SENTENCE_VIEW_BUILDER,
    US_OR_IN_STATE_SENTENCE_VIEW_BUILDER,
    US_OR_NO_CONVICTIONS_SINCE_SENTENCE_START_VIEW_BUILDER,
    US_OR_NOT_CONDITIONAL_DISCHARGE_OR_DIVERSION_SENTENCE_VIEW_BUILDER,
    US_OR_SENTENCE_IMPOSITION_DATE_ELIGIBLE_VIEW_BUILDER,
    US_OR_SERVED_6_MONTHS_SUPERVISION_VIEW_BUILDER,
    US_OR_SERVED_HALF_SENTENCE_VIEW_BUILDER,
    US_OR_STATUTE_ELIGIBLE_VIEW_BUILDER,
    US_TN_ZERO_TOLERANCE_CODES_VIEW_BUILDER,
    US_MA_PROJECTED_DATES_VIEW_BUILDER,
    US_MO_SCREENERS_PREPROCESSED_VIEW_BUILDER,
    US_MO_PROGRAM_TRACKS_VIEW_BUILDER,
    US_MO_RESTRICTIVE_HOUSING_RECORD_VIEW_BUILDER,
    US_MO_MOSOP_PRIO_GROUPS_VIEW_BUILDER,
    US_MO_SENTENCING_DATES_PREPROCESSED_VIEW_BUILDER,
    US_UT_EARLY_DISCHARGE_SESSIONS_PREPROCESSING_VIEW_BUILDER,
    US_TN_PAYMENTS_PREPROCESSED_VIEW_BUILDER,
    US_TN_EXEMPTIONS_PREPROCESSED_VIEW_BUILDER,
    US_TN_INVOICES_PREPROCESSED_VIEW_BUILDER,
    US_TN_FINES_FEES_SESSIONS_PREPROCESSED_VIEW_BUILDER,
    INVOICES_PREPROCESSED_VIEW_BUILDER,
    PAYMENTS_PREPROCESSED_VIEW_BUILDER,
    CONSECUTIVE_PAYMENTS_PREPROCESSED_VIEW_BUILDER,
    US_TN_PERMANENT_EXEMPTIONS_PREPROCESSED_VIEW_BUILDER,
    PERMANENT_EXEMPTIONS_PREPROCESSED_VIEW_BUILDER,
    FINES_FEES_SESSIONS_VIEW_BUILDER,
    US_TN_CAF_Q1_VIEW_BUILDER,
    US_TN_CAF_Q2_VIEW_BUILDER,
    US_TN_CAF_Q6_VIEW_BUILDER,
    US_TN_CAF_Q7_VIEW_BUILDER,
    US_TN_CAF_Q8_VIEW_BUILDER,
    US_TN_PRIOR_RECORD_PREPROCESSED_VIEW_BUILDER,
    US_TN_RECOMMENDED_CUSTODY_LEVEL_SPANS_VIEW_BUILDER,
    US_TN_INCARCERATION_INCIDENTS_PREPROCESSED_VIEW_BUILDER,
    INCARCERATION_INCIDENTS_PREPROCESSED_VIEW_BUILDER,
    RECOMMENDED_CUSTODY_LEVEL_SPANS_VIEW_BUILDER,
    US_MO_CLASSIFICATION_HEARINGS_PREPROCESSED_RECORD_VIEW_BUILDER,
    US_CA_SUSTAINABLE_HOUSING_STATUS_PERIODS_VIEW_BUILDER,
    US_TN_RELEVANT_CONTACT_CODES_VIEW_BUILDER,
    US_TN_CONTACT_COMMENTS_PREPROCESSED_VIEW_BUILDER,
    US_TN_CUSTODY_LEVEL_SESSIONS_PREPROCESSED_VIEW_BUILDER,
    US_TN_SEGREGATION_STAYS_VIEW_BUILDER,
    US_TN_SEGREGATION_LISTS_VIEW_BUILDER,
    WORKFLOWS_PERSON_IMPACT_FUNNEL_STATUS_SESSIONS_VIEW_BUILDER,
    US_AZ_ACTION_QUEUE_VIEW_BUILDER,
    US_AZ_HOME_PLAN_PREPROCESSED_VIEW_BUILDER,
    US_AZ_PROJECTED_DATES_VIEW_BUILDER,
    US_AZ_AGREEMENT_FORM_SIGNATURES_VIEW_BUILDER,
    US_TN_CUSTODY_CLASSIFICATION_ASSESSMENT_DATES_PREPROCESSED_VIEW_BUILDER,
    CUSTODY_CLASSIFICATION_ASSESSMENT_DATES_VIEW_BUILDER,
    NUMBER_MONTHS_BETWEEN_CUSTODY_DOWNGRADE_AND_ASSESSMENT_DUE_VIEW_BUILDER,
    WORKFLOWS_USER_CASELOAD_ACCESS_SESSIONS_VIEW_BUILDER,
    US_AR_OVG_TIMELINE_VIEW_BUILDER,
    US_AR_OVG_EVENTS_PREPROCESSED_VIEW_BUILDER,
    US_AR_RESIDENT_APPROVED_VISITORS_PREPROCESSED_VIEW_BUILDER,
    INSIGHTS_CASELOAD_CATEGORY_SESSIONS_VIEW_BUILDER,
    INSIGHTS_SUPERVISION_OFFICER_CASELOAD_CATEGORY_SESSIONS_VIEW_BUILDER,
    WORKFLOWS_LIVE_COMPLETION_EVENT_TYPES_BY_STATE_VIEW_BUILDER,
    WORKFLOWS_PROVISIONED_USER_REGISTRATION_SESSIONS_VIEW_BUILDER,
    WORKFLOWS_RECORD_ARCHIVE_SURFACEABLE_PERSON_SESSIONS_VIEW_BUILDER,
    WORKFLOWS_RECORD_ARCHIVE_SURFACEABLE_CASELOAD_SESSIONS_VIEW_BUILDER,
    PRODUCT_ROSTER_ARCHIVE_SESSIONS_VIEW_BUILDER,
    INSIGHTS_PROVISIONED_USER_REGISTRATION_SESSIONS_VIEW_BUILDER,
    ALL_AUTH0_LOGIN_EVENTS_VIEW_BUILDER,
    INSIGHTS_USER_AUTH0_REGISTRATIONS_VIEW_BUILDER,
    WORKFLOWS_USER_AUTH0_REGISTRATIONS_VIEW_BUILDER,
    INSIGHTS_SEGMENT_EVENTS_VIEW_BUILDER,
    WORKFLOWS_PERSON_MARKED_INELIGIBLE_STATUS_SESSION_DETAILS_VIEW_BUILDER,
    INSIGHTS_SUPERVISION_OFFICER_OUTLIER_STATUS_ARCHIVE_SESSIONS_VIEW_BUILDER,
    INSIGHTS_SUPERVISOR_OUTLIER_STATUS_ARCHIVE_SESSIONS_VIEW_BUILDER,
    INSIGHTS_USER_IMPACT_FUNNEL_STATUS_SESSIONS_VIEW_BUILDER,
    WORKFLOWS_USER_PERSON_ASSIGNMENT_SESSIONS_VIEW_BUILDER,
    INSIGHTS_USER_PERSON_ASSIGNMENT_SESSIONS_VIEW_BUILDER,
    INSIGHTS_OFFICER_OUTLIER_USAGE_COHORT_VIEW_BUILDER,
    WORKFLOWS_USER_SNOOZE_STARTS_VIEW_BUILDER,
    US_TN_SUPERVISION_LEVEL_RAW_TEXT_SESSIONS_INFERRED_VIEW_BUILDER,
    US_TX_SUPERVISION_STAFF_IN_CRITICALLY_UNDERSTAFFED_LOCATION_SESSIONS_PREPROCESSED_VIEW_BUILDER,
    SUPERVISION_STAFF_IN_CRITICALLY_UNDERSTAFFED_LOCATION_SESSIONS_PREPROCESSED_VIEW_BUILDER,
    US_TX_CONTACT_CADENCE_SPANS_VIEW_BUILDER,
    US_MA_EARNED_CREDIT_ACTIVITY_PREPROCESSED_VIEW_BUILDER,
    EARNED_CREDIT_ACTIVITY_VIEW_BUILDER,
    US_MA_PERSON_PROJECTED_DATE_SESSIONS_PREPROCESSED_VIEW_BUILDER,
]
