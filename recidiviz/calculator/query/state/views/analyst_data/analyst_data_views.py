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
"""All views needed for analyst data"""
from typing import List

from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.state.views.analyst_data.agent_supervisor_preprocessed import (
    AGENT_SUPERVISOR_PREPROCESSED_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.analyst_data.consecutive_payments_preprocessed import (
    CONSECUTIVE_PAYMENTS_PREPROCESSED_VIEW_BUILDER,
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
from recidiviz.calculator.query.state.views.analyst_data.fines_fees_sessions import (
    FINES_FEES_SESSIONS_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.analyst_data.incarceration_incidents_preprocessed import (
    INCARCERATION_INCIDENTS_PREPROCESSED_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.analyst_data.invoices_preprocessed import (
    INVOICES_PREPROCESSED_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.analyst_data.models.analyst_data_view_collector import (
    get_spans_and_events_view_builders,
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
from recidiviz.calculator.query.state.views.analyst_data.projected_discharges import (
    PROJECTED_DISCHARGES_VIEW_BUILDER,
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
from recidiviz.calculator.query.state.views.analyst_data.specialized_agents_preprocessed import (
    SPECIALIZED_AGENTS_PREPROCESSED_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.analyst_data.supervision_clients_to_officers_ratio_quarterly import (
    SUPERVISION_CLIENTS_TO_OFFICERS_RATIO_QUARTERLY_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.analyst_data.supervision_officer_primary_office import (
    SUPERVISION_OFFICER_PRIMARY_OFFICE_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.analyst_data.supervision_population_attributes_by_district_by_month import (
    SUPERVISION_POPULATION_ATTRIBUTES_BY_DISTRICT_BY_MONTH_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.analyst_data.supervision_population_attributes_by_supervision_office_by_month import (
    SUPERVISION_POPULATION_ATTRIBUTES_BY_SUPERVISION_OFFICE_BY_MONTH_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.analyst_data.supervision_population_by_officer_daily_windows import (
    SUPERVISION_POPULATION_BY_OFFICER_DAILY_WINDOWS_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.analyst_data.us_ca.us_ca_sustainable_housing_status_periods import (
    US_CA_SUSTAINABLE_HOUSING_STATUS_PERIODS_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.analyst_data.us_id.us_id_behavior_responses import (
    US_ID_BEHAVIOR_RESPONSES_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.analyst_data.us_id.us_id_early_discharge_requests import (
    US_ID_EARLY_DISCHARGE_REQUESTS_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.analyst_data.us_id.us_id_early_discharge_sessions_preprocessing import (
    US_ID_EARLY_DISCHARGE_SESSIONS_PREPROCESSING_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.analyst_data.us_id.us_id_ppo_metrics_early_discharge_reduction import (
    US_ID_PPO_METRICS_EARLY_DISCHARGE_REDUCTION_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.analyst_data.us_id.us_id_ppo_metrics_early_discharge_requests import (
    US_ID_PPO_METRICS_EARLY_DISCHARGE_REQUESTS_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.analyst_data.us_id.us_id_ppo_metrics_early_discharge_terminations import (
    US_ID_PPO_METRICS_EARLY_DISCHARGE_TERMINATIONS_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.analyst_data.us_id.us_id_ppo_metrics_early_discharges import (
    US_ID_PPO_METRICS_EARLY_DISCHARGES_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.analyst_data.us_id.us_id_ppo_metrics_successful_supervision_terminations import (
    US_ID_PPO_METRICS_SUCCESSFUL_SUPERVISION_TERMINATIONS_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.analyst_data.us_id.us_id_ppo_metrics_supervision_level import (
    US_ID_PPO_METRICS_SUPERVISION_LEVEL_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.analyst_data.us_id.us_id_raw_supervision_contacts import (
    US_ID_RAW_SUPERVISION_CONTACTS_VIEW_BUILDER,
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
from recidiviz.calculator.query.state.views.analyst_data.us_me.us_me_day_0_early_discharge import (
    US_ME_DAY_0_EARLY_DISCHARGE_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.analyst_data.us_me.us_me_day_0_sccp import (
    US_ME_DAY_0_SCCP_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.analyst_data.us_me.us_me_day_0_supervision_downgrade import (
    US_ME_DAY_0_SUPERVISION_DOWNGRADE_VIEW_BUILDER,
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
from recidiviz.calculator.query.state.views.analyst_data.us_mi.us_mi_early_discharge_sessions_preprocessing import (
    US_MI_EARLY_DISCHARGE_SESSIONS_PREPROCESSING_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.analyst_data.us_mi.us_mi_supervision_level_raw_text_mappings import (
    US_MI_SUPERVISION_LEVEL_RAW_TEXT_MAPPINGS_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.analyst_data.us_mo.us_mo_classification_hearings_preprocessed import (
    US_MO_CLASSIFICATION_HEARINGS_PREPROCESSED_RECORD_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.analyst_data.us_mo.us_mo_mosop_prio_eligibility import (
    PRIORITIZED_ELIGIBILITY,
)
from recidiviz.calculator.query.state.views.analyst_data.us_mo.us_mo_mosop_prio_groups import (
    US_MO_MOSOP_PRIO_GROUPS_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.analyst_data.us_mo.us_mo_program_tracks import (
    US_MO_PROGRAM_TRACKS_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.analyst_data.us_mo.us_mo_screeners_preprocessed import (
    US_MO_SCREENERS_PREPROCESSED_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.analyst_data.us_mo.us_mo_sentencing_dates_preprocessed import (
    US_MO_SENTENCING_DATES_PREPROCESSED_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.analyst_data.us_nd.us_nd_day_0_early_termination import (
    US_ND_DAY_0_EARLY_TERMINATION_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.analyst_data.us_nd.us_nd_day_0_overdue_discharge import (
    US_ND_DAY_0_OVERDUE_DISCHARGE_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.analyst_data.us_nd.us_nd_early_discharge_sessions_preprocessing import (
    US_ND_EARLY_DISCHARGE_SESSIONS_PREPROCESSING_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.analyst_data.us_or.us_or_earned_discharge_sentence_eligibility_spans import (
    US_OR_EARNED_DISCHARGE_SENTENCE_ELIGIBILITY_SPANS_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.analyst_data.us_or.us_or_no_convictions_since_sentence_start import (
    US_OR_NO_CONVICTIONS_SINCE_SENTENCE_START_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.analyst_data.us_or.us_or_sentenced_after_august_2013 import (
    US_OR_SENTENCED_AFTER_AUGUST_2013_VIEW_BUILDER,
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
from recidiviz.calculator.query.state.views.analyst_data.us_pa.us_pa_agent_supervisor_preprocessed import (
    US_PA_AGENT_SUPERVISOR_PREPROCESSED_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.analyst_data.us_pa.us_pa_raw_required_treatment import (
    US_PA_RAW_REQUIRED_TREATMENT_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.analyst_data.us_pa.us_pa_specialized_agents_preprocessed import (
    US_PA_SPECIALIZED_AGENTS_PREPROCESSED_VIEW_BUILDER,
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
from recidiviz.calculator.query.state.views.analyst_data.us_tn.us_tn_compliant_reporting_c4_isc_eligiblility_sessions import (
    US_TN_COMPLIANT_REPORTING_C4_ISC_ELIGIBILITY_SESSIONS_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.analyst_data.us_tn.us_tn_compliant_reporting_cr_rejection_ineligible import (
    US_TN_COMPLIANT_REPORTING_CR_REJECTION_INELIGIBLE_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.analyst_data.us_tn.us_tn_compliant_reporting_drug_screen_eligible import (
    US_TN_COMPLIANT_REPORTING_DRUG_SCREEN_ELIGIBLE_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.analyst_data.us_tn.us_tn_compliant_reporting_eligibility_sessions import (
    US_TN_COMPLIANT_REPORTING_ELIGIBILITY_SESSIONS_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.analyst_data.us_tn.us_tn_compliant_reporting_eligible import (
    US_TN_COMPLIANT_REPORTING_ELIGIBLE_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.analyst_data.us_tn.us_tn_compliant_reporting_fees_eligibility_sessions import (
    US_TN_COMPLIANT_REPORTING_FEES_ELIGIBILITY_SESSIONS_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.analyst_data.us_tn.us_tn_compliant_reporting_funnel import (
    US_TN_COMPLIANT_REPORTING_FUNNEL_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.analyst_data.us_tn.us_tn_compliant_reporting_guardrail import (
    US_TN_COMPLIANT_REPORTING_GUARDRAIL_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.analyst_data.us_tn.us_tn_compliant_reporting_judicial_order_ineligible import (
    US_TN_COMPLIANT_REPORTING_JUDICIAL_ORDER_INELIGIBLE_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.analyst_data.us_tn.us_tn_compliant_reporting_logic import (
    US_TN_COMPLIANT_REPORTING_LOGIC_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.analyst_data.us_tn.us_tn_compliant_reporting_offense_eligible import (
    US_TN_COMPLIANT_REPORTING_OFFENSE_ELIGIBLE_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.analyst_data.us_tn.us_tn_compliant_reporting_referral import (
    US_TN_COMPLIANT_REPORTING_REFERRAL_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.analyst_data.us_tn.us_tn_compliant_reporting_sanction_ineligible import (
    US_TN_COMPLIANT_REPORTING_SANCTION_INELIGIBLE_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.analyst_data.us_tn.us_tn_compliant_reporting_supervision_level_eligible import (
    US_TN_COMPLIANT_REPORTING_SUPERVISION_LEVEL_ELIGIBLE_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.analyst_data.us_tn.us_tn_compliant_reporting_workflow_status import (
    US_TN_COMPLIANT_REPORTING_WORKFLOW_STATUS_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.analyst_data.us_tn.us_tn_contact_comments_preprocessed import (
    US_TN_CONTACT_COMMENTS_PREPROCESSED_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.analyst_data.us_tn.us_tn_cr_raw_sentence_preprocessing import (
    US_TN_CR_RAW_SENTENCE_PREPROCESSING_VIEW_BUILDER,
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
from recidiviz.calculator.query.state.views.analyst_data.us_tn.us_tn_overdue_for_discharge import (
    US_TN_OVERDUE_FOR_DISCHARGE_VIEW_BUILDER,
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
from recidiviz.calculator.query.state.views.analyst_data.us_tn.us_tn_sentence_logic import (
    US_TN_SENTENCE_LOGIC_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.analyst_data.us_tn.us_tn_tepe_relevant_codes import (
    US_TN_TEPE_RELEVANT_CODES_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.analyst_data.us_tn.us_tn_zero_tolerance_codes import (
    US_TN_ZERO_TOLERANCE_CODES_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.analyst_data.workflows_officer_events import (
    WORKFLOWS_OFFICER_EVENTS_VIEW_BUILDER,
)

ANALYST_DATA_VIEW_BUILDERS: List[SimpleBigQueryViewBuilder] = [
    EARLY_DISCHARGE_SESSIONS_VIEW_BUILDER,
    EARLY_DISCHARGE_SESSIONS_WITH_OFFICER_AND_SUPERVISOR_VIEW_BUILDER,
    EARLY_DISCHARGE_REPORTS_PER_OFFICER_VIEW_BUILDER,
    WORKFLOWS_OFFICER_EVENTS_VIEW_BUILDER,
    POPULATION_DENSITY_BY_SUPERVISION_OFFICE_VIEW_BUILDER,
    PROJECTED_DISCHARGES_VIEW_BUILDER,
    PSA_RISK_SCORES_VIEW_BUILDER,
    SESSION_COHORT_REINCARCERATION_VIEW_BUILDER,
    SUPERVISION_OFFICER_PRIMARY_OFFICE_VIEW_BUILDER,
    SUPERVISION_POPULATION_ATTRIBUTES_BY_DISTRICT_BY_MONTH_VIEW_BUILDER,
    SUPERVISION_POPULATION_ATTRIBUTES_BY_SUPERVISION_OFFICE_BY_MONTH_VIEW_BUILDER,
    SUPERVISION_POPULATION_BY_OFFICER_DAILY_WINDOWS_VIEW_BUILDER,
    SUPERVISION_CLIENTS_TO_OFFICERS_RATIO_QUARTERLY_VIEW_BUILDER,
    US_ID_BEHAVIOR_RESPONSES_VIEW_BUILDER,
    US_ID_EARLY_DISCHARGE_REQUESTS_VIEW_BUILDER,
    US_ID_EARLY_DISCHARGE_SESSIONS_PREPROCESSING_VIEW_BUILDER,
    US_ID_PPO_METRICS_EARLY_DISCHARGE_REDUCTION_VIEW_BUILDER,
    US_ID_PPO_METRICS_EARLY_DISCHARGE_REQUESTS_VIEW_BUILDER,
    US_ID_PPO_METRICS_EARLY_DISCHARGE_TERMINATIONS_VIEW_BUILDER,
    US_ID_PPO_METRICS_EARLY_DISCHARGES_VIEW_BUILDER,
    US_ID_PPO_METRICS_SUCCESSFUL_SUPERVISION_TERMINATIONS_VIEW_BUILDER,
    US_ID_PPO_METRICS_SUPERVISION_LEVEL_VIEW_BUILDER,
    US_ID_RAW_SUPERVISION_CONTACTS_VIEW_BUILDER,
    US_IX_EARLY_DISCHARGE_SESSIONS_PREPROCESSING_VIEW_BUILDER,
    US_IX_PAROLE_DATES_SPANS_PREPROCESSING_VIEW_BUILDER,
    US_IX_DETAINER_SPANS_VIEW_BUILDER,
    US_ME_DAY_0_EARLY_DISCHARGE_VIEW_BUILDER,
    US_ME_DAY_0_SCCP_VIEW_BUILDER,
    US_ME_DAY_0_SUPERVISION_DOWNGRADE_VIEW_BUILDER,
    US_ME_EARLY_DISCHARGE_SESSIONS_PREPROCESSING_VIEW_BUILDER,
    US_ME_SENTENCE_TERM_VIEW_BUILDER,
    US_ME_INVOICES_PREPROCESSED_VIEW_BUILDER,
    US_ME_PAYMENTS_PREPROCESSED_VIEW_BUILDER,
    US_ME_FINES_FEES_SESSIONS_PREPROCESSED_VIEW_BUILDER,
    US_MI_EARLY_DISCHARGE_SESSIONS_PREPROCESSING_VIEW_BUILDER,
    US_MI_SUPERVISION_LEVEL_RAW_TEXT_MAPPINGS_VIEW_BUILDER,
    US_ND_EARLY_DISCHARGE_SESSIONS_PREPROCESSING_VIEW_BUILDER,
    US_ND_DAY_0_EARLY_TERMINATION_VIEW_BUILDER,
    US_ND_DAY_0_OVERDUE_DISCHARGE_VIEW_BUILDER,
    US_OR_EARNED_DISCHARGE_SENTENCE_ELIGIBILITY_SPANS_VIEW_BUILDER,
    US_OR_NO_CONVICTIONS_SINCE_SENTENCE_START_VIEW_BUILDER,
    US_OR_SENTENCED_AFTER_AUGUST_2013_VIEW_BUILDER,
    US_OR_SERVED_6_MONTHS_SUPERVISION_VIEW_BUILDER,
    US_OR_SERVED_HALF_SENTENCE_VIEW_BUILDER,
    US_OR_STATUTE_ELIGIBLE_VIEW_BUILDER,
    US_PA_RAW_REQUIRED_TREATMENT_VIEW_BUILDER,
    US_TN_COMPLIANT_REPORTING_LOGIC_VIEW_BUILDER,
    US_TN_COMPLIANT_REPORTING_ELIGIBLE_VIEW_BUILDER,
    US_TN_OVERDUE_FOR_DISCHARGE_VIEW_BUILDER,
    US_TN_COMPLIANT_REPORTING_REFERRAL_VIEW_BUILDER,
    US_TN_SENTENCE_LOGIC_VIEW_BUILDER,
    US_TN_COMPLIANT_REPORTING_WORKFLOW_STATUS_VIEW_BUILDER,
    US_TN_COMPLIANT_REPORTING_FUNNEL_VIEW_BUILDER,
    US_TN_COMPLIANT_REPORTING_GUARDRAIL_VIEW_BUILDER,
    US_TN_COMPLIANT_REPORTING_C4_ISC_ELIGIBILITY_SESSIONS_VIEW_BUILDER,
    US_TN_COMPLIANT_REPORTING_SUPERVISION_LEVEL_ELIGIBLE_VIEW_BUILDER,
    US_TN_COMPLIANT_REPORTING_ELIGIBILITY_SESSIONS_VIEW_BUILDER,
    US_TN_COMPLIANT_REPORTING_FEES_ELIGIBILITY_SESSIONS_VIEW_BUILDER,
    US_TN_COMPLIANT_REPORTING_JUDICIAL_ORDER_INELIGIBLE_VIEW_BUILDER,
    US_TN_COMPLIANT_REPORTING_OFFENSE_ELIGIBLE_VIEW_BUILDER,
    US_TN_COMPLIANT_REPORTING_CR_REJECTION_INELIGIBLE_VIEW_BUILDER,
    US_TN_COMPLIANT_REPORTING_SANCTION_INELIGIBLE_VIEW_BUILDER,
    US_TN_COMPLIANT_REPORTING_DRUG_SCREEN_ELIGIBLE_VIEW_BUILDER,
    US_TN_CR_RAW_SENTENCE_PREPROCESSING_VIEW_BUILDER,
    US_PA_SPECIALIZED_AGENTS_PREPROCESSED_VIEW_BUILDER,
    SPECIALIZED_AGENTS_PREPROCESSED_VIEW_BUILDER,
    US_PA_AGENT_SUPERVISOR_PREPROCESSED_VIEW_BUILDER,
    AGENT_SUPERVISOR_PREPROCESSED_VIEW_BUILDER,
    US_TN_ZERO_TOLERANCE_CODES_VIEW_BUILDER,
    US_MO_SCREENERS_PREPROCESSED_VIEW_BUILDER,
    US_MO_PROGRAM_TRACKS_VIEW_BUILDER,
    US_MO_MOSOP_PRIO_GROUPS_VIEW_BUILDER,
    US_MO_SENTENCING_DATES_PREPROCESSED_VIEW_BUILDER,
    PRIORITIZED_ELIGIBILITY,
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
    *get_spans_and_events_view_builders(),
    US_CA_SUSTAINABLE_HOUSING_STATUS_PERIODS_VIEW_BUILDER,
    US_TN_TEPE_RELEVANT_CODES_VIEW_BUILDER,
    US_TN_CONTACT_COMMENTS_PREPROCESSED_VIEW_BUILDER,
]
