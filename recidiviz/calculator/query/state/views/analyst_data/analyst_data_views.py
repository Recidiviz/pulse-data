# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2022 Recidiviz, Inc.
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
from recidiviz.calculator.query.state.views.analyst_data.early_discharge_sessions import (
    EARLY_DISCHARGE_SESSIONS_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.analyst_data.offense_type_mapping import (
    OFFENSE_TYPE_MAPPING_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.analyst_data.officer_events import (
    OFFICER_EVENTS_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.analyst_data.person_events import (
    PERSON_EVENTS_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.analyst_data.person_statuses import (
    PERSON_STATUSES_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.analyst_data.population_density_by_supervision_office import (
    POPULATION_DENSITY_BY_SUPERVISION_OFFICE_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.analyst_data.projected_discharges import (
    PROJECTED_DISCHARGES_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.analyst_data.session_cohort_reincarceration import (
    SESSION_COHORT_REINCARCERATION_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.analyst_data.state_charge_with_labels import (
    STATE_CHARGE_WITH_LABELS_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.analyst_data.supervision_aggregated_metrics import (
    SUPERVISION_AGGREGATED_METRICS_VIEW_BUILDERS,
)
from recidiviz.calculator.query.state.views.analyst_data.supervision_officer_office_metrics import (
    SUPERVISION_OFFICER_OFFICE_METRICS_VIEW_BUILDER,
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
from recidiviz.calculator.query.state.views.analyst_data.us_id.us_id_behavior_responses import (
    US_ID_BEHAVIOR_RESPONSES_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.analyst_data.us_id.us_id_day_0_lsu import (
    US_ID_DAY_0_LSU_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.analyst_data.us_id.us_id_day_0_overdue_discharge import (
    US_ID_DAY_0_OVERDUE_DISCHARGE_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.analyst_data.us_id.us_id_day_0_supervision_downgrade import (
    US_ID_DAY_0_SUPERVISION_DOWNGRADE_VIEW_BUILDER,
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
from recidiviz.calculator.query.state.views.analyst_data.us_me.us_me_day_0_early_discharge import (
    US_ME_DAY_0_EARLY_DISCHARGE_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.analyst_data.us_me.us_me_day_0_sccp import (
    US_ME_DAY_0_SCCP_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.analyst_data.us_me.us_me_day_0_supervision_downgrade import (
    US_ME_DAY_0_SUPERVISION_DOWNGRADE_VIEW_BUILDER,
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
from recidiviz.calculator.query.state.views.analyst_data.us_pa.us_pa_raw_required_treatment import (
    US_PA_RAW_REQUIRED_TREATMENT_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.analyst_data.us_pa.us_pa_raw_treatment_classification_codes import (
    US_PA_RAW_TREATMENT_CLASSIFICATION_CODES_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.analyst_data.us_tn.us_tn_compliant_reporting_eligible import (
    US_TN_COMPLIANT_REPORTING_ELIGIBLE_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.analyst_data.us_tn.us_tn_compliant_reporting_funnel import (
    US_TN_COMPLIANT_REPORTING_FUNNEL_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.analyst_data.us_tn.us_tn_compliant_reporting_guardrail import (
    US_TN_COMPLIANT_REPORTING_GUARDRAIL_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.analyst_data.us_tn.us_tn_compliant_reporting_logic import (
    US_TN_COMPLIANT_REPORTING_LOGIC_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.analyst_data.us_tn.us_tn_compliant_reporting_referral import (
    US_TN_COMPLIANT_REPORTING_REFERRAL_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.analyst_data.us_tn.us_tn_compliant_reporting_workflow_status import (
    US_TN_COMPLIANT_REPORTING_WORKFLOW_STATUS_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.analyst_data.us_tn.us_tn_overdue_for_discharge import (
    US_TN_OVERDUE_FOR_DISCHARGE_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.analyst_data.us_tn.us_tn_sentence_logic import (
    US_TN_SENTENCE_LOGIC_VIEW_BUILDER,
)

ANALYST_DATA_VIEW_BUILDERS: List[SimpleBigQueryViewBuilder] = [
    EARLY_DISCHARGE_SESSIONS_VIEW_BUILDER,
    OFFENSE_TYPE_MAPPING_VIEW_BUILDER,
    OFFICER_EVENTS_VIEW_BUILDER,
    PERSON_EVENTS_VIEW_BUILDER,
    PERSON_STATUSES_VIEW_BUILDER,
    POPULATION_DENSITY_BY_SUPERVISION_OFFICE_VIEW_BUILDER,
    PROJECTED_DISCHARGES_VIEW_BUILDER,
    SESSION_COHORT_REINCARCERATION_VIEW_BUILDER,
    STATE_CHARGE_WITH_LABELS_VIEW_BUILDER,
    SUPERVISION_OFFICER_OFFICE_METRICS_VIEW_BUILDER,
    SUPERVISION_POPULATION_ATTRIBUTES_BY_DISTRICT_BY_MONTH_VIEW_BUILDER,
    SUPERVISION_POPULATION_ATTRIBUTES_BY_SUPERVISION_OFFICE_BY_MONTH_VIEW_BUILDER,
    SUPERVISION_POPULATION_BY_OFFICER_DAILY_WINDOWS_VIEW_BUILDER,
    US_ID_BEHAVIOR_RESPONSES_VIEW_BUILDER,
    US_ID_DAY_0_LSU_VIEW_BUILDER,
    US_ID_DAY_0_OVERDUE_DISCHARGE_VIEW_BUILDER,
    US_ID_DAY_0_SUPERVISION_DOWNGRADE_VIEW_BUILDER,
    US_ID_EARLY_DISCHARGE_REQUESTS_VIEW_BUILDER,
    US_ID_EARLY_DISCHARGE_SESSIONS_PREPROCESSING_VIEW_BUILDER,
    US_ID_PPO_METRICS_EARLY_DISCHARGE_REDUCTION_VIEW_BUILDER,
    US_ID_PPO_METRICS_EARLY_DISCHARGE_REQUESTS_VIEW_BUILDER,
    US_ID_PPO_METRICS_EARLY_DISCHARGE_TERMINATIONS_VIEW_BUILDER,
    US_ID_PPO_METRICS_EARLY_DISCHARGES_VIEW_BUILDER,
    US_ID_PPO_METRICS_SUCCESSFUL_SUPERVISION_TERMINATIONS_VIEW_BUILDER,
    US_ID_PPO_METRICS_SUPERVISION_LEVEL_VIEW_BUILDER,
    US_ID_RAW_SUPERVISION_CONTACTS_VIEW_BUILDER,
    US_ME_DAY_0_EARLY_DISCHARGE_VIEW_BUILDER,
    US_ME_DAY_0_SCCP_VIEW_BUILDER,
    US_ME_DAY_0_SUPERVISION_DOWNGRADE_VIEW_BUILDER,
    US_ND_EARLY_DISCHARGE_SESSIONS_PREPROCESSING_VIEW_BUILDER,
    US_ND_DAY_0_EARLY_TERMINATION_VIEW_BUILDER,
    US_ND_DAY_0_OVERDUE_DISCHARGE_VIEW_BUILDER,
    US_PA_RAW_REQUIRED_TREATMENT_VIEW_BUILDER,
    US_PA_RAW_TREATMENT_CLASSIFICATION_CODES_VIEW_BUILDER,
    US_TN_COMPLIANT_REPORTING_LOGIC_VIEW_BUILDER,
    US_TN_COMPLIANT_REPORTING_ELIGIBLE_VIEW_BUILDER,
    US_TN_OVERDUE_FOR_DISCHARGE_VIEW_BUILDER,
    US_TN_COMPLIANT_REPORTING_REFERRAL_VIEW_BUILDER,
    US_TN_SENTENCE_LOGIC_VIEW_BUILDER,
    US_TN_COMPLIANT_REPORTING_WORKFLOW_STATUS_VIEW_BUILDER,
    US_TN_COMPLIANT_REPORTING_FUNNEL_VIEW_BUILDER,
    US_TN_COMPLIANT_REPORTING_GUARDRAIL_VIEW_BUILDER,
] + SUPERVISION_AGGREGATED_METRICS_VIEW_BUILDERS
