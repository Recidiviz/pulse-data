# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2020 Recidiviz, Inc.
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
from recidiviz.calculator.query.state.views.analyst_data.admission_start_reason_dedup_priority import (
    ADMISSION_START_REASON_DEDUP_PRIORITY_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.analyst_data.assessment_score_sessions import (
    ASSESSMENT_SCORE_SESSIONS_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.analyst_data.compartment_level_2_dedup_priority import (
    COMPARTMENT_LEVEL_2_DEDUP_PRIORITY_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.analyst_data.compartment_sentences import (
    COMPARTMENT_SENTENCES_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.analyst_data.compartment_session_end_reasons import (
    COMPARTMENT_SESSION_END_REASONS_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.analyst_data.compartment_session_start_reasons import (
    COMPARTMENT_SESSION_START_REASONS_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.analyst_data.compartment_sessions import (
    COMPARTMENT_SESSIONS_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.analyst_data.compartment_sessions_unnested import (
    COMPARTMENT_SESSIONS_UNNESTED_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.analyst_data.dataflow_sessions import (
    DATAFLOW_SESSIONS_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.analyst_data.event_based_metrics_by_district import (
    EVENT_BASED_METRICS_BY_DISTRICT_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.analyst_data.event_based_metrics_by_supervision_officer import (
    EVENT_BASED_METRICS_BY_SUPERVISION_OFFICER_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.analyst_data.incarceration_super_sessions import (
    INCARCERATION_SUPER_SESSIONS_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.analyst_data.location_sessions import (
    LOCATION_SESSIONS_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.analyst_data.person_demographics import (
    PERSON_DEMOGRAPHICS_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.analyst_data.reincarceration_sessions_from_dataflow import (
    REINCARCERATION_SESSIONS_FROM_DATAFLOW_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.analyst_data.reincarceration_sessions_from_sessions import (
    REINCARCERATION_SESSIONS_FROM_SESSIONS_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.analyst_data.release_termination_reason_dedup_priority import (
    RELEASE_TERMINATION_REASON_DEDUP_PRIORITY_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.analyst_data.revocation_sessions import (
    REVOCATION_SESSIONS_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.analyst_data.supervision_level_compliance_requirements import (
    SUPERVISION_LEVEL_COMPLIANCE_REQUIREMENTS_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.analyst_data.supervision_level_dedup_priority import (
    SUPERVISION_LEVEL_DEDUP_PRIORITY_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.analyst_data.supervision_level_sessions import (
    SUPERVISION_LEVEL_SESSIONS_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.analyst_data.supervision_officer_caseload_health_metrics import (
    SUPERVISION_OFFICER_CASELOAD_HEALTH_METRICS_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.analyst_data.supervision_officer_sessions import (
    SUPERVISION_OFFICER_SESSIONS_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.analyst_data.supervision_population_attributes_by_district_by_month import (
    SUPERVISION_POPULATION_ATTRIBUTES_BY_DISTRICT_BY_MONTH_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.analyst_data.supervision_population_by_officer_daily_windows import (
    SUPERVISION_POPULATION_BY_OFFICER_DAILY_WINDOWS_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.analyst_data.supervision_super_sessions import (
    SUPERVISION_SUPER_SESSIONS_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.analyst_data.system_sessions import (
    SYSTEM_SESSIONS_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.analyst_data.us_id.us_id_employment_sessions import (
    US_ID_EMPLOYMENT_SESSIONS_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.analyst_data.us_id.us_id_positive_urine_analysis_sessions import (
    US_ID_POSITIVE_URINE_ANALYSIS_SESSIONS_VIEW_BUILDER,
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
from recidiviz.calculator.query.state.views.analyst_data.us_id_incarceration_population_metrics_preprocessed import (
    US_ID_INCARCERATION_POPULATION_METRICS_PREPROCESSED_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.analyst_data.us_id_supervision_out_of_state_population_metrics_preprocessed import (
    US_ID_SUPERVISION_OUT_OF_STATE_POPULATION_METRICS_PREPROCESSED_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.analyst_data.us_id_supervision_population_metrics_preprocessed import (
    US_ID_SUPERVISION_POPULATION_METRICS_PREPROCESSED_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.analyst_data.us_mo_supervision_population_metrics_preprocessed import (
    US_MO_SUPERVISION_POPULATION_METRICS_PREPROCESSED_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.analyst_data.violation_type_dedup_priority import (
    VIOLATION_TYPE_DEDUP_PRIORITY_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.analyst_data.violations_sessions import (
    VIOLATIONS_SESSIONS_VIEW_BUILDER,
)

ANALYST_DATA_VIEW_BUILDERS: List[SimpleBigQueryViewBuilder] = [
    PERSON_DEMOGRAPHICS_VIEW_BUILDER,
    ASSESSMENT_SCORE_SESSIONS_VIEW_BUILDER,
    COMPARTMENT_LEVEL_2_DEDUP_PRIORITY_VIEW_BUILDER,
    ADMISSION_START_REASON_DEDUP_PRIORITY_VIEW_BUILDER,
    RELEASE_TERMINATION_REASON_DEDUP_PRIORITY_VIEW_BUILDER,
    COMPARTMENT_SESSION_START_REASONS_VIEW_BUILDER,
    COMPARTMENT_SESSION_END_REASONS_VIEW_BUILDER,
    COMPARTMENT_SESSIONS_VIEW_BUILDER,
    SUPERVISION_SUPER_SESSIONS_VIEW_BUILDER,
    REVOCATION_SESSIONS_VIEW_BUILDER,
    REINCARCERATION_SESSIONS_FROM_DATAFLOW_VIEW_BUILDER,
    REINCARCERATION_SESSIONS_FROM_SESSIONS_VIEW_BUILDER,
    SYSTEM_SESSIONS_VIEW_BUILDER,
    COMPARTMENT_SESSIONS_UNNESTED_VIEW_BUILDER,
    COMPARTMENT_SENTENCES_VIEW_BUILDER,
    VIOLATIONS_SESSIONS_VIEW_BUILDER,
    US_ID_PPO_METRICS_EARLY_DISCHARGES_VIEW_BUILDER,
    US_ID_PPO_METRICS_SUCCESSFUL_SUPERVISION_TERMINATIONS_VIEW_BUILDER,
    US_ID_PPO_METRICS_EARLY_DISCHARGE_TERMINATIONS_VIEW_BUILDER,
    US_ID_PPO_METRICS_EARLY_DISCHARGE_REDUCTION_VIEW_BUILDER,
    US_ID_PPO_METRICS_SUPERVISION_LEVEL_VIEW_BUILDER,
    US_ID_PPO_METRICS_EARLY_DISCHARGE_REQUESTS_VIEW_BUILDER,
    SUPERVISION_POPULATION_ATTRIBUTES_BY_DISTRICT_BY_MONTH_VIEW_BUILDER,
    VIOLATION_TYPE_DEDUP_PRIORITY_VIEW_BUILDER,
    DATAFLOW_SESSIONS_VIEW_BUILDER,
    SUPERVISION_LEVEL_SESSIONS_VIEW_BUILDER,
    SUPERVISION_LEVEL_DEDUP_PRIORITY_VIEW_BUILDER,
    SUPERVISION_OFFICER_SESSIONS_VIEW_BUILDER,
    LOCATION_SESSIONS_VIEW_BUILDER,
    US_ID_EMPLOYMENT_SESSIONS_VIEW_BUILDER,
    US_ID_POSITIVE_URINE_ANALYSIS_SESSIONS_VIEW_BUILDER,
    US_ID_SUPERVISION_POPULATION_METRICS_PREPROCESSED_VIEW_BUILDER,
    US_ID_SUPERVISION_OUT_OF_STATE_POPULATION_METRICS_PREPROCESSED_VIEW_BUILDER,
    SUPERVISION_POPULATION_BY_OFFICER_DAILY_WINDOWS_VIEW_BUILDER,
    EVENT_BASED_METRICS_BY_SUPERVISION_OFFICER_VIEW_BUILDER,
    EVENT_BASED_METRICS_BY_DISTRICT_VIEW_BUILDER,
    US_ID_INCARCERATION_POPULATION_METRICS_PREPROCESSED_VIEW_BUILDER,
    INCARCERATION_SUPER_SESSIONS_VIEW_BUILDER,
    US_MO_SUPERVISION_POPULATION_METRICS_PREPROCESSED_VIEW_BUILDER,
    SUPERVISION_LEVEL_COMPLIANCE_REQUIREMENTS_VIEW_BUILDER,
    SUPERVISION_OFFICER_CASELOAD_HEALTH_METRICS_VIEW_BUILDER,
]
