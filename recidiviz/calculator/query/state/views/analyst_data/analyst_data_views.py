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
from recidiviz.calculator.query.state.views.analyst_data.event_based_metrics_by_district import (
    EVENT_BASED_METRICS_BY_DISTRICT_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.analyst_data.event_based_metrics_by_supervision_officer import (
    EVENT_BASED_METRICS_BY_SUPERVISION_OFFICER_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.analyst_data.officer_events import (
    OFFICER_EVENTS_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.analyst_data.person_events import (
    PERSON_EVENTS_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.analyst_data.projected_discharges import (
    PROJECTED_DISCHARGES_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.analyst_data.supervision_officer_caseload_health_metrics import (
    SUPERVISION_OFFICER_CASELOAD_HEALTH_METRICS_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.analyst_data.supervision_population_attributes_by_district_by_month import (
    SUPERVISION_POPULATION_ATTRIBUTES_BY_DISTRICT_BY_MONTH_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.analyst_data.supervision_population_by_officer_daily_windows import (
    SUPERVISION_POPULATION_BY_OFFICER_DAILY_WINDOWS_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.analyst_data.us_id.us_id_behavior_responses import (
    US_ID_BEHAVIOR_RESPONSES_VIEW_BUILDER,
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
from recidiviz.calculator.query.state.views.analyst_data.us_pa.us_pa_raw_required_treatment import (
    US_PA_RAW_REQUIRED_TREATMENT_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.analyst_data.us_pa.us_pa_raw_treatment_classification_codes import (
    US_PA_RAW_TREATMENT_CLASSIFICATION_CODES_VIEW_BUILDER,
)

ANALYST_DATA_VIEW_BUILDERS: List[SimpleBigQueryViewBuilder] = [
    EVENT_BASED_METRICS_BY_SUPERVISION_OFFICER_VIEW_BUILDER,
    EVENT_BASED_METRICS_BY_DISTRICT_VIEW_BUILDER,
    OFFICER_EVENTS_VIEW_BUILDER,
    PERSON_EVENTS_VIEW_BUILDER,
    PROJECTED_DISCHARGES_VIEW_BUILDER,
    SUPERVISION_OFFICER_CASELOAD_HEALTH_METRICS_VIEW_BUILDER,
    SUPERVISION_POPULATION_ATTRIBUTES_BY_DISTRICT_BY_MONTH_VIEW_BUILDER,
    SUPERVISION_POPULATION_BY_OFFICER_DAILY_WINDOWS_VIEW_BUILDER,
    US_ID_BEHAVIOR_RESPONSES_VIEW_BUILDER,
    US_ID_PPO_METRICS_EARLY_DISCHARGE_REDUCTION_VIEW_BUILDER,
    US_ID_PPO_METRICS_EARLY_DISCHARGE_TERMINATIONS_VIEW_BUILDER,
    US_ID_PPO_METRICS_EARLY_DISCHARGES_VIEW_BUILDER,
    US_ID_PPO_METRICS_EARLY_DISCHARGE_REQUESTS_VIEW_BUILDER,
    US_ID_PPO_METRICS_SUCCESSFUL_SUPERVISION_TERMINATIONS_VIEW_BUILDER,
    US_ID_PPO_METRICS_SUPERVISION_LEVEL_VIEW_BUILDER,
    US_ID_RAW_SUPERVISION_CONTACTS_VIEW_BUILDER,
    US_PA_RAW_TREATMENT_CLASSIFICATION_CODES_VIEW_BUILDER,
    US_PA_RAW_REQUIRED_TREATMENT_VIEW_BUILDER,
]
