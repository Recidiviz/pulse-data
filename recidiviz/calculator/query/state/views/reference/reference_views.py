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
"""Reference views used by other views."""
from typing import List

from recidiviz.big_query.big_query_view import BigQueryViewBuilder
from recidiviz.calculator.query.state.views.reference.admission_types_per_state_for_matrix import (
    ADMISSION_TYPES_PER_STATE_FOR_MATRIX_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.reference.agent_external_id_to_full_names import (
    AGENT_EXTERNAL_ID_TO_FULL_NAMES_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.reference.augmented_agent_info import (
    AUGMENTED_AGENT_INFO_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.reference.covid_report_weeks import (
    COVID_REPORT_WEEKS_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.reference.dashboard_user_restrictions import (
    DASHBOARD_USER_RESTRICTIONS_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.reference.event_based_admissions import (
    EVENT_BASED_ADMISSIONS_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.reference.event_based_commitments_from_supervision import (
    EVENT_BASED_COMMITMENTS_FROM_SUPERVISION_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.reference.event_based_commitments_from_supervision_for_matrix import (
    EVENT_BASED_COMMITMENTS_FROM_SUPERVISION_FOR_MATRIX_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.reference.event_based_program_referrals import (
    EVENT_BASED_PROGRAM_REFERRALS_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.reference.event_based_supervision_populations import (
    EVENT_BASED_SUPERVISION_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.reference.event_based_supervision_populations_with_commitments_for_rate_denominators import (
    EVENT_BASED_SUPERVISION_POPULATIONS_WITH_COMMITMENTS_FOR_RATE_DENOMINATORS_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.reference.incarceration_location_ids_to_names import (
    INCARCERATION_LOCATION_IDS_TO_NAMES_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.reference.incarceration_period_judicial_district_association import (
    INCARCERATION_PERIOD_JUDICIAL_DISTRICT_ASSOCIATION_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.reference.liberty_to_prison_transitions import (
    LIBERTY_TO_PRISON_TRANSITIONS_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.reference.overdue_discharge_alert_exclusions import (
    OVERDUE_DISCHARGE_ALERT_EXCLUSIONS_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.reference.overdue_discharge_outcomes import (
    OVERDUE_DISCHARGE_OUTCOMES_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.reference.persons_to_recent_county_of_residence import (
    PERSONS_TO_RECENT_COUNTY_OF_RESIDENCE_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.reference.persons_with_last_known_address import (
    PERSONS_WITH_LAST_KNOWN_ADDRESS_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.reference.prison_to_supervision_transitions import (
    PRISON_TO_SUPERVISION_TRANSITIONS_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.reference.revocations_matrix_by_person import (
    REVOCATIONS_MATRIX_BY_PERSON_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.reference.sentence_judicial_district_association import (
    SENTENCE_JUDICIAL_DISTRICT_ASSOCIATION_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.reference.single_day_incarceration_population_for_spotlight import (
    SINGLE_DAY_INCARCERATION_POPULATION_FOR_SPOTLIGHT_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.reference.single_day_supervision_population_for_spotlight import (
    SINGLE_DAY_SUPERVISION_POPULATION_FOR_SPOTLIGHT_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.reference.supervision_case_compliance_metrics import (
    SUPERVISION_CASE_COMPLIANCE_METRICS_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.reference.supervision_matrix_by_person import (
    SUPERVISION_MATRIX_BY_PERSON_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.reference.supervision_mismatches_by_day import (
    SUPERVISION_MISMATCHES_BY_DAY_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.reference.supervision_period_judicial_district_association import (
    SUPERVISION_PERIOD_JUDICIAL_DISTRICT_ASSOCIATION_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.reference.supervision_period_to_agent_association import (
    SUPERVISION_PERIOD_TO_AGENT_ASSOCIATION_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.reference.supervision_termination_matrix_by_person import (
    SUPERVISION_TERMINATION_MATRIX_BY_PERSON_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.reference.supervision_to_liberty_transitions import (
    SUPERVISION_TO_LIBERTY_TRANSITIONS_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.reference.supervision_to_prison_transitions import (
    SUPERVISION_TO_PRISON_TRANSITIONS_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.reference.us_id_case_update_info import (
    US_ID_CASE_UPDATE_INFO_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.reference.us_mo_sentence_statuses import (
    US_MO_SENTENCE_STATUSES_VIEW_BUILDER,
)

REFERENCE_VIEW_BUILDERS: List[BigQueryViewBuilder] = [
    DASHBOARD_USER_RESTRICTIONS_VIEW_BUILDER,
    EVENT_BASED_COMMITMENTS_FROM_SUPERVISION_FOR_MATRIX_VIEW_BUILDER,
    AUGMENTED_AGENT_INFO_VIEW_BUILDER,
    SUPERVISION_PERIOD_TO_AGENT_ASSOCIATION_VIEW_BUILDER,
    PERSONS_WITH_LAST_KNOWN_ADDRESS_VIEW_BUILDER,
    PERSONS_TO_RECENT_COUNTY_OF_RESIDENCE_VIEW_BUILDER,
    EVENT_BASED_ADMISSIONS_VIEW_BUILDER,
    EVENT_BASED_PROGRAM_REFERRALS_VIEW_BUILDER,
    EVENT_BASED_COMMITMENTS_FROM_SUPERVISION_VIEW_BUILDER,
    EVENT_BASED_SUPERVISION_VIEW_BUILDER,
    EVENT_BASED_SUPERVISION_POPULATIONS_WITH_COMMITMENTS_FOR_RATE_DENOMINATORS_VIEW_BUILDER,
    INCARCERATION_LOCATION_IDS_TO_NAMES_VIEW_BUILDER,
    INCARCERATION_PERIOD_JUDICIAL_DISTRICT_ASSOCIATION_VIEW_BUILDER,
    REVOCATIONS_MATRIX_BY_PERSON_VIEW_BUILDER,
    SUPERVISION_MATRIX_BY_PERSON_VIEW_BUILDER,
    ADMISSION_TYPES_PER_STATE_FOR_MATRIX_VIEW_BUILDER,
    US_MO_SENTENCE_STATUSES_VIEW_BUILDER,
    COVID_REPORT_WEEKS_VIEW_BUILDER,
    SENTENCE_JUDICIAL_DISTRICT_ASSOCIATION_VIEW_BUILDER,
    SUPERVISION_PERIOD_JUDICIAL_DISTRICT_ASSOCIATION_VIEW_BUILDER,
    SUPERVISION_TERMINATION_MATRIX_BY_PERSON_VIEW_BUILDER,
    SINGLE_DAY_INCARCERATION_POPULATION_FOR_SPOTLIGHT_VIEW_BUILDER,
    SINGLE_DAY_SUPERVISION_POPULATION_FOR_SPOTLIGHT_VIEW_BUILDER,
    AGENT_EXTERNAL_ID_TO_FULL_NAMES_VIEW_BUILDER,
    LIBERTY_TO_PRISON_TRANSITIONS_VIEW_BUILDER,
    SUPERVISION_TO_LIBERTY_TRANSITIONS_VIEW_BUILDER,
    SUPERVISION_TO_PRISON_TRANSITIONS_VIEW_BUILDER,
    PRISON_TO_SUPERVISION_TRANSITIONS_VIEW_BUILDER,
    OVERDUE_DISCHARGE_OUTCOMES_VIEW_BUILDER,
    OVERDUE_DISCHARGE_ALERT_EXCLUSIONS_VIEW_BUILDER,
    SUPERVISION_CASE_COMPLIANCE_METRICS_VIEW_BUILDER,
    SUPERVISION_MISMATCHES_BY_DAY_VIEW_BUILDER,
    US_ID_CASE_UPDATE_INFO_VIEW_BUILDER,
]
