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
from recidiviz.calculator.query.state.views.reference.incarceration_location_ids_to_names import (
    INCARCERATION_LOCATION_IDS_TO_NAMES_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.reference.incarceration_period_judicial_district_association import (
    INCARCERATION_PERIOD_JUDICIAL_DISTRICT_ASSOCIATION_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.reference.normalized_supervision_period_to_agent_association import (
    NORMALIZED_SUPERVISION_PERIOD_TO_AGENT_ASSOCIATION_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.reference.persons_to_recent_county_of_residence import (
    PERSONS_TO_RECENT_COUNTY_OF_RESIDENCE_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.reference.persons_with_last_known_address import (
    PERSONS_WITH_LAST_KNOWN_ADDRESS_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.reference.sentence_judicial_district_association import (
    SENTENCE_JUDICIAL_DISTRICT_ASSOCIATION_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.reference.supervision_period_judicial_district_association import (
    SUPERVISION_PERIOD_JUDICIAL_DISTRICT_ASSOCIATION_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.reference.supervision_period_to_agent_association import (
    SUPERVISION_PERIOD_TO_AGENT_ASSOCIATION_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.reference.us_id_case_update_info import (
    US_ID_CASE_UPDATE_INFO_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.reference.us_mo_sentence_statuses import (
    US_MO_SENTENCE_STATUSES_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.reference.us_tn_reidentified_users import (
    US_TN_REIDENTIFIED_USERS_VIEW_BUILDER,
)

REFERENCE_VIEW_BUILDERS: List[BigQueryViewBuilder] = [
    DASHBOARD_USER_RESTRICTIONS_VIEW_BUILDER,
    AUGMENTED_AGENT_INFO_VIEW_BUILDER,
    SUPERVISION_PERIOD_TO_AGENT_ASSOCIATION_VIEW_BUILDER,
    PERSONS_WITH_LAST_KNOWN_ADDRESS_VIEW_BUILDER,
    PERSONS_TO_RECENT_COUNTY_OF_RESIDENCE_VIEW_BUILDER,
    INCARCERATION_LOCATION_IDS_TO_NAMES_VIEW_BUILDER,
    INCARCERATION_PERIOD_JUDICIAL_DISTRICT_ASSOCIATION_VIEW_BUILDER,
    US_MO_SENTENCE_STATUSES_VIEW_BUILDER,
    COVID_REPORT_WEEKS_VIEW_BUILDER,
    SENTENCE_JUDICIAL_DISTRICT_ASSOCIATION_VIEW_BUILDER,
    SUPERVISION_PERIOD_JUDICIAL_DISTRICT_ASSOCIATION_VIEW_BUILDER,
    AGENT_EXTERNAL_ID_TO_FULL_NAMES_VIEW_BUILDER,
    US_ID_CASE_UPDATE_INFO_VIEW_BUILDER,
    US_TN_REIDENTIFIED_USERS_VIEW_BUILDER,
    NORMALIZED_SUPERVISION_PERIOD_TO_AGENT_ASSOCIATION_VIEW_BUILDER,
]
