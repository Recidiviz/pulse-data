# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2021 Recidiviz, Inc.
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
"""Case Triage view configuration."""
from typing import Dict, Sequence, Type

from recidiviz.big_query.big_query_view import BigQueryViewBuilder
from recidiviz.big_query.selected_columns_big_query_view import (
    SelectedColumnsBigQueryViewBuilder,
)
from recidiviz.calculator.query.state.views.reference.dashboard_user_restrictions import (
    DASHBOARD_USER_RESTRICTIONS_VIEW_BUILDER,
)
from recidiviz.case_triage.views.client_contact_info import (
    CLIENT_CONTACT_INFO_VIEW_BUILDER,
)
from recidiviz.case_triage.views.client_eligibility_criteria import (
    CLIENT_ELIGIBILITY_CRITERIA_VIEW_BUILDER,
)
from recidiviz.case_triage.views.employment_periods import (
    CURRENT_EMPLOYMENT_PERIODS_VIEW_BUILDER,
)
from recidiviz.case_triage.views.etl_client_events import CLIENT_EVENTS_VIEW_BUILDER
from recidiviz.case_triage.views.etl_clients import CLIENT_LIST_VIEW_BUILDER
from recidiviz.case_triage.views.etl_officers import OFFICER_LIST_VIEW_BUILDER
from recidiviz.case_triage.views.etl_opportunities import TOP_OPPORTUNITIES_VIEW_BUILDER
from recidiviz.case_triage.views.last_known_date_of_employment import (
    LAST_KNOWN_DATE_OF_EMPLOYMENT_VIEW_BUILDER,
)
from recidiviz.persistence.database.base_schema import CaseTriageBase
from recidiviz.persistence.database.schema.case_triage.schema import (
    DashboardUserRestrictions,
    ETLClient,
    ETLClientEvent,
    ETLOfficer,
    ETLOpportunity,
)

CASE_TRIAGE_EXPORTED_VIEW_BUILDERS: Sequence[SelectedColumnsBigQueryViewBuilder] = [
    CLIENT_LIST_VIEW_BUILDER,
    OFFICER_LIST_VIEW_BUILDER,
    TOP_OPPORTUNITIES_VIEW_BUILDER,
    CLIENT_EVENTS_VIEW_BUILDER,
]

VIEW_BUILDERS_FOR_VIEWS_TO_UPDATE: Sequence[BigQueryViewBuilder] = [
    CLIENT_CONTACT_INFO_VIEW_BUILDER,
    CLIENT_ELIGIBILITY_CRITERIA_VIEW_BUILDER,
    CLIENT_LIST_VIEW_BUILDER,
    CURRENT_EMPLOYMENT_PERIODS_VIEW_BUILDER,
    LAST_KNOWN_DATE_OF_EMPLOYMENT_VIEW_BUILDER,
    OFFICER_LIST_VIEW_BUILDER,
    TOP_OPPORTUNITIES_VIEW_BUILDER,
    CLIENT_EVENTS_VIEW_BUILDER,
]


# Map from ETL Schema to associated view builder
ETL_TABLES: Dict[Type[CaseTriageBase], SelectedColumnsBigQueryViewBuilder] = {
    DashboardUserRestrictions: DASHBOARD_USER_RESTRICTIONS_VIEW_BUILDER,
    ETLClient: CLIENT_LIST_VIEW_BUILDER,
    ETLOfficer: OFFICER_LIST_VIEW_BUILDER,
    ETLOpportunity: TOP_OPPORTUNITIES_VIEW_BUILDER,
    ETLClientEvent: CLIENT_EVENTS_VIEW_BUILDER,
}
