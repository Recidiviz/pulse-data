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
from typing import Sequence

from recidiviz.big_query.big_query_view import BigQueryViewBuilder
from recidiviz.big_query.selected_columns_big_query_view import (
    SelectedColumnsBigQueryViewBuilder,
)
from recidiviz.case_triage.views.employment_periods import (
    CURRENT_EMPLOYMENT_PERIODS_VIEW_BUILDER,
)
from recidiviz.case_triage.views.etl_clients import CLIENT_LIST_VIEW_BUILDER
from recidiviz.case_triage.views.etl_officers import OFFICER_LIST_VIEW_BUILDER
from recidiviz.case_triage.views.etl_opportunities import TOP_OPPORTUNITIES_VIEW_BUILDER
from recidiviz.case_triage.views.last_known_date_of_employment import (
    LAST_KNOWN_DATE_OF_EMPLOYMENT_VIEW_BUILDER,
)
from recidiviz.case_triage.views.latest_assessments import (
    LATEST_ASSESSMENTS_VIEW_BUILDER,
)

CASE_TRIAGE_EXPORTED_VIEW_BUILDERS: Sequence[SelectedColumnsBigQueryViewBuilder] = [
    CLIENT_LIST_VIEW_BUILDER,
    OFFICER_LIST_VIEW_BUILDER,
    TOP_OPPORTUNITIES_VIEW_BUILDER,
]

VIEW_BUILDERS_FOR_VIEWS_TO_UPDATE: Sequence[BigQueryViewBuilder] = [
    CLIENT_LIST_VIEW_BUILDER,
    CURRENT_EMPLOYMENT_PERIODS_VIEW_BUILDER,
    LAST_KNOWN_DATE_OF_EMPLOYMENT_VIEW_BUILDER,
    LATEST_ASSESSMENTS_VIEW_BUILDER,
    OFFICER_LIST_VIEW_BUILDER,
    TOP_OPPORTUNITIES_VIEW_BUILDER,
]
