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
from recidiviz.calculator.query.state.views.sentencing.case_insights_record import (
    SENTENCING_CASE_INSIGHTS_RECORD_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.sentencing.case_record import (
    SENTENCING_CASE_RECORD_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.sentencing.charge_record import (
    SENTENCING_CHARGE_RECORD_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.sentencing.client_record import (
    SENTENCING_CLIENT_RECORD_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.sentencing.community_opportunity_record import (
    SENTENCING_COMMUNITY_OPPORTUNITY_RECORD_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.sentencing.counties_and_districts import (
    SENTENCING_COUNTIES_AND_DISTRICTS_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.sentencing.recidivism_event import (
    RECIDIVISM_EVENT_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.sentencing.sentence_cohort import (
    SENTENCE_COHORT_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.sentencing.staff_record import (
    SENTENCING_STAFF_RECORD_VIEW_BUILDER,
)

CASE_INSIGHTS_VIEW_BUILDERS: List[BigQueryViewBuilder] = [
    RECIDIVISM_EVENT_VIEW_BUILDER,
    SENTENCE_COHORT_VIEW_BUILDER,
]

SENTENCING_VIEW_BUILDERS: List[BigQueryViewBuilder] = [
    SENTENCING_CASE_RECORD_VIEW_BUILDER,
    SENTENCING_CLIENT_RECORD_VIEW_BUILDER,
    SENTENCING_STAFF_RECORD_VIEW_BUILDER,
    SENTENCING_COMMUNITY_OPPORTUNITY_RECORD_VIEW_BUILDER,
    SENTENCING_CHARGE_RECORD_VIEW_BUILDER,
    SENTENCING_CASE_INSIGHTS_RECORD_VIEW_BUILDER,
    SENTENCING_COUNTIES_AND_DISTRICTS_VIEW_BUILDER,
]
