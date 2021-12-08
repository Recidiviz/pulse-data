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
"""All views for the PO Report."""
from typing import List

from recidiviz.big_query.big_query_view import BigQueryViewBuilder
from recidiviz.calculator.query.state.views.line_staff_validation.caseload_and_district import (
    CASELOAD_AND_DISTRICT_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.line_staff_validation.contacts_and_assessments import (
    CONTACTS_AND_ASSESSMENTS_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.line_staff_validation.looker_dashboard import (
    LOOKER_DASHBOARD_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.line_staff_validation.metrics import (
    METRICS_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.line_staff_validation.metrics_from_po_report import (
    METRICS_FROM_PO_REPORT_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.line_staff_validation.offense_types import (
    OFFENSE_TYPES_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.line_staff_validation.overdue_discharges import (
    OVERDUE_DISCHARGES_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.line_staff_validation.po_events import (
    PO_EVENTS_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.line_staff_validation.recommended_downgrades import (
    RECOMMENDED_DOWNGRADES_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.line_staff_validation.violations import (
    VIOLATIONS_VIEW_BUILDER,
)

# NOTE: These views must be listed in order of dependency. For example, if view Y depends on view X, then view X should
# appear in the list before view Y.
LINE_STAFF_VALIDATION_VIEW_BUILDERS: List[BigQueryViewBuilder] = [
    CASELOAD_AND_DISTRICT_VIEW_BUILDER,
    CONTACTS_AND_ASSESSMENTS_VIEW_BUILDER,
    METRICS_VIEW_BUILDER,
    LOOKER_DASHBOARD_VIEW_BUILDER,
    METRICS_FROM_PO_REPORT_VIEW_BUILDER,
    OFFENSE_TYPES_VIEW_BUILDER,
    OVERDUE_DISCHARGES_VIEW_BUILDER,
    PO_EVENTS_VIEW_BUILDER,
    RECOMMENDED_DOWNGRADES_VIEW_BUILDER,
    VIOLATIONS_VIEW_BUILDER,
]
