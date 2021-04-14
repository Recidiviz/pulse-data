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
"""All the views for the vitals report."""
from typing import List

from recidiviz.big_query.big_query_view import BigQueryViewBuilder
from recidiviz.calculator.query.state.views.vitals_report.overdue_lsir_by_po_by_day import (
    OVERDUE_LSIR_BY_PO_BY_DAY_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.vitals_report.supervision_population_by_po_by_day import (
    SUPERVISION_POPULATION_BY_PO_BY_DAY_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.vitals_report.supervision_population_due_for_release_by_po_by_day import (
    SUPERVISION_POPULATION_DUE_FOR_RELEASE_BY_PO_BY_DAY_VIEW_BUILDER,
)


# NOTE: These views must be listed in order of dependency. For example, if view Y depends on view X, then view X should
# appear in the list before view Y.
VITALS_REPORT_VIEW_BUILDERS: List[BigQueryViewBuilder] = [
    SUPERVISION_POPULATION_BY_PO_BY_DAY_VIEW_BUILDER,
    OVERDUE_LSIR_BY_PO_BY_DAY_VIEW_BUILDER,
    SUPERVISION_POPULATION_DUE_FOR_RELEASE_BY_PO_BY_DAY_VIEW_BUILDER,
]
