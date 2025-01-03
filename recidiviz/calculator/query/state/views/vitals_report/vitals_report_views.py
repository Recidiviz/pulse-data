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

from recidiviz.aggregated_metrics.aggregated_metrics_view_collector import (
    collect_aggregated_metric_view_builders_for_collection,
)
from recidiviz.big_query.big_query_view import BigQueryViewBuilder
from recidiviz.calculator.query.state.views.vitals_report.overdue_lsir_by_po_by_day import (
    OVERDUE_LSIR_BY_PO_BY_DAY_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.vitals_report.supervision_case_compliance_spans import (
    SUPERVISION_CASE_COMPLIANCE_SPANS_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.vitals_report.supervision_downgrade_opportunities_by_po_by_day import (
    SUPERVISION_DOWNGRADE_OPPORTUNITIES_BY_PO_BY_DAY_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.vitals_report.supervision_officers_and_districts import (
    SUPERVISION_OFFICERS_AND_DISTRICTS_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.vitals_report.supervision_population_by_po_by_day import (
    SUPERVISION_POPULATION_BY_PO_BY_DAY_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.vitals_report.supervision_population_due_for_release_by_po_by_day import (
    SUPERVISION_POPULATION_DUE_FOR_RELEASE_BY_PO_BY_DAY_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.vitals_report.timely_contact_by_po_by_day import (
    TIMELY_CONTACT_BY_PO_BY_DAY_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.vitals_report.vitals_aggregated_metrics import (
    VITALS_AGGREGATED_METRICS_COLLECTION_CONFIG,
)

VITALS_REPORT_VIEW_BUILDERS: List[BigQueryViewBuilder] = [
    SUPERVISION_OFFICERS_AND_DISTRICTS_VIEW_BUILDER,
    SUPERVISION_POPULATION_BY_PO_BY_DAY_VIEW_BUILDER,
    OVERDUE_LSIR_BY_PO_BY_DAY_VIEW_BUILDER,
    SUPERVISION_POPULATION_DUE_FOR_RELEASE_BY_PO_BY_DAY_VIEW_BUILDER,
    TIMELY_CONTACT_BY_PO_BY_DAY_VIEW_BUILDER,
    SUPERVISION_DOWNGRADE_OPPORTUNITIES_BY_PO_BY_DAY_VIEW_BUILDER,
    SUPERVISION_CASE_COMPLIANCE_SPANS_VIEW_BUILDER,
    *collect_aggregated_metric_view_builders_for_collection(
        VITALS_AGGREGATED_METRICS_COLLECTION_CONFIG
    ),
]
