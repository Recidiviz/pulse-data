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
"""All views for the PO Report."""
from typing import List

from recidiviz.big_query.big_query_view import BigQueryViewBuilder
from recidiviz.calculator.query.state.views.po_report.officer_supervision_district_association import \
    OFFICER_SUPERVISION_DISTRICT_ASSOCIATION_VIEW_BUILDER
from recidiviz.calculator.query.state.views.po_report.po_monthly_report_data import PO_MONTHLY_REPORT_DATA_VIEW_BUILDER
from recidiviz.calculator.query.state.views.po_report.revocations_by_officer_by_month import \
    REVOCATIONS_BY_OFFICER_BY_MONTH_VIEW_BUILDER
from recidiviz.calculator.query.state.views.po_report.supervision_absconsion_terminations_by_officer_by_month import \
    SUPERVISION_ABSCONSION_TERMINATIONS_BY_OFFICER_BY_MONTH_VIEW_BUILDER
from recidiviz.calculator.query.state.views.po_report.supervision_compliance_by_officer_by_month import \
    SUPERVISION_COMPLIANCE_BY_OFFICER_BY_MONTH_VIEW_BUILDER
from recidiviz.calculator.query.state.views.po_report.supervision_discharges_by_officer_by_month import \
    SUPERVISION_DISCHARGES_BY_OFFICER_BY_MONTH_VIEW_BUILDER
from recidiviz.calculator.query.state.views.po_report.supervision_early_discharge_requests_by_officer_by_month import \
    SUPERVISION_EARLY_DISCHARGE_REQUESTS_BY_OFFICER_BY_MONTH_VIEW_BUILDER

# NOTE: These views must be listed in order of dependency. For example, if view Y depends on view X, then view X should
# appear in the list before view Y.
PO_REPORT_VIEW_BUILDERS: List[BigQueryViewBuilder] = [
    OFFICER_SUPERVISION_DISTRICT_ASSOCIATION_VIEW_BUILDER,
    REVOCATIONS_BY_OFFICER_BY_MONTH_VIEW_BUILDER,
    SUPERVISION_DISCHARGES_BY_OFFICER_BY_MONTH_VIEW_BUILDER,
    SUPERVISION_ABSCONSION_TERMINATIONS_BY_OFFICER_BY_MONTH_VIEW_BUILDER,
    SUPERVISION_COMPLIANCE_BY_OFFICER_BY_MONTH_VIEW_BUILDER,
    SUPERVISION_EARLY_DISCHARGE_REQUESTS_BY_OFFICER_BY_MONTH_VIEW_BUILDER,
    # PO_MONTHLY_REPORT_DATA_VIEW_BUILDER must be last in this list because it relies on the materialized versions of
    # all of the other PO Report views
    PO_MONTHLY_REPORT_DATA_VIEW_BUILDER
]
