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
from recidiviz.calculator.query.state.views.po_report.absconsion_reports_by_person_by_month import (
    ABSCONSION_REPORTS_BY_PERSON_BY_MONTH_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.po_report.current_action_items_by_person import (
    CURRENT_ACTION_ITEMS_BY_PERSON_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.po_report.officer_supervision_district_association import (
    OFFICER_SUPERVISION_DISTRICT_ASSOCIATION_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.po_report.po_monthly_report_data import (
    PO_MONTHLY_REPORT_DATA_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.po_report.report_data_by_person_by_month import (
    REPORT_DATA_BY_PERSON_BY_MONTH_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.po_report.revocation_reports_by_person_by_month import (
    REVOCATION_REPORTS_BY_PERSON_BY_MONTH_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.po_report.sendgrid_po_report_email_events import (
    SENDGRID_PO_REPORT_EMAIL_EVENTS_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.po_report.successful_supervision_completions_by_person_by_month import (
    SUCCESSFUL_SUPERVISION_COMPLETIONS_BY_PERSON_BY_MONTH_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.po_report.supervision_compliance_by_person_by_month import (
    SUPERVISION_COMPLIANCE_BY_PERSON_BY_MONTH_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.po_report.supervision_downgrades_by_person_by_month import (
    SUPERVISION_DOWNGRADES_BY_PERSON_BY_MONTH_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.po_report.supervision_earned_discharge_requests_by_person_by_month import (
    SUPERVISION_EARNED_DISCHARGE_REQUESTS_BY_PERSON_BY_MONTH_VIEW_BUILDER,
)

# NOTE: These views must be listed in order of dependency. For example, if view Y depends on view X, then view X should
# appear in the list before view Y.
PO_REPORT_VIEW_BUILDERS: List[BigQueryViewBuilder] = [
    OFFICER_SUPERVISION_DISTRICT_ASSOCIATION_VIEW_BUILDER,
    ABSCONSION_REPORTS_BY_PERSON_BY_MONTH_VIEW_BUILDER,
    REVOCATION_REPORTS_BY_PERSON_BY_MONTH_VIEW_BUILDER,
    SUCCESSFUL_SUPERVISION_COMPLETIONS_BY_PERSON_BY_MONTH_VIEW_BUILDER,
    SUPERVISION_COMPLIANCE_BY_PERSON_BY_MONTH_VIEW_BUILDER,
    SUPERVISION_DOWNGRADES_BY_PERSON_BY_MONTH_VIEW_BUILDER,
    SUPERVISION_EARNED_DISCHARGE_REQUESTS_BY_PERSON_BY_MONTH_VIEW_BUILDER,
    REPORT_DATA_BY_PERSON_BY_MONTH_VIEW_BUILDER,
    CURRENT_ACTION_ITEMS_BY_PERSON_VIEW_BUILDER,
    # PO_MONTHLY_REPORT_DATA_VIEW_BUILDER must be last in this list because it relies on the materialized versions of
    # all of the other PO Report views
    PO_MONTHLY_REPORT_DATA_VIEW_BUILDER,
    SENDGRID_PO_REPORT_EMAIL_EVENTS_VIEW_BUILDER,
]
