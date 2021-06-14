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

"""Utilities for selecting from available report contexts."""

from recidiviz.common.constants.states import StateCode
from recidiviz.reporting.context.po_monthly_report.constants import ReportType
from recidiviz.reporting.context.po_monthly_report.context import PoMonthlyReportContext
from recidiviz.reporting.context.report_context import ReportContext
from recidiviz.reporting.context.top_opportunities.context import (
    TopOpportunitiesReportContext,
)
from recidiviz.reporting.recipient import Recipient


def get_report_context(
    state_code: StateCode, report_type: ReportType, recipient: Recipient
) -> ReportContext:
    """Returns the appropriate report context for the given parameters, choosing the correct ReportContext
    implementation.

    Args:
        state_code: State identifier for the recipient
        report_type: The type of report to be sent to the recipient
        recipient: The retrieved data for this recipient
    """
    if report_type == ReportType.POMonthlyReport:
        return PoMonthlyReportContext(state_code, recipient)
    if report_type == ReportType.TopOpportunities:
        return TopOpportunitiesReportContext(state_code, recipient)

    raise KeyError(f"Unrecognized report type: {report_type}")
