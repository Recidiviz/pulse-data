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

from recidiviz.reporting.context.po_monthly_report.context import PoMonthlyReportContext
from recidiviz.reporting.context.report_context import ReportContext


def get_report_context(state_code: str, report_type: str, recipient_data: dict) -> ReportContext:
    """Returns the appropriate report context for the given parameters, choosing the correct ReportContext
    implementation.

    Args:
        state_code: State identifier for the recipient
        report_type: The type of report to be sent to the recipient
        recipient_data: The retrieved data for this recipient
    """
    if report_type == "po_monthly_report":
        return PoMonthlyReportContext(state_code, recipient_data)

    raise KeyError(f"Unrecognized report type: {report_type}")
