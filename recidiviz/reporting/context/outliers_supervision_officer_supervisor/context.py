# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2023 Recidiviz, Inc.
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
"""Report context for the Outliers Supervision Officer Supervisor email."""
from typing import List

from jinja2 import Template

from recidiviz.reporting.context.po_monthly_report.constants import ReportType
from recidiviz.reporting.context.report_context import ReportContext


class OutliersSupervisionOfficerSupervisorContext(ReportContext):
    """Report context for the Outliers Supervision Officer Supervisor email."""

    def get_report_type(self) -> ReportType:
        return ReportType.OutliersSupervisionOfficerSupervisor

    def get_required_recipient_data_fields(self) -> List[str]:
        # TODO(#21175): Blocked on update query for report data
        return []

    @property
    def html_template(self) -> Template:
        # TODO(#21159): Blocked on design changes
        return self.jinja_env.get_template("")

    def _prepare_for_generation(self) -> dict:
        # TODO(#21159): Blocked on design changes
        return {}
