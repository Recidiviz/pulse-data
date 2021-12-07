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
"""Contains the factory class for creating PoMonthlyReportMetricsDelegate objects"""

from recidiviz.common.constants.states import StateCode
from recidiviz.reporting.context.po_monthly_report.state_utils.po_monthly_report_metrics_delegate import (
    PoMonthlyReportMetricsDelegate,
)
from recidiviz.reporting.context.po_monthly_report.state_utils.us_id.us_id_metrics_delegate import (
    UsIdMetricsDelegate,
)
from recidiviz.reporting.context.po_monthly_report.state_utils.us_mo.us_mo_metrics_delegate import (
    UsMoMetricsDelegate,
)
from recidiviz.reporting.context.po_monthly_report.state_utils.us_pa.us_pa_metrics_delegate import (
    UsPaMetricsDelegate,
)


class PoMonthlyReportMetricsDelegateFactory:
    @classmethod
    def build(cls, *, state_code: StateCode) -> PoMonthlyReportMetricsDelegate:
        if state_code == StateCode.US_ID:
            return UsIdMetricsDelegate()
        if state_code == StateCode.US_PA:
            return UsPaMetricsDelegate()
        if state_code == StateCode.US_MO:
            return UsMoMetricsDelegate()
        raise ValueError(f"Unexpected state_code provided: {state_code}")
