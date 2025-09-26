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
"""Include constants for use by email reports."""
from enum import Enum

import attr

from recidiviz.common.constants.states import StateCode

DEFAULT_EMAIL_SUBJECT = "Your monthly Recidiviz report"


class ReportType(Enum):
    OutliersSupervisionOfficerSupervisor = "outliers_supervision_officer_supervisor"


@attr.s(auto_attribs=True)
class Batch:
    state_code: StateCode
    report_type: ReportType
    batch_id: str


BRAND_STYLES = {
    "fonts": {
        "serif": "font-family: 'Libre Baskerville', Garamond, serif; line-height: 1.33; letter-spacing: -0.03em;",
        "sans_serif": "font-family: 'Libre Franklin', sans-serif; line-height: 1.33; letter-spacing: -0.01em;",
    },
}
