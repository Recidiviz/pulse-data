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
"""Constants for line staff tools admin"""
from typing import Dict

from recidiviz.admin_panel.line_staff_tools.raw_data import (
    US_TN_STANDARDS_ADMIN_SCHEMA,
    US_TN_STANDARDS_DUE_SCHEMA,
    RawDataConfig,
)
from recidiviz.common.constants.states import StateCode

EMAIL_STATE_CODES = [StateCode.US_ID, StateCode.US_PA, StateCode.US_MO, StateCode.US_IX]

RAW_FILES_CONFIG: Dict[StateCode, Dict[str, RawDataConfig]] = {
    StateCode.US_TN: {
        "STANDARDS_DUE": RawDataConfig(
            table_name="us_tn_standards_due",
            schema=US_TN_STANDARDS_DUE_SCHEMA,
        ),
        "STANDARDS_ADMIN": RawDataConfig(
            table_name="us_tn_standards_admin",
            schema=US_TN_STANDARDS_ADMIN_SCHEMA,
        ),
    }
}
