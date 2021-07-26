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
"""Utils for freshness checks for Case Triage."""
from recidiviz.validation.views.utils.freshness_validation import (
    current_date_add,
    current_date_sub,
)

FRESHNESS_FRAGMENT = f"{current_date_sub(days=1)} AND {current_date_add(days=100)}"

DT_CLAUSE = "SAFE_CAST(SPLIT({datetime_field}, ' ')[OFFSET(0)] AS DATE)"
