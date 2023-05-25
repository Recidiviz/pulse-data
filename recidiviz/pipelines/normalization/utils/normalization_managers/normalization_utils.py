# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2022 Recidiviz, Inc.
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
"""Util functions used by more than one EntityNormalizationManager."""

from typing import List

from recidiviz.persistence.entity.state.entities import PeriodType


def drop_fuzzy_matched_periods(periods: List[PeriodType]) -> List[PeriodType]:
    """Drops periods with the string 'FUZZY_MATCHED' in the external_id."""
    return [
        p for p in periods if not p.external_id or "FUZZY_MATCHED" not in p.external_id
    ]
