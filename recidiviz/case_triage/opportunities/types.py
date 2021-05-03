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
"""Implements common types for opportunities."""
from enum import Enum


class OpportunityDoesNotExistError(ValueError):
    pass


class OpportunityType(Enum):
    OVERDUE_DISCHARGE = "OVERDUE_DISCHARGE"
    OVERDUE_DOWNGRADE = "OVERDUE_DOWNGRADE"
    EARLY_DISCHARGE = "EARLY_DISCHARGE"


class OpportunityDeferralType(Enum):
    REMINDER = "REMINDER"
    ACTION_TAKEN = "ACTION_TAKEN"
    INCORRECT_DATA = "INCORRECT_DATA"
