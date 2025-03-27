# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2025 Recidiviz, Inc.
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
# ============================================================================
"""Utils for categorizing the type of product as relevant to calculating impact
via transitions breadth and depth metrics"""
from enum import Enum


class ProductTransitionType(Enum):
    """Represents the type of transition event based on the product or product component
    that is anticipated to drive that type of transition"""

    SUPERVISOR_OUTCOMES_MODULE = "SUPERVISOR_OUTCOMES_MODULE"
    SUPERVISOR_OPPORTUNITIES_MODULE_DISCRETIONARY = (
        "SUPERVISOR_OPPORTUNITIES_MODULE_DISCRETIONARY"
    )
    SUPERVISOR_OPPORTUNITIES_MODULE_MANDATORY = (
        "SUPERVISOR_OPPORTUNITIES_MODULE_MANDATORY"
    )
    WORKFLOWS_DISCRETIONARY = "WORKFLOWS_DISCRETIONARY"
    WORKFLOWS_MANDATORY = "WORKFLOWS_MANDATORY"

    @property
    def pretty_name(self) -> str:
        return self.value.lower()
