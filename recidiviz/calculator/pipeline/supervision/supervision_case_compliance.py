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
"""Information related to whether a supervision case is meeting compliance standards."""
from datetime import date

import attr

from recidiviz.common.attr_mixins import BuildableAttr


@attr.s(frozen=True)
class SupervisionCaseCompliance(BuildableAttr):
    """Stores information related to whether a supervision case is meeting compliance standards."""

    # The date the on which the case's compliance was evaluated
    date_of_evaluation: date = attr.ib(default=None)

    # Whether or not a risk assessment has been completed for this person with enough recency to satisfy compliance
    # measures
    assessment_up_to_date: bool = attr.ib(default=None)

    # TODO(3304): Implement residence verification and contact compliance measures
