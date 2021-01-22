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
from typing import Optional

import attr

from recidiviz.common.attr_mixins import BuildableAttr


@attr.s(frozen=True)
class SupervisionCaseCompliance(BuildableAttr):
    """Stores information related to whether a supervision case is meeting compliance standards, and counts of
    compliance-related tasks that occurred in the month of the evaluation."""

    # The date the on which the case's compliance was evaluated
    date_of_evaluation: date = attr.ib(default=None)

    # The number of risk assessments conducted on this person in the month of the date_of_evaluation, preceding the
    # date_of_evaluation
    assessment_count: int = attr.ib(default=None)

    # The date that the last assessment happened. If no assessment has yet happened, this is None.
    most_recent_assessment_date: Optional[date] = attr.ib(default=None)

    # Whether or not a risk assessment has been completed for this person with enough recency to satisfy compliance
    # measures. Should be unset if we do not know the compliance standards for this person.
    assessment_up_to_date: Optional[bool] = attr.ib(default=None)

    # The number of face-to-face contacts with this person in the month of the date_of_evaluation, preceding the
    # date_of_evaluation
    face_to_face_count: int = attr.ib(default=None)

    # The date that the last face-to-face contact happened. If no meetings have yet happened, this is None.
    most_recent_face_to_face_date: Optional[date] = attr.ib(default=None)

    # Whether or not the supervision officer has had face-to-face contact with the person on supervision recently
    # enough to satisfy compliance measures. Should be unset if we do not know the compliance standards for this person.
    face_to_face_frequency_sufficient: Optional[bool] = attr.ib(default=None)
