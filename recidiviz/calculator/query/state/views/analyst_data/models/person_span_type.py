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
"""Defines PersonSpanType enum."""

from enum import Enum


class PersonSpanType(Enum):
    """Category of span-shaped data"""

    ASSESSMENT_SCORE_SESSION = "ASSESSMENT_SCORE_SESSION"
    COMPARTMENT_SESSION = "COMPARTMENT_SESSION"
    COMPLETED_CONTACT_SESSION = "COMPLETED_CONTACT_SESSION"
    CUSTODY_LEVEL_SESSION = "CUSTODY_LEVEL_SESSION"
    EMPLOYMENT_PERIOD = "EMPLOYMENT_PERIOD"
    EMPLOYMENT_STATUS_SESSION = "EMPLOYMENT_STATUS_SESSION"
    HOUSING_TYPE_SESSION = "HOUSING_TYPE_SESSION"
    JUSTICE_IMPACT_SESSION = "JUSTICE_IMPACT_SESSION"
    PERSON_DEMOGRAPHICS = "PERSON_DEMOGRAPHICS"
    SENTENCE_SPAN = "SENTENCE_SPAN"
    SUPERVISION_LEVEL_DOWNGRADE_ELIGIBLE = "SUPERVISION_LEVEL_DOWNGRADE_ELIGIBLE"
    SUPERVISION_LEVEL_SESSION = "SUPERVISION_LEVEL_SESSION"
    SUPERVISION_OFFICER_SESSION = "SUPERVISION_OFFICER_SESSION"
    TASK_ELIGIBILITY_SESSION = "TASK_ELIGIBILITY_SESSION"
