# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2019 Recidiviz, Inc.
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
"""Reference views used by other views."""
# pylint: disable=line-too-long
from recidiviz.calculator.bq.dashboard.views.reference.most_recent_calculate_job import \
    MOST_RECENT_CALCULATE_JOB_VIEW
from recidiviz.calculator.bq.dashboard.views.reference.incarceration_admissions_60_days import \
    INCARCERATION_ADMISSIONS_60_DAYS_VIEW

REF_VIEWS = [
    MOST_RECENT_CALCULATE_JOB_VIEW,
    INCARCERATION_ADMISSIONS_60_DAYS_VIEW,
]
