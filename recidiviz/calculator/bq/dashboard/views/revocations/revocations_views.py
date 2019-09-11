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
"""Views related to revocations of supervision."""
# pylint: disable=line-too-long
from recidiviz.calculator.bq.dashboard.views.revocations.revocations_by_county_60_days import \
    REVOCATIONS_BY_COUNTY_60_DAYS_VIEW
from recidiviz.calculator.bq.dashboard.views.revocations.revocations_by_month import \
    REVOCATIONS_BY_MONTH_VIEW
from recidiviz.calculator.bq.dashboard.views.revocations.revocations_by_officer_60_days import \
    REVOCATIONS_BY_OFFICER_60_DAYS_VIEW
from recidiviz.calculator.bq.dashboard.views.revocations.revocations_by_race_60_days import \
    REVOCATIONS_BY_RACE_60_DAYS_VIEW
from recidiviz.calculator.bq.dashboard.views.revocations.revocations_by_supervision_type_by_month import \
    REVOCATIONS_BY_SUPERVISION_TYPE_BY_MONTH_VIEW
from recidiviz.calculator.bq.dashboard.views.revocations.revocations_by_violation_type_by_month import \
    REVOCATIONS_BY_VIOLATION_TYPE_BY_MONTH_VIEW


REVOCATIONS_VIEWS = [
    REVOCATIONS_BY_MONTH_VIEW,
    REVOCATIONS_BY_COUNTY_60_DAYS_VIEW,
    REVOCATIONS_BY_OFFICER_60_DAYS_VIEW,
    REVOCATIONS_BY_RACE_60_DAYS_VIEW,
    REVOCATIONS_BY_SUPERVISION_TYPE_BY_MONTH_VIEW,
    REVOCATIONS_BY_VIOLATION_TYPE_BY_MONTH_VIEW
]
