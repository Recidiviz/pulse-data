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
"""Views related to admissions."""
from recidiviz.calculator.query.state.views.admissions.admissions_by_type_by_period import \
    ADMISSIONS_BY_TYPE_BY_PERIOD_VIEW
from recidiviz.calculator.query.state.views.admissions.admissions_by_type_by_month import \
    ADMISSIONS_BY_TYPE_BY_MONTH_VIEW
from recidiviz.calculator.query.state.views.admissions.admissions_versus_releases_by_month import \
    ADMISSIONS_VERSUS_RELEASES_BY_MONTH_VIEW
from recidiviz.calculator.query.state.views.admissions.admissions_versus_releases_by_period import \
    ADMISSIONS_VERSUS_RELEASES_BY_PERIOD_VIEW

ADMISSIONS_VIEWS = [
    ADMISSIONS_BY_TYPE_BY_PERIOD_VIEW,
    ADMISSIONS_BY_TYPE_BY_MONTH_VIEW,
    ADMISSIONS_VERSUS_RELEASES_BY_MONTH_VIEW,
    ADMISSIONS_VERSUS_RELEASES_BY_PERIOD_VIEW,
]
