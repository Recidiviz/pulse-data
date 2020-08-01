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
"""Public dashboard views related to incarceration."""
# pylint: disable=line-too-long
from typing import List

from recidiviz.big_query.big_query_view import BigQueryViewBuilder
from recidiviz.calculator.query.state.views.public_dashboard.incarceration.incarceration_lengths_by_demographics import \
    INCARCERATION_LENGTHS_BY_DEMOGRAPHICS_VIEW_BUILDER
from recidiviz.calculator.query.state.views.public_dashboard.incarceration.incarceration_population_by_admission_reason import \
    INCARCERATION_POPULATION_BY_ADMISSION_REASON_VIEW_BUILDER
from recidiviz.calculator.query.state.views.public_dashboard.incarceration.incarceration_population_by_facility_by_demographics import \
    INCARCERATION_POPULATION_BY_FACILITY_BY_DEMOGRAPHICS_VIEW_BUILDER
from recidiviz.calculator.query.state.views.public_dashboard.incarceration.incarceration_releases_by_type_by_period import \
    INCARCERATION_RELEASES_BY_TYPE_BY_PERIOD_VIEW_BUILDER

INCARCERATION_VIEW_BUILDERS: List[BigQueryViewBuilder] = [
    INCARCERATION_LENGTHS_BY_DEMOGRAPHICS_VIEW_BUILDER,
    INCARCERATION_POPULATION_BY_ADMISSION_REASON_VIEW_BUILDER,
    INCARCERATION_POPULATION_BY_FACILITY_BY_DEMOGRAPHICS_VIEW_BUILDER,
    INCARCERATION_RELEASES_BY_TYPE_BY_PERIOD_VIEW_BUILDER
]
