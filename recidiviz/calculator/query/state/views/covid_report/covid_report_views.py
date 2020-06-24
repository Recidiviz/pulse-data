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
"""All views needed for the COVID-19 Report."""
from typing import List

from recidiviz.big_query.big_query_view import BigQueryViewBuilder
from recidiviz.calculator.query.state.views.covid_report.facility_population_by_age_with_capacity_by_day import \
    FACILITY_POPULATION_BY_AGE_WITH_CAPACITY_BY_DAY_VIEW_BUILDER
from recidiviz.calculator.query.state.views.covid_report.releases_by_type_by_week import \
    RELEASES_BY_TYPE_BY_WEEK_VIEW_BUILDER
from recidiviz.calculator.query.state.views.covid_report.supervision_termination_by_type_by_week import \
    SUPERVISION_TERMINATIONS_BY_TYPE_BY_WEEK_VIEW_BUILDER
from recidiviz.calculator.query.state.views.covid_report.us_nd.admissions_to_cpp_by_week import \
    ADMISSIONS_TO_CPP_BY_WEEK_VIEW_BUILDER
from recidiviz.calculator.query.state.views.covid_report.us_nd.incidents_by_facility_by_week import \
    INCIDENTS_BY_FACILITY_BY_WEEK_VIEW_BUILDER

COVID_REPORT_VIEW_BUILDERS: List[BigQueryViewBuilder] = [
    ADMISSIONS_TO_CPP_BY_WEEK_VIEW_BUILDER,
    FACILITY_POPULATION_BY_AGE_WITH_CAPACITY_BY_DAY_VIEW_BUILDER,
    INCIDENTS_BY_FACILITY_BY_WEEK_VIEW_BUILDER,
    RELEASES_BY_TYPE_BY_WEEK_VIEW_BUILDER,
    SUPERVISION_TERMINATIONS_BY_TYPE_BY_WEEK_VIEW_BUILDER
]
