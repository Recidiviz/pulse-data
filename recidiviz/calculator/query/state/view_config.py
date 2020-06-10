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
"""Dashboard view configuration."""

# Where the dashboard views and materialized tables live
from typing import Dict, List

from recidiviz.big_query.big_query_view import BigQueryView
from recidiviz.calculator.query.state.dataset_config import REFERENCE_TABLES_DATASET, DASHBOARD_VIEWS_DATASET, \
    COVID_REPORT_DATASET
from recidiviz.calculator.query.state.views.admissions import admissions_views
from recidiviz.calculator.query.state.views.admissions.us_nd.admissions_to_cpp_by_week import \
    ADMISSIONS_TO_CPP_BY_WEEK_VIEW
from recidiviz.calculator.query.state.views.incarceration_facilities.facility_population_by_age_with_capacity_by_day \
    import FACILITY_POPULATION_BY_AGE_WITH_CAPACITY_BY_DAY_VIEW
from recidiviz.calculator.query.state.views.incarceration_facilities.us_nd.incidents_by_facility_by_week import \
    INCIDENTS_BY_FACILITY_BY_WEEK_VIEW
from recidiviz.calculator.query.state.views.program_evaluation import program_evaluation_views
from recidiviz.calculator.query.state.views.reference import reference_views
from recidiviz.calculator.query.state.views.reincarcerations import reincarcerations_views
from recidiviz.calculator.query.state.views.releases.releases_by_type_by_week import RELEASES_BY_TYPE_BY_WEEK_VIEW
from recidiviz.calculator.query.state.views.revocation_analysis import revocation_analysis_views
from recidiviz.calculator.query.state.views.revocations import revocations_views
from recidiviz.calculator.query.state.views.supervision import supervision_views
from recidiviz.calculator.query.state.views.supervision.supervision_termination_by_type_by_week import \
    SUPERVISION_TERMINATIONS_BY_TYPE_BY_WEEK_VIEW

VIEWS_TO_UPDATE: Dict[str, List[BigQueryView]] = {
    REFERENCE_TABLES_DATASET: reference_views.REF_VIEWS,
    DASHBOARD_VIEWS_DATASET: (
        admissions_views.ADMISSIONS_VIEWS +
        reincarcerations_views.REINCARCERATIONS_VIEWS +
        revocations_views.REVOCATIONS_VIEWS +
        supervision_views.SUPERVISION_VIEWS +
        program_evaluation_views.PROGRAM_EVALUATION_VIEWS +
        revocation_analysis_views.REVOCATION_ANALYSIS_VIEWS
    ),
    COVID_REPORT_DATASET: [
        ADMISSIONS_TO_CPP_BY_WEEK_VIEW,
        FACILITY_POPULATION_BY_AGE_WITH_CAPACITY_BY_DAY_VIEW,
        INCIDENTS_BY_FACILITY_BY_WEEK_VIEW,
        RELEASES_BY_TYPE_BY_WEEK_VIEW,
        SUPERVISION_TERMINATIONS_BY_TYPE_BY_WEEK_VIEW
    ]
}
