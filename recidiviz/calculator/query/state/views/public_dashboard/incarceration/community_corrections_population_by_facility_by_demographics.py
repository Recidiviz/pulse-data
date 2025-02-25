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
"""Most recent daily community corrections population count with facility and demographic breakdowns."""
from recidiviz.calculator.query.state.state_specific_query_strings import (
    SpotlightFacilityType,
)
from recidiviz.calculator.query.state.views.public_dashboard.incarceration.population_by_facility_by_demographics_template import (
    get_view_builder,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

COMMUNITY_CORRECTIONS_POPULATION_BY_FACILITY_BY_DEMOGRAPHICS_VIEW_NAME = (
    "community_corrections_population_by_facility_by_demographics"
)

COMMUNITY_CORRECTIONS_POPULATION_BY_FACILITY_BY_DEMOGRAPHICS_VIEW_DESCRIPTION = """Most recent daily community corrections population count with facility and demographic breakdowns."""

COMMUNITY_CORRECTIONS_POPULATION_BY_FACILITY_BY_DEMOGRAPHICS_VIEW_BUILDER = get_view_builder(
    view_id=COMMUNITY_CORRECTIONS_POPULATION_BY_FACILITY_BY_DEMOGRAPHICS_VIEW_NAME,
    description=COMMUNITY_CORRECTIONS_POPULATION_BY_FACILITY_BY_DEMOGRAPHICS_VIEW_DESCRIPTION,
    facility_type=SpotlightFacilityType.COMMUNITY,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        COMMUNITY_CORRECTIONS_POPULATION_BY_FACILITY_BY_DEMOGRAPHICS_VIEW_BUILDER.build_and_print()
