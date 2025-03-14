# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2024 Recidiviz, Inc.
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
"""A view detailing the incarceration population at the facility level for Maine."""
from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.common.constants.states import StateCode
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override
from recidiviz.validation.views import dataset_config

VIEW_QUERY_TEMPLATE = """
SELECT 
    'US_ME' AS state_code,
    EXTRACT(YEAR FROM date_of_incarceration) AS year,
    EXTRACT(MONTH FROM date_of_incarceration) AS month,
    date_of_incarceration AS date_of_stay,
    CASE 
        WHEN facility_name = 'Mountain View Adult Center' or facility_name = 'Charleston Correctional Facility'
        THEN 'MOUNTAIN VIEW CORRECTIONAL FACILITY'
    ELSE UPPER(facility_name) END AS facility,
    COUNT(DISTINCT client_id) AS population_count,
    'US_ME' AS region_code
FROM `{project_id}.{us_me_validation_oneoff_dataset}.incarceration_by_person_by_facility_validation`
GROUP BY 1,2,3,4,5,7
"""

US_ME_INCARCERATION_POPULATION_BY_FACILITY_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=dataset_config.validation_dataset_for_state(StateCode.US_ME),
    view_id="incarceration_population_by_facility",
    description="A view detailing the incarceration population at the facility level for Maine",
    view_query_template=VIEW_QUERY_TEMPLATE,
    us_me_validation_oneoff_dataset=dataset_config.validation_oneoff_dataset_for_state(
        StateCode.US_ME
    ),
    should_materialize=True,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        US_ME_INCARCERATION_POPULATION_BY_FACILITY_VIEW_BUILDER.build_and_print()
