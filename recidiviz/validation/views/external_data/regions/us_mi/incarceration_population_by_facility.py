# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2022 Recidiviz, Inc.
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
"""A view containing incarceration population by facility information for Michigan."""
from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.common.constants.states import StateCode
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override
from recidiviz.validation.views import dataset_config

VIEW_QUERY_TEMPLATE = """
SELECT 
    'US_MI' AS state_code,
    EXTRACT(YEAR FROM date_of_stay) AS year,
    EXTRACT(MONTH FROM date_of_stay) AS month,
    date_of_stay,
    SUBSTRING(Facility, 0, 3) AS facility,
    SAFE_CAST(Total_Count AS INT64) AS population_count,
    'US_MI' AS region_code
FROM `{project_id}.{us_mi_validation_dataset}.cb_971_report_unified_materialized`
WHERE UPPER(Facility) NOT LIKE '%TOTAL'
"""

US_MI_INCARCERATION_POPULATION_BY_FACILITY_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=dataset_config.validation_dataset_for_state(StateCode.US_MI),
    view_id="incarceration_population_by_facility_view",
    description="A unified view of all OOR reports that report per person information for MIDOC.",
    view_query_template=VIEW_QUERY_TEMPLATE,
    us_mi_validation_dataset=dataset_config.validation_dataset_for_state(
        StateCode.US_MI
    ),
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        US_MI_INCARCERATION_POPULATION_BY_FACILITY_VIEW_BUILDER.build_and_print()
