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
"""Table mapping supervision office to population density for most recent year
available"""

from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.county.views.vera.vera_view_constants import (
    VERA_DATASET,
)
from recidiviz.calculator.query.state.dataset_config import (
    ANALYST_VIEWS_DATASET,
    STATIC_REFERENCE_TABLES_DATASET,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

POPULATION_DENSITY_BY_SUPERVISION_OFFICE_VIEW_NAME = (
    "population_density_by_supervision_office"
)

POPULATION_DENSITY_BY_SUPERVISION_OFFICE_VIEW_DESCRIPTION = (
    "Table mapping supervision office to population density for most recent year "
    "available"
)

POPULATION_DENSITY_BY_SUPERVISION_OFFICE_QUERY_TEMPLATE = """
    /*{description}*/
    
    SELECT 
        g.state_code,
        g.region,
        g.district AS supervision_district,
        g.office AS supervision_office,
        v.Year AS year,
        SAFE_DIVIDE(SUM(v.total_pop), SUM(v.land_area)) AS population_density,
    FROM
        `{project_id}.{static_reference_dataset}.county_to_supervision_office_materialized` g
    LEFT JOIN 
        `{project_id}.{vera_dataset}.incarceration_trends` v
    ON 
        CAST(g.county_fips AS INT64) = v.fips
    GROUP BY 1, 2, 3, 4, 5
    
    -- Get the population density information from the most recent year available
    QUALIFY ROW_NUMBER() OVER (PARTITION BY office ORDER BY year DESC) = 1

    """

POPULATION_DENSITY_BY_SUPERVISION_OFFICE_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=ANALYST_VIEWS_DATASET,
    view_id=POPULATION_DENSITY_BY_SUPERVISION_OFFICE_VIEW_NAME,
    view_query_template=POPULATION_DENSITY_BY_SUPERVISION_OFFICE_QUERY_TEMPLATE,
    description=POPULATION_DENSITY_BY_SUPERVISION_OFFICE_VIEW_DESCRIPTION,
    static_reference_dataset=STATIC_REFERENCE_TABLES_DATASET,
    vera_dataset=VERA_DATASET,
    should_materialize=False,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        POPULATION_DENSITY_BY_SUPERVISION_OFFICE_VIEW_BUILDER.build_and_print()
