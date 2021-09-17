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
"""View with fips and urbanicity from 2016"""

from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.county import dataset_config
from recidiviz.calculator.query.county.views.vera import vera_view_constants
from recidiviz.calculator.query.county.views.vera.county_names import (
    COUNTY_NAMES_VIEW_BUILDER,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

VERA_DATASET = vera_view_constants.VERA_DATASET
INCARCERATION_TRENDS_TABLE = vera_view_constants.INCARCERATION_TRENDS_TABLE

URBANICITY_VIEW_NAME = "urbanicity"

URBANICITY_VIEW_DESCRIPTION = """
View with fips and urbanicity from 2016.
"""

URBANICITY_VIEW_QUERY_TEMPLATE = """
SELECT 
CAST(counties.fips AS INT64) as fips, itp.urbanicity
FROM `{project_id}.{views_dataset}.{county_names_view}` as counties
JOIN {project_id}.{vera_dataset}.{incarceration_trends_table} as itp
ON counties.fips = CAST(itp.fips AS STRING)
WHERE itp.year = 2016
"""

URBANICITY_VIEW_BUILDER: SimpleBigQueryViewBuilder = SimpleBigQueryViewBuilder(
    dataset_id=dataset_config.VIEWS_DATASET,
    view_id=URBANICITY_VIEW_NAME,
    view_query_template=URBANICITY_VIEW_QUERY_TEMPLATE,
    description=URBANICITY_VIEW_DESCRIPTION,
    vera_dataset=VERA_DATASET,
    views_dataset=dataset_config.VIEWS_DATASET,
    county_names_view=COUNTY_NAMES_VIEW_BUILDER.view_id,
    incarceration_trends_table=INCARCERATION_TRENDS_TABLE,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        URBANICITY_VIEW_BUILDER.build_and_print()
