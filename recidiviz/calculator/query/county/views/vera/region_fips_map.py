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
"""View with fips, region"""

from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.county import dataset_config
from recidiviz.calculator.query.county.views.vera import vera_view_constants
from recidiviz.persistence.database.schema.county.schema import Person
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

VERA_DATASET = vera_view_constants.VERA_DATASET

REGION_FIPS_MAP_VIEW_NAME = "region_fips_map"

REGION_FIPS_MAP_VIEW_DESCRIPTION = """
A view that contains fips, region"
FROM census.person table.
"""

REGION_FIPS_MAP_VIEW_QUERY_TEMPLATE = """
SELECT
  SUBSTR(jurisdiction_id, 0, 5) AS fips,
  region
FROM
  `{project_id}.{base_dataset}.{person_table}` Person
GROUP BY
  fips, region
ORDER BY
  fips
"""

REGION_FIPS_MAP_VIEW_BUILDER: SimpleBigQueryViewBuilder = SimpleBigQueryViewBuilder(
    dataset_id=dataset_config.VIEWS_DATASET,
    view_id=REGION_FIPS_MAP_VIEW_NAME,
    view_query_template=REGION_FIPS_MAP_VIEW_QUERY_TEMPLATE,
    description=REGION_FIPS_MAP_VIEW_DESCRIPTION,
    vera_dataset=VERA_DATASET,
    person_table=Person.__tablename__,
    base_dataset=dataset_config.COUNTY_BASE_DATASET,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        REGION_FIPS_MAP_VIEW_BUILDER.build_and_print()
