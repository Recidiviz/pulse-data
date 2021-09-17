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
"""View with fips, year, total_resident_pop"""

from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.county import dataset_config
from recidiviz.calculator.query.county.views.vera import vera_view_constants
from recidiviz.calculator.query.county.views.vera.vera_view_constants import (
    BRIDGED_RACE_POPULATION_ESTIMATED_TABLE,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

VERA_DATASET = vera_view_constants.VERA_DATASET
INCARCERATION_TRENDS_TABLE = vera_view_constants.INCARCERATION_TRENDS_TABLE

TOTAL_RESIDENT_POPULATION_COUNTS_VIEW_NAME = "total_resident_population_counts"

TOTAL_RESIDENT_POPULATION_COUNTS_VIEW_DESCRIPTION = """
A view that contains fips, year, total_resident_pop"
FROM bridged_race_population_estimates, renaming total_pop15to64 as 
total_resident_pop.
"""

TOTAL_RESIDENT_POPULATION_COUNTS_VIEW_QUERY_TEMPLATE = """
SELECT year, fips, total_pop_15to64 AS total_resident_pop
FROM `{project_id}.{vera_dataset}.{bridged_race_population_estimates}`

"""

TOTAL_RESIDENT_POPULATION_COUNTS_VIEW_BUILDER: SimpleBigQueryViewBuilder = (
    SimpleBigQueryViewBuilder(
        dataset_id=dataset_config.VIEWS_DATASET,
        view_id=TOTAL_RESIDENT_POPULATION_COUNTS_VIEW_NAME,
        view_query_template=TOTAL_RESIDENT_POPULATION_COUNTS_VIEW_QUERY_TEMPLATE,
        description=TOTAL_RESIDENT_POPULATION_COUNTS_VIEW_DESCRIPTION,
        vera_dataset=VERA_DATASET,
        bridged_race_population_estimates=BRIDGED_RACE_POPULATION_ESTIMATED_TABLE,
    )
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        TOTAL_RESIDENT_POPULATION_COUNTS_VIEW_BUILDER.build_and_print()
