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

"""A view which provides a comparison of justice counts population with
internal incarceration population counts by month."""

from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.justice_counts.dataset_config import (
    JUSTICE_COUNTS_CORRECTIONS_DATASET,
)
from recidiviz.calculator.query.state import dataset_config as state_dataset_config
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override
from recidiviz.validation.views import dataset_config

INCARCERATION_POPULATION_BY_STATE_BY_DATE_JUSTICE_COUNTS_COMPARISON_VIEW_NAME = (
    "incarceration_population_by_state_by_date_justice_counts_comparison"
)

INCARCERATION_POPULATION_BY_STATE_BY_DATE_JUSTICE_COUNTS_COMPARISON_DESCRIPTION = """ Comparison of justice counts population with internal incarceration population counts by month """

INCARCERATION_POPULATION_BY_STATE_BY_DATE_JUSTICE_COUNTS_COMPARISON_QUERY_TEMPLATE = """
    /*{description}*/
    SELECT 
        jc_pop.state_code as region_code, 
        jc_pop.date_reported, 
        jc_pop.value as justice_counts_total_population, 
        internal_pop.population_count as internal_total_population
    FROM (
        SELECT
            state_code,
            date_of_stay as population_date,
            COUNT(DISTINCT(person_id)) AS population_count
        FROM `{project_id}.{metrics_dataset}.most_recent_incarceration_population_metrics_materialized`
        GROUP BY state_code, population_date
    ) as internal_pop 
    JOIN 
        `{project_id}.{justice_counts_corrections_dataset}.population_prison_output_monthly_materialized` as jc_pop 
    ON 
    (
        jc_pop.state_code = internal_pop.state_code 
        AND jc_pop.date_reported = internal_pop.population_date
    )
    ORDER BY jc_pop.state_code, jc_pop.date_reported
"""

INCARCERATION_POPULATION_BY_STATE_BY_DATE_JUSTICE_COUNTS_COMPARISON_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=dataset_config.VIEWS_DATASET,
    view_id=INCARCERATION_POPULATION_BY_STATE_BY_DATE_JUSTICE_COUNTS_COMPARISON_VIEW_NAME,
    view_query_template=INCARCERATION_POPULATION_BY_STATE_BY_DATE_JUSTICE_COUNTS_COMPARISON_QUERY_TEMPLATE,
    description=INCARCERATION_POPULATION_BY_STATE_BY_DATE_JUSTICE_COUNTS_COMPARISON_DESCRIPTION,
    justice_counts_corrections_dataset=JUSTICE_COUNTS_CORRECTIONS_DATASET,
    metrics_dataset=state_dataset_config.DATAFLOW_METRICS_MATERIALIZED_DATASET,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        INCARCERATION_POPULATION_BY_STATE_BY_DATE_JUSTICE_COUNTS_COMPARISON_VIEW_BUILDER.build_and_print()
