# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2021 Recidiviz, Inc.
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
"""A view that can be used to validate existence of active contribution to the incarceration or supervision
populations after a person has a period ending in death.
"""


from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.state import dataset_config as state_dataset_config
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override
from recidiviz.validation.views import dataset_config

ACTIVE_IN_POPULATION_AFTER_DEATH_DATE_VIEW_NAME = (
    "active_in_population_after_death_date"
)

ACTIVE_IN_POPULATION_AFTER_DEATH_DATE_DESCRIPTION = """
Builds existence validation table to ensure that people who have a period ending with reason DEATH have not been
counted in our population metrics after their death.
"""

ACTIVE_IN_POPULATION_AFTER_DEATH_DATE_QUERY_TEMPLATE = """
/*{description}*/
WITH death_periods AS (
    /*Incarceration and supervision periods with end reasons of DEATH and their end dates*/
    SELECT DISTINCT 
    state_code, person_id, MAX(termination_date) AS death_date, 'supervision_period' AS death_date_period_type
    FROM `{project_id}.state.state_supervision_period`
    WHERE termination_reason = 'DEATH'
    GROUP BY state_code, person_id
    UNION ALL 
    SELECT state_code, person_id, MAX(release_date) AS death_date, 'incarceration_period' AS death_date_period_type
    FROM `{project_id}.state.state_incarceration_period`
    WHERE release_reason = 'DEATH'
    GROUP BY state_code, person_id
),
most_recent_metrics AS (
    /* The most recent appearance of the person in the supervision_population or incarceration_population metrics */
    SELECT 
    state_code, person_id, MAX(date_of_supervision) AS most_recent_population_date, 
    'supervision_population' as most_recent_population_date_metric
    FROM `{project_id}.dataflow_metrics_materialized.most_recent_supervision_population_metrics_materialized`
    GROUP BY state_code, person_id
    UNION ALL 
    SELECT
    state_code, person_id, MAX(date_of_stay) AS most_recent_population_date,
    'incarceration_population' as most_recent_population_date_metric
    FROM `{project_id}.dataflow_metrics_materialized.most_recent_incarceration_population_metrics_included_in_state_population_materialized`
    GROUP BY state_code, person_id
)

SELECT state_code as region_code, person_id, death_periods.death_date, death_periods.death_date_period_type, 
most_recent_metrics.most_recent_population_date, most_recent_metrics.most_recent_population_date_metric
FROM death_periods 
LEFT JOIN most_recent_metrics
USING (person_id, state_code)
WHERE most_recent_population_date > death_date

"""

ACTIVE_IN_POPULATION_AFTER_DEATH_DATE_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=dataset_config.VIEWS_DATASET,
    view_id=ACTIVE_IN_POPULATION_AFTER_DEATH_DATE_VIEW_NAME,
    view_query_template=ACTIVE_IN_POPULATION_AFTER_DEATH_DATE_QUERY_TEMPLATE,
    description=ACTIVE_IN_POPULATION_AFTER_DEATH_DATE_DESCRIPTION,
    state_dataset=state_dataset_config.STATE_BASE_DATASET,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        ACTIVE_IN_POPULATION_AFTER_DEATH_DATE_VIEW_BUILDER.build_and_print()
