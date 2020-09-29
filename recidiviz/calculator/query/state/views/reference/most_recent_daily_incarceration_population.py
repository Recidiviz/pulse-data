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
"""Event based incarceration population for the most recent date of incarceration."""
# pylint: disable=trailing-whitespace, line-too-long
from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query import bq_utils
from recidiviz.calculator.query.state import dataset_config, state_specific_query_strings
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

MOST_RECENT_DAILY_INCARCERATION_POPULATION_VIEW_NAME = \
    'most_recent_daily_incarceration_population'

MOST_RECENT_DAILY_INCARCERATION_POPULATION_DESCRIPTION = \
    """Event based incarceration population for the most recent date of incarceration."""

MOST_RECENT_DAILY_INCARCERATION_POPULATION_QUERY_TEMPLATE = \
    """
    /*{description}*/
    WITH most_recent_job_id AS (
      SELECT
        state_code,
        metric_date as date_of_stay,
        job_id,
        metric_type
      FROM
        `{project_id}.{reference_views_dataset}.most_recent_daily_job_id_by_metric_and_state_code_materialized`
    ), population_with_race_or_ethnicity_priorities AS (
      SELECT
         *,
         ROW_NUMBER () OVER (PARTITION BY state_code, person_id ORDER BY representation_priority) as inclusion_priority
      FROM
        `{project_id}.{metrics_dataset}.incarceration_population_metrics`
      INNER JOIN
        most_recent_job_id
      USING (state_code, job_id, date_of_stay, metric_type),
        {race_or_ethnicity_dimension}
      LEFT JOIN
         `{project_id}.{static_reference_dataset}.state_race_ethnicity_population_counts`
      USING (state_code, race_or_ethnicity)
      WHERE metric_period_months = 0
      AND methodology = 'EVENT'
      {state_specific_facility_exclusion}
    )
    
    SELECT
      state_code,
      person_id,
      admission_reason,
      race_or_ethnicity as prioritized_race_or_ethnicity,
      IFNULL(gender, 'EXTERNAL_UNKNOWN') as gender,
      IFNULL(age_bucket, 'EXTERNAL_UNKNOWN') as age_bucket,
      IFNULL(judicial_district_code, 'EXTERNAL_UNKNOWN') as judicial_district_code,
      date_of_stay,
      facility
    FROM
      population_with_race_or_ethnicity_priorities
    WHERE inclusion_priority = 1
    """

MOST_RECENT_DAILY_INCARCERATION_POPULATION_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=dataset_config.REFERENCE_VIEWS_DATASET,
    view_id=MOST_RECENT_DAILY_INCARCERATION_POPULATION_VIEW_NAME,
    should_materialize=True,
    view_query_template=MOST_RECENT_DAILY_INCARCERATION_POPULATION_QUERY_TEMPLATE,
    description=MOST_RECENT_DAILY_INCARCERATION_POPULATION_DESCRIPTION,
    metrics_dataset=dataset_config.DATAFLOW_METRICS_DATASET,
    reference_views_dataset=dataset_config.REFERENCE_VIEWS_DATASET,
    static_reference_dataset=dataset_config.STATIC_REFERENCE_TABLES_DATASET,
    race_or_ethnicity_dimension=bq_utils.unnest_race_and_ethnicity(),
    state_specific_facility_exclusion=state_specific_query_strings.state_specific_facility_exclusion()
)

if __name__ == '__main__':
    with local_project_id_override(GCP_PROJECT_STAGING):
        MOST_RECENT_DAILY_INCARCERATION_POPULATION_VIEW_BUILDER.build_and_print()
