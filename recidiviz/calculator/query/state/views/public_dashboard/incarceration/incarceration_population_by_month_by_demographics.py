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
"""First of the month incarceration population counts broken down by demographics."""
# pylint: disable=trailing-whitespace, line-too-long

from recidiviz.metrics.metric_big_query_view import MetricBigQueryViewBuilder
from recidiviz.calculator.query import bq_utils
from recidiviz.calculator.query.state import dataset_config, state_specific_query_strings
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

INCARCERATION_POPULATION_BY_MONTH_BY_DEMOGRAPHICS_VIEW_NAME = 'incarceration_population_by_month_by_demographics'

INCARCERATION_POPULATION_BY_MONTH_BY_DEMOGRAPHICS_VIEW_DESCRIPTION = \
        """First of the month incarceration population counts broken down by demographics."""

# TODO(#4294): Migrate this view to use the prioritized_race_or_ethnicity in the metric table, rather than the
#   manually derived value from population_with_prioritized_race_or_ethnicity below.
INCARCERATION_POPULATION_BY_MONTH_BY_DEMOGRAPHICS_VIEW_QUERY_TEMPLATE = \
    """
    /*{description}*/
    
    WITH population_with_race_or_ethnicity_priorities AS (
      SELECT
         * EXCEPT (prioritized_race_or_ethnicity),
         ROW_NUMBER () OVER (PARTITION BY state_code, year, month, date_of_stay, person_id ORDER BY representation_priority) as inclusion_priority
      FROM
        `{project_id}.{materialized_metrics_dataset}.most_recent_incarceration_population_metrics_materialized`,
        {race_ethnicity_dimension}
      LEFT JOIN
         `{project_id}.{static_reference_dataset}.state_race_ethnicity_population_counts`
      USING (state_code, race_or_ethnicity)
      WHERE metric_period_months = 0
        AND methodology = 'EVENT'
        AND date_of_stay = DATE(year, month, 1)
        {state_specific_facility_exclusion}
        -- 20 years worth of monthly population metrics --
        AND date_of_stay >= DATE_SUB(DATE_TRUNC(CURRENT_DATE('US/Pacific'), MONTH), INTERVAL 239 MONTH)
    ), population_with_prioritized_race_or_ethnicity AS (
      SELECT
        * EXCEPT(race_or_ethnicity),
        race_or_ethnicity as prioritized_race_or_ethnicity
      FROM
        population_with_race_or_ethnicity_priorities 
      WHERE inclusion_priority = 1
    )
    
    
    SELECT
      state_code,
      date_of_stay as population_date,
      {state_specific_race_or_ethnicity_groupings},
      gender,
      age_bucket,
      COUNT(DISTINCT(person_id)) AS population_count
    FROM
      population_with_prioritized_race_or_ethnicity,
    {unnested_race_or_ethnicity_dimension},
    {gender_dimension},
    {age_dimension}
    WHERE (race_or_ethnicity != 'ALL' AND gender = 'ALL' AND age_bucket = 'ALL') -- Race breakdown
      OR (race_or_ethnicity = 'ALL' AND gender != 'ALL' AND age_bucket = 'ALL') -- Gender breakdown
      OR (race_or_ethnicity = 'ALL' AND gender = 'ALL' AND age_bucket != 'ALL') -- Age breakdown
      OR (race_or_ethnicity = 'ALL' AND gender = 'ALL' AND age_bucket = 'ALL') -- State-wide count
    GROUP BY state_code, population_date, race_or_ethnicity, gender, age_bucket
    ORDER BY state_code, population_date, race_or_ethnicity, gender, age_bucket
    """

INCARCERATION_POPULATION_BY_MONTH_BY_DEMOGRAPHICS_VIEW_BUILDER = MetricBigQueryViewBuilder(
    dataset_id=dataset_config.PUBLIC_DASHBOARD_VIEWS_DATASET,
    view_id=INCARCERATION_POPULATION_BY_MONTH_BY_DEMOGRAPHICS_VIEW_NAME,
    view_query_template=INCARCERATION_POPULATION_BY_MONTH_BY_DEMOGRAPHICS_VIEW_QUERY_TEMPLATE,
    dimensions=['state_code', 'population_date', 'race_or_ethnicity', 'gender', 'age_bucket'],
    description=INCARCERATION_POPULATION_BY_MONTH_BY_DEMOGRAPHICS_VIEW_DESCRIPTION,
    materialized_metrics_dataset=dataset_config.DATAFLOW_METRICS_MATERIALIZED_DATASET,
    reference_views_dataset=dataset_config.REFERENCE_VIEWS_DATASET,
    static_reference_dataset=dataset_config.STATIC_REFERENCE_TABLES_DATASET,
    race_ethnicity_dimension=bq_utils.unnest_race_and_ethnicity(),
    unnested_race_or_ethnicity_dimension=bq_utils.unnest_column('prioritized_race_or_ethnicity', 'race_or_ethnicity'),
    gender_dimension=bq_utils.unnest_column('gender', 'gender'),
    age_dimension=bq_utils.unnest_column('age_bucket', 'age_bucket'),
    state_specific_race_or_ethnicity_groupings=state_specific_query_strings.state_specific_race_or_ethnicity_groupings(),
    state_specific_facility_exclusion=state_specific_query_strings.state_specific_facility_exclusion(),
)

if __name__ == '__main__':
    with local_project_id_override(GCP_PROJECT_STAGING):
        INCARCERATION_POPULATION_BY_MONTH_BY_DEMOGRAPHICS_VIEW_BUILDER.build_and_print()
