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
"""All individuals who have been referred to Free Through Recovery by metric month period, broken down by
race/ethnicity, where a person is counted towards only the race/ethnicity category that is least represented in the
population of the state."""
# pylint: disable=trailing-whitespace, line-too-long

from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query import bq_utils
from recidiviz.calculator.query.state import dataset_config
from recidiviz.utils.environment import GAE_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

FTR_REFERRALS_BY_PRIORITIZED_RACE_AND_ETHNICITY_BY_PERIOD_VIEW_NAME = \
    'ftr_referrals_by_prioritized_race_and_ethnicity_by_period'

FTR_REFERRALS_BY_PRIORITIZED_RACE_AND_ETHNICITY_BY_PERIOD_DESCRIPTION = """
All individuals who have been referred to Free Through Recovery by metric month period, broken down by
race/ethnicity, where a person is counted towards only the race/ethnicity category that is least represented in the
population of the state.
"""

FTR_REFERRALS_BY_PRIORITIZED_RACE_AND_ETHNICITY_BY_PERIOD_QUERY_TEMPLATE = \
    """
    /*{description}*/
    WITH referrals_with_race_or_ethnicity_priority AS (
      SELECT
        state_code,
        person_id,
        metric_period_months,
        race_or_ethnicity,
        ROW_NUMBER () OVER (PARTITION BY state_code, metric_period_months, person_id ORDER BY representation_priority) as inclusion_priority
      FROM `{project_id}.{reference_dataset}.event_based_program_referrals`,
        {metric_period_dimension},
        {race_or_ethnicity_dimension}
      LEFT JOIN
        `{project_id}.{reference_dataset}.state_race_ethnicity_population_counts` 
      USING (state_code, race_or_ethnicity)
      WHERE {metric_period_condition}
    )
    
    SELECT
      state_code,
      metric_period_months,
      {state_specific_race_or_ethnicity_groupings},
      COUNT(DISTINCT(person_id)) as ftr_referral_count
    FROM
      referrals_with_race_or_ethnicity_priority,
      {unnested_race_or_ethnicity_dimension}
    WHERE inclusion_priority = 1
    AND state_code = 'US_ND'
    GROUP BY state_code, metric_period_months, race_or_ethnicity
    ORDER BY state_code, metric_period_months, race_or_ethnicity
    """

FTR_REFERRALS_BY_PRIORITIZED_RACE_AND_ETHNICITY_BY_PERIOD_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=dataset_config.PUBLIC_DASHBOARD_VIEWS_DATASET,
    view_id=FTR_REFERRALS_BY_PRIORITIZED_RACE_AND_ETHNICITY_BY_PERIOD_VIEW_NAME,
    view_query_template=FTR_REFERRALS_BY_PRIORITIZED_RACE_AND_ETHNICITY_BY_PERIOD_QUERY_TEMPLATE,
    description=FTR_REFERRALS_BY_PRIORITIZED_RACE_AND_ETHNICITY_BY_PERIOD_DESCRIPTION,
    reference_dataset=dataset_config.REFERENCE_TABLES_DATASET,
    metric_period_dimension=bq_utils.unnest_metric_period_months(),
    race_or_ethnicity_dimension=bq_utils.unnest_race_and_ethnicity(),
    metric_period_condition=bq_utils.metric_period_condition(),
    state_specific_race_or_ethnicity_groupings=bq_utils.state_specific_race_or_ethnicity_groupings(),
    unnested_race_or_ethnicity_dimension=bq_utils.unnest_column('race_or_ethnicity', 'race_or_ethnicity')
)

if __name__ == '__main__':
    with local_project_id_override(GAE_PROJECT_STAGING):
        FTR_REFERRALS_BY_PRIORITIZED_RACE_AND_ETHNICITY_BY_PERIOD_VIEW_BUILDER.build_and_print()
