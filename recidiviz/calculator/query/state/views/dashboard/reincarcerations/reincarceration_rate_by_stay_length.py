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
"""Reincarceration rates by stay length

The release cohort is the most recent calendar year with a full 1-year
follow-up period that has completed. For example, in the year 2019, the
release cohort of 2017 is the most recent calendar year where the next year
(2018) has completed. The follow-up period is 1 year.
"""
# pylint: disable=trailing-whitespace
from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query import bq_utils
from recidiviz.calculator.query.state import dataset_config
from recidiviz.utils.environment import GAE_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

REINCARCERATION_RATE_BY_STAY_LENGTH_VIEW_NAME = \
    'reincarceration_rate_by_stay_length'

REINCARCERATION_RATE_BY_STAY_LENGTH_DESCRIPTION = \
    """Reincarceration rate by stay length."""

REINCARCERATION_RATE_BY_STAY_LENGTH_QUERY_TEMPLATE = \
    """
    /*{description}*/
    SELECT
      state_code,
      release_cohort,
      follow_up_period,
      SUM(recidivated_releases) AS reincarceration_count,
      SUM(recidivated_releases)/COUNT(*) AS recidivism_rate,
      stay_length_bucket,
      district
    FROM `{project_id}.{metrics_dataset}.recidivism_rate_metrics`
    JOIN `{project_id}.{reference_dataset}.most_recent_job_id_by_metric_and_state_code` job
      USING (state_code, job_id),
    {district_dimension}
    WHERE methodology = 'PERSON'
      AND person_id IS NOT NULL
      AND follow_up_period = 1
      AND district IS NOT NULL
      AND release_cohort = EXTRACT(YEAR FROM DATE_SUB(CURRENT_DATE(), INTERVAL 2 YEAR))
      AND job.metric_type = 'RECIDIVISM_RATE'
    GROUP BY state_code, release_cohort, follow_up_period, stay_length_bucket, district
    ORDER BY state_code, release_cohort, follow_up_period, stay_length_bucket, district
    """

REINCARCERATION_RATE_BY_STAY_LENGTH_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=dataset_config.DASHBOARD_VIEWS_DATASET,
    view_id=REINCARCERATION_RATE_BY_STAY_LENGTH_VIEW_NAME,
    view_query_template=REINCARCERATION_RATE_BY_STAY_LENGTH_QUERY_TEMPLATE,
    description=REINCARCERATION_RATE_BY_STAY_LENGTH_DESCRIPTION,
    metrics_dataset=dataset_config.DATAFLOW_METRICS_DATASET,
    reference_dataset=dataset_config.REFERENCE_TABLES_DATASET,
    district_dimension=bq_utils.unnest_district(
        district_column='county_of_residence'),
)

if __name__ == '__main__':
    with local_project_id_override(GAE_PROJECT_STAGING):
        REINCARCERATION_RATE_BY_STAY_LENGTH_VIEW_BUILDER.build_and_print()
