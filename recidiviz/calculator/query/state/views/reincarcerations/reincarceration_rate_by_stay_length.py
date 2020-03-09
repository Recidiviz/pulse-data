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
from recidiviz.calculator.query import bqview
from recidiviz.calculator.query.state import view_config
from recidiviz.utils import metadata

PROJECT_ID = metadata.project_id()
VIEWS_DATASET = view_config.DASHBOARD_VIEWS_DATASET
METRICS_DATASET = view_config.DATAFLOW_METRICS_DATASET

REINCARCERATION_RATE_BY_STAY_LENGTH_VIEW_NAME = \
    'reincarceration_rate_by_stay_length'

REINCARCERATION_RATE_BY_STAY_LENGTH_DESCRIPTION = \
    """Reincarceration rate by stay length."""

REINCARCERATION_RATE_BY_STAY_LENGTH_QUERY = \
    """
    /*{description}*/
    SELECT 
      state_code, 
      release_cohort, 
      follow_up_period, 
      recidivism_rate, 
      stay_length_bucket, 
      IFNULL(county_of_residence, 'ALL') AS district
    FROM `{project_id}.{metrics_dataset}.recidivism_rate_metrics`
    JOIN `{project_id}.{views_dataset}.most_recent_job_id_by_metric_and_state_code` job
      USING (state_code, job_id)
    WHERE methodology = 'PERSON'
      AND age_bucket IS NULL
      AND race IS NULL
      AND ethnicity IS NULL
      AND gender IS NULL
      AND person_id IS NULL
      AND person_external_id IS NULL
      AND return_type IS NULL
      AND from_supervision_type IS NULL
      AND source_violation_type IS NULL
      AND release_facility IS NULL
      AND stay_length_bucket IS NOT NULL
      AND release_cohort = EXTRACT(YEAR FROM DATE_ADD(CURRENT_DATE(), INTERVAL -2 YEAR))
      AND follow_up_period = 1
      AND job.metric_type = 'RECIDIVISM_RATE'
    ORDER BY state_code, release_cohort, follow_up_period, stay_length_bucket
    """.format(
        description=REINCARCERATION_RATE_BY_STAY_LENGTH_DESCRIPTION,
        project_id=PROJECT_ID,
        metrics_dataset=METRICS_DATASET,
        views_dataset=VIEWS_DATASET
    )

REINCARCERATION_RATE_BY_STAY_LENGTH_VIEW = bqview.BigQueryView(
    view_id=REINCARCERATION_RATE_BY_STAY_LENGTH_VIEW_NAME,
    view_query=REINCARCERATION_RATE_BY_STAY_LENGTH_QUERY
)

if __name__ == '__main__':
    print(REINCARCERATION_RATE_BY_STAY_LENGTH_VIEW.view_id)
    print(REINCARCERATION_RATE_BY_STAY_LENGTH_VIEW.view_query)
