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
"""
All individuals who have been referred to Free Through Recovery by metric
period months, broken down by gender.
"""

from recidiviz.calculator.query import bq_utils
from recidiviz.calculator.query.state import dataset_config
from recidiviz.metrics.metric_big_query_view import MetricBigQueryViewBuilder
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

FTR_REFERRALS_BY_GENDER_BY_PERIOD_VIEW_NAME = "ftr_referrals_by_gender_by_period"

FTR_REFERRALS_BY_GENDER_BY_PERIOD_DESCRIPTION = """
 All individuals who have been referred to Free Through Recovery by metric
 period months, broken down by gender.
"""

FTR_REFERRALS_BY_GENDER_BY_PERIOD_QUERY_TEMPLATE = """
    /*{description}*/
    WITH supervision AS (
      SELECT
        state_code,
        COUNT(DISTINCT person_id) AS total_supervision_count,
        supervision_type,
        district,
        metric_period_months,
        IFNULL(gender, 'EXTERNAL_UNKNOWN') as gender
      FROM `{project_id}.{shared_metric_views_dataset}.event_based_supervision_populations`,
      {metric_period_dimension}
      WHERE {metric_period_condition}
      GROUP BY state_code, supervision_type, district, metric_period_months, gender
    ), 
    referrals AS (
      SELECT
        state_code,
        COUNT(DISTINCT person_id) AS count,
        supervision_type,
        district,
        metric_period_months,
        IFNULL(gender, 'EXTERNAL_UNKNOWN') as gender
      FROM `{project_id}.{shared_metric_views_dataset}.event_based_program_referrals`,
      {metric_period_dimension}
      WHERE {metric_period_condition}
      GROUP BY state_code, supervision_type, district, metric_period_months, gender
    )
    SELECT
      state_code,
      gender,
      IFNULL(referrals.count, 0) as count,
      IFNULL(total_supervision_count, 0) as total_supervision_count,
      supervision_type,
      district,
      metric_period_months
    FROM supervision
    FULL OUTER JOIN referrals
    USING (state_code, supervision_type, district, metric_period_months, gender)
    WHERE supervision_type in ('ALL', 'PAROLE', 'PROBATION')
      AND district IS NOT NULL
      AND state_code = 'US_ND'
    ORDER BY state_code, gender, district, supervision_type, metric_period_months
    """

FTR_REFERRALS_BY_GENDER_BY_PERIOD_VIEW_BUILDER = MetricBigQueryViewBuilder(
    dataset_id=dataset_config.DASHBOARD_VIEWS_DATASET,
    view_id=FTR_REFERRALS_BY_GENDER_BY_PERIOD_VIEW_NAME,
    view_query_template=FTR_REFERRALS_BY_GENDER_BY_PERIOD_QUERY_TEMPLATE,
    dimensions=(
        "state_code",
        "metric_period_months",
        "district",
        "supervision_type",
        "gender",
    ),
    description=FTR_REFERRALS_BY_GENDER_BY_PERIOD_DESCRIPTION,
    shared_metric_views_dataset=dataset_config.SHARED_METRIC_VIEWS_DATASET,
    metric_period_dimension=bq_utils.unnest_metric_period_months(),
    metric_period_condition=bq_utils.metric_period_condition(),
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        FTR_REFERRALS_BY_GENDER_BY_PERIOD_VIEW_BUILDER.build_and_print()
