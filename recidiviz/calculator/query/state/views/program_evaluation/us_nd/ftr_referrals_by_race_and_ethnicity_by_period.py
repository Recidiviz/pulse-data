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
"""All individuals who have been referred to Free Through Recovery by metric
month period, broken down by race/ethnicity.
"""
# pylint: disable=trailing-whitespace, line-too-long

from recidiviz.big_query.big_query_view import BigQueryView
from recidiviz.calculator.query import bq_utils
from recidiviz.calculator.query.state import view_config
from recidiviz.utils import metadata

PROJECT_ID = metadata.project_id()
REFERENCE_DATASET = view_config.REFERENCE_TABLES_DATASET

FTR_REFERRALS_BY_RACE_AND_ETHNICITY_BY_PERIOD_VIEW_NAME = \
    'ftr_referrals_by_race_and_ethnicity_by_period'

FTR_REFERRALS_BY_RACE_AND_ETHNICITY_BY_PERIOD_DESCRIPTION = """
 All individuals who have been referred to Free Through Recovery by metric
 month period, broken down by race/ethnicity.
"""

FTR_REFERRALS_BY_RACE_AND_ETHNICITY_BY_PERIOD_QUERY = \
    """
    /*{description}*/
    SELECT
      state_code,
      race_or_ethnicity,
      IFNULL(ref.count, 0) AS count,
      total_supervision_count,
      supervision_type,
      district,
      metric_period_months
    FROM (
      SELECT
        state_code,
        COUNT(DISTINCT person_id) AS total_supervision_count,
        supervision_type,
        district,
        metric_period_months,
        race_or_ethnicity
      FROM `{project_id}.{reference_dataset}.event_based_supervision_populations`,
      {metric_period_dimension},
      {race_or_ethnicity_dimension}
      WHERE {metric_period_condition}
      GROUP BY state_code, supervision_type, district, metric_period_months, race_or_ethnicity
    ) pop
    LEFT JOIN (
      SELECT
        state_code,
        COUNT(DISTINCT person_id) AS count,
        supervision_type,
        district,
        metric_period_months,
        race_or_ethnicity
      FROM `{project_id}.{reference_dataset}.event_based_program_referrals`,
      {metric_period_dimension},
      {race_or_ethnicity_dimension}
      WHERE {metric_period_condition}
      GROUP BY state_code, supervision_type, district, metric_period_months, race_or_ethnicity
    ) ref
    USING (state_code, supervision_type, district, metric_period_months, race_or_ethnicity)
    WHERE supervision_type in ('ALL', 'PAROLE', 'PROBATION')
      AND district IS NOT NULL
      AND race_or_ethnicity NOT IN ('EXTERNAL_UNKNOWN', 'NOT_HISPANIC')
      AND state_code = 'US_ND'
    ORDER BY state_code, race_or_ethnicity, district, supervision_type, metric_period_months
    """.format(
        description=FTR_REFERRALS_BY_RACE_AND_ETHNICITY_BY_PERIOD_DESCRIPTION,
        project_id=PROJECT_ID,
        reference_dataset=REFERENCE_DATASET,
        metric_period_dimension=bq_utils.unnest_metric_period_months(),
        race_or_ethnicity_dimension=bq_utils.unnest_race_and_ethnicity(),
        metric_period_condition=bq_utils.metric_period_condition(),
    )

FTR_REFERRALS_BY_RACE_AND_ETHNICITY_BY_PERIOD_VIEW = BigQueryView(
    view_id=FTR_REFERRALS_BY_RACE_AND_ETHNICITY_BY_PERIOD_VIEW_NAME,
    view_query=FTR_REFERRALS_BY_RACE_AND_ETHNICITY_BY_PERIOD_QUERY
)

if __name__ == '__main__':
    print(FTR_REFERRALS_BY_RACE_AND_ETHNICITY_BY_PERIOD_VIEW.view_id)
    print(FTR_REFERRALS_BY_RACE_AND_ETHNICITY_BY_PERIOD_VIEW.view_query)
