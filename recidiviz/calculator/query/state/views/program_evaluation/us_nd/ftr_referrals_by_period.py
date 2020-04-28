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
period months.
"""
# pylint: disable=trailing-whitespace

from recidiviz.calculator.query import bqview, bq_utils
from recidiviz.calculator.query.state import view_config

from recidiviz.utils import metadata

PROJECT_ID = metadata.project_id()
REFERENCE_DATASET = view_config.REFERENCE_TABLES_DATASET

FTR_REFERRALS_BY_PERIOD_VIEW_NAME = 'ftr_referrals_by_period'

FTR_REFERRAL_DESCRIPTION = \
    """All individuals who have been referred to Free Through Recovery by
    metric period months.
    """

# TODO(2549): Filter by FTR specifically once the metadata exists.
FTR_REFERRAL_QUERY = \
    """
    /*{description}*/
    SELECT
      pop.state_code,
      district,
      IFNULL(ref.count, 0) AS count,
      total_supervision_count,
      supervision_type,
      metric_period_months
    FROM (
      SELECT
        state_code, metric_period_months,
        COUNT(DISTINCT person_id) AS total_supervision_count,
        supervision_type,
        district
      FROM `{project_id}.{reference_dataset}.event_based_supervision_populations`,
      {metric_period_dimension}
      WHERE {metric_period_condition}
      GROUP BY state_code, metric_period_months, supervision_type, district
    ) pop
    LEFT JOIN (
      SELECT
        state_code, metric_period_months,
        COUNT(DISTINCT person_id) AS count,
        supervision_type,
        district
      FROM `{project_id}.{reference_dataset}.event_based_program_referrals`,
      {metric_period_dimension}
      WHERE {metric_period_condition}
      GROUP BY state_code, metric_period_months, supervision_type, district
    ) ref
    USING (state_code, metric_period_months, supervision_type, district)
    WHERE supervision_type in ('ALL', 'PAROLE', 'PROBATION')
      AND district IS NOT NULL
      AND state_code = 'US_ND'
    ORDER BY state_code, district, supervision_type, metric_period_months
""".format(
        description=FTR_REFERRAL_DESCRIPTION,
        project_id=PROJECT_ID,
        reference_dataset=REFERENCE_DATASET,
        metric_period_dimension=bq_utils.unnest_metric_period_months(),
        metric_period_condition=bq_utils.metric_period_condition(),
    )

FTR_REFERRALS_BY_PERIOD_VIEW = bqview.BigQueryView(
    view_id=FTR_REFERRALS_BY_PERIOD_VIEW_NAME,
    view_query=FTR_REFERRAL_QUERY
)

if __name__ == '__main__':
    print(FTR_REFERRALS_BY_PERIOD_VIEW.view_id)
    print(FTR_REFERRALS_BY_PERIOD_VIEW.view_query)
