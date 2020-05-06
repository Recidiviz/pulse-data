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
"""Reincarcerations by month."""
# pylint: disable=trailing-whitespace
from recidiviz.big_query.big_query_view import BigQueryView
from recidiviz.calculator.query import bq_utils
from recidiviz.calculator.query.state import view_config
REINCARCERATIONS_BY_MONTH_VIEW_NAME = 'reincarcerations_by_month'

REINCARCERATIONS_BY_MONTH_DESCRIPTION = """ Reincarcerations by month """

REINCARCERATIONS_BY_MONTH_QUERY_TEMPLATE = \
    """
    /*{description}*/
    SELECT
      state_code, year, month, district,
      IFNULL(ret.returns, 0) as returns,
      IFNULL(adm.total_admissions, 0) as total_admissions
    FROM (
      SELECT
        state_code, year, month,
        district,
        COUNT(person_id) as total_admissions
      FROM `{project_id}.{reference_dataset}.event_based_admissions`
      GROUP BY state_code, year, month, district
    ) adm
    LEFT JOIN (
      SELECT
        state_code, year, month,
        district,
        COUNT(person_id) AS returns
      FROM `{project_id}.{metrics_dataset}.recidivism_count_metrics`
      JOIN `{project_id}.{reference_dataset}.most_recent_job_id_by_metric_and_state_code` job
        USING (state_code, job_id, year, month, metric_period_months),
      {district_dimension}
      WHERE methodology = 'PERSON'
        AND person_id IS NOT NULL
        AND metric_period_months = 1
        AND month IS NOT NULL
        AND year >= EXTRACT(YEAR FROM DATE_SUB(CURRENT_DATE('US/Pacific'), INTERVAL 3 YEAR))
        AND job.metric_type = 'RECIDIVISM_COUNT'
      GROUP BY state_code, year, month, district
    ) ret
    USING (state_code, year, month, district)
    WHERE district IS NOT NULL
    ORDER BY state_code, year, month, district
    """

REINCARCERATIONS_BY_MONTH_VIEW = BigQueryView(
    dataset_id=view_config.DASHBOARD_VIEWS_DATASET,
    view_id=REINCARCERATIONS_BY_MONTH_VIEW_NAME,
    view_query_template=REINCARCERATIONS_BY_MONTH_QUERY_TEMPLATE,
    description=REINCARCERATIONS_BY_MONTH_DESCRIPTION,
    metrics_dataset=view_config.DATAFLOW_METRICS_DATASET,
    reference_dataset=view_config.REFERENCE_TABLES_DATASET,
    district_dimension=bq_utils.unnest_district(district_column='county_of_residence'),
)

if __name__ == '__main__':
    print(REINCARCERATIONS_BY_MONTH_VIEW.view_id)
    print(REINCARCERATIONS_BY_MONTH_VIEW.view_query)
