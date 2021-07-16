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
"""Admissions by metric period months"""
# pylint: disable=trailing-whitespace

from recidiviz.calculator.query import bq_utils
from recidiviz.calculator.query.state import dataset_config
from recidiviz.metrics.metric_big_query_view import MetricBigQueryViewBuilder
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

ADMISSIONS_BY_TYPE_BY_PERIOD_VIEW_NAME = "admissions_by_type_by_period"

ADMISSIONS_BY_TYPE_BY_PERIOD_DESCRIPTION = (
    """Admissions by type by metric period months."""
)

ADMISSIONS_BY_TYPE_BY_PERIOD_QUERY_TEMPLATE = """
    /*{description}*/
    -- Combine commitments from supervision with new admission incarcerations
    WITH combined_admissions AS (
      SELECT
        state_code, year, month,
        person_id,
        admission_date,
        most_severe_violation_type,
        supervision_type,
        district
      FROM `{project_id}.{reference_views_dataset}.event_based_commitments_from_supervision_materialized`

      UNION ALL

      SELECT
        state_code, year, month,
        person_id,
        admission_date,
        'NEW_ADMISSION' AS most_severe_violation_type,
        'ALL' as supervision_type, 'ALL' as district
      FROM `{project_id}.{reference_views_dataset}.event_based_admissions`
      WHERE admission_reason = 'NEW_ADMISSION'
    ),
    -- Use the most recent admission per person/supervision/district/period
    most_recent_admission AS (
      SELECT
        state_code, metric_period_months, person_id,
        most_severe_violation_type, supervision_type, district,
        ROW_NUMBER() OVER (PARTITION BY state_code, metric_period_months, person_id, supervision_type, district
                           ORDER BY admission_date DESC) AS admission_rank
      FROM combined_admissions,
      {metric_period_dimension}
      WHERE {metric_period_condition}
    )
    SELECT
        state_code,
        new_admissions,
        technicals,
        non_technicals,
        SAFE_SUBTRACT(all_violation_types_count, (new_admissions + technicals + non_technicals)) as unknown_revocations,
        supervision_type,
        district,
        metric_period_months
    FROM (
        SELECT
          state_code, metric_period_months,
          COUNT(IF(most_severe_violation_type = 'NEW_ADMISSION', person_id, NULL)) AS new_admissions,
          COUNT(IF(most_severe_violation_type = 'TECHNICAL', person_id, NULL)) AS technicals,
          COUNT(IF(most_severe_violation_type IN ('ABSCONDED', 'ESCAPED', 'FELONY', 'MISDEMEANOR', 'LAW'), person_id, NULL)) AS non_technicals,
          COUNT(person_id) AS all_violation_types_count,
          supervision_type,
          district
        FROM most_recent_admission
        WHERE admission_rank = 1
        GROUP BY state_code, metric_period_months, supervision_type, district
    )
    ORDER BY state_code, supervision_type, district, metric_period_months
"""

ADMISSIONS_BY_TYPE_BY_PERIOD_VIEW_BUILDER = MetricBigQueryViewBuilder(
    dataset_id=dataset_config.DASHBOARD_VIEWS_DATASET,
    view_id=ADMISSIONS_BY_TYPE_BY_PERIOD_VIEW_NAME,
    view_query_template=ADMISSIONS_BY_TYPE_BY_PERIOD_QUERY_TEMPLATE,
    dimensions=("state_code", "metric_period_months", "supervision_type", "district"),
    description=ADMISSIONS_BY_TYPE_BY_PERIOD_DESCRIPTION,
    reference_views_dataset=dataset_config.REFERENCE_VIEWS_DATASET,
    metric_period_dimension=bq_utils.unnest_metric_period_months(),
    metric_period_condition=bq_utils.metric_period_condition(),
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        ADMISSIONS_BY_TYPE_BY_PERIOD_VIEW_BUILDER.build_and_print()
