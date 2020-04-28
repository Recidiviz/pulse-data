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
from recidiviz.calculator.query import bqview, bq_utils
from recidiviz.calculator.query.state import view_config
from recidiviz.utils import metadata

PROJECT_ID = metadata.project_id()
REFERENCE_DATASET = view_config.REFERENCE_TABLES_DATASET

ADMISSIONS_BY_TYPE_BY_PERIOD_VIEW_NAME = 'admissions_by_type_by_period'

ADMISSIONS_BY_TYPE_BY_PERIOD_DESCRIPTION = \
    """Admissions by type by metric period months."""

ADMISSIONS_BY_TYPE_BY_PERIOD_QUERY = \
    """
    /*{description}*/
    -- Combine supervision revocations with new admission incarcerations
    WITH combined_admissions AS (
      SELECT
        state_code, year, month,
        person_id,
        revocation_admission_date AS admission_date,
        source_violation_type,
        supervision_type,
        district
      FROM `{project_id}.{reference_dataset}.event_based_revocations`

      UNION ALL

      SELECT
        state_code, year, month,
        person_id,
        admission_date,
        'NEW_ADMISSION' AS source_violation_type,
        'ALL' as supervision_type, 'ALL' as district
      FROM `{project_id}.{reference_dataset}.event_based_admissions`
      WHERE admission_reason = 'NEW_ADMISSION'
    ),
    -- Use the most recent admission per person/supervision/district/period
    most_recent_admission AS (
      SELECT
        state_code, metric_period_months, person_id,
        source_violation_type, supervision_type, district,
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
          COUNT(IF(source_violation_type = 'NEW_ADMISSION', person_id, NULL)) AS new_admissions,
          COUNT(IF(source_violation_type = 'TECHNICAL', person_id, NULL)) AS technicals,
          COUNT(IF(source_violation_type IN ('ABSCONDED', 'FELONY'), person_id, NULL)) AS non_technicals,
          COUNT(person_id) AS all_violation_types_count,
          supervision_type,
          district
        FROM most_recent_admission
        WHERE admission_rank = 1
        GROUP BY state_code, metric_period_months, supervision_type, district
    )
    ORDER BY state_code, supervision_type, district, metric_period_months
""".format(
        description=ADMISSIONS_BY_TYPE_BY_PERIOD_DESCRIPTION,
        project_id=PROJECT_ID,
        reference_dataset=REFERENCE_DATASET,
        metric_period_dimension=bq_utils.unnest_metric_period_months(),
        metric_period_condition=bq_utils.metric_period_condition(),
    )

ADMISSIONS_BY_TYPE_BY_PERIOD_VIEW = bqview.BigQueryView(
    view_id=ADMISSIONS_BY_TYPE_BY_PERIOD_VIEW_NAME,
    view_query=ADMISSIONS_BY_TYPE_BY_PERIOD_QUERY
)

if __name__ == '__main__':
    print(ADMISSIONS_BY_TYPE_BY_PERIOD_VIEW.view_id)
    print(ADMISSIONS_BY_TYPE_BY_PERIOD_VIEW.view_query)
