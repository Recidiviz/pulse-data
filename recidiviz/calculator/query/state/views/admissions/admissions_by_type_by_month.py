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
"""Admissions by type by month"""
# pylint: disable=trailing-whitespace
from recidiviz.big_query.big_query_view import BigQueryView
from recidiviz.calculator.query.state import view_config

from recidiviz.utils import metadata

PROJECT_ID = metadata.project_id()
REFERENCE_DATASET = view_config.REFERENCE_TABLES_DATASET

ADMISSIONS_BY_TYPE_BY_MONTH_VIEW_NAME = 'admissions_by_type_by_month'

ADMISSIONS_BY_TYPE_BY_MONTH_DESCRIPTION = """ Admissions by type by month """

ADMISSIONS_BY_TYPE_BY_MONTH_QUERY = \
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
        state_code, year, month, person_id,
        source_violation_type, supervision_type, district,
        ROW_NUMBER() OVER (PARTITION BY state_code, year, month, person_id, supervision_type, district
                           ORDER BY admission_date DESC) AS admission_rank
      FROM combined_admissions
    )
    SELECT
        state_code, year, month,
        new_admissions,
        technicals,
        non_technicals,
        SAFE_SUBTRACT(all_violation_types_count, (new_admissions + technicals + non_technicals)) as unknown_revocations,
        supervision_type,
        district
    FROM (
        SELECT
          state_code, year, month,
          COUNT(IF(source_violation_type = 'NEW_ADMISSION', person_id, NULL)) AS new_admissions,
          COUNT(IF(source_violation_type = 'TECHNICAL', person_id, NULL)) AS technicals,
          COUNT(IF(source_violation_type IN ('ABSCONDED', 'FELONY'), person_id, NULL)) AS non_technicals,
          COUNT(person_id) AS all_violation_types_count,
          supervision_type,
          district
        FROM most_recent_admission
        WHERE admission_rank = 1
        GROUP BY state_code, year, month, supervision_type, district
    )
    ORDER BY state_code, year, month, district, supervision_type
""".format(
        description=ADMISSIONS_BY_TYPE_BY_MONTH_DESCRIPTION,
        project_id=PROJECT_ID,
        reference_dataset=REFERENCE_DATASET,
    )

ADMISSIONS_BY_TYPE_BY_MONTH_VIEW = BigQueryView(
    view_id=ADMISSIONS_BY_TYPE_BY_MONTH_VIEW_NAME,
    view_query=ADMISSIONS_BY_TYPE_BY_MONTH_QUERY
)

if __name__ == '__main__':
    print(ADMISSIONS_BY_TYPE_BY_MONTH_VIEW.view_id)
    print(ADMISSIONS_BY_TYPE_BY_MONTH_VIEW.view_query)
