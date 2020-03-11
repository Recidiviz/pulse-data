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
from recidiviz.calculator.query import bqview
from recidiviz.calculator.query.state import view_config

from recidiviz.utils import metadata

PROJECT_ID = metadata.project_id()
METRICS_DATASET = view_config.DATAFLOW_METRICS_DATASET
VIEWS_DATASET = view_config.DASHBOARD_VIEWS_DATASET

ADMISSIONS_BY_TYPE_BY_MONTH_VIEW_NAME = 'admissions_by_type_by_month'

ADMISSIONS_BY_TYPE_BY_MONTH_DESCRIPTION = """ Admissions by type by month """

ADMISSIONS_BY_TYPE_BY_MONTH_QUERY = \
    """
    /*{description}*/
    SELECT
      state_code, year, month,
      IFNULL(new_admissions, 0) as new_admissions,
      IFNULL(technicals, 0) as technicals,
      IFNULL(non_technicals, 0) as non_technicals,
      IFNULL(unknown_revocations, 0) as unknown_revocations,
      supervision_type,
      district
    FROM (
      SELECT
        state_code, year, month,
        technicals,
        (felony_count + absconsion_count) as non_technicals,
        SAFE_SUBTRACT(all_violation_types_count, (felony_count + technicals + absconsion_count)) as unknown_revocations,
        supervision_type,
        supervising_district_external_id as district
      FROM (
        SELECT
          state_code, year, month,
          SUM(IF(violation_type = 'FELONY', count, 0)) AS felony_count,
          SUM(IF(violation_type = 'TECHNICAL', count, 0)) AS technicals,
          SUM(IF(violation_type = 'ABSCONDED', count, 0)) AS absconsion_count,
          SUM(IF(violation_type = 'ALL_VIOLATION_TYPES', count, 0)) AS all_violation_types_count,
          supervision_type,
          supervising_district_external_id,
          metric_period_months
        FROM (
          SELECT
            state_code, year, month, count,
            IFNULL(source_violation_type, 'ALL_VIOLATION_TYPES') as violation_type,
            IFNULL(supervision_type, 'ALL') as supervision_type,
            IFNULL(supervising_district_external_id, 'ALL') as supervising_district_external_id,
            metric_period_months
          FROM `{project_id}.{metrics_dataset}.supervision_revocation_metrics`
          JOIN `{project_id}.{views_dataset}.most_recent_job_id_by_metric_and_state_code` job
            USING (state_code, job_id, year, month, metric_period_months)
          WHERE methodology = 'PERSON'
            AND month IS NOT NULL
            AND metric_period_months = 1
            AND assessment_score_bucket IS NULL
            AND assessment_type IS NULL
            AND supervising_officer_external_id IS NULL
            AND revocation_type IS NULL
            AND age_bucket IS NULL
            AND race IS NULL
            AND ethnicity IS NULL
            AND gender IS NULL
            AND case_type IS NULL
            AND person_id IS NULL
            AND person_external_id IS NULL
            AND year >= EXTRACT(YEAR FROM DATE_ADD(CURRENT_DATE(), INTERVAL -3 YEAR))
            AND job.metric_type = 'SUPERVISION_REVOCATION'
        )
        GROUP BY state_code, year, month, supervision_type, supervising_district_external_id, metric_period_months
      )
    ) rev
    FULL OUTER JOIN (
      SELECT
        state_code, year, month,
        SUM(count) as new_admissions,
        'ALL' as supervision_type, 'ALL' as district
      FROM `{project_id}.{metrics_dataset}.incarceration_admission_metrics`
      JOIN `{project_id}.{views_dataset}.most_recent_job_id_by_metric_and_state_code` job
        USING (state_code, job_id, year, month, metric_period_months)
      WHERE methodology = 'PERSON'
        AND month IS NOT NULL
        AND metric_period_months = 1
        AND admission_reason = 'NEW_ADMISSION'
        AND facility IS NULL
        AND age_bucket IS NULL
        AND race IS NULL
        AND ethnicity IS NULL
        AND gender IS NULL
        AND person_id IS NULL
        AND person_external_id IS NULL
        AND specialized_purpose_for_incarceration IS NULL
        AND admission_reason_raw_text IS NULL
        AND admission_date IS NULL
        AND supervision_type_at_admission IS NULL
        AND year >= EXTRACT(YEAR FROM DATE_ADD(CURRENT_DATE(), INTERVAL -3 YEAR))
        AND job.metric_type = 'INCARCERATION_ADMISSION'
      GROUP BY state_code, year, month
    )
    USING (state_code, year, month, supervision_type, district)
    ORDER BY state_code, year, month, district, supervision_type
""".format(
        description=ADMISSIONS_BY_TYPE_BY_MONTH_DESCRIPTION,
        project_id=PROJECT_ID,
        metrics_dataset=METRICS_DATASET,
        views_dataset=VIEWS_DATASET,
    )

ADMISSIONS_BY_TYPE_BY_MONTH_VIEW = bqview.BigQueryView(
    view_id=ADMISSIONS_BY_TYPE_BY_MONTH_VIEW_NAME,
    view_query=ADMISSIONS_BY_TYPE_BY_MONTH_QUERY
)

if __name__ == '__main__':
    print(ADMISSIONS_BY_TYPE_BY_MONTH_VIEW.view_id)
    print(ADMISSIONS_BY_TYPE_BY_MONTH_VIEW.view_query)
