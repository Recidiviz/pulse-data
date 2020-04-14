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
"""Revocations by violation type by month."""
# pylint: disable=trailing-whitespace, line-too-long
from recidiviz.calculator.query import bqview
from recidiviz.calculator.query.state import view_config
from recidiviz.utils import metadata

PROJECT_ID = metadata.project_id()
METRICS_DATASET = view_config.DATAFLOW_METRICS_DATASET
REFERENCE_DATASET = view_config.REFERENCE_TABLES_DATASET

REVOCATIONS_BY_VIOLATION_TYPE_BY_MONTH_VIEW_NAME = \
    'revocations_by_violation_type_by_month'

REVOCATIONS_BY_VIOLATION_TYPE_BY_MONTH_DESCRIPTION = \
    """ Revocations by violation type by month """

REVOCATIONS_BY_VIOLATION_TYPE_BY_MONTH_QUERY = \
    """
    /*{description}*/
    SELECT
        state_code, year, month,
        IFNULL(felony_count, 0) AS felony_count,
        IFNULL(absconsion_count, 0) AS absconsion_count,
        IFNULL(technical_count, 0) AS technical_count,
        IFNULL(SAFE_SUBTRACT(all_violation_types_count, (felony_count + technical_count + absconsion_count)), 0) AS unknown_count,
        total_supervision_count,
        supervision_type,
        supervising_district_external_id AS district
    FROM (
        SELECT
            state_code, year, month, count AS total_supervision_count,
            IFNULL(supervision_type, 'ALL') AS supervision_type,
            IFNULL(supervising_district_external_id, 'ALL') AS supervising_district_external_id
        FROM `{project_id}.{metrics_dataset}.supervision_population_metrics`
        JOIN `{project_id}.{reference_dataset}.most_recent_job_id_by_metric_and_state_code` job
          USING (state_code, job_id, year, month, metric_period_months)
        WHERE methodology = 'PERSON'
            AND month IS NOT NULL
            AND assessment_score_bucket IS NULL
            AND assessment_type IS NULL
            AND supervising_officer_external_id IS NULL
            AND age_bucket IS NULL
            AND race IS NULL
            AND ethnicity IS NULL
            AND gender IS NULL
            AND most_severe_violation_type IS NULL
            AND most_severe_violation_type_subtype IS NULL
            AND response_count IS NULL
            AND case_type IS NULL
            AND person_id IS NULL
            AND person_external_id IS NULL
            AND supervision_level IS NULL
            AND supervision_level_raw_text IS NULL
            AND metric_period_months = 1
            AND year >= EXTRACT(YEAR FROM DATE_ADD(CURRENT_DATE(), INTERVAL -3 YEAR))
            AND job.metric_type = 'SUPERVISION_POPULATION'
    ) pop
    LEFT JOIN (
        SELECT
            state_code, year, month,
            IFNULL(supervision_type, 'ALL') as supervision_type,
            SUM(IF(source_violation_type = 'FELONY', count, 0)) AS felony_count,
            SUM(IF(source_violation_type = 'TECHNICAL', count, 0)) AS technical_count,
            SUM(IF(source_violation_type = 'ABSCONDED', count, 0)) AS absconsion_count,
            SUM(IF(source_violation_type IS NULL, count, 0)) AS all_violation_types_count,
            IFNULL(supervising_district_external_id, 'ALL') AS supervising_district_external_id
        FROM `{project_id}.{metrics_dataset}.supervision_revocation_metrics`
        JOIN `{project_id}.{reference_dataset}.most_recent_job_id_by_metric_and_state_code` job
          USING (state_code, job_id, year, month, metric_period_months)
        WHERE methodology = 'PERSON'
            AND month IS NOT NULL
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
            AND metric_period_months = 1
            AND year >= EXTRACT(YEAR FROM DATE_ADD(CURRENT_DATE(), INTERVAL -3 YEAR))
            AND job.metric_type = 'SUPERVISION_REVOCATION'
        GROUP BY state_code, year, month, supervision_type, supervising_district_external_id
    ) rev
    USING (state_code, year, month, supervision_type, supervising_district_external_id)
    WHERE supervision_type in ('ALL', 'PAROLE', 'PROBATION')
    ORDER BY state_code, year, month, district, supervision_type
    """.format(
        description=REVOCATIONS_BY_VIOLATION_TYPE_BY_MONTH_DESCRIPTION,
        project_id=PROJECT_ID,
        metrics_dataset=METRICS_DATASET,
        reference_dataset=REFERENCE_DATASET,
        )

REVOCATIONS_BY_VIOLATION_TYPE_BY_MONTH_VIEW = bqview.BigQueryView(
    view_id=REVOCATIONS_BY_VIOLATION_TYPE_BY_MONTH_VIEW_NAME,
    view_query=REVOCATIONS_BY_VIOLATION_TYPE_BY_MONTH_QUERY
)

if __name__ == '__main__':
    print(REVOCATIONS_BY_VIOLATION_TYPE_BY_MONTH_VIEW.view_id)
    print(REVOCATIONS_BY_VIOLATION_TYPE_BY_MONTH_VIEW.view_query)
