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
"""Revocations by race and ethnicity by metric period months."""
# pylint: disable=trailing-whitespace, line-too-long
from recidiviz.calculator.query import export_config, bqview
from recidiviz.calculator.query.state import view_config
from recidiviz.utils import metadata

PROJECT_ID = metadata.project_id()
METRICS_DATASET = view_config.DATAFLOW_METRICS_DATASET
VIEWS_DATASET = view_config.DASHBOARD_VIEWS_DATASET
BASE_DATASET = export_config.STATE_BASE_TABLES_BQ_DATASET

REVOCATIONS_BY_RACE_AND_ETHNICITY_BY_PERIOD_VIEW_NAME = \
    'revocations_by_race_and_ethnicity_by_period'

REVOCATIONS_BY_RACE_AND_ETHNICITY_BY_PERIOD_DESCRIPTION = \
    """Revocations by race and ethnicity by metric period months."""

REVOCATIONS_BY_RACE_AND_ETHNICITY_BY_PERIOD_QUERY = \
    """
    /*{description}*/
    SELECT
      pop.state_code,
      race_or_ethnicity,
      IFNULL(rev.count, 0) AS revocation_count,
      pop.count AS total_supervision_count,
      supervision_type,
      supervising_district_external_id AS district,
      metric_period_months
    FROM (
      SELECT
        state_code, year, month, count,
        IFNULL(supervision_type, 'ALL') as supervision_type,
        IFNULL(supervising_district_external_id, 'ALL') as supervising_district_external_id,
        metric_period_months,
        race as race_or_ethnicity
      FROM `{project_id}.{metrics_dataset}.supervision_population_metrics`
      JOIN `{project_id}.{views_dataset}.most_recent_job_id_by_metric_and_state_code` job
        USING (state_code, job_id, year, month, metric_period_months)
      WHERE methodology = 'PERSON'
        AND month IS NOT NULL
        AND assessment_score_bucket IS NULL
        AND assessment_type IS NULL
        AND supervising_officer_external_id IS NULL
        AND age_bucket IS NULL
        AND race is NOT NULL
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
        AND year = EXTRACT(YEAR FROM CURRENT_DATE('US/Pacific'))
        AND month = EXTRACT(MONTH FROM CURRENT_DATE('US/Pacific'))
        AND job.metric_type = 'SUPERVISION_POPULATION'

      UNION ALL

      SELECT
        state_code, year, month, count,
        IFNULL(supervision_type, 'ALL') as supervision_type,
        IFNULL(supervising_district_external_id, 'ALL') as supervising_district_external_id,
        metric_period_months,
        ethnicity as race_or_ethnicity
      FROM `{project_id}.{metrics_dataset}.supervision_population_metrics`
      JOIN `{project_id}.{views_dataset}.most_recent_job_id_by_metric_and_state_code` job
        USING (state_code, job_id, year, month, metric_period_months)
      WHERE methodology = 'PERSON'
        AND month IS NOT NULL
        AND assessment_score_bucket IS NULL
        AND assessment_type IS NULL
        AND supervising_officer_external_id IS NULL
        AND age_bucket IS NULL
        AND race is NULL
        AND ethnicity IS NOT NULL
        AND gender IS NULL
        AND most_severe_violation_type IS NULL
        AND most_severe_violation_type_subtype IS NULL
        AND response_count IS NULL
        AND case_type IS NULL
        AND person_id IS NULL
        AND person_external_id IS NULL
        AND supervision_level IS NULL
        AND supervision_level_raw_text IS NULL
        AND year = EXTRACT(YEAR FROM CURRENT_DATE('US/Pacific'))
        AND month = EXTRACT(MONTH FROM CURRENT_DATE('US/Pacific'))
        AND job.metric_type = 'SUPERVISION_POPULATION'
    ) pop
    LEFT JOIN (
      SELECT
        state_code, year, month, count,
        IFNULL(supervision_type, 'ALL') as supervision_type,
        IFNULL(supervising_district_external_id, 'ALL') as supervising_district_external_id,
        metric_period_months,
        race as race_or_ethnicity
      FROM `{project_id}.{metrics_dataset}.supervision_revocation_metrics`
      JOIN `{project_id}.{views_dataset}.most_recent_job_id_by_metric_and_state_code` job
        USING (state_code, job_id, year, month, metric_period_months)
      WHERE methodology = 'PERSON'
        AND month IS NOT NULL
        AND assessment_score_bucket IS NULL
        AND assessment_type IS NULL
        AND revocation_type IS NULL
        AND source_violation_type IS NULL
        AND supervising_officer_external_id IS NULL
        AND age_bucket IS NULL
        AND race is NOT NULL
        AND ethnicity IS NULL
        AND gender IS NULL
        AND case_type IS NULL
        AND person_id IS NULL
        AND person_external_id IS NULL
        AND year = EXTRACT(YEAR FROM CURRENT_DATE('US/Pacific'))
        AND month = EXTRACT(MONTH FROM CURRENT_DATE('US/Pacific'))
        AND job.metric_type = 'SUPERVISION_REVOCATION'

      UNION ALL

      SELECT
        state_code, year, month, count,
        IFNULL(supervision_type, 'ALL') as supervision_type,
        IFNULL(supervising_district_external_id, 'ALL') as supervising_district_external_id,
        metric_period_months,
        ethnicity as race_or_ethnicity
      FROM `{project_id}.{metrics_dataset}.supervision_revocation_metrics`
      JOIN `{project_id}.{views_dataset}.most_recent_job_id_by_metric_and_state_code` job
        USING (state_code, job_id, year, month, metric_period_months)
      WHERE methodology = 'PERSON'
        AND month IS NOT NULL
        AND assessment_score_bucket IS NULL
        AND assessment_type IS NULL
        AND revocation_type IS NULL
        AND source_violation_type IS NULL
        AND supervising_officer_external_id IS NULL
        AND age_bucket IS NULL
        AND race is NULL
        AND ethnicity IS NOT NULL
        AND gender IS NULL
        AND case_type IS NULL
        AND person_id IS NULL
        AND person_external_id IS NULL
        AND year = EXTRACT(YEAR FROM CURRENT_DATE('US/Pacific'))
        AND month = EXTRACT(MONTH FROM CURRENT_DATE('US/Pacific'))
        AND job.metric_type = 'SUPERVISION_REVOCATION'
    ) rev
    USING (state_code, year, month, supervision_type, supervising_district_external_id, metric_period_months, race_or_ethnicity)
    WHERE supervision_type in ('ALL', 'PAROLE', 'PROBATION')
    ORDER BY state_code, race_or_ethnicity, district, supervision_type, metric_period_months
    """.format(
        description=REVOCATIONS_BY_RACE_AND_ETHNICITY_BY_PERIOD_DESCRIPTION,
        project_id=PROJECT_ID,
        metrics_dataset=METRICS_DATASET,
        views_dataset=VIEWS_DATASET,
    )

REVOCATIONS_BY_RACE_AND_ETHNICITY_BY_PERIOD_VIEW = bqview.BigQueryView(
    view_id=REVOCATIONS_BY_RACE_AND_ETHNICITY_BY_PERIOD_VIEW_NAME,
    view_query=REVOCATIONS_BY_RACE_AND_ETHNICITY_BY_PERIOD_QUERY
)

if __name__ == '__main__':
    print(REVOCATIONS_BY_RACE_AND_ETHNICITY_BY_PERIOD_VIEW.view_id)
    print(REVOCATIONS_BY_RACE_AND_ETHNICITY_BY_PERIOD_VIEW.view_query)
