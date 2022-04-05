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

from recidiviz.calculator.query import bqview
from recidiviz.calculator.query.state import view_config

from recidiviz.utils import metadata

PROJECT_ID = metadata.project_id()
METRICS_DATASET = view_config.DATAFLOW_METRICS_DATASET
VIEWS_DATASET = view_config.DASHBOARD_VIEWS_DATASET

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
      supervising_district_external_id AS district,
      IFNULL(ref.count, 0) AS count, 
      pop.count AS total_supervision_count,
      supervision_type, 
      metric_period_months 
    FROM (
      SELECT 
        state_code, metric_period_months, count, 
        IFNULL(supervision_type, 'ALL') AS supervision_type, 
        IFNULL(supervising_district_external_id, 'ALL') AS supervising_district_external_id 
      FROM `{project_id}.{metrics_dataset}.supervision_population_metrics`
      JOIN `{project_id}.{views_dataset}.most_recent_job_id_by_metric_and_state_code` job
        USING (state_code, job_id, year, month, metric_period_months)
      WHERE methodology = 'PERSON'
        AND assessment_score_bucket IS NULL
        AND assessment_type IS NULL
        AND supervising_officer_external_id IS NULL
        AND age_bucket IS NULL
        AND race IS NULL
        AND ethnicity IS NULL
        AND gender IS NULL
        AND year = EXTRACT(YEAR FROM CURRENT_DATE('US/Pacific'))
        AND month = EXTRACT(MONTH FROM CURRENT_DATE('US/Pacific'))
        AND job.metric_type = 'SUPERVISION_POPULATION'
    ) pop
    LEFT JOIN (
      SELECT 
        state_code, metric_period_months, count, 
        IFNULL(supervision_type, 'ALL') AS supervision_type, 
        IFNULL(supervising_district_external_id, 'ALL') AS supervising_district_external_id  
      FROM `{project_id}.{metrics_dataset}.program_referral_metrics`
      JOIN `{project_id}.{views_dataset}.most_recent_job_id_by_metric_and_state_code` job
        USING (state_code, job_id, year, month, metric_period_months)
      WHERE methodology = 'PERSON'
        AND program_id IS NULL
        AND assessment_score_bucket IS NULL
        AND assessment_type IS NULL
        AND supervising_officer_external_id IS NULL
        AND age_bucket IS NULL
        AND race IS NULL
        AND ethnicity IS NULL
        AND gender IS NULL
        AND year = EXTRACT(YEAR FROM CURRENT_DATE('US/Pacific'))
        AND month = EXTRACT(MONTH FROM CURRENT_DATE('US/Pacific'))
        AND job.metric_type = 'PROGRAM_REFERRAL'
    ) ref
    USING (state_code, metric_period_months, supervision_type, supervising_district_external_id)
    WHERE supervision_type in ('ALL', 'PAROLE', 'PROBATION')
      AND state_code = 'US_ND'
    ORDER BY state_code, district, supervision_type, metric_period_months
""".format(
        description=FTR_REFERRAL_DESCRIPTION,
        project_id=PROJECT_ID,
        metrics_dataset=METRICS_DATASET,
        views_dataset=VIEWS_DATASET,
    )

FTR_REFERRALS_BY_PERIOD_VIEW = bqview.BigQueryView(
    view_id=FTR_REFERRALS_BY_PERIOD_VIEW_NAME,
    view_query=FTR_REFERRAL_QUERY
)

if __name__ == '__main__':
    print(FTR_REFERRALS_BY_PERIOD_VIEW.view_id)
    print(FTR_REFERRALS_BY_PERIOD_VIEW.view_query)
