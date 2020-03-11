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
"""Reincarcerations by metric period month."""
# pylint: disable=trailing-whitespace
from recidiviz.calculator.query import bqview
from recidiviz.calculator.query.state import view_config
from recidiviz.utils import metadata

PROJECT_ID = metadata.project_id()
VIEWS_DATASET = view_config.DASHBOARD_VIEWS_DATASET
METRICS_DATASET = view_config.DATAFLOW_METRICS_DATASET

REINCARCERATIONS_BY_PERIOD_VIEW_NAME = 'reincarcerations_by_period'

REINCARCERATIONS_BY_PERIOD_DESCRIPTION = \
    """Reincarcerations by metric period month."""

REINCARCERATIONS_BY_PERIOD_QUERY = \
    """
    /*{description}*/
    SELECT
      state_code, metric_period_months, district,
      IFNULL(ret.returns, 0) as returns,
      IFNULL(adm.total_admissions, 0) as total_admissions
    FROM (
      SELECT
        state_code, metric_period_months,
        IFNULL(county_of_residence, 'ALL') AS district,
        sum(count) as total_admissions
      FROM `{project_id}.{metrics_dataset}.incarceration_admission_metrics`
      JOIN `{project_id}.{views_dataset}.most_recent_job_id_by_metric_and_state_code` job
        USING (state_code, job_id, year, month, metric_period_months)
      WHERE methodology = 'PERSON'
        AND facility IS NULL
        AND age_bucket IS NULL
        AND race IS NULL
        AND ethnicity IS NULL
        AND gender IS NULL
        AND person_id IS NULL
        AND person_external_id IS NULL
        AND specialized_purpose_for_incarceration IS NULL
        AND admission_reason IS NULL
        AND admission_reason_raw_text IS NULL
        AND admission_date IS NULL
        AND supervision_type_at_admission IS NULL
        AND year = EXTRACT(YEAR FROM CURRENT_DATE('US/Pacific'))
        AND month = EXTRACT(MONTH FROM CURRENT_DATE('US/Pacific'))
        AND job.metric_type = 'INCARCERATION_ADMISSION'
      GROUP BY state_code, metric_period_months, county_of_residence
    ) adm
    LEFT JOIN (
      SELECT
        state_code,
        IFNULL(county_of_residence, 'ALL') AS district,
        metric_period_months,
        returns
      FROM `{project_id}.{metrics_dataset}.recidivism_count_metrics`
      JOIN `{project_id}.{views_dataset}.most_recent_job_id_by_metric_and_state_code` job
        USING (state_code, job_id, year, month, metric_period_months)
      WHERE methodology = 'PERSON'
        AND age_bucket IS NULL
        AND stay_length_bucket IS NULL
        AND race IS NULL
        AND ethnicity IS NULL
        AND gender IS NULL
        AND person_id IS NULL
        AND person_external_id IS NULL
        AND release_facility IS NULL
        AND return_type IS NULL
        AND from_supervision_type IS NULL
        AND source_violation_type IS NULL
        AND year = EXTRACT(YEAR FROM CURRENT_DATE('US/Pacific'))
        AND month = EXTRACT(MONTH FROM CURRENT_DATE('US/Pacific'))
        AND job.metric_type = 'RECIDIVISM_COUNT'
    ) ret
    USING (state_code, metric_period_months, district)
    ORDER BY state_code, metric_period_months, district
    """.format(
        description=REINCARCERATIONS_BY_PERIOD_DESCRIPTION,
        project_id=PROJECT_ID,
        metrics_dataset=METRICS_DATASET,
        views_dataset=VIEWS_DATASET,
    )

REINCARCERATIONS_BY_PERIOD_VIEW = bqview.BigQueryView(
    view_id=REINCARCERATIONS_BY_PERIOD_VIEW_NAME,
    view_query=REINCARCERATIONS_BY_PERIOD_QUERY
)

if __name__ == '__main__':
    print(REINCARCERATIONS_BY_PERIOD_VIEW.view_id)
    print(REINCARCERATIONS_BY_PERIOD_VIEW.view_query)
