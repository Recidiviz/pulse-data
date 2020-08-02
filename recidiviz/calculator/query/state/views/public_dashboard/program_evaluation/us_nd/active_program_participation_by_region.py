# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2020 Recidiviz, Inc.
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
"""Active FTR participation counts by the region of the program location."""
# pylint: disable=trailing-whitespace, line-too-long
from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query import bq_utils
from recidiviz.calculator.query.state import dataset_config
from recidiviz.utils.environment import GAE_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

ACTIVE_PROGRAM_PARTICIPATION_BY_REGION_VIEW_NAME = 'active_program_participation_by_region'

ACTIVE_PROGRAM_PARTICIPATION_BY_REGION_VIEW_DESCRIPTION = \
    """Active program participation counts by the region of the program location."""

ACTIVE_PROGRAM_PARTICIPATION_BY_REGION_VIEW_QUERY_TEMPLATE = \
    """
    /*{description}*/
    SELECT
      state_code,
      supervision_type,
      region_id,
      COUNT(DISTINCT(person_id)) as participation_count
    FROM
      `{project_id}.{metrics_dataset}.program_participation_metrics`
      JOIN `{project_id}.{reference_dataset}.most_recent_job_id_by_metric_and_state_code` job
        USING (state_code, job_id, year, month, metric_period_months)
    LEFT JOIN
      `{project_id}.{reference_dataset}.program_locations`
    USING (state_code, program_location_id)
      WHERE {current_month_condition}
        AND state_code = 'US_ND'
        AND methodology = 'PERSON'
        AND metric_period_months = 0
        AND person_id IS NOT NULL
        AND supervision_type IN ('PAROLE', 'PROBATION')
        AND job.metric_type = 'PROGRAM_PARTICIPATION'
    GROUP BY state_code, supervision_type, region_id
    ORDER BY state_code, supervision_type, region_id
    """

ACTIVE_PROGRAM_PARTICIPATION_BY_REGION_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=dataset_config.PUBLIC_DASHBOARD_VIEWS_DATASET,
    view_id=ACTIVE_PROGRAM_PARTICIPATION_BY_REGION_VIEW_NAME,
    view_query_template=ACTIVE_PROGRAM_PARTICIPATION_BY_REGION_VIEW_QUERY_TEMPLATE,
    description=ACTIVE_PROGRAM_PARTICIPATION_BY_REGION_VIEW_DESCRIPTION,
    base_dataset=dataset_config.STATE_BASE_DATASET,
    reference_dataset=dataset_config.REFERENCE_TABLES_DATASET,
    metrics_dataset=dataset_config.DATAFLOW_METRICS_DATASET,
    current_month_condition=bq_utils.current_month_condition(),
)

if __name__ == '__main__':
    with local_project_id_override(GAE_PROJECT_STAGING):
        ACTIVE_PROGRAM_PARTICIPATION_BY_REGION_VIEW_BUILDER.build_and_print()
