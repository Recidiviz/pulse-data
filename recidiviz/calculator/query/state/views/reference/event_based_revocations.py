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
"""Event Based Revocations."""
# pylint: disable=trailing-whitespace, line-too-long

from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query import bq_utils
from recidiviz.calculator.query.state import dataset_config
from recidiviz.utils.environment import GAE_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

EVENT_BASED_REVOCATIONS_VIEW_NAME = 'event_based_revocations'

EVENT_BASED_REVOCATIONS_DESCRIPTION = """
 Revocation data on the person level with revocation violation and admission information

 Expanded Dimensions: district, supervision_type
 """

EVENT_BASED_REVOCATIONS_QUERY_TEMPLATE = \
    """
    /*{description}*/
    SELECT
      person_id, state_code, year, month,
      supervision_type,
      district,
      judicial_district_code,
      supervising_officer_external_id AS officer_external_id,
      source_violation_type,
      revocation_admission_date,
      race,
      ethnicity,
      gender,
      age_bucket
    FROM `{project_id}.{metrics_dataset}.supervision_revocation_metrics` m
    JOIN `{project_id}.{reference_dataset}.most_recent_job_id_by_metric_and_state_code` job
      USING (state_code, job_id, year, month, metric_period_months, metric_type),
    {district_dimension},
    {supervision_dimension}
    WHERE methodology = 'EVENT'
      AND m.metric_period_months = 1
      AND person_id IS NOT NULL
      AND district IS NOT NULL
      AND month IS NOT NULL
      AND year >= EXTRACT(YEAR FROM DATE_SUB(CURRENT_DATE(), INTERVAL 3 YEAR))
    """

EVENT_BASED_REVOCATIONS_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=dataset_config.REFERENCE_TABLES_DATASET,
    view_id=EVENT_BASED_REVOCATIONS_VIEW_NAME,
    view_query_template=EVENT_BASED_REVOCATIONS_QUERY_TEMPLATE,
    description=EVENT_BASED_REVOCATIONS_DESCRIPTION,
    metrics_dataset=dataset_config.DATAFLOW_METRICS_DATASET,
    reference_dataset=dataset_config.REFERENCE_TABLES_DATASET,
    district_dimension=bq_utils.unnest_district(),
    supervision_dimension=bq_utils.unnest_supervision_type(),
)

if __name__ == '__main__':
    with local_project_id_override(GAE_PROJECT_STAGING):
        EVENT_BASED_REVOCATIONS_VIEW_BUILDER.build_and_print()
