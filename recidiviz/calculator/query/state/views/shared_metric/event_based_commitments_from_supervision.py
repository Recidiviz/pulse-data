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
"""Event Based Commitments From Supervision."""

from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query import bq_utils
from recidiviz.calculator.query.state import dataset_config
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

EVENT_BASED_COMMITMENTS_FROM_SUPERVISION_VIEW_NAME = (
    "event_based_commitments_from_supervision"
)

EVENT_BASED_COMMITMENTS_FROM_SUPERVISION_DESCRIPTION = """
 Commitment from supervision admission data on the person level with violation and 
 admission information.

 Expanded Dimensions: district, supervision_type
 """

EVENT_BASED_COMMITMENTS_FROM_SUPERVISION_QUERY_TEMPLATE = """
    /*{description}*/
    SELECT
      person_id, state_code, year, month,
      supervision_type,
      IFNULL(district, 'EXTERNAL_UNKNOWN') as district,
      supervising_officer_external_id AS officer_external_id,
      most_severe_violation_type,
      admission_date,
      prioritized_race_or_ethnicity as race_or_ethnicity,
      gender,
      age
    FROM `{project_id}.{materialized_metrics_dataset}.most_recent_incarceration_commitment_from_supervision_metrics_included_in_state_population_materialized`,
    {district_dimension},
    {supervision_type_dimension}
    WHERE {thirty_six_month_filter}
    """

EVENT_BASED_COMMITMENTS_FROM_SUPERVISION_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=dataset_config.SHARED_METRIC_VIEWS_DATASET,
    view_id=EVENT_BASED_COMMITMENTS_FROM_SUPERVISION_VIEW_NAME,
    view_query_template=EVENT_BASED_COMMITMENTS_FROM_SUPERVISION_QUERY_TEMPLATE,
    description=EVENT_BASED_COMMITMENTS_FROM_SUPERVISION_DESCRIPTION,
    materialized_metrics_dataset=dataset_config.DATAFLOW_METRICS_MATERIALIZED_DATASET,
    district_dimension=bq_utils.unnest_district(),
    supervision_type_dimension=bq_utils.unnest_supervision_type(),
    thirty_six_month_filter=bq_utils.thirty_six_month_filter(),
    should_materialize=True,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        EVENT_BASED_COMMITMENTS_FROM_SUPERVISION_VIEW_BUILDER.build_and_print()
