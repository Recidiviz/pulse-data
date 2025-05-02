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
"""Event Based Supervision by Person."""

from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query import bq_utils
from recidiviz.calculator.query.state import dataset_config
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

EVENT_BASED_SUPERVISION_VIEW_NAME = "event_based_supervision_populations"

EVENT_BASED_SUPERVISION_DESCRIPTION = """
 Supervision data on the person level with demographic information

 Expanded Dimensions: district, supervision_type
 """

EVENT_BASED_SUPERVISION_QUERY_TEMPLATE = """
    SELECT
      pop.state_code,
      pop.person_id,
      year, month, date_of_supervision,
      supervision_type,
      district,
      staff.external_id AS officer_external_id,
      prioritized_race_or_ethnicity as race_or_ethnicity,
      gender, {age_bucket}, assessment_score_bucket,
    FROM `{project_id}.{materialized_metrics_dataset}.most_recent_supervision_population_span_to_single_day_metrics_materialized` pop
    LEFT JOIN
        `{project_id}.sessions.state_staff_id_to_legacy_supervising_officer_external_id_materialized` staff
    ON
        pop.supervising_officer_staff_id = staff.staff_id
    -- TODO(#39399): Should we filter to 'RISK' assessments only here?
    LEFT JOIN `{project_id}.{sessions_dataset}.assessment_score_sessions_materialized` a
    ON a.state_code = pop.state_code AND
    a.person_id = pop.person_id AND
    pop.date_of_supervision BETWEEN a.assessment_date AND {end_date}
    ,
    {district_dimension},
    {supervision_type_dimension}
    WHERE district IS NOT NULL
      AND {thirty_six_month_filter}
    """

EVENT_BASED_SUPERVISION_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=dataset_config.SHARED_METRIC_VIEWS_DATASET,
    view_id=EVENT_BASED_SUPERVISION_VIEW_NAME,
    view_query_template=EVENT_BASED_SUPERVISION_QUERY_TEMPLATE,
    description=EVENT_BASED_SUPERVISION_DESCRIPTION,
    materialized_metrics_dataset=dataset_config.DATAFLOW_METRICS_MATERIALIZED_DATASET,
    sessions_dataset=dataset_config.SESSIONS_DATASET,
    age_bucket=bq_utils.age_bucket_grouping(),
    district_dimension=bq_utils.unnest_district(),
    supervision_type_dimension=bq_utils.unnest_supervision_type(),
    thirty_six_month_filter=bq_utils.thirty_six_month_filter(),
    end_date=bq_utils.nonnull_end_date_exclusive_clause("a.score_end_date_exclusive"),
    should_materialize=True,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        EVENT_BASED_SUPERVISION_VIEW_BUILDER.build_and_print()
