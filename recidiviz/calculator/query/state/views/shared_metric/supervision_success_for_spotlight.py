# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2023 Recidiviz, Inc.
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
"""Supervision success data with corresponding judicial district for the public dashboard"""
from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query import bq_utils
from recidiviz.calculator.query.state import dataset_config
from recidiviz.calculator.query.state.views.public_dashboard.utils import (
    spotlight_age_buckets,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

SUPERVISION_SUCCESS_FOR_SPOTLIGHT_VIEW_NAME = "supervision_success_for_spotlight"

SUPERVISION_SUCCESS_FOR_SPOTLIGHT_VIEW_DESCRIPTION = """Supervision success data with corresponding judicial district for the public dashboard."""

SUPERVISION_SUCCESS_FOR_SPOTLIGHT_VIEW_QUERY_TEMPLATE = """
    SELECT
        sup.state_code,
        sup.person_id,
        year,
        month,
        successful_completion,
        supervision_type,
        supervising_district_external_id,
        sent.judicial_district AS judicial_district_code,
        gender,
        prioritized_race_or_ethnicity,
        {age_bucket},
    FROM `{project_id}.{materialized_metrics_dataset}.most_recent_supervision_success_metrics_materialized` sup
    LEFT JOIN `{project_id}.{sessions_dataset}.sentences_preprocessed_materialized` sent
      ON sup.state_code = sent.state_code
      AND sup.person_id = sent.person_id
      -- Follow the dataflow supervision success methodology and limit to sentences
      -- projected to finish in this year/month
      AND DATE(year, month, 1) = DATE_TRUNC(sent.projected_completion_date_min, MONTH)
    WHERE {thirty_six_month_filter}
    -- Pick the judicial district code from the sentences where it is hydrated and then
    -- prioritize the sentence that started the closest to the projected supervision success date
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY state_code, person_id, year, month, supervision_type, supervising_district_external_id
        ORDER BY judicial_district = "EXTERNAL_UNKNOWN", effective_date DESC, date_imposed DESC, judicial_district
    ) = 1
    """

SUPERVISION_SUCCESS_FOR_SPOTLIGHT_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=dataset_config.SHARED_METRIC_VIEWS_DATASET,
    view_id=SUPERVISION_SUCCESS_FOR_SPOTLIGHT_VIEW_NAME,
    view_query_template=SUPERVISION_SUCCESS_FOR_SPOTLIGHT_VIEW_QUERY_TEMPLATE,
    description=SUPERVISION_SUCCESS_FOR_SPOTLIGHT_VIEW_DESCRIPTION,
    materialized_metrics_dataset=dataset_config.DATAFLOW_METRICS_MATERIALIZED_DATASET,
    sessions_dataset=dataset_config.SESSIONS_DATASET,
    age_bucket=spotlight_age_buckets(),
    thirty_six_month_filter=bq_utils.thirty_six_month_filter(),
    clustering_fields=["state_code"],
    should_materialize=True,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        SUPERVISION_SUCCESS_FOR_SPOTLIGHT_VIEW_BUILDER.build_and_print()
