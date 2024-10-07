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

"""A view which provides a comparison of internal supervision population counts by supervision type to external counts
provided by the state."""

from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.state import dataset_config as state_dataset_config
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override
from recidiviz.validation.views import dataset_config

SUPERVISION_POPULATION_BY_TYPE_EXTERNAL_COMPARISON_VIEW_NAME = (
    "supervision_population_by_type_external_comparison"
)

SUPERVISION_POPULATION_BY_TYPE_EXTERNAL_COMPARISON_DESCRIPTION = """ Comparison of internal and external supervision population counts by supervision type """


SUPERVISION_POPULATION_BY_TYPE_EXTERNAL_COMPARISON_QUERY_TEMPLATE = """
    WITH external_validation_dates_and_supervision_types AS (
        -- Only compare states and months for which we have external validation data
        SELECT DISTINCT state_code, date_of_supervision, supervision_type FROM
        `{project_id}.{external_accuracy_dataset}.supervision_population_by_type_materialized`
    ), internal_supervision_population AS (
        SELECT
            state_code, date_of_supervision,
            COUNT(DISTINCT(person_id)) as internal_population_count,
            supervision_type
        FROM `{project_id}.{materialized_metrics_dataset}.most_recent_supervision_population_span_to_single_day_metrics_materialized`
        WHERE included_in_state_population
        GROUP BY state_code, date_of_supervision, supervision_type
    ), relevant_internal_supervision_population AS (
        SELECT * FROM
          external_validation_dates_and_supervision_types      
        LEFT JOIN
          internal_supervision_population        
        USING(state_code, date_of_supervision, supervision_type)
    ),
    comparison AS (
        SELECT
          state_code,
          date_of_supervision,
          supervision_type,
          IFNULL(population_count, 0) as external_population_count,
          IFNULL(internal_population_count, 0) as internal_population_count
        FROM
          `{project_id}.{external_accuracy_dataset}.supervision_population_by_type_materialized`
        FULL OUTER JOIN
          relevant_internal_supervision_population
        USING (state_code, date_of_supervision, supervision_type)
    )
    SELECT *, state_code AS region_code
    FROM comparison
    ORDER BY state_code, date_of_supervision, supervision_type
"""

SUPERVISION_POPULATION_BY_TYPE_EXTERNAL_COMPARISON_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=dataset_config.VIEWS_DATASET,
    view_id=SUPERVISION_POPULATION_BY_TYPE_EXTERNAL_COMPARISON_VIEW_NAME,
    view_query_template=SUPERVISION_POPULATION_BY_TYPE_EXTERNAL_COMPARISON_QUERY_TEMPLATE,
    description=SUPERVISION_POPULATION_BY_TYPE_EXTERNAL_COMPARISON_DESCRIPTION,
    external_accuracy_dataset=dataset_config.EXTERNAL_ACCURACY_DATASET,
    materialized_metrics_dataset=state_dataset_config.DATAFLOW_METRICS_MATERIALIZED_DATASET,
    should_materialize=True,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        SUPERVISION_POPULATION_BY_TYPE_EXTERNAL_COMPARISON_VIEW_BUILDER.build_and_print()
