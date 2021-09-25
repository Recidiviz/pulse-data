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
"""Generator to grab specified dataflow_metrics views and keep only the single day metrics
 corresponding to a most recent [state, metric_type] combo."""
from typing import Dict, List

from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.state.dataset_config import (
    DATAFLOW_METRICS_MATERIALIZED_DATASET,
)
from recidiviz.calculator.query.state.views.dataflow_metrics_materialized.most_recent_dataflow_metrics import (
    generate_metric_view_names,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

METRIC_DATE_COLUMNS_BY_SINGLE_DAY_METRICS_TABLES_TO_MATERIALIZE: Dict[str, str] = {
    "supervision_population_metrics": "date_of_supervision",
    "incarceration_population_metrics": "date_of_stay",
    "program_participation_metrics": "date_of_participation",
}

MOST_RECENT_SINGLE_DAY_JOBS_TEMPLATE: str = """
    /*{description}*/
    WITH all_job_ids AS (
        SELECT DISTINCT
            job_id,
            state_code,
            {metric_date_column}
        FROM
            `{project_id}.{materialized_metrics_dataset}.most_recent_{metric_table_view_name}_materialized`),
    ranked_job_ids AS (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY state_code ORDER BY {metric_date_column} DESC, job_id DESC) AS recency_rank
      FROM all_job_ids
      WHERE {metric_date_column} <= CURRENT_DATE('US/Pacific')
    )
    SELECT *
    FROM `{project_id}.{materialized_metrics_dataset}.most_recent_{metric_table_view_name}_materialized`
    JOIN (
        SELECT state_code, job_id, {metric_date_column}
        FROM ranked_job_ids
        WHERE recency_rank = 1
    )
    USING (state_code, job_id, {metric_date_column})
    """


def _make_most_recent_single_day_metric_view_builder(
    metric_name: str,
) -> List[SimpleBigQueryViewBuilder]:
    description = (
        f"{metric_name} output for the most recent single day recorded for this metric"
    )
    metric_table_view_names = generate_metric_view_names(metric_name)
    view_builders: List[SimpleBigQueryViewBuilder] = []

    for metric_table_view_name in metric_table_view_names:
        view_builders.append(
            SimpleBigQueryViewBuilder(
                dataset_id=DATAFLOW_METRICS_MATERIALIZED_DATASET,
                view_id=f"most_recent_single_day_{metric_table_view_name}",
                view_query_template=MOST_RECENT_SINGLE_DAY_JOBS_TEMPLATE,
                description=description,
                metric_date_column=METRIC_DATE_COLUMNS_BY_SINGLE_DAY_METRICS_TABLES_TO_MATERIALIZE[
                    metric_name
                ],
                metric_table_view_name=metric_table_view_name,
                materialized_metrics_dataset=DATAFLOW_METRICS_MATERIALIZED_DATASET,
                should_materialize=True,
            )
        )
    return view_builders


def generate_most_recent_single_day_metrics_view_builders(
    metric_tables: List[str],
) -> List[SimpleBigQueryViewBuilder]:
    return [
        view_builder
        for metric_table in metric_tables
        for view_builder in _make_most_recent_single_day_metric_view_builder(
            metric_table
        )
    ]


MOST_RECENT_SINGLE_DAY_METRICS_VIEW_BUILDERS: List[
    SimpleBigQueryViewBuilder
] = generate_most_recent_single_day_metrics_view_builders(
    list(METRIC_DATE_COLUMNS_BY_SINGLE_DAY_METRICS_TABLES_TO_MATERIALIZE.keys())
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        for builder in MOST_RECENT_SINGLE_DAY_METRICS_VIEW_BUILDERS:
            builder.build_and_print()
