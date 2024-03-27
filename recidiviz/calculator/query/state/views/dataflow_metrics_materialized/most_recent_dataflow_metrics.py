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
"""Generator to grab all dataflow_metrics views and keep only the rows corresponding to
a most recent [job id, state, metric_type] combo."""
from typing import List

from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.state.dataset_config import (
    DATAFLOW_METRICS_DATASET,
    DATAFLOW_METRICS_MATERIALIZED_DATASET,
    SESSIONS_DATASET,
)
from recidiviz.pipelines.dataflow_config import DATAFLOW_METRICS_TO_TABLES
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

METRICS_VIEWS_TO_MATERIALIZE: List[str] = list(DATAFLOW_METRICS_TO_TABLES.values())

VIEWS_TO_SPLIT_ON_INCLUDED_IN_STATE_POPULATION: List[str] = [
    view
    for view in DATAFLOW_METRICS_TO_TABLES.values()
    if view.startswith("incarceration") and not "population_span" in view
]

MOST_RECENT_JOBS_TEMPLATE: str = """
    WITH job_recency as (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY state_code, metric_type ORDER BY job_id DESC) AS recency_rank,
        FROM (
            -- TODO(#28414): This includes old metrics that we stopped calculating. Those
            -- should be filtered out.
            SELECT DISTINCT state_code, metric_type, job_id
            FROM `{project_id}.{metrics_dataset}.{metric_table}`
        )
    )    
    SELECT
        metric.*,
        -- TODO(#28413): Person demographics passes *UNKNOWN through, and replaces NULL with PRESENT_WITHOUT_INFO, while the metric pipelines return NULL in these cases.
        IF(demographics.prioritized_race_or_ethnicity IN ('INTERNAL_UNKNOWN', 'EXTERNAL_UNKNOWN', 'PRESENT_WITHOUT_INFO'),
           NULL,
           demographics.prioritized_race_or_ethnicity) as prioritized_race_or_ethnicity
    FROM `{project_id}.{metrics_dataset}.{metric_table}` metric
    JOIN (
        SELECT state_code, metric_type, job_id
        FROM job_recency
        WHERE recency_rank = 1
    )
    USING (state_code, metric_type, job_id)
    LEFT JOIN `{project_id}.{sessions_dataset}.person_demographics_materialized` demographics
    USING (state_code, person_id)
    {metrics_filter}
    """


def generate_metric_view_names(metric_name: str) -> List[str]:
    if metric_name in VIEWS_TO_SPLIT_ON_INCLUDED_IN_STATE_POPULATION:
        return [
            f"{metric_name}_included_in_state_population",
            f"{metric_name}_not_included_in_state_population",
        ]
    return [metric_name]


def make_most_recent_metric_view_builders(
    metric_name: str,
    split_on_included_in_population: bool = True,
) -> List[SimpleBigQueryViewBuilder]:
    """Returns view builders that determine the most recent metrics for each metric name.

    If split_on_included_in_population, will create two views for metrics that can be split on included_in_state_population.
    """
    description = f"{metric_name} for the most recent job run"
    view_id = f"most_recent_{metric_name}"

    if (
        metric_name in VIEWS_TO_SPLIT_ON_INCLUDED_IN_STATE_POPULATION
        and split_on_included_in_population
    ):
        return [
            SimpleBigQueryViewBuilder(
                dataset_id=DATAFLOW_METRICS_MATERIALIZED_DATASET,
                view_id=f"{view_id}_included_in_state_population",
                view_query_template=MOST_RECENT_JOBS_TEMPLATE,
                description=description
                + ", for output that is included in the state's population.",
                metrics_dataset=DATAFLOW_METRICS_DATASET,
                metric_table=metric_name,
                sessions_dataset=SESSIONS_DATASET,
                should_materialize=True,
                metrics_filter="WHERE included_in_state_population = TRUE",
            ),
            SimpleBigQueryViewBuilder(
                dataset_id=DATAFLOW_METRICS_MATERIALIZED_DATASET,
                view_id=f"{view_id}_not_included_in_state_population",
                view_query_template=MOST_RECENT_JOBS_TEMPLATE,
                description=description
                + ", for output that is not included in the state's population.",
                metrics_dataset=DATAFLOW_METRICS_DATASET,
                metric_table=metric_name,
                sessions_dataset=SESSIONS_DATASET,
                should_materialize=True,
                metrics_filter="WHERE included_in_state_population = FALSE",
            ),
        ]
    return [
        SimpleBigQueryViewBuilder(
            dataset_id=DATAFLOW_METRICS_MATERIALIZED_DATASET,
            view_id=view_id,
            view_query_template=MOST_RECENT_JOBS_TEMPLATE,
            description=description,
            metrics_dataset=DATAFLOW_METRICS_DATASET,
            metric_table=metric_name,
            sessions_dataset=SESSIONS_DATASET,
            should_materialize=True,
            metrics_filter="",
        )
    ]


def generate_most_recent_metrics_view_builders(
    metric_tables: List[str],
) -> List[SimpleBigQueryViewBuilder]:
    return [
        view_builder
        for metric_table in metric_tables
        for view_builder in make_most_recent_metric_view_builders(metric_table)
    ]


MOST_RECENT_METRICS_VIEW_BUILDERS: List[
    SimpleBigQueryViewBuilder
] = generate_most_recent_metrics_view_builders(METRICS_VIEWS_TO_MATERIALIZE)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        for builder in MOST_RECENT_METRICS_VIEW_BUILDERS:
            builder.build_and_print()
