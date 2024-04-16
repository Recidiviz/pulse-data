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
    DATAFLOW_METRICS_MATERIALIZED_DATASET,
)
from recidiviz.pipelines.dataflow_config import (
    DATAFLOW_METRICS_TO_TABLES,
    DATAFLOW_TABLES_TO_METRICS,
)
from recidiviz.pipelines.utils.identifier_models import SupervisionLocationMixin
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
            FROM `{project_id}.dataflow_metrics.{metric_table}`
        )
    ),
    filtered_rows AS (
        SELECT
            metric.* {except_clause},
            -- TODO(#28413): Person demographics passes *UNKNOWN through, and replaces NULL with PRESENT_WITHOUT_INFO, while the metric pipelines return NULL in these cases.
            IF(demographics.prioritized_race_or_ethnicity IN ('INTERNAL_UNKNOWN', 'EXTERNAL_UNKNOWN', 'PRESENT_WITHOUT_INFO'),
               NULL,
               demographics.prioritized_race_or_ethnicity) as prioritized_race_or_ethnicity,
            {inner_extra_columns_clause}
        FROM `{project_id}.dataflow_metrics.{metric_table}` metric
        JOIN (
            SELECT state_code, metric_type, job_id
            FROM job_recency
            WHERE recency_rank = 1
        )
        USING (state_code, metric_type, job_id)
        LEFT JOIN `{project_id}.sessions.person_demographics_materialized` demographics
        USING (state_code, person_id)
        {extra_join_clause}
        {metrics_filter}
    )
    SELECT 
        *,
        {outer_extra_columns_clause}
    FROM filtered_rows
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

    except_clause = ""
    outer_extra_columns_clause = ""
    inner_extra_columns_clause = ""
    extra_join_clause = ""

    metric_class = DATAFLOW_TABLES_TO_METRICS[metric_name]
    if issubclass(metric_class, SupervisionLocationMixin):
        except_clause = "EXCEPT(level_2_supervision_location_external_id)"
        outer_extra_columns_clause = """
        -- TODO(#4709): This field is deprecated and downstream uses should be deleted 
        -- entirely.
        CASE
            WHEN state_code = 'US_ND' THEN level_1_supervision_location_external_id
            ELSE COALESCE(level_2_supervision_location_external_id, level_1_supervision_location_external_id)
        END AS supervising_district_external_id,
        """

        extra_join_clause = """
                LEFT JOIN  (
                    SELECT state_code, level_1_supervision_location_external_id, level_2_supervision_location_external_id
                    FROM `{project_id}.reference_views.supervision_location_ids_to_names_materialized`
                ) location_ids
                USING (state_code, level_1_supervision_location_external_id)
        """

        inner_extra_columns_clause = """
        CASE
            -- TODO(#19343): It's relatively common for someone in PA to be on supervision
            --  and have a district assigned but no office assigned. For these periods
            --  we can't use the office (level 1 location) to derive the district, but
            --  we still want to be able to track this person in district-level metrics.
            --  For this reason we stuff both office and district information in the 
            --  `supervision_site` field on the SP, then parse out the district info 
            --  (level 2 location) in a state-specific pipeline delegate. In an ideal
            --  world the `supervision_site` field would be hydrated with. In a world
            --  where we refactor how supervision locations are represented entirely,
            --  the locations we reference in ingested data would be a location_id from
            --  the location_metadata view. In PA that location_id is the Org_cd which
            --  does not exist when there is no office. We could generate fake
            --  location_ids for the "district but no office scenario" and use those.            
            WHEN state_code = 'US_PA' THEN metric.level_2_supervision_location_external_id
            -- TODO(#28755): Correct ND data for supervision offices 17 and 19 in 
            -- location_metadata or confirm that the data in that view is correct and 
            -- we can remove this custom ND logic that makes the output of these views
            -- backwards compatible with old metric pipeline output.
            WHEN state_code = 'US_ND' THEN 
                CASE 
                    WHEN metric.level_1_supervision_location_external_id = '17' THEN 'Central Office'
                    WHEN metric.level_1_supervision_location_external_id = '19' THEN 'Region 1'
                    ELSE location_ids.level_2_supervision_location_external_id
                END
            ELSE location_ids.level_2_supervision_location_external_id
        END AS level_2_supervision_location_external_id,
        """

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
                metric_table=metric_name,
                should_materialize=True,
                clustering_fields=["state_code"],
                metrics_filter="WHERE included_in_state_population = TRUE",
                except_clause=except_clause,
                inner_extra_columns_clause=inner_extra_columns_clause,
                outer_extra_columns_clause=outer_extra_columns_clause,
                extra_join_clause=extra_join_clause,
            ),
            SimpleBigQueryViewBuilder(
                dataset_id=DATAFLOW_METRICS_MATERIALIZED_DATASET,
                view_id=f"{view_id}_not_included_in_state_population",
                view_query_template=MOST_RECENT_JOBS_TEMPLATE,
                description=description
                + ", for output that is not included in the state's population.",
                metric_table=metric_name,
                should_materialize=True,
                clustering_fields=["state_code"],
                metrics_filter="WHERE included_in_state_population = FALSE",
                except_clause=except_clause,
                inner_extra_columns_clause=inner_extra_columns_clause,
                outer_extra_columns_clause=outer_extra_columns_clause,
                extra_join_clause=extra_join_clause,
            ),
        ]
    return [
        SimpleBigQueryViewBuilder(
            dataset_id=DATAFLOW_METRICS_MATERIALIZED_DATASET,
            view_id=view_id,
            view_query_template=MOST_RECENT_JOBS_TEMPLATE,
            description=description,
            metric_table=metric_name,
            should_materialize=True,
            clustering_fields=["state_code"],
            metrics_filter="",
            except_clause=except_clause,
            inner_extra_columns_clause=inner_extra_columns_clause,
            outer_extra_columns_clause=outer_extra_columns_clause,
            extra_join_clause=extra_join_clause,
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
