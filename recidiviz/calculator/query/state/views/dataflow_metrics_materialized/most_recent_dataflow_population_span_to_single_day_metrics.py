# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2022 Recidiviz, Inc.
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
"""Generator to transform the most recent population span metrics into day-by-day
metrics.

Single-day metrics are only generated for states that have products enabled that use
these single-day metrics downstream.
"""
from typing import Dict, List

from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.bq_utils import list_to_query_string
from recidiviz.calculator.query.state.dataset_config import (
    DATAFLOW_METRICS_MATERIALIZED_DATASET,
)
from recidiviz.metrics.export.products.product_configs import (
    PRODUCTS_CONFIG_PATH,
    ProductConfigs,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

POPULATION_SPAN_METRIC_TO_DAY_BY_DAY_METRIC: Dict[str, str] = {
    "supervision_population_span_metrics": "date_of_supervision",
    "incarceration_population_span_metrics": "date_of_stay",
}

MOST_RECENT_POPULATION_SPAN_TO_SINGLE_DAY_METRICS_TEMPLATE: str = """
    SELECT * EXCEPT (start_date_inclusive, end_date_exclusive),
    EXTRACT(YEAR FROM {metric_date_column}) AS year,
    EXTRACT(MONTH FROM {metric_date_column}) AS month
    FROM (
        SELECT *
        FROM
        `{project_id}.{materialized_metrics_dataset}.most_recent_{population_span_metrics_table}_materialized`
        -- Filter to states with shipped products that use these single-day metrics.
        WHERE state_code IN ({state_codes_list})
    ),
    UNNEST(GENERATE_DATE_ARRAY(start_date_inclusive,
    -- End date *inclusive* or today
    DATE_SUB(
        -- End date exclusive or tomorrow
        IFNULL(end_date_exclusive, 
            DATE_ADD(CURRENT_DATE('US/Eastern'), INTERVAL 1 DAY)
        ), 
    INTERVAL 1 DAY), INTERVAL 1 DAY)) AS {metric_date_column}
"""


def _make_most_recent_population_span_to_single_day_metric_view_builder(
    metric_name: str,
) -> SimpleBigQueryViewBuilder:
    product_configs = ProductConfigs.from_file(PRODUCTS_CONFIG_PATH)
    public_dashboard_enabled_states = (
        product_configs.get_states_with_export_enabled_in_any_env("PUBLIC_DASHBOARD")
    )
    pathways_enabled_states = product_configs.get_states_with_export_enabled_in_any_env(
        "PATHWAYS_EVENT_LEVEL"
    ) | product_configs.get_states_with_export_enabled_in_any_env("PATHWAYS")

    single_day_metric_states_by_metric_name = {
        "supervision_population_span_metrics": sorted(public_dashboard_enabled_states),
        "incarceration_population_span_metrics": sorted(
            public_dashboard_enabled_states | pathways_enabled_states
        ),
    }

    description = (
        f"{metric_name} output converting to the most recent single day metrics"
    )
    metric_table_view_name = metric_name.rstrip("_metrics")
    return SimpleBigQueryViewBuilder(
        dataset_id=DATAFLOW_METRICS_MATERIALIZED_DATASET,
        view_id=f"most_recent_{metric_table_view_name}_to_single_day_metrics",
        view_query_template=MOST_RECENT_POPULATION_SPAN_TO_SINGLE_DAY_METRICS_TEMPLATE,
        description=description,
        metric_date_column=POPULATION_SPAN_METRIC_TO_DAY_BY_DAY_METRIC[metric_name],
        state_codes_list=list_to_query_string(
            single_day_metric_states_by_metric_name[metric_name], quoted=True
        ),
        population_span_metrics_table=metric_name,
        materialized_metrics_dataset=DATAFLOW_METRICS_MATERIALIZED_DATASET,
        should_materialize=True,
        clustering_fields=["state_code"],
    )


def generate_most_recent_population_span_to_single_day_metric_view_builders(
    metric_tables: List[str],
) -> List[SimpleBigQueryViewBuilder]:
    return [
        _make_most_recent_population_span_to_single_day_metric_view_builder(
            metric_table
        )
        for metric_table in metric_tables
    ]


MOST_RECENT_POPULATION_SPAN_TO_SINGLE_DAY_METRICS_VIEW_BUILDERS: List[
    SimpleBigQueryViewBuilder
] = generate_most_recent_population_span_to_single_day_metric_view_builders(
    list(POPULATION_SPAN_METRIC_TO_DAY_BY_DAY_METRIC.keys())
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        for builder in MOST_RECENT_POPULATION_SPAN_TO_SINGLE_DAY_METRICS_VIEW_BUILDERS:
            print(f"******* {builder.address.to_str()} *********")
            builder.build_and_print()
