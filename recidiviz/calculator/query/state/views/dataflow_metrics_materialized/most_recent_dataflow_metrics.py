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
"""Generator to grab all dataflow_metrics views and join them to
most_recent_job_id_by_metric_and_state_code_materialized"""
from typing import List, Dict

from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.dataflow_config import (
    DATAFLOW_METRICS_TO_TABLES,
)
from recidiviz.calculator.query.state.dataset_config import (
    DATAFLOW_METRICS_MATERIALIZED_DATASET,
    DATAFLOW_METRICS_DATASET,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

DEFAULT_JOIN_INDICES: str = "job_id, state_code, year, month, metric_type"

METRIC_TABLES_JOIN_OVERRIDES: Dict[str, str] = {
    "recidivism_rate_metrics": "job_id, state_code, metric_type",
}

METRICS_VIEWS_TO_MATERIALIZE: List[str] = list(DATAFLOW_METRICS_TO_TABLES.values())

MOST_RECENT_JOBS_TEMPLATE: str = """
    /*{description}*/
    SELECT *
    FROM `{project_id}.{metrics_dataset}.{metric_view}`
    JOIN
        `{project_id}.{materialized_metrics_dataset}.most_recent_job_id_by_metric_and_state_code_materialized`
        USING ({join_indices})
    """


def _make_most_recent_metric_view_builder(
    metric_name: str,
) -> SimpleBigQueryViewBuilder:
    description = f"{metric_name} for the most recent job run"
    view_id = f"most_recent_{metric_name}"
    join_indices = METRIC_TABLES_JOIN_OVERRIDES.get(metric_name, DEFAULT_JOIN_INDICES)
    return SimpleBigQueryViewBuilder(
        dataset_id=DATAFLOW_METRICS_MATERIALIZED_DATASET,
        view_id=view_id,
        view_query_template=MOST_RECENT_JOBS_TEMPLATE,
        description=description,
        join_indices=join_indices,
        metrics_dataset=DATAFLOW_METRICS_DATASET,
        metric_view=metric_name,
        materialized_metrics_dataset=DATAFLOW_METRICS_MATERIALIZED_DATASET,
        should_materialize=True,
    )


def generate_most_recent_metrics_view_builders(
    metric_views: List[str],
) -> List[SimpleBigQueryViewBuilder]:
    return [
        _make_most_recent_metric_view_builder(metric_view)
        for metric_view in metric_views
    ]


MOST_RECENT_METRICS_VIEW_BUILDERS: List[
    SimpleBigQueryViewBuilder
] = generate_most_recent_metrics_view_builders(METRICS_VIEWS_TO_MATERIALIZE)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        for builder in MOST_RECENT_METRICS_VIEW_BUILDERS:
            builder.build_and_print()
