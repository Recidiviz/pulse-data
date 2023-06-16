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
"""Aggregated metrics at the officer-level for supervision-related metrics"""
from recidiviz.aggregated_metrics.dataset_config import AGGREGATED_METRICS_DATASET_ID
from recidiviz.big_query.selected_columns_big_query_view import (
    SelectedColumnsBigQueryViewBuilder,
)
from recidiviz.calculator.query.state import dataset_config
from recidiviz.calculator.query.state.views.analyst_data.models.metric_unit_of_analysis_type import (
    METRIC_UNITS_OF_ANALYSIS_BY_TYPE,
    MetricUnitOfAnalysisType,
)
from recidiviz.calculator.query.state.views.outliers.supervision_metrics_helpers import (
    supervision_metric_query_template,
)
from recidiviz.calculator.query.state.views.outliers.utils import (
    format_state_specific_officer_aggregated_metric_filters,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

SUPERVISION_OFFICER_METRICS_VIEW_NAME = "supervision_officer_metrics"

SUPERVISION_OFFICER_METRICS_DESCRIPTION = (
    """Aggregated metrics at the officer-level for supervision-related metrics"""
)


SUPERVISION_OFFICER_METRICS_QUERY_TEMPLATE = f"""
WITH 
filtered_supervision_officer_aggregated_metrics AS (
  SELECT *
  FROM `{{project_id}}.{{aggregated_metrics_dataset}}.supervision_officer_aggregated_metrics_materialized`
  WHERE 
    {format_state_specific_officer_aggregated_metric_filters()}
),
supervision_officer_metrics AS (
    {supervision_metric_query_template(unit_of_analysis=METRIC_UNITS_OF_ANALYSIS_BY_TYPE[MetricUnitOfAnalysisType.SUPERVISION_OFFICER], cte_source="filtered_supervision_officer_aggregated_metrics")}
)

SELECT 
    {{columns}}
FROM supervision_officer_metrics
INNER JOIN `{{project_id}}.{{outliers_views_dataset}}.supervision_officers_materialized` officers
  ON officers.external_id = supervision_officer_metrics.officer_id and officers.state_code = supervision_officer_metrics.state_code
"""

SUPERVISION_OFFICER_METRICS_VIEW_BUILDER = SelectedColumnsBigQueryViewBuilder(
    dataset_id=dataset_config.OUTLIERS_VIEWS_DATASET,
    view_id=SUPERVISION_OFFICER_METRICS_VIEW_NAME,
    view_query_template=SUPERVISION_OFFICER_METRICS_QUERY_TEMPLATE,
    description=SUPERVISION_OFFICER_METRICS_DESCRIPTION,
    aggregated_metrics_dataset=AGGREGATED_METRICS_DATASET_ID,
    outliers_views_dataset=dataset_config.OUTLIERS_VIEWS_DATASET,
    should_materialize=True,
    columns=[
        "supervision_officer_metrics.state_code",
        "metric_id",
        "metric_value",
        "period",
        "end_date",
        "officer_id",
    ],
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        SUPERVISION_OFFICER_METRICS_VIEW_BUILDER.build_and_print()
