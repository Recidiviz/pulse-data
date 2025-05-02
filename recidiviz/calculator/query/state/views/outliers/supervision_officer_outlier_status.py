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
"""The outlier status of an officer (or staff member with a caseload) relative to MetricBenchmarks"""
from typing import List

from recidiviz.big_query.selected_columns_big_query_view import (
    SelectedColumnsBigQueryViewBuilder,
)
from recidiviz.calculator.query.bq_utils import list_to_query_string
from recidiviz.calculator.query.state import dataset_config
from recidiviz.calculator.query.state.views.outliers.outliers_enabled_states import (
    get_outliers_enabled_states_for_bigquery,
)
from recidiviz.outliers.outliers_configs import (
    METRICS_BY_OUTCOME_TYPE,
    get_outliers_backend_config,
)
from recidiviz.outliers.types import MetricOutcome
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

_VIEW_NAME = "supervision_officer_outlier_status"

_DESCRIPTION = """The outlier status of an officer (or staff member with a caseload) relative to MetricBenchmarks"""


def get_metric_id_by_outcome(metric_outcome: MetricOutcome) -> List[str]:
    return sorted(metric.name for metric in METRICS_BY_OUTCOME_TYPE[metric_outcome])


def get_primary_caseload_category_types_by_state() -> str:
    return "\nUNION ALL\n".join(
        [
            f'SELECT "{state_code}" AS state_code, "{get_outliers_backend_config(state_code).primary_category_type.value}" AS primary_category_type'
            for state_code in get_outliers_enabled_states_for_bigquery()
        ]
    )


def get_feature_variants_by_metric() -> str:
    query_parts = []
    for state_code in get_outliers_enabled_states_for_bigquery():
        config = get_outliers_backend_config(state_code)
        for metric in config.metrics:
            query_parts.append(
                f"""
SELECT
  "{state_code}" AS state_code,
  "{metric.name}" AS metric_id,
  {f'"{metric.feature_variant}"' if metric.feature_variant else "NULL"} AS feature_variant,
  {f'"{metric.inverse_feature_variant}"' if metric.inverse_feature_variant else "NULL"} AS inverse_feature_variant,
"""
            )
    return "\nUNION ALL\n".join(query_parts)


_QUERY_TEMPLATE = f"""
WITH
primary_caseload_category_types_by_state AS (
  {get_primary_caseload_category_types_by_state()}
)
, feature_variants_by_metric AS (
  {get_feature_variants_by_metric()}
)
, officer_metrics_with_caseload_type AS (
  SELECT 
    m.state_code,
    m.officer_id,
    m.metric_id,
    m.period,
    m.end_date,
    m.metric_value AS metric_rate,
    category_type,
    caseload_category,
    category_type = c.primary_category_type AS is_surfaced_category_type,
    -- TODO(#31634): Remove caseload_type
    caseload_category AS caseload_type,
    feature_variant,
    inverse_feature_variant
  FROM `{{project_id}}.{{outliers_views_dataset}}.supervision_officer_metrics_materialized` m
  INNER JOIN primary_caseload_category_types_by_state c
    USING(state_code)
  INNER JOIN feature_variants_by_metric
    USING(state_code, metric_id)
  WHERE
    m.value_type = 'RATE'
)
, outlier_status_statewide AS (
  SELECT
    m.*,
    b.target,
    b.threshold,
    CASE 
      -- Adverse metrics
      WHEN 
        m.metric_id IN ({list_to_query_string(get_metric_id_by_outcome(MetricOutcome.ADVERSE), quoted=True)}) 
        THEN CASE
            WHEN m.metric_rate < b.target THEN 'MET'
            WHEN m.metric_rate - b.target <= b.threshold THEN 'NEAR'
            ELSE 'FAR'
        END
      -- Favorable metrics
      WHEN 
        m.metric_id IN ({list_to_query_string(get_metric_id_by_outcome(MetricOutcome.FAVORABLE), quoted=True)}) 
        THEN CASE
            WHEN b.target - b.threshold <= 0 AND m.metric_rate = 0.0 THEN 'FAR'  -- Zero rate target status strategy 
            WHEN m.metric_rate >= b.target THEN 'MET'
            WHEN b.target - m.metric_rate <= b.threshold THEN 'NEAR'
            ELSE 'FAR'
        END
    END AS status,
    b.top_x_pct,
    b.top_x_pct_percentile_value,
    CASE 
      WHEN
        b.top_x_pct IS NOT NULL
        THEN CASE
          -- If the metric is an ADVERSE metric, highlighting the top 10% means highlighting officers with rates below the 10th percentile value
          WHEN m.metric_id IN ({list_to_query_string(get_metric_id_by_outcome(MetricOutcome.ADVERSE), quoted=True)}) THEN m.metric_rate < b.top_x_pct_percentile_value
          -- If the metric is a FAVORABLE metric, highlighting the top 10% means highlighting officers with rates above the 90th percentile value
          WHEN m.metric_id IN ({list_to_query_string(get_metric_id_by_outcome(MetricOutcome.FAVORABLE), quoted=True)}) THEN m.metric_rate > b.top_x_pct_percentile_value
        END
      ELSE NULL 
    END AS is_top_x_pct
  FROM officer_metrics_with_caseload_type m
  INNER JOIN `{{project_id}}.{{outliers_views_dataset}}.metric_benchmarks_materialized` b 
    USING (state_code, end_date, period, metric_id, category_type, caseload_type)
)

SELECT 
  {{columns}}
FROM outlier_status_statewide
"""

SUPERVISION_OFFICER_OUTLIER_STATUS_VIEW_BUILDER = SelectedColumnsBigQueryViewBuilder(
    dataset_id=dataset_config.OUTLIERS_VIEWS_DATASET,
    view_id=_VIEW_NAME,
    view_query_template=_QUERY_TEMPLATE,
    description=_DESCRIPTION,
    should_materialize=True,
    columns=[
        "state_code",
        "officer_id",
        "metric_id",
        "period",
        "end_date",
        "metric_rate",
        "category_type",
        "caseload_type",
        "caseload_category",
        "is_surfaced_category_type",
        "target",
        "threshold",
        "status",
        "top_x_pct",
        "top_x_pct_percentile_value",
        "is_top_x_pct",
        "feature_variant",
        "inverse_feature_variant",
    ],
    outliers_views_dataset=dataset_config.OUTLIERS_VIEWS_DATASET,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        SUPERVISION_OFFICER_OUTLIER_STATUS_VIEW_BUILDER.build_and_print()
