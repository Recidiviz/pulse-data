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
"""A view revealing when the purpose_for_incarceration value on an admission metric
does not match the purpose_for_incarceration value on a population metric for the same
person_id and metric date.

Mismatches here indicate that IP normalization is not handling zero-day incarceration
periods with different PFI values than the PFI on another period that starts on the same
day but extends to at least the next day.
"""

from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.bq_utils import nonnull_end_date_exclusive_clause
from recidiviz.calculator.query.state import dataset_config as state_dataset_config
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override
from recidiviz.validation.views import dataset_config

ADMISSION_PFI_POP_PFI_MISMATCH_VIEW_NAME = "admission_pfi_pop_pfi_mismatch"

ADMISSION_PFI_POP_PFI_MISMATCH_DESCRIPTION = """
A view revealing when the purpose_for_incarceration value on an admission metric
does not match the purpose_for_incarceration value on a population metric for the same
person_id and metric date.

Mismatches here indicate that IP normalization is not handling zero-day incarceration 
periods with different PFI values than the PFI on another period that starts on the same
day but extends to at least the next day.
"""

ADMISSION_PFI_POP_PFI_MISMATCH_QUERY_TEMPLATE = f"""
SELECT state_code, state_code as region_code, person_id, metric_date, included_in_state_population, admission_pfi, population_pfi
FROM
(
    SELECT 
        admission_metrics.state_code,
        admission_metrics.person_id,
        metric_date,
        admission_metrics.included_in_state_population,
        admission_pfi,
        population_pfi
    FROM (
        SELECT 
            state_code,
            person_id,
            admission_date as metric_date, 
            included_in_state_population,
            specialized_purpose_for_incarceration as admission_pfi
        FROM `{{project_id}}.{{materialized_metrics_dataset}}.most_recent_incarceration_admission_metrics_included_in_state_population_materialized`
    ) admission_metrics
    LEFT JOIN (
        SELECT state_code, person_id, start_date_inclusive, end_date_exclusive, included_in_state_population, purpose_for_incarceration as population_pfi
        FROM `{{project_id}}.{{materialized_metrics_dataset}}.most_recent_incarceration_population_span_metrics_materialized`
    ) pop_metrics
    ON 
        admission_metrics.state_code = pop_metrics.state_code
        AND admission_metrics.person_id = pop_metrics.person_id
        AND admission_metrics.included_in_state_population = pop_metrics.included_in_state_population        
        AND metric_date 
            BETWEEN pop_metrics.start_date_inclusive 
                AND {nonnull_end_date_exclusive_clause("pop_metrics.end_date_exclusive")}

    UNION ALL

    SELECT 
        admission_metrics.state_code,
        admission_metrics.person_id,
        metric_date,
        admission_metrics.included_in_state_population,
        admission_pfi,
        population_pfi
    FROM (
        SELECT 
            state_code,
            person_id,
            admission_date as metric_date, 
            included_in_state_population,
            specialized_purpose_for_incarceration as admission_pfi
        FROM `{{project_id}}.{{materialized_metrics_dataset}}.most_recent_incarceration_admission_metrics_not_included_in_state_population_materialized`
    ) admission_metrics
    LEFT JOIN (
        SELECT state_code, person_id, start_date_inclusive, end_date_exclusive, included_in_state_population, purpose_for_incarceration as population_pfi
        FROM `{{project_id}}.{{materialized_metrics_dataset}}.most_recent_incarceration_population_span_metrics_materialized`
    ) pop_metrics
    ON 
        admission_metrics.state_code = pop_metrics.state_code
        AND admission_metrics.person_id = pop_metrics.person_id
        AND admission_metrics.included_in_state_population = pop_metrics.included_in_state_population
        AND metric_date 
            BETWEEN pop_metrics.start_date_inclusive 
                AND {nonnull_end_date_exclusive_clause("pop_metrics.end_date_exclusive")}
)
WHERE admission_pfi != population_pfi
"""

ADMISSION_PFI_POP_PFI_MISMATCH_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=dataset_config.VIEWS_DATASET,
    view_id=ADMISSION_PFI_POP_PFI_MISMATCH_VIEW_NAME,
    view_query_template=ADMISSION_PFI_POP_PFI_MISMATCH_QUERY_TEMPLATE,
    description=ADMISSION_PFI_POP_PFI_MISMATCH_DESCRIPTION,
    materialized_metrics_dataset=state_dataset_config.DATAFLOW_METRICS_MATERIALIZED_DATASET,
    should_materialize=True,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        ADMISSION_PFI_POP_PFI_MISMATCH_VIEW_BUILDER.build_and_print()
