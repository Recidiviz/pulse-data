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
"""A view that returns the last N months of entity counts aggregated by date columns specified
in ENTITIES_WITH_EXPECTED_STABLE_COUNTS_OVER_TIME"""
from datetime import date
from typing import Dict, List, Tuple

from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.common.constants.states import StateCode
from recidiviz.ingest.views.dataset_config import NORMALIZED_STATE_DATASET
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override
from recidiviz.utils.string import StrictStringFormatter
from recidiviz.validation.views import dataset_config
from recidiviz.validation.views.state.stable_counts.stable_counts import (
    ENTITIES_WITH_EXPECTED_STABLE_COUNTS_OVER_TIME,
    DateCol,
)

VALIDATION_WINDOW_IN_MONTHS = 12

LAST_N_MONTHS_ENTITY_TEMPLATE = """
WITH {col}_{entity}_counts AS (
    SELECT 
        DATE_TRUNC({col}, MONTH) AS month,
        state_code as region_code,
        COUNT(*) AS {col}_count,
    FROM `{{project_id}}.{{normalized_state_dataset}}.{entity}`
    WHERE DATE_TRUNC({col}, MONTH) >= DATE_SUB(DATE_TRUNC(CURRENT_DATE('US/Eastern'), MONTH), INTERVAL 12 MONTH)
    GROUP BY 1,2
),
last_12_month_dates AS (
    SELECT DATE_TRUNC(date, MONTH) AS month,
    FROM UNNEST(
    GENERATE_DATE_ARRAY(
        DATE_SUB(CURRENT_DATE('US/Eastern'), INTERVAL 12 MONTH), 
        CURRENT_DATE('US/Eastern'), 
        INTERVAL 1 MONTH)
        ) AS date
),
cross_joined AS (
    SELECT * 
    FROM last_12_month_dates
    CROSS JOIN (
        SELECT distinct region_code 
        FROM {col}_{entity}_counts
        )
),
all_months_filled_zeroes AS (
    SELECT 
        cross_joined.month AS month, 
        cross_joined.region_code AS region_code, 
        COALESCE({col}_count, 0) AS {col}_count
    FROM {col}_{entity}_counts 
    FULL JOIN cross_joined on {col}_{entity}_counts.month = cross_joined.month 
        and {col}_{entity}_counts.region_code = cross_joined.region_code 
    )
    
SELECT * FROM (
    SELECT * FROM (
        SELECT 
            month,
            region_code, 
            {col}_count, 
            LAG({col}_count) OVER (PARTITION BY region_code ORDER BY month) AS previous_month_{col}_count, 
        FROM all_months_filled_zeroes
        -- select where the month < first of the current month
        -- A 1 month delay (DATE_ADD(month, INTERVAL 1 MONTH) is added to ensure that we don't start 
        -- running these validations on the current month since they will fail until the month ends 
        -- and we've received all necessary data to calculate month over month changes
        WHERE DATE_ADD(month, INTERVAL 1 MONTH) <= DATE_SUB(CURRENT_DATE('US/Eastern'), INTERVAL 7 DAY)
    )
    -- Limit to most recent {validation_window_months} months
    -- We apply this filter here after the LAG caluclation has been done so we do not
    -- compare the first month of the validation to previous month that has been filtered out 
    WHERE month > DATE_SUB(DATE_TRUNC(CURRENT_DATE('US/Eastern'), MONTH), INTERVAL 12 MONTH)
)
WHERE {exemptions}
"""


def exemptions_string_builder(exemptions: Dict[StateCode, List[date]]) -> str:
    if not exemptions:
        return "TRUE"
    region_exemption_clauses = []
    for region_code, exemption_dates in exemptions.items():
        exemption_months_str = ", ".join([f'"{str(d)}"' for d in exemption_dates])
        region_exemption_clauses.append(
            f'NOT (region_code = "{region_code.name}" AND month IN ({exemption_months_str}))'
        )
    return "\n      AND ".join(region_exemption_clauses)


def validation_query_for_stable_counts(
    field_name: str, entity_name: str, exemptions: Dict[StateCode, List[date]]
) -> str:
    return StrictStringFormatter().format(
        LAST_N_MONTHS_ENTITY_TEMPLATE,
        col=field_name,
        entity=entity_name,
        validation_window_months=VALIDATION_WINDOW_IN_MONTHS,
        exemptions=exemptions_string_builder(exemptions),
    )


def view_builder_for_entity_and_date_col(
    entity: str, date_col: DateCol
) -> SimpleBigQueryViewBuilder:
    return SimpleBigQueryViewBuilder(
        dataset_id=dataset_config.VIEWS_DATASET,
        view_id=f"{entity}_by_{date_col.date_column_name}_stable_counts",
        description=f"Check for stable counts of {entity} over time, aggregated by {date_col.date_column_name}",
        view_query_template=validation_query_for_stable_counts(
            field_name=date_col.date_column_name,
            entity_name=entity,
            exemptions=date_col.exemptions,
        ),
        normalized_state_dataset=NORMALIZED_STATE_DATASET,
        should_materialize=True,
    )


# Use this to build configured validations
# this dictionary is (entity, date_col) -> to the big query view builder needed
# this way we can construct validations too from it
VALIDATION_VIEW_BUILDERS_BY_ENTITY_AND_DATE_COL: Dict[
    Tuple[str, str], SimpleBigQueryViewBuilder
] = {
    (entity, date_col.date_column_name): view_builder_for_entity_and_date_col(
        entity, date_col
    )
    for entity, stable_counts_config in ENTITIES_WITH_EXPECTED_STABLE_COUNTS_OVER_TIME.items()
    for date_col in stable_counts_config.date_columns_to_check
}

# This is the list that will get collected
VALIDATION_VIEW_BUILDERS: List[SimpleBigQueryViewBuilder] = [
    *VALIDATION_VIEW_BUILDERS_BY_ENTITY_AND_DATE_COL.values()
]


if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        for view_builder in VALIDATION_VIEW_BUILDERS:
            view_builder.build_and_print()
