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
"""Creates a view that calculates periods of time during which a person on Parole in
California had a consistent sustainable housing status."""

from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.sessions_query_fragments import aggregate_adjacent_spans
from recidiviz.calculator.query.state.dataset_config import (
    ANALYST_VIEWS_DATASET,
    US_CA_RAW_DATASET,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

US_CA_SUSTAINABLE_HOUSING_STATUS_PERIODS_VIEW_NAME = (
    "us_ca_sustainable_housing_status_periods"
)

US_CA_SUSTAINABLE_HOUSING_STATUS_PERIODS_VIEW_DESCRIPTION = "Creates a view that calculates periods of time during which a person on Parole in California had a consistent sustainable housing status."

US_CA_SUSTAINABLE_HOUSING_STATUS_PERIODS_QUERY_TEMPLATE = f"""
WITH
formatted_cte AS (
    SELECT DISTINCT
        OffenderId,
        SAFE_CAST(ADDREFFECTIVEDATE AS DATETIME) AS start_date,
        SAFE_CAST(ADDRENDDATE AS DATETIME) AS end_date,
        CASE
            WHEN AddressTypeDesc IN ("Community Program", "Physical (Home)") THEN 1
            ELSE 0
        END AS sustainable_housing,
    FROM `{{project_id}}.{{us_ca_raw_dataset}}.ParoleHousing_latest`
),
sessions_cte AS (
{aggregate_adjacent_spans(
    table_name='formatted_cte',
    index_columns=['OffenderId'],
    attribute='sustainable_housing',
)}
)

SELECT
    OffenderId,
    start_date,
    end_date,
    sustainable_housing
FROM sessions_cte
ORDER BY OffenderId, start_date
"""

US_CA_SUSTAINABLE_HOUSING_STATUS_PERIODS_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=ANALYST_VIEWS_DATASET,
    us_ca_raw_dataset=US_CA_RAW_DATASET,
    view_id=US_CA_SUSTAINABLE_HOUSING_STATUS_PERIODS_VIEW_NAME,
    description=US_CA_SUSTAINABLE_HOUSING_STATUS_PERIODS_VIEW_DESCRIPTION,
    view_query_template=US_CA_SUSTAINABLE_HOUSING_STATUS_PERIODS_QUERY_TEMPLATE,
    should_materialize=True,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        US_CA_SUSTAINABLE_HOUSING_STATUS_PERIODS_VIEW_BUILDER.build_and_print()
