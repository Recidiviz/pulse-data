# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2025 Recidiviz, Inc.
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
"""View that represents spans of time over which a supervision staff member in TX
is assigned to a critically understaffed office"""

from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.sessions_query_fragments import (
    aggregate_adjacent_spans,
    create_intersection_spans,
    create_sub_sessions_with_attributes,
)
from recidiviz.calculator.query.state.dataset_config import ANALYST_VIEWS_DATASET
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

_VIEW_NAME = (
    "us_tx_supervision_staff_in_critically_understaffed_location_sessions_preprocessed"
)

_QUERY_TEMPLATE = f"""
WITH staff_location_periods AS (
    SELECT
        state_code,
        staff_id,
        start_date,
        end_date AS end_date_exclusive,
        location_external_id,
    FROM
        `{{project_id}}.normalized_state.state_staff_location_period`
    WHERE
        state_code = "US_TX"
)
, critically_understaffed_locations AS (
    SELECT
        "US_TX" AS state_code,
        CONCAT(OFFC_REGION, '-', OFFC_DISTRICT) AS location_external_id,
        DPO AS location_name,
        DATE(CAST(YEAR AS INT64), CAST(MONTH AS INT64), 1) AS start_date,
        CASE
            WHEN DATE_ADD(DATE(CAST(YEAR AS INT64), CAST(MONTH AS INT64), 1), INTERVAL 1 MONTH) > CURRENT_DATE("US/Eastern")
            THEN CAST(NULL AS DATE)
            ELSE DATE_ADD(DATE(CAST(YEAR AS INT64), CAST(MONTH AS INT64), 1), INTERVAL 1 MONTH)
        END AS end_date_exclusive,
    FROM
        `{{project_id}}.us_tx_raw_data_up_to_date_views.manual_upload_critically_understaffed_locations_latest`
)
,
-- Join together staff locations and critically understaffed roster based on date
-- overlap and location external id
intersection_spans AS (
    {create_intersection_spans(
        table_1_name="staff_location_periods",
        table_2_name="critically_understaffed_locations",
        index_columns=["state_code", "location_external_id"],
        table_1_columns=["staff_id"],
        table_2_columns=[],
    )}
)
,
-- Sub-sessionize and dedup to account for overlapping location assignments
{create_sub_sessions_with_attributes(
    table_name="intersection_spans",
    end_date_field_name="end_date_exclusive",
    index_columns=["state_code", "staff_id"]
)}
,
sub_sessions_dedup AS (
    SELECT DISTINCT
        state_code,
        staff_id,
        start_date,
        end_date_exclusive,
    FROM
        sub_sessions_with_attributes
)
-- Re-aggregate adjacent spans
{aggregate_adjacent_spans(
    table_name="sub_sessions_dedup",
    index_columns=["state_code", "staff_id"],
    attribute=[],
    end_date_field_name="end_date_exclusive"
)}
"""

US_TX_SUPERVISION_STAFF_IN_CRITICALLY_UNDERSTAFFED_LOCATION_SESSIONS_PREPROCESSED_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=ANALYST_VIEWS_DATASET,
    view_id=_VIEW_NAME,
    description=__doc__,
    view_query_template=_QUERY_TEMPLATE,
    should_materialize=False,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        US_TX_SUPERVISION_STAFF_IN_CRITICALLY_UNDERSTAFFED_LOCATION_SESSIONS_PREPROCESSED_VIEW_BUILDER.build_and_print()
