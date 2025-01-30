# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2024 Recidiviz, Inc.
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
"""Arizona state-specific preprocessing for early releases from incarceration to the TPR or DTP program"""

from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.bq_utils import nonnull_end_date_clause
from recidiviz.calculator.query.state.dataset_config import ANALYST_VIEWS_DATASET
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

US_AZ_EARLY_RELEASES_FROM_INCARCERATION_VIEW_NAME = (
    "us_az_early_releases_from_incarceration"
)

US_AZ_EARLY_RELEASES_FROM_INCARCERATION_QUERY_TEMPLATE = f"""
WITH
# Pull the eligible release date from state task deadline
# to determine if the early release was on time vs overdue
projected_release_date_spans AS (
    SELECT
        state_code,
        person_id,
        task_subtype,
        eligible_date AS eligible_release_date,
        CAST(DATE_ADD(update_datetime, INTERVAL 1 DAY) AS DATE) AS start_date,
        LEAD(CAST(DATE_ADD(update_datetime, INTERVAL 1 DAY) AS DATE)) OVER (
            PARTITION BY state_code, person_id, task_subtype
            ORDER BY update_datetime
        ) AS end_date_exclusive,
    FROM
        `{{project_id}}.normalized_state.state_task_deadline`
    WHERE
        state_code = "US_AZ"
        AND eligible_date IS NOT NULL
        AND eligible_date > "1900-01-01"
        AND task_type = "DISCHARGE_FROM_INCARCERATION"
        AND task_subtype IN (
            "DRUG TRANSITION RELEASE",
            "STANDARD TRANSITION RELEASE"
        )
)
SELECT
    sessions.state_code,
    sessions.person_id,
    eligible_release_date,
    sessions.end_date_exclusive AS release_date,
    CASE
        WHEN end_reason_raw_text = "STANDARD TRANSITION RELEASE" THEN "TPR"
        WHEN end_reason_raw_text = "DRUG TRANSITION RELEASE" THEN "DTP"
    END AS release_type,
FROM
    `{{project_id}}.sessions.compartment_sessions_materialized` sessions
INNER JOIN
    `{{project_id}}.sessions.compartment_session_end_reasons_materialized` session_ends
USING
    (state_code, person_id, compartment_level_1, end_date, end_reason)
LEFT JOIN
    projected_release_date_spans projected_release_dates
ON
    sessions.state_code = projected_release_dates.state_code
    AND sessions.person_id = projected_release_dates.person_id
    AND session_ends.end_reason_raw_text = projected_release_dates.task_subtype
    AND sessions.end_date_exclusive
        BETWEEN projected_release_dates.start_date AND {nonnull_end_date_clause("projected_release_dates.end_date_exclusive")}
WHERE
    session_ends.end_reason_raw_text IN (
        "STANDARD TRANSITION RELEASE",
        "DRUG TRANSITION RELEASE"
    )
"""

US_AZ_EARLY_RELEASES_FROM_INCARCERATION_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=ANALYST_VIEWS_DATASET,
    view_id=US_AZ_EARLY_RELEASES_FROM_INCARCERATION_VIEW_NAME,
    description=__doc__,
    view_query_template=US_AZ_EARLY_RELEASES_FROM_INCARCERATION_QUERY_TEMPLATE,
    should_materialize=True,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        US_AZ_EARLY_RELEASES_FROM_INCARCERATION_VIEW_BUILDER.build_and_print()
