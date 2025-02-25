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
"""View with drug screen with a non-null result"""
from recidiviz.calculator.query.bq_utils import nonnull_end_date_exclusive_clause
from recidiviz.observations.event_observation_big_query_view_builder import (
    EventObservationBigQueryViewBuilder,
)
from recidiviz.observations.event_type import EventType
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

_VIEW_DESCRIPTION = "Drug screens with a non-null result"

_SOURCE_DATA_QUERY_TEMPLATE = f"""
SELECT
    d.state_code,
    d.person_id,
    drug_screen_date,
    is_positive_result,
    substance_detected,
    d.is_positive_result
        AND ROW_NUMBER() OVER (
            PARTITION BY
                sss.person_id, sss.supervision_super_session_id, is_positive_result
            ORDER BY drug_screen_date
        ) = 1 AS is_initial_within_supervision_super_session,
FROM
    `{{project_id}}.sessions.drug_screens_preprocessed_materialized` d
INNER JOIN
    `{{project_id}}.sessions.supervision_super_sessions_materialized` sss
ON
    d.person_id = sss.person_id
    AND d.drug_screen_date BETWEEN sss.start_date AND {nonnull_end_date_exclusive_clause("sss.end_date_exclusive")}
WHERE
    is_positive_result IS NOT NULL
"""

VIEW_BUILDER: EventObservationBigQueryViewBuilder = EventObservationBigQueryViewBuilder(
    event_type=EventType.DRUG_SCREEN,
    description=_VIEW_DESCRIPTION,
    sql_source=_SOURCE_DATA_QUERY_TEMPLATE,
    attribute_cols=[
        "is_positive_result",
        "substance_detected",
        "is_initial_within_supervision_super_session",
    ],
    event_date_col="drug_screen_date",
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
