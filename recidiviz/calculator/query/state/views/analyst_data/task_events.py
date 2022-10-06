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
"""A view that maps person_id to the external IDs used in Workflows"""

from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.state import dataset_config
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

TASK_EVENTS_VIEW_NAME = "task_events"

TASK_EVENTS_DESCRIPTION = (
    """Track the primary outcome event for each decarceral task type"""
)


TASK_EVENTS_QUERY_TEMPLATE = """
    WITH early_discharge AS (
        SELECT
            state_code,
            person_id,
            "EARLY_DISCHARGE" AS task_type,
            DATE_ADD(end_date, INTERVAL 1 DAY) AS event_date,
        FROM `{project_id}.{analyst_data}.early_discharge_sessions_materialized`
        WHERE early_discharge = 1
    ),
    full_term_discharge AS (
        SELECT
            state_code,
            person_id,
            "FULL_TERM_DISCHARGE" AS task_type,
            DATE_ADD(end_date, INTERVAL 1 DAY) AS event_date,
        FROM `{project_id}.{analyst_data}.early_discharge_sessions_materialized`
        WHERE early_discharge = 0
            AND compartment_level_1 = "SUPERVISION"
            AND outflow_to_level_1 = "LIBERTY"
    ),
    transfer_to_limited AS (
        SELECT
            state_code,
            person_id,
            "TRANSFER_TO_LIMITED_SUPERVISION" AS task_type,
            start_date AS event_date,
        FROM `{project_id}.{sessions_data}.supervision_level_sessions_materialized`
        WHERE supervision_level = "LIMITED"
    ),
    supervision_level_downgrade AS (
        SELECT
            state_code,
            person_id,
            "SUPERVISION_LEVEL_DOWNGRADE" AS task_type,
            start_date AS event_date,
        FROM `{project_id}.{sessions_data}.supervision_level_sessions_materialized`
        WHERE supervision_downgrade = 1
            AND supervision_level != "LIMITED"
    )
    SELECT * FROM early_discharge
    UNION ALL
    SELECT * FROM full_term_discharge
    UNION ALL
    SELECT * FROM transfer_to_limited
    UNION ALL
    SELECT * FROM supervision_level_downgrade
"""

TASK_EVENTS_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=dataset_config.ANALYST_VIEWS_DATASET,
    view_id=TASK_EVENTS_VIEW_NAME,
    view_query_template=TASK_EVENTS_QUERY_TEMPLATE,
    description=TASK_EVENTS_DESCRIPTION,
    analyst_data=dataset_config.ANALYST_VIEWS_DATASET,
    sessions_data=dataset_config.SESSIONS_DATASET,
    should_materialize=True,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        TASK_EVENTS_VIEW_BUILDER.build_and_print()
