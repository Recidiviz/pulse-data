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
"""View of client referral form status update events logged from UI

python -m recidiviz.calculator.query.state.views.practices.clients_latest_referral_status
"""

from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.state import dataset_config
from recidiviz.calculator.query.state.views.practices.user_event_template import (
    user_event_template,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

CLIENTS_LATEST_REFERRAL_STATUS_VIEW_NAME = "clients_latest_referral_status"

CLIENTS_LATEST_REFERRAL_STATUS_DESCRIPTION = """
    View of client referral form status update events logged from UI
    """


CLIENTS_LATEST_REFERRAL_STATUS_QUERY_TEMPLATE = f"""
    /*{{description}}*/
    WITH 
    status_updates AS (
        {user_event_template(
            "frontend_opportunity_status_updated", add_columns=["status", "opportunity_type"]
        )}
    )
    SELECT
        * EXCEPT (rn, opportunity_type),
        -- this field was initially left out of the tracking calls by mistake;
        -- if it's missing we can infer that it was supposed to be "compliantReporting"
        -- because that was the only possible value at the time
        IFNULL(opportunity_type, "compliantReporting") AS opportunity_type,
    FROM (
        SELECT
            person_id,
            state_code,
            person_external_id,
            timestamp,
            status,
            opportunity_type,
            ROW_NUMBER() OVER (
                PARTITION BY person_id, opportunity_type
                ORDER BY timestamp DESC
            ) AS rn,
        FROM status_updates
    )
    -- use most recent status update to eliminate completions that were later undone
    WHERE rn = 1
"""

CLIENTS_LATEST_REFERRAL_STATUS_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=dataset_config.PRACTICES_VIEWS_DATASET,
    view_id=CLIENTS_LATEST_REFERRAL_STATUS_VIEW_NAME,
    view_query_template=CLIENTS_LATEST_REFERRAL_STATUS_QUERY_TEMPLATE,
    description=CLIENTS_LATEST_REFERRAL_STATUS_DESCRIPTION,
    practices_views_dataset=dataset_config.PRACTICES_VIEWS_DATASET,
    reference_views_dataset=dataset_config.REFERENCE_VIEWS_DATASET,
    segment_dataset=dataset_config.PULSE_DASHBOARD_SEGMENT_DATASET,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        CLIENTS_LATEST_REFERRAL_STATUS_VIEW_BUILDER.build_and_print()
