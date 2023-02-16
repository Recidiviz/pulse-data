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
"""
Aggregated view of several segment events that detail if the client opportunity has
been "viewed" in the app by an officer or state official, and if they have been marked
as ineligible ("DENIED") for the opportunity.
"""

from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.state import dataset_config
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

CLIENTS_LATEST_REFERRAL_STATUS_EXTENDED_VIEW_NAME = (
    "clients_latest_referral_status_extended"
)

CLIENTS_LATEST_REFERRAL_STATUS_EXTENDED_DESCRIPTION = """
Extension of the `clients_latest_referral_status` view in order to include all of the
following events around a client's opportunity referral status:
- OPPORTUNITY_PREVIEWED: a state agent has viewed the client's opportunity (used for form-free opportunities)
- REFERRAL_FORM_VIEWED: the client's referral form has been viewed by a state agent
- IN_PROGRESS: the client's referral form has been started by a state agent
- COMPLETED: the client's referral form has been completed and printed by a state agent
- DENIED: the client has been marked ineligible for the opportunity by a state agent
"""

CLIENTS_LATEST_REFERRAL_STATUS_EXTENDED_QUERY_TEMPLATE = """
WITH all_events AS (
    -- Track if the opportunity record has been previewed in the app
    SELECT
        state_code,
        person_id,
        person_external_id,
        timestamp,
        "OPPORTUNITY_PREVIEWED" AS status,
        opportunity_type,
        9999 AS rank,
        NULL as denied_reasons,
    FROM `{project_id}.{workflows_views_dataset}.clients_opportunity_previewed`

    UNION ALL

    -- Track if the opportunity referral form has been viewed in the app
    SELECT
        state_code,
        person_id,
        person_external_id,
        timestamp,
        "REFERRAL_FORM_VIEWED" AS status,
        opportunity_type,
        9999 AS rank,
        NULL as denied_reasons,
    FROM `{project_id}.{workflows_views_dataset}.clients_referral_form_viewed`

    UNION ALL

    -- Track if the client's opportunity referral status has been updated in the app,
    -- includes status about the form (IN_PROGRESS, COMPLETED) and eligibility (DENIED)
    SELECT
        state_code,
        person_id,
        person_external_id,
        timestamp,
        status,
        opportunity_type,
        1 AS rank,
        denied_reasons,
    FROM `{project_id}.{workflows_views_dataset}.clients_latest_referral_status`
)
SELECT 
*
FROM all_events
-- Keep the highest ranked and then most recent status per client and opportunity
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY state_code, person_id, opportunity_type 
    ORDER BY rank, timestamp DESC
) = 1
"""

CLIENTS_LATEST_REFERRAL_STATUS_EXTENDED_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=dataset_config.WORKFLOWS_VIEWS_DATASET,
    view_id=CLIENTS_LATEST_REFERRAL_STATUS_EXTENDED_VIEW_NAME,
    view_query_template=CLIENTS_LATEST_REFERRAL_STATUS_EXTENDED_QUERY_TEMPLATE,
    description=CLIENTS_LATEST_REFERRAL_STATUS_EXTENDED_DESCRIPTION,
    workflows_views_dataset=dataset_config.WORKFLOWS_VIEWS_DATASET,
    should_materialize=True,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        CLIENTS_LATEST_REFERRAL_STATUS_EXTENDED_VIEW_BUILDER.build_and_print()
