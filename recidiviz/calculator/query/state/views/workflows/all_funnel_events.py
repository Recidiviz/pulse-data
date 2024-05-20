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
"""View that contains all funnel-related events. This includes eligibility milestones,
surfaced, viewed, and congratulated events.

python -m recidiviz.calculator.query.state.views.workflows.all_funnel_events
"""

from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.state import dataset_config
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

ALL_FUNNEL_EVENTS_VIEW_NAME = "all_funnel_events"

ALL_FUNNEL_EVENTS_DESCRIPTION = """
    View of funnel steps including eligibility, surfaced, viewed, and congratulated
"""

ALL_FUNNEL_EVENTS_QUERY_TEMPLATE = """
    WITH all_funnel_events AS (
        WITH milestone_eligibility AS (
            SELECT 
                client_record.person_external_id,
                client_record.state_code,
                "ELIGIBLE" AS funnel_step,
                m.type AS milestone_type,
                m.text AS milestone_text,
                m.milestone_date AS funnel_date
                FROM `{project_id}.{workflows_views_dataset}.client_record_materialized` client_record,
                UNNEST(client_record.milestones) AS m
                WHERE client_record.state_code = 'US_CA'
        )
        SELECT *
        FROM milestone_eligibility
        UNION ALL
        SELECT
            clients_surfaced.person_external_id,
            clients_surfaced.state_code,
            "SURFACED" AS funnel_step,
            NULL AS milestone_type,
            NULL AS milestone_text,
            DATE(clients_surfaced.timestamp)  AS funnel_date
        FROM `{project_id}.{workflows_views_dataset}.clients_surfaced` clients_surfaced
        INNER JOIN milestone_eligibility ON clients_surfaced.state_code = milestone_eligibility.state_code AND clients_surfaced.person_external_id = milestone_eligibility.person_external_id AND milestone_eligibility.funnel_date <= DATE(clients_surfaced.timestamp)
        WHERE clients_surfaced.state_code = 'US_CA'
        UNION ALL
        SELECT 
            clients_profile_viewed.person_external_id,
            clients_profile_viewed.state_code,
            "VIEWED" AS funnel_step,
            NULL AS milestone_type,
            NULL AS milestone_text,
            DATE(clients_profile_viewed.timestamp) AS funnel_date
        FROM `{project_id}.{workflows_views_dataset}.clients_profile_viewed` clients_profile_viewed
        INNER JOIN milestone_eligibility ON clients_profile_viewed.state_code = milestone_eligibility.state_code AND clients_profile_viewed.person_external_id = milestone_eligibility.person_external_id AND milestone_eligibility.funnel_date <= DATE(clients_profile_viewed.timestamp)
        WHERE clients_profile_viewed.state_code = 'US_CA'
        UNION ALL
        SELECT
            clients_milestones_side_panel_opened.person_external_id,
            clients_milestones_side_panel_opened.state_code,
            "VIEWED" AS funnel_step,
            NULL AS milestone_type,
            NULL AS milestone_text,
            DATE(clients_milestones_side_panel_opened.timestamp) AS funnel_date
        FROM `{project_id}.{workflows_views_dataset}.clients_milestones_side_panel_opened` clients_milestones_side_panel_opened
        UNION ALL
        SELECT
            clients_milestones_congratulations_sent.person_external_id,
            clients_milestones_congratulations_sent.state_code,
            "CONGRATULATED" AS funnel_step,
            NULL AS milestone_type,
            NULL AS milestone_text,
            DATE(clients_milestones_congratulations_sent.timestamp) AS funnel_date
        FROM `{project_id}.{workflows_views_dataset}.clients_milestones_congratulations_sent` clients_milestones_congratulations_sent
        UNION ALL
        SELECT
            clients_milestones_congratulated_another_way.person_external_id,
            clients_milestones_congratulated_another_way.state_code,
            "CONGRATULATED" AS funnel_step,
            NULL AS milestone_type,
            NULL AS milestone_text,
            DATE(clients_milestones_congratulated_another_way.timestamp) AS funnel_date
        FROM `{project_id}.{workflows_views_dataset}.clients_milestones_congratulated_another_way` AS clients_milestones_congratulated_another_way
    )
    
    SELECT
        person_external_id,
        state_code,
        funnel_step,
        milestone_type,
        milestone_text,
        funnel_date
    FROM all_funnel_events
"""

ALL_FUNNEL_EVENTS_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=dataset_config.WORKFLOWS_VIEWS_DATASET,
    view_id=ALL_FUNNEL_EVENTS_VIEW_NAME,
    view_query_template=ALL_FUNNEL_EVENTS_QUERY_TEMPLATE,
    description=ALL_FUNNEL_EVENTS_DESCRIPTION,
    workflows_views_dataset=dataset_config.WORKFLOWS_VIEWS_DATASET,
    should_materialize=True,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        ALL_FUNNEL_EVENTS_VIEW_BUILDER.build_and_print()
