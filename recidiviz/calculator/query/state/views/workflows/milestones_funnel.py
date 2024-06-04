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
"""A view that displays individuals eligible for milestones in Workflows"""

from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.state import dataset_config
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

MILESTONES_FUNNEL_VIEW_NAME = "milestones_funnel"

MILESTONES_FUNNEL_DESCRIPTION = (
    """Displays individuals eligible for at least one milestone"""
)

MILESTONES_FUNNEL_QUERY_TEMPLATE = """
	WITH all_funnel_steps AS (
		SELECT
			client_record_materialized.person_name,
			client_record_materialized.person_external_id,
			client_record_materialized.officer_id,
			client_record_materialized.state_code,
			client_record_materialized.district,
			client_record_materialized.all_eligible_opportunities,
			('usCaSupervisionLevelDowngrade' IN UNNEST(client_record_materialized.all_eligible_opportunities)
			OR 'usIdSupervisionLevelDowngrade' IN UNNEST(client_record_materialized.all_eligible_opportunities)
			OR 'usMiSupervisionLevelDowngrade' IN UNNEST(client_record_materialized.all_eligible_opportunities)
			OR 'supervisionLevelDowngrade' IN UNNEST(client_record_materialized.all_eligible_opportunities)
			) AS is_eligible_sld,
		CASE
			WHEN clients_milestones_congratulations_sent.person_id IS NOT NULL THEN "TEXT_VIA_TOOL"
			WHEN clients_milestones_congratulated_another_way.person_id IS NOT NULL THEN "ANOTHER_WAY"
			ELSE NULL
		END AS method_of_congratulations,
		all_funnel_events.funnel_step,
		all_funnel_events.milestone_type,
        all_funnel_events.milestone_text,
		all_funnel_events.funnel_date
		FROM `{project_id}.{workflows_views_dataset}.client_record_materialized` AS client_record_materialized
        LEFT JOIN `{project_id}.{workflows_views_dataset}.clients_milestones_congratulations_sent` clients_milestones_congratulations_sent USING (person_external_id)
		LEFT JOIN `{project_id}.{workflows_views_dataset}.clients_milestones_congratulated_another_way` clients_milestones_congratulated_another_way USING (person_external_id)
		INNER JOIN `{project_id}.{workflows_views_dataset}.all_funnel_events_materialized` all_funnel_events ON client_record_materialized.state_code = all_funnel_events.state_code AND client_record_materialized.person_external_id = all_funnel_events.person_external_id
		WHERE array_length(milestones) != 0
	)
    SELECT
		person_name,
		person_external_id,
		officer_id,
		state_code,
		district,
		is_eligible_sld,
		method_of_congratulations,
		funnel_step,
		milestone_type,
		milestone_text,
		MAX(funnel_date) AS max_funnel_date
    FROM all_funnel_steps
    GROUP BY person_name, person_external_id, officer_id, state_code, district, is_eligible_sld, method_of_congratulations, funnel_step, milestone_type, milestone_text
"""

MILESTONES_FUNNEL_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=dataset_config.WORKFLOWS_VIEWS_DATASET,
    view_id=MILESTONES_FUNNEL_VIEW_NAME,
    view_query_template=MILESTONES_FUNNEL_QUERY_TEMPLATE,
    description=MILESTONES_FUNNEL_DESCRIPTION,
    workflows_views_dataset=dataset_config.WORKFLOWS_VIEWS_DATASET,
    should_materialize=True,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        MILESTONES_FUNNEL_VIEW_BUILDER.build_and_print()
