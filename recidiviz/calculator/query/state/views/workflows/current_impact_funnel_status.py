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
"""
The latest referral status and eligibility type for each justice impacted individual
across all launched Workflows opportunities. This view is the source for the Workflows
External Impact Funnel dashboard in Looker. It includes additional information about the
individual (name, location, officer, etc.) to use as drill downs and aggregations in the
dashboard.
"""

from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.bq_utils import nonnull_end_date_exclusive_clause
from recidiviz.calculator.query.state import dataset_config
from recidiviz.task_eligibility.dataset_config import TASK_ELIGIBILITY_DATASET_ID
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

CURRENT_IMPACT_FUNNEL_STATUS_VIEW_NAME = "current_impact_funnel_status"

CURRENT_IMPACT_FUNNEL_STATUS_DESCRIPTION = """
The latest referral status (not viewed, in progress, denied, etc.) and eligibility type
(eligible vs almost eligible) for each justice impacted individual across all launched
Workflows opportunities. This view is one source for the Workflows External Impact
Funnel dashboard in Looker. It includes additional information about the individual
(name, location, officer, etc.) to use as drill downs and aggregations in the dashboard.
"""

CURRENT_IMPACT_FUNNEL_STATUS_QUERY_TEMPLATE = f"""
WITH eligibility AS (
  -- Query the current eligibility status (remaining criteria needed) for each opportunity
  SELECT
    state_code,
    pei.person_external_id,
    completion_event_type,
    ARRAY_LENGTH(ineligible_criteria) AS remaining_criteria_needed,
  FROM `{{project_id}}.{{task_eligibility_dataset}}.all_tasks_materialized` tes
  INNER JOIN `{{project_id}}.{{reference_views_dataset}}.task_to_completion_event` tce
    USING (task_name)
  INNER JOIN `{{project_id}}.{{workflows_views_dataset}}.person_id_to_external_id_materialized` pei
    USING (state_code, person_id)
  WHERE CURRENT_DATE BETWEEN start_date AND {nonnull_end_date_exclusive_clause("end_date")}
)
SELECT
  records.* EXCEPT (all_eligible_opportunities, person_name),
  opportunity_type,
  completion_event_type,
  timestamp AS last_view_time,
  eligibility.remaining_criteria_needed,
  COALESCE(status, "NOT_VIEWED") AS status,
  denied_reasons,
  -- Parse the client/resident name and officer name
  INITCAP(JSON_VALUE(PARSE_JSON(person_name), '$.given_names'))
    || " "
    || INITCAP(JSON_VALUE(PARSE_JSON(person_name), '$.surname')) AS person_name,
  INITCAP(JSON_VALUE(PARSE_JSON(officer_name), '$.given_names'))
    || " "
    || INITCAP(JSON_VALUE(PARSE_JSON(officer_name), '$.surname')) AS officer_name,
FROM `{{project_id}}.{{workflows_views_dataset}}.person_record_materialized` records,
UNNEST (all_eligible_opportunities) AS opportunity_type
LEFT JOIN `{{project_id}}.{{reference_views_dataset}}.workflows_opportunity_configs_materialized`
  USING (state_code, opportunity_type)
LEFT JOIN eligibility
  USING (state_code, person_external_id, completion_event_type)
LEFT JOIN `{{project_id}}.{{workflows_views_dataset}}.clients_latest_referral_status_extended_materialized`
  USING (state_code, person_external_id, opportunity_type)
LEFT JOIN (
  SELECT
    state_code, legacy_supervising_officer_external_id AS officer_id, full_name_json AS officer_name
  FROM `{{project_id}}.reference_views.state_staff_with_names`
  WHERE full_name_json IS NOT NULL
) state_staff
  USING (state_code, officer_id)
"""

CURRENT_IMPACT_FUNNEL_STATUS_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=dataset_config.WORKFLOWS_VIEWS_DATASET,
    view_id=CURRENT_IMPACT_FUNNEL_STATUS_VIEW_NAME,
    view_query_template=CURRENT_IMPACT_FUNNEL_STATUS_QUERY_TEMPLATE,
    description=CURRENT_IMPACT_FUNNEL_STATUS_DESCRIPTION,
    reference_views_dataset=dataset_config.REFERENCE_VIEWS_DATASET,
    task_eligibility_dataset=TASK_ELIGIBILITY_DATASET_ID,
    workflows_views_dataset=dataset_config.WORKFLOWS_VIEWS_DATASET,
    should_materialize=True,
    clustering_fields=["state_code", "opportunity_type"],
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        CURRENT_IMPACT_FUNNEL_STATUS_VIEW_BUILDER.build_and_print()
