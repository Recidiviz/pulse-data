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
"""A view comparing the most recent archived client and resident record and the
current client and resident record by task type to determine if major changes
in eligibility have occurred.
"""

from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.state.dataset_config import (
    REFERENCE_VIEWS_DATASET,
    WORKFLOWS_VIEWS_DATASET,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override
from recidiviz.validation.views import dataset_config

CLIENT_AND_RESIDENT_RECORD_PERCENT_CHANGE_IN_ELIGIBILITY_EXCEEDED_VIEW_NAME = (
    "client_and_resident_record_percent_change_in_eligibility_exceeded"
)

CLIENT_AND_RESIDENT_RECORD_PERCENT_CHANGE_IN_ELIGIBILITY_EXCEEDED_DESCRIPTION = """
Identifies when a considerable change in exports to client/resident records has occurred for a given task type. NOTE: This validation may pass on subsequent runs after failing even if the underlying issue is not resolved.
"""

CLIENT_AND_RESIDENT_RECORD_PERCENT_CHANGE_IN_ELIGIBILITY_EXCEEDED_QUERY_TEMPLATE = """
WITH archived_eligibility_records AS (
  -- Combine all archive eligibility rows together
  SELECT
    state_code,
    export_date,
    all_eligible_opportunities,
    person_external_id,
  FROM `{project_id}.{workflows_views_dataset}.client_record_archive_materialized`
  UNION ALL
  SELECT
    state_code,
    export_date,
    all_eligible_opportunities,
    person_external_id,
  FROM `{project_id}.{workflows_views_dataset}.resident_record_archive_materialized`
),
current_eligibility_records AS (
  -- Combine all current eligibility rows together
  SELECT
    state_code,
    all_eligible_opportunities,
    person_external_id,
  FROM `{project_id}.{workflows_views_dataset}.client_record_materialized`
  UNION ALL
  SELECT
    state_code,
    all_eligible_opportunities,
    person_external_id,
  FROM `{project_id}.{workflows_views_dataset}.resident_record_materialized`
),
previous_export_count AS (
  SELECT
    state_code,
    opportunity_type,
    export_date AS last_export_date,
    COUNT(DISTINCT person_external_id) AS total_clients
  FROM archived_eligibility_records,
  UNNEST(SPLIT(all_eligible_opportunities, ",")) AS opportunity_type
  WHERE opportunity_type != ""
    -- Drop records for legacy opportunities
    AND state_code != "US_ID"
    AND opportunity_type != "sccp"
    AND export_date < CURRENT_DATE('US/Pacific')
  GROUP BY 1, 2, 3
  -- Pick the latest export date per state/opportunity
  QUALIFY ROW_NUMBER() OVER (
    PARTITION BY state_code, opportunity_type
    ORDER BY export_date DESC
  ) = 1
),
current_live_opportunities AS (
  SELECT DISTINCT state_code, opportunity_type
  FROM `{project_id}.{reference_views_dataset}.workflows_opportunity_configs_materialized`
)
SELECT
  state_code AS region_code,
  opportunity_type,
  previous_export_count.last_export_date,
  COALESCE(previous_export_count.total_clients, 0) AS last_export_eligibility_count,
  COUNT(DISTINCT current_eligibility_records.person_external_id) AS current_eligibility_count
FROM current_eligibility_records
CROSS JOIN UNNEST(current_eligibility_records.all_eligible_opportunities) AS opportunity_type
FULL OUTER JOIN previous_export_count
  USING (state_code, opportunity_type)
INNER JOIN current_live_opportunities
  USING (state_code, opportunity_type)
GROUP BY 1, 2, 3, 4
ORDER BY 1, 2, 3
"""

CLIENT_AND_RESIDENT_RECORD_PERCENT_CHANGE_IN_ELIGIBILITY_EXCEEDED_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=dataset_config.VIEWS_DATASET,
    view_id=CLIENT_AND_RESIDENT_RECORD_PERCENT_CHANGE_IN_ELIGIBILITY_EXCEEDED_VIEW_NAME,
    view_query_template=CLIENT_AND_RESIDENT_RECORD_PERCENT_CHANGE_IN_ELIGIBILITY_EXCEEDED_QUERY_TEMPLATE,
    description=CLIENT_AND_RESIDENT_RECORD_PERCENT_CHANGE_IN_ELIGIBILITY_EXCEEDED_DESCRIPTION,
    workflows_views_dataset=WORKFLOWS_VIEWS_DATASET,
    reference_views_dataset=REFERENCE_VIEWS_DATASET,
    should_materialize=True,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        CLIENT_AND_RESIDENT_RECORD_PERCENT_CHANGE_IN_ELIGIBILITY_EXCEEDED_VIEW_BUILDER.build_and_print()
