# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2020 Recidiviz, Inc.
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
"""A view revealing when a parole agent's badge number changes."""

from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.common.constants.states import StateCode
from recidiviz.ingest.direct.dataset_config import raw_tables_dataset_for_region
from recidiviz.ingest.direct.types.direct_ingest_instance import DirectIngestInstance
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override
from recidiviz.validation.views import dataset_config

PA_BADGE_NUMBER_CHANGES_VIEW_NAME = "pa_badge_number_changes"

PA_BADGE_NUMBER_CHANGES_DESCRIPTION = """This view shows us when badge numbers have
changed. It only cares about BadgeNumbers after 1/1/2024 as there were some data issues
we don't care about. It works by looking at PA's emails and seeing if anyone has
multiple badge numbers per email."""

PA_BADGE_NUMBER_CHANGES_QUERY_TEMPLATE = """
WITH
problems AS (
  SELECT *
  FROM `{project_id}.{us_ca_raw_data_up_to_date_dataset}.AgentParole` ap
  WHERE BadgeNumber IS NOT NULL AND EMAILADDRESS IS NOT NULL
    AND update_datetime > DATETIME('2024-01-01')
  QUALIFY COUNT(DISTINCT BadgeNumber) OVER (PARTITION BY EMAILADDRESS) > 1
),
grouped AS (
  SELECT 
    ParoleAgentName,
    EMAILADDRESS,
    BadgeNumber,
    ARRAY_AGG(update_datetime ORDER BY update_datetime DESC) AS update_datetimes
  FROM problems
  GROUP BY ParoleAgentName, EMAILADDRESS, BadgeNumber
)
SELECT *,
    "US_CA" as region_code
FROM grouped
"""

PA_BADGE_NUMBER_CHANGES_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=dataset_config.VIEWS_DATASET,
    view_id=PA_BADGE_NUMBER_CHANGES_VIEW_NAME,
    view_query_template=PA_BADGE_NUMBER_CHANGES_QUERY_TEMPLATE,
    description=PA_BADGE_NUMBER_CHANGES_DESCRIPTION,
    us_ca_raw_data_up_to_date_dataset=raw_tables_dataset_for_region(
        state_code=StateCode.US_CA, instance=DirectIngestInstance.PRIMARY
    ),
    should_materialize=True,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        PA_BADGE_NUMBER_CHANGES_VIEW_BUILDER.build_and_print()
