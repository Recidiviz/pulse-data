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

"""A view revealing if any new offense codes have appeared in 'us_mi_state_charge_v2'."""

from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.common.constants.states import StateCode
from recidiviz.ingest.direct.dataset_config import raw_latest_views_dataset_for_region
from recidiviz.ingest.direct.types.direct_ingest_instance import DirectIngestInstance
from recidiviz.ingest.views.dataset_config import NORMALIZED_STATE_DATASET
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override
from recidiviz.validation.views import dataset_config

FLAG_NEW_OFFENSE_CODES_VIEW_NAME = "flag_new_offense_codes"

FLAG_NEW_OFFENSE_CODES_DESCRIPTION = """This view reveals any new offense codes that
have appeared in 'us_mi_state_charge_v2', only if they are new and contain a previously
known code as a suffix or prefix. Any new offense codes returned by this validation
should be reviewed to determine whether any of these codes should be added to the
exclusions list for MI supervision workflow opportunities."""

FLAG_NEW_OFFENSE_CODES_QUERY_TEMPLATE = """
  WITH state_charges AS (
    SELECT 
      * 
    FROM `{project_id}.{normalized_state_dataset}.state_charge`
    WHERE state_code = 'US_MI'
  ),
  codes AS (
    SELECT 
      * 
    FROM `{project_id}.{raw_data_up_to_date_views_dataset}.RECIDIVIZ_REFERENCE_offense_exclusion_list_latest`
  ),
  check_codes_cte AS (
    SELECT
      sg.statute,
      oc.statute_code
    FROM 
      state_charges sg 
    LEFT JOIN codes oc
      ON (sg.statute LIKE CONCAT('%', oc.statute_code) OR sg.statute LIKE CONCAT(oc.statute_code, '%'))
    WHERE 
      sg.statute NOT IN (SELECT statute_code FROM codes) AND oc.statute_code IS NOT NULL
  )
  SELECT
    'US_MI' AS region_code,
    'US_MI' AS state_code,
    cc.statute AS new_code,
    COUNT(*) AS num_rows_with_new_code
  FROM 
    check_codes_cte cc
  group by statute;
"""

FLAG_NEW_OFFENSE_CODES_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=dataset_config.VIEWS_DATASET,
    view_id=FLAG_NEW_OFFENSE_CODES_VIEW_NAME,
    view_query_template=FLAG_NEW_OFFENSE_CODES_QUERY_TEMPLATE,
    description=FLAG_NEW_OFFENSE_CODES_DESCRIPTION,
    should_materialize=True,
    normalized_state_dataset=NORMALIZED_STATE_DATASET,
    raw_data_up_to_date_views_dataset=raw_latest_views_dataset_for_region(
        state_code=StateCode.US_MI, instance=DirectIngestInstance.PRIMARY
    ),
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        FLAG_NEW_OFFENSE_CODES_VIEW_BUILDER.build_and_print()
