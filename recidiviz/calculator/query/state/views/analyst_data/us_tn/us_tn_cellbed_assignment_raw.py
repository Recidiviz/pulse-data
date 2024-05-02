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
"""Computes current facility / unit from raw TN data"""

from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.state.dataset_config import ANALYST_VIEWS_DATASET
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

US_TN_CELLBED_ASSIGNMENT_RAW_VIEW_NAME = "us_tn_cellbed_assignment_raw"

US_TN_CELLBED_ASSIGNMENT_RAW_VIEW_DESCRIPTION = (
    """Computes current facility / unit from raw TN data"""
)

US_TN_CELLBED_ASSIGNMENT_RAW_QUERY_TEMPLATE = """
    -- TODO(#24959): Deprecate usage when re-run is over
    -- TODO(#27428): Once source of facility ID in TN is reconciled, this can be removed
      SELECT
        state_code,
        person_id,
            /* From TN: "The ‘Requested’ location columns contain the new housing assignment 
                (where the person is being moved to). The ‘Assigned’ columns show the person’s assigned bed 
                when the new cell bed assignment was entered. Same for the ‘Actual’ columns – this is where he 
                ‘actually’ was when the request was made (this is only different from Assigned when the person 
                has been moved to another institution temporarily)." Our interpretation of this is that Requested and
                Actual work sometimes together and sometimes on a "lag"; Actual should be updated to show the same
                thing as Requested, but sometimes it isnt, so we rely on Requested most, then Actual. 
                For current population, Requested is always hydrated, so the COALESCE is not strictly
                needed but is a catch all if Requested is ever missing */
            COALESCE(RequestedSiteID, ActualSiteID, AssignedSiteID) AS facility_id,
            COALESCE(RequestedUnitID, ActualUnitID, AssignedUnitID) AS unit_id,
      FROM `{project_id}.us_tn_raw_data_up_to_date_views.CellBedAssignment_latest` c
      INNER JOIN `{project_id}.normalized_state.state_person_external_id` pei
        ON c.OffenderID = pei.external_id
        AND pei.state_code = "US_TN"
      -- The latest assignment is not always the one with an open assignment. This can occur when someone is assigned to a facility 
      -- but temporarily sent to another facility (e.g. Special needs facility). Most people only have 1 open assignment, unless
      -- they are currently temporarily sent elsewhere (~11 people out of 18k) so we further deduplicate by choosing the latest assignment
      WHERE EndDate IS NULL
      QUALIFY ROW_NUMBER() OVER(PARTITION BY OffenderID ORDER BY CAST(AssignmentDateTime AS DATETIME) DESC) = 1
        
"""

US_TN_CELLBED_ASSIGNMENT_RAW_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=ANALYST_VIEWS_DATASET,
    view_id=US_TN_CELLBED_ASSIGNMENT_RAW_VIEW_NAME,
    description=US_TN_CELLBED_ASSIGNMENT_RAW_VIEW_DESCRIPTION,
    view_query_template=US_TN_CELLBED_ASSIGNMENT_RAW_QUERY_TEMPLATE,
    should_materialize=True,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        US_TN_CELLBED_ASSIGNMENT_RAW_VIEW_BUILDER.build_and_print()
