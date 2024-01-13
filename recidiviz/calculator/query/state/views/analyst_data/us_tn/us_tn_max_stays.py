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
"""
Snapshot view of everyone currently in Max Custody in TDOC Facilities
"""

from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.state.dataset_config import ANALYST_VIEWS_DATASET
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

US_TN_MAX_STAYS_VIEW_NAME = "us_tn_max_stays"

US_TN_MAX_STAYS_VIEW_DESCRIPTION = (
    """Snapshot view of everyone currently in Max Custody in TDOC Facilities"""
)

US_TN_MAX_STAYS_QUERY_TEMPLATE = """
    WITH max_today as (
         SELECT
           c.*,
           facility_id,
           unit_id,
           external_id,
           JSON_EXTRACT_SCALAR(full_name,'$.given_names') AS first_name,
           JSON_EXTRACT_SCALAR(full_name,'$.surname') AS last_name,
           DATE_DIFF(COALESCE(end_date_exclusive, CURRENT_DATE('US/Pacific')), start_date, DAY) as length_of_stay_days,
     FROM `{project_id}.sessions.custody_level_sessions_materialized` c
     INNER JOIN `{project_id}.normalized_state.state_person_external_id`
        USING(person_id, state_code)
     INNER JOIN `{project_id}.normalized_state.state_person`
        USING(person_id, state_code)
     -- TODO(#25107): Remove this join when custody level information aligns with location information
     INNER JOIN (
        SELECT
            OffenderID AS external_id,
            COALESCE(RequestedSiteID, ActualSiteID, AssignedSiteID) AS facility_id,
            COALESCE(RequestedUnitID, ActualUnitID, AssignedUnitID) AS unit_id,
          FROM `{project_id}.us_tn_raw_data_up_to_date_views.CellBedAssignment_latest`
          -- Ensures that someone is actively assigned to a TDOC bed at the moment
          WHERE EndDate IS NULL
          QUALIFY ROW_NUMBER() OVER(PARTITION BY OffenderID ORDER BY CAST(AssignmentDateTime AS DATETIME) DESC) = 1
     )
     USING(external_id)
     WHERE c.state_code = 'US_TN'
       AND c.custody_level = 'MAXIMUM'
       AND c.end_date_exclusive is null
    )
    SELECT
            person_id,
            external_id,
            first_name,
            last_name,
            start_date,
            facility_id AS current_facility_id,
            unit_id AS current_unit_id,
            length_of_stay_days,
            ROUND(length_of_stay_days/365,1) AS length_of_stay_years
    FROM max_today
"""

US_TN_MAX_STAYS_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=ANALYST_VIEWS_DATASET,
    view_id=US_TN_MAX_STAYS_VIEW_NAME,
    description=US_TN_MAX_STAYS_VIEW_DESCRIPTION,
    view_query_template=US_TN_MAX_STAYS_QUERY_TEMPLATE,
    should_materialize=True,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        US_TN_MAX_STAYS_VIEW_BUILDER.build_and_print()
