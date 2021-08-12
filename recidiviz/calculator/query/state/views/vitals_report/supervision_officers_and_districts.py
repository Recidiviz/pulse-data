# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2021 Recidiviz, Inc.
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
"""Supervision officer to district mapping"""
from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.state import (
    dataset_config,
    state_specific_query_strings,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

SUPERVISION_OFFICERS_AND_DISTRICTS_VIEW_NAME = "supervision_officers_and_districts"

SUPERVISION_OFFICERS_AND_DISTRICTS_DESCRIPTION = """
Mapping of supervision officers and their districts
"""

enabled_states = ("US_ND", "US_ID")

SUPERVISION_OFFICERS_AND_DISTRICTS_QUERY_TEMPLATE = f"""
    /*{{description}}*/
   WITH us_id_roster AS (
        SELECT DISTINCT 
            'US_ID' AS state_code,
            external_id AS supervising_officer_external_id, 
            district AS supervising_district_external_id
        FROM `{{project_id}}.{{static_reference_tables}}.us_id_roster`
   )
   SELECT 
        sup_pop.state_code,
        IF(us_id_roster.supervising_district_external_id IS NOT NULL, us_id_roster.supervising_district_external_id, sup_pop.supervising_district_external_id) AS supervising_district_external_id,
        IF(us_id_roster.supervising_officer_external_id IS NOT NULL, us_id_roster.supervising_officer_external_id, sup_pop.supervising_officer_external_id) AS supervising_officer_external_id,
        {{vitals_state_specific_district_id}},
        {{vitals_state_specific_district_name}}
   FROM `{{project_id}}.{{materialized_metrics_dataset}}.most_recent_supervision_population_metrics_materialized` sup_pop
   LEFT JOIN us_id_roster 
      ON sup_pop.state_code = us_id_roster.state_code
      AND sup_pop.supervising_officer_external_id = us_id_roster.supervising_officer_external_id 
   LEFT JOIN `{{project_id}}.{{reference_views_dataset}}.supervision_location_ids_to_names` locations
        ON sup_pop.state_code = locations.state_code
        AND {{vitals_state_specific_join_with_supervision_location_ids}}
   
   WHERE date_of_supervision > DATE_SUB(CURRENT_DATE(), INTERVAL 217 DAY) -- 217 = 210 days back for avgs + 7-day buffer for late data
        AND sup_pop.state_code in {enabled_states}
        AND (
            (us_id_roster.supervising_officer_external_id IS NOT NULL 
                AND us_id_roster.supervising_district_external_id = sup_pop.level_2_supervision_location_external_id
                AND sup_pop.state_code = 'US_ID')
            OR (us_id_roster.supervising_officer_external_id IS NULL AND sup_pop.state_code != 'US_ID')
        )
   GROUP BY 1,2,3,4,5
"""

SUPERVISION_OFFICERS_AND_DISTRICTS_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=dataset_config.VITALS_REPORT_DATASET,
    view_id=SUPERVISION_OFFICERS_AND_DISTRICTS_VIEW_NAME,
    view_query_template=SUPERVISION_OFFICERS_AND_DISTRICTS_QUERY_TEMPLATE,
    description=SUPERVISION_OFFICERS_AND_DISTRICTS_DESCRIPTION,
    materialized_metrics_dataset=dataset_config.DATAFLOW_METRICS_MATERIALIZED_DATASET,
    static_reference_tables=dataset_config.STATIC_REFERENCE_TABLES_DATASET,
    reference_views_dataset=dataset_config.REFERENCE_VIEWS_DATASET,
    vitals_state_specific_district_id=state_specific_query_strings.vitals_state_specific_district_id(
        "sup_pop"
    ),
    vitals_state_specific_district_name=state_specific_query_strings.vitals_state_specific_district_name(
        "sup_pop"
    ),
    vitals_state_specific_join_with_supervision_location_ids=state_specific_query_strings.vitals_state_specific_join_with_supervision_location_ids(
        "sup_pop"
    ),
    should_materialize=True,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        SUPERVISION_OFFICERS_AND_DISTRICTS_VIEW_BUILDER.build_and_print()
