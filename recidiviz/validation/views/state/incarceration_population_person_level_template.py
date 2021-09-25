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
"""A query template for doing person-level incarceration population validation against an external dataset."""

INCARCERATION_POPULATION_PERSON_LEVEL_EXTERNAL_COMPARISON_QUERY_TEMPLATE = """
WITH 
external_data AS (
  -- NOTE: You can replace this part of the query with your own query to test the SELECT query you will use to generate
  -- data to insert into the `incarceration_population_person_level` table.
  SELECT region_code, person_external_id, date_of_stay, facility
  FROM `{{project_id}}.{{external_accuracy_dataset}}.incarceration_population_person_level`
), external_data_with_ids AS (
    -- Find the internal person_id for the people in the external data
    SELECT region_code, date_of_stay, facility, external_data.person_external_id, person_id
    FROM external_data
    LEFT JOIN `{{project_id}}.{{state_base_dataset}}.state_person_external_id` all_state_person_ids
    ON region_code = all_state_person_ids.state_code AND external_data.person_external_id = all_state_person_ids.external_id
    -- Limit to 'US_PA_CONT' id_type for US_PA
    WHERE region_code != 'US_PA' or id_type = 'US_PA_CONT'
),
sanitized_internal_metrics AS (
  SELECT
      state_code AS region_code, 
      date_of_stay, 
      person_external_id, 
      person_id,
      # Idaho provided metrics only have distinct counts for Jefferson and Bonneville jails. All of the rest are grouped
      # together under "Count Jails". Although internally, we fine grained populations for jails across the state, to
      # match the Idaho report, group all non Jefferson/Bonneville jails under "County Jails"
      IF(state_code = 'US_ID' AND
         (facility LIKE '%SHERIFF%' OR facility LIKE '%JAIL%')
          AND facility NOT LIKE 'BONNEVILLE%'
          AND facility NOT LIKE 'JEFFERSON%',
          'COUNTY JAILS', facility) AS facility, 
   FROM `{{project_id}}.{{materialized_metrics_dataset}}.most_recent_incarceration_population_metrics_included_in_state_population_materialized`
),
internal_metrics_for_valid_regions_and_dates AS (
  SELECT * FROM
  -- Only compare regions and months for which we have external validation data
  (SELECT DISTINCT region_code, date_of_stay FROM external_data_with_ids)
  LEFT JOIN sanitized_internal_metrics
  USING (region_code, date_of_stay)
)
SELECT
  region_code,
  date_of_stay,
  CAST(external_data.person_id AS STRING) AS external_data_person_id,
  CAST(internal_data.person_id AS STRING) AS internal_data_person_id,
  external_data.person_external_id AS external_data_person_external_id,
  external_data.facility AS external_facility,
  internal_data.facility AS internal_facility,
FROM
    external_data_with_ids external_data
FULL OUTER JOIN
    internal_metrics_for_valid_regions_and_dates internal_data
USING (region_code, date_of_stay, person_id)
"""


def incarceration_population_person_level_query(include_unmatched_people: bool) -> str:
    """
    Returns a query template for doing person-level incarceration population validation against an external dataset.

    Args:
        include_unmatched_people: (bool) Whether to include rows where the internal and external datasets disagree about
         whether this person was incarcerated at all on a given day.
    """
    filter_clause = (
        ""
        if include_unmatched_people
        else "WHERE external_data.person_external_id IS NOT NULL AND internal_data.person_external_id IS NOT NULL"
    )
    return (
        INCARCERATION_POPULATION_PERSON_LEVEL_EXTERNAL_COMPARISON_QUERY_TEMPLATE.format(
            filter_clause=filter_clause
        )
    )
