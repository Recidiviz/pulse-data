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
"""A query template for doing person-level supervision population validation against an external dataset."""

SUPERVISION_POPULATION_PERSON_LEVEL_EXTERNAL_COMPARISON_QUERY_TEMPLATE = """
WITH 
external_data AS (
  -- NOTE: You can replace this part of the query with your own query to test the SELECT query you will use to generate
  -- data to insert into the `supervision_population_person_level` table.
  SELECT region_code, person_external_id, date_of_supervision, district, supervising_officer, supervision_level
  FROM `{{project_id}}.{{external_accuracy_dataset}}.supervision_population_person_level`
),
sanitized_internal_metrics AS (
  SELECT
      state_code AS region_code, 
      date_of_supervision, 
      person_external_id, 
      CASE  
        # TODO(#3830): Check back in with ID to see if they have rectified their historical data. If so, we can remove
        #  this case.
        # US_ID - All low supervision unit POs had inconsistent data before July 2020.
        WHEN state_code = 'US_ID' 
            AND supervising_officer_external_id IN ('REGARCIA', 'CAMCDONA', 'SLADUKE', 'COLIMSUP') 
            AND date_of_supervision < '2020-07-01' THEN 'LSU'
        ELSE supervising_officer_external_id
      END AS supervising_officer_external_id,
      supervision_level_raw_text,
      supervising_district_external_id,
      ROW_NUMBER() OVER (PARTITION BY state_code, date_of_supervision, person_external_id
                        ORDER BY supervising_officer_external_id DESC, supervision_level_raw_text DESC, supervising_district_external_id DESC)
                        AS inclusion_order
   FROM `{{project_id}}.{{materialized_metrics_dataset}}.most_recent_supervision_population_metrics_materialized`
   WHERE CASE
     WHEN state_code = 'US_ID' 
     THEN
       # Idaho only gives us population numbers for folks explicitly on active probation, parole, or dual supervision.
       # The following groups are folks we consider a part of the SupervisionPopulation even though ID does not:
       #    - `INFORMAL_PROBATION` - although IDOC does not actively supervise these folks, they can be revoked 
       #       and otherwise punished as if they were actively on supervision. 
       #    - `INTERNAL_UNKNOWN` - vast majority of these people are folks with active bench warrants
       supervision_type IN ('PROBATION', 'PAROLE', 'DUAL') 
       # TODO(#3831): Add bit to SupervisionPopulation metric to describe absconsion instead of this filter. 
       AND supervising_district_external_id IS NOT NULL
     WHEN state_code = 'US_ND'
     THEN
       # North Dakota only gives us supervision validation data for those on Parole (both regular and IC) currently.
       supervision_type IN ('PAROLE', 'DUAL')
     ELSE TRUE
   END
),
internal_metrics_for_valid_regions_and_dates AS (
  SELECT * FROM
  -- Only compare regions and months for which we have external validation data
  (SELECT DISTINCT region_code, date_of_supervision FROM external_data)
  LEFT JOIN
    sanitized_internal_metrics
  USING (region_code, date_of_supervision)
  WHERE inclusion_order = 1
)
SELECT
      region_code,
      date_of_supervision,
      external_data.person_external_id AS external_person_external_id,
      internal_data.person_external_id AS internal_person_external_id,
      external_data.district AS external_district,
      internal_data.supervising_district_external_id AS internal_district,
      external_data.supervision_level AS external_supervision_level,
      internal_data.supervision_level_raw_text AS internal_supervision_level,
      external_data.supervising_officer AS external_supervising_officer,
      internal_data.supervising_officer_external_id AS internal_supervising_officer,
FROM
  external_data
FULL OUTER JOIN
  internal_metrics_for_valid_regions_and_dates internal_data
USING(region_code, date_of_supervision, person_external_id)
{filter_clause}
ORDER BY region_code, date_of_supervision, person_external_id;
"""


def supervision_population_person_level_query(include_unmatched_people: bool) -> str:
    """
    Returns a query template for doing person-level supervision population validation against an external dataset.

    Args:
        include_unmatched_people: (bool) Whether to include rows where the internal and external datasets disagree about
         whether this person should be on supervision at all on a given day.
    """
    filter_clause = (
        ""
        if include_unmatched_people
        else "WHERE external_data.person_external_id IS NOT NULL AND internal_data.person_external_id IS NOT NULL"
    )
    return (
        SUPERVISION_POPULATION_PERSON_LEVEL_EXTERNAL_COMPARISON_QUERY_TEMPLATE.format(
            filter_clause=filter_clause
        )
    )
