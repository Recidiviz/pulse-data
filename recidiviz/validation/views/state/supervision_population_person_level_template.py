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

from typing import Set

from recidiviz.calculator.query.bq_utils import (
    exclude_rows_with_missing_fields,
    nonnull_end_date_exclusive_clause,
)
from recidiviz.utils.string import StrictStringFormatter

SUPERVISION_POPULATION_PERSON_LEVEL_EXTERNAL_COMPARISON_QUERY_TEMPLATE = """
WITH 
external_data AS (
  -- NOTE: You can replace this part of the query with your own query to test the SELECT query you will use to generate
  -- data to insert into the `supervision_population_person_level` table.
  SELECT
    region_code,
    person_external_id,
    external_id_type,
    date_of_supervision,
    district,
    supervising_officer,
    supervision_level
  FROM `{{project_id}}.{{external_accuracy_dataset}}.supervision_population_person_level_materialized`
),
external_data_with_ids AS (
    -- Find the internal person_id for the people in the external data
    SELECT
      region_code,
      date_of_supervision,
      district,
      supervising_officer,
      supervision_level,
      external_data.person_external_id,
      COALESCE(CAST(person_id AS STRING), 'UNKNOWN_PERSON') as person_id
    FROM external_data
    LEFT JOIN `{{project_id}}.{{normalized_state_dataset}}.state_person_external_id` all_state_person_ids
    ON region_code = all_state_person_ids.state_code AND external_data.person_external_id = all_state_person_ids.external_id
    -- Limit to the correct ID type in states that have multiple
    AND external_data.external_id_type = all_state_person_ids.id_type
),
dates_per_region AS (
    -- Only compare regions and months for which we have external validation data
    SELECT DISTINCT region_code, date_of_supervision FROM external_data {external_data_required_fields_clause}
),
sanitized_internal_metrics AS (
  SELECT
      state_code AS region_code, 
      date_of_supervision, 
      person_external_id, 
      CAST(person_id AS STRING) AS person_id,
      CASE  
        -- TODO(#3830): Check back in with ID to see if they have rectified their historical data. If so, we can remove
        --  this case.
        -- US_ID - All low supervision unit POs had inconsistent data before July 2020.
        WHEN state_code IN ('US_ID', 'US_IX')
            AND TO_HEX(SHA256(staff.external_id)) IN 
            ('789cd597900acda58c5bd19411f90fb8e2a0ec187431206364d8251c914df136', 
            'a01cca9d9f8b0ddccced52b6253f059ff40a01b771f99391e6dc861d96257abc', 
            '8d9748a2dd0cac176e24914a51a4905117bb26bf41fb1e995368116cd931ff5c', 
            'aa1b0ae429c7878ed99aca291216390d2d7bf9fd051e0da9012cdc1de0827f49') 
            AND date_of_supervision < '2020-07-01' THEN 'LSU'
        WHEN state_code IN ('US_IX')
            AND TO_HEX(SHA256(staff.external_id)) IN 
            ('92b690fedfae7ea8024eb6ea6d53f64cd0a4d20e44acf71417dca4f0d28f5c74', 
            '98224c72c0fac473034984353b622b25993807d617ae4437245626771df20d8d', 
            '92a7194ae5db4ecb83f724e83d0d50b3c216561849b48f4d60946b3b0a301a3a', 
            '620e9c1f98e4730c1968dd7e14627cdff6689e377fa8ff7d5be4fd3540b57543') 
            AND date_of_supervision < '2020-07-01' THEN 'LSU'
        ELSE staff.external_id
      END AS supervising_officer_external_id,
      CASE
        WHEN state_code = 'US_IX' and (supervision_level_raw_text like 'LOW SUPERVISION UNIT%' or supervision_level_raw_text like 'LOW SUPERVSN UNIT%') THEN 'LIMITED SUPERVISION'
        ELSE supervision_level_raw_text
      END AS supervision_level_raw_text,
      CASE
        WHEN state_code IN ('US_MO','US_TN')
          THEN level_1_supervision_location_external_id
        ELSE level_2_supervision_location_external_id
      END AS supervising_district_external_id,
  FROM `{{project_id}}.{{materialized_metrics_dataset}}.most_recent_supervision_population_span_metrics_materialized` dataflow
  LEFT JOIN
    `{{project_id}}.sessions.state_staff_id_to_legacy_supervising_officer_external_id_materialized` staff
  ON
    dataflow.supervising_officer_staff_id = staff.staff_id
  INNER JOIN dates_per_region dates
      ON dataflow.state_code = dates.region_code
      AND dates.date_of_supervision BETWEEN dataflow.start_date_inclusive AND {non_null_end_date}
  WHERE CASE
      WHEN state_code IN ('US_ID', 'US_IX')
      THEN
        -- Idaho only gives us population numbers for folks explicitly on active probation, parole, or dual supervision.
        -- The following groups are folks we consider a part of the SupervisionPopulation even though ID does not:
        --    - `INFORMAL_PROBATION` - although IDOC does not actively supervise these folks, they can be revoked
        --       and otherwise punished as if they were actively on supervision.
        --    - `INTERNAL_UNKNOWN` - vast majority of these people are folks with active bench warrants
        supervision_type IN ('PROBATION', 'PAROLE', 'DUAL')
        -- TODO(#3831): Add bit to SupervisionPopulation metric to describe absconsion instead of this filter.
        AND supervising_district_external_id IS NOT NULL
      ELSE TRUE
      END
      AND CASE 
        WHEN state_code = 'US_AZ' 
        -- People who are on an abcsonsion status or who are being supervised in custody are 
        -- not included in Arizona's supervision population reports.
        THEN supervision_level NOT IN ('ABSCONSION','IN_CUSTODY')
      ELSE TRUE
      END
      AND included_in_state_population
  QUALIFY ROW_NUMBER() OVER (
      PARTITION BY state_code, date_of_supervision, person_external_id
      ORDER BY staff.external_id DESC, supervision_level_raw_text DESC, supervising_district_external_id DESC
  ) = 1
)
SELECT
      region_code,
      date_of_supervision,
      external_data.person_id AS external_person_id,
      internal_data.person_id AS internal_person_id,
      external_data.person_external_id AS external_person_external_id,
      internal_data.person_external_id AS internal_person_external_id,
      external_data.district AS external_district,
      internal_data.supervising_district_external_id AS internal_district,
      external_data.supervision_level AS external_supervision_level,
      internal_data.supervision_level_raw_text AS internal_supervision_level,
      external_data.supervising_officer AS external_supervising_officer,
      internal_data.supervising_officer_external_id AS internal_supervising_officer,
FROM
  external_data_with_ids external_data
FULL OUTER JOIN
  sanitized_internal_metrics internal_data
USING(region_code, date_of_supervision, person_id)
{filter_clause}
"""


def supervision_population_person_level_query(
    include_unmatched_people: bool, external_data_required_fields: Set[str]
) -> str:
    """
    Returns a query template for doing person-level supervision population validation against an external dataset.

    Args:
        include_unmatched_people: (bool) Whether to include rows where the internal and
         external datasets disagree about whether this person should be on supervision
         at all on a given day.
        external_data_required_fields: (bool) If there is external data for a given date
         and region but none of the rows have non-null values for these fields it will
         be excluded. For instance, if we want to run a query that compare supervision
         district, this allows us to filter out external data that doesn't have
         supervision district information.
    """
    filter_clause = exclude_rows_with_missing_fields(
        set()
        if include_unmatched_people
        else {"external_data.person_external_id", "internal_data.person_external_id"}
    )
    return StrictStringFormatter().format(
        SUPERVISION_POPULATION_PERSON_LEVEL_EXTERNAL_COMPARISON_QUERY_TEMPLATE,
        filter_clause=filter_clause,
        external_data_required_fields_clause=exclude_rows_with_missing_fields(
            external_data_required_fields
        ),
        non_null_end_date=nonnull_end_date_exclusive_clause(
            "dataflow.end_date_exclusive"
        ),
    )
