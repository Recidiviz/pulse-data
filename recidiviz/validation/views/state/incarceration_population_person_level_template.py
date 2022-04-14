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

from typing import Set

from recidiviz.calculator.query.bq_utils import exclude_rows_with_missing_fields
from recidiviz.utils.string import StrictStringFormatter

# TODO(#10054): Remove facility normalization once handled in ingest.
from recidiviz.validation.views.utils.state_specific_query_strings import (
    state_specific_dataflow_facility_name_transformation,
)

INCARCERATION_POPULATION_PERSON_LEVEL_EXTERNAL_COMPARISON_QUERY_TEMPLATE = """
WITH 
external_data AS (
  -- NOTE: You can replace this part of the query with your own query to test the SELECT query you will use to generate
  -- data to insert into the `incarceration_population_person_level` table.
  SELECT
    region_code,
    person_external_id,
    date_of_stay,
    CASE region_code
      WHEN 'US_PA' THEN UPPER(LEFT(facility, 3))
      ELSE facility
    END AS facility
  FROM `{{project_id}}.{{external_accuracy_dataset}}.incarceration_population_person_level`
), external_data_with_ids AS (
    -- Find the internal person_id for the people in the external data
    SELECT
      region_code,
      date_of_stay,
      facility,
      external_data.person_external_id,
      COALESCE(CAST(person_id AS STRING), 'UNKNOWN_PERSON') AS person_id
    FROM external_data
    LEFT JOIN `{{project_id}}.{{state_base_dataset}}.state_person_external_id` all_state_person_ids
    ON region_code = all_state_person_ids.state_code AND external_data.person_external_id = all_state_person_ids.external_id
    -- Limit to incarceration IDs in states that have multiple
    AND (region_code != 'US_ND' OR id_type = 'US_ND_ELITE')
    AND (region_code != 'US_PA' OR id_type = 'US_PA_CONT')
),
sanitized_internal_metrics AS (
  SELECT
      state_code AS region_code, 
      date_of_stay, 
      person_external_id, 
      CAST(person_id AS STRING) AS person_id,
      {state_specific_dataflow_facility_name_transformation},
   FROM `{{project_id}}.{{materialized_metrics_dataset}}.most_recent_incarceration_population_metrics_included_in_state_population_materialized`
),
internal_metrics_for_valid_regions_and_dates AS (
  SELECT * FROM
  -- Only compare regions and months for which we have external validation data
  (SELECT DISTINCT region_code, date_of_stay FROM external_data_with_ids {external_data_required_fields_clause})
  LEFT JOIN sanitized_internal_metrics
  USING (region_code, date_of_stay)
)
SELECT
  region_code,
  date_of_stay,
  external_data.person_id AS external_data_person_id,
  internal_data.person_id AS internal_data_person_id,
  external_data.person_external_id AS external_data_person_external_id,
  external_data.facility AS external_facility,
  internal_data.facility AS internal_facility,
FROM
    external_data_with_ids external_data
FULL OUTER JOIN
    internal_metrics_for_valid_regions_and_dates internal_data
USING (region_code, date_of_stay, person_id)
{filter_clause}
"""


def incarceration_population_person_level_query(
    include_unmatched_people: bool, external_data_required_fields: Set[str]
) -> str:
    """
    Returns a query template for doing person-level incarceration population validation against an external dataset.

    Args:
        include_unmatched_people: (bool) Whether to include rows where the internal and
         external datasets disagree about whether this person was incarcerated at all on
         a given day.
        external_data_required_fields: (bool) If there is external data for a given date
         and region but none of the rows have non-null values for these fields it will
         be excluded. For instance, if we want to run a query that compares facility,
         this allows us to filter out external data that doesn't have facility
         information.
    """
    filter_clause = exclude_rows_with_missing_fields(
        set()
        if include_unmatched_people
        else {"external_data.person_external_id", "internal_data.person_external_id"}
    )
    return StrictStringFormatter().format(
        INCARCERATION_POPULATION_PERSON_LEVEL_EXTERNAL_COMPARISON_QUERY_TEMPLATE,
        filter_clause=filter_clause,
        external_data_required_fields_clause=exclude_rows_with_missing_fields(
            external_data_required_fields
        ),
        state_specific_dataflow_facility_name_transformation=state_specific_dataflow_facility_name_transformation(),
    )
