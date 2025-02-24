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

from recidiviz.calculator.query.bq_utils import (
    exclude_rows_with_missing_fields,
    nonnull_end_date_exclusive_clause,
)
from recidiviz.utils.string import StrictStringFormatter
from recidiviz.validation.views.utils.state_specific_query_strings import (
    state_specific_dataflow_facility_name_transformation,
)

INCARCERATION_POPULATION_PERSON_LEVEL_EXTERNAL_COMPARISON_QUERY_TEMPLATE = """
WITH 
external_data AS (
  -- NOTE: You can replace this part of the query with your own query to test the SELECT query you will use to generate
  -- data to insert into the `incarceration_population_person_level` table.
  SELECT
    state_code,
    person_external_id,
    external_id_type,
    date_of_stay,
    CASE state_code
      WHEN 'US_PA' THEN UPPER(LEFT(facility, 3))
      ELSE facility
    END AS facility
  FROM `{{project_id}}.{{external_accuracy_dataset}}.incarceration_population_person_level_materialized`
), external_data_with_ids AS (
    -- Find the internal person_id for the people in the external data
    SELECT
      external_data.state_code,
      date_of_stay,
      facility,
      external_data.person_external_id,
      COALESCE(CAST(person_id AS STRING), 'UNKNOWN_PERSON') AS person_id
    FROM external_data
    LEFT JOIN `{{project_id}}.{{normalized_state_dataset}}.state_person_external_id` all_state_person_ids
    ON external_data.state_code = all_state_person_ids.state_code AND external_data.person_external_id = all_state_person_ids.external_id
    -- Limit to the correct ID type in states that have multiple
    AND external_data.external_id_type = all_state_person_ids.id_type
),
dates_per_region AS (
    -- Only compare regions and months for which we have external validation data
    SELECT DISTINCT state_code, date_of_stay FROM external_data_with_ids {external_data_required_fields_clause}
),
sanitized_internal_metrics AS (
  SELECT 
    * EXCEPT (facility),
    {state_specific_dataflow_facility_name_transformation},
  FROM (
    SELECT
      dataflow.state_code, 
      date_of_stay, 
      person_external_id, 
      CAST(person_id AS STRING) AS person_id,
      facility,
    FROM `{{project_id}}.{{materialized_metrics_dataset}}.most_recent_incarceration_population_span_metrics_materialized` dataflow
    INNER JOIN dates_per_region dates
      ON dataflow.state_code = dates.state_code
      AND dates.date_of_stay BETWEEN dataflow.start_date_inclusive AND {non_null_end_date}
    WHERE included_in_state_population
    -- For MI, exclude incarceration periods that are TEMPORARY_CUSTODY incarceration periods inferred during normalization
    -- (which have custodial authority set to 'INTERNAL_UNKNOWN' as opposed to 'STATE_PRISON' like all other ingested incarceration periods)
    AND (dataflow.state_code <> 'US_MI' OR custodial_authority = 'STATE_PRISON')
    -- Also exclude inferred temporary custody periods for TN. TN inferred periods use the 'COUNTY' custodial authority, which is used for
    -- ingested periods as well, so instead check incarceration_type (which is 'INTERNAL_UNKNOWN' for inferred periods, and defaults to
    -- 'STATE_PRISON' for all other periods).
    -- TODO(#38505): Revisit these exclusions if we can find another way to prevent these periods from counting toward state populations.
    AND (dataflow.state_code <> 'US_TN' OR incarceration_type = 'STATE_PRISON')
  )
)
SELECT
  state_code,
  state_code AS region_code,
  date_of_stay,
  external_data.person_id AS external_data_person_id,
  internal_data.person_id AS internal_data_person_id,
  external_data.person_external_id AS external_data_person_external_id,
  external_data.facility AS external_facility,
  internal_data.facility AS internal_facility,
FROM
    external_data_with_ids external_data
FULL OUTER JOIN
    sanitized_internal_metrics internal_data
USING (state_code, date_of_stay, person_id)
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
        non_null_end_date=nonnull_end_date_exclusive_clause(
            "dataflow.end_date_exclusive"
        ),
    )
