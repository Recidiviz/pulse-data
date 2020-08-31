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

# pylint: disable=trailing-whitespace
INCARCERATION_POPULATION_PERSON_LEVEL_EXTERNAL_COMPARISON_QUERY_TEMPLATE = \
    """
WITH 
external_data AS (
  -- NOTE: You can replace this part of the query with your own query to test the SELECT query you will use to generate
  -- data to insert into the `incarceration_population_person_level` table.
  SELECT region_code, person_external_id, date_of_stay, facility
  FROM `{{project_id}}.{{external_accuracy_dataset}}.incarceration_population_person_level`
),
sanitized_internal_metrics AS (
  SELECT
      state_code AS region_code, 
      date_of_stay, 
      person_external_id, 
      facility,
   FROM `{{project_id}}.{{metrics_dataset}}.incarceration_population_metrics`
   JOIN `{{project_id}}.{{reference_views_dataset}}.most_recent_job_id_by_metric_and_state_code_materialized` job
      USING (state_code, job_id, year, month, metric_period_months)
   WHERE metric_period_months = 0
   AND methodology = 'PERSON'
),
internal_metrics_for_valid_regions_and_dates AS (
  SELECT * FROM
  -- Only compare regions and months for which we have external validation data
  (SELECT DISTINCT region_code, date_of_stay FROM external_data)
  LEFT JOIN
    sanitized_internal_metrics
  USING (region_code, date_of_stay)
)
SELECT
      region_code,
      date_of_stay,
      external_data.person_external_id AS external_person_external_id,
      internal_data.person_external_id AS internal_person_external_id,
      external_data.facility AS external_facility,
      internal_data.facility AS internal_facility,
FROM
  external_data
FULL OUTER JOIN
  internal_metrics_for_valid_regions_and_dates internal_data
USING(region_code, date_of_stay, person_external_id)
{filter_clause}
ORDER BY region_code, date_of_stay, person_external_id;
"""


def incarceration_population_person_level_query(include_unmatched_people: bool) -> str:
    """
    Returns a query template for doing person-level incarceration population validation against an external dataset.

    Args:
        include_unmatched_people: (bool) Whether to include rows where the internal and external datasets disagree about
         whether this person was incarcerated at all on a given day.
    """
    filter_clause = \
        "" if include_unmatched_people \
        else "WHERE external_data.person_external_id IS NOT NULL AND internal_data.person_external_id IS NOT NULL"
    return INCARCERATION_POPULATION_PERSON_LEVEL_EXTERNAL_COMPARISON_QUERY_TEMPLATE.format(filter_clause=filter_clause)
