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
"""A query template for doing person-level recidivism validation against an external dataset."""

# pylint: disable=trailing-whitespace, line-too-long
RECIDIVISM_PERSON_LEVEL_EXTERNAL_COMPARISON_QUERY_TEMPLATE = \
    """
    WITH external_data AS (
      -- NOTE: You can replace this part of the query with your own query to test the SELECT query you will use to generate
      -- data to insert into the `recidivism_person_level` table.
      SELECT region_code, release_cohort, follow_up_period, person_external_id, recidivated 
      FROM `{{project_id}}.{{external_accuracy_dataset}}.recidivism_person_level`
    ), releases AS (
      SELECT
        state_code,
        release_cohort,
        follow_up_period,
        recidivated_releases,
        person_external_id,
        ROW_NUMBER() OVER (PARTITION BY state_code, release_cohort, follow_up_period, person_id ORDER BY release_date ASC, recidivated_releases DESC) as release_order
        FROM `{{project_id}}.{{materialized_metrics_dataset}}.most_recent_recidivism_rate_metrics`
      WHERE methodology = 'EVENT'
    ), internal_metrics AS (
      SELECT
        state_code AS region_code,
        release_cohort,
        follow_up_period,
        recidivated_releases as recidivated,
        person_external_id
      FROM releases
      WHERE release_order = 1
    ), internal_metrics_for_valid_regions_and_dates AS (
      SELECT * FROM
      -- Only compare regions and years for which we have external validation data
      (SELECT DISTINCT region_code, release_cohort, follow_up_period FROM external_data)
      LEFT JOIN internal_metrics
      USING (region_code, release_cohort, follow_up_period)
    )
    
    
    SELECT
      region_code,
      release_cohort,
      follow_up_period,
      external_data.person_external_id as external_person_external_id,
      internal_data.person_external_id as internal_person_external_id,
      external_data.recidivated as external_recidivated,
      internal_data.recidivated as internal_recidivated
    FROM
     external_data
    FULL OUTER JOIN 
      internal_metrics_for_valid_regions_and_dates internal_data
    USING (region_code, release_cohort, follow_up_period, person_external_id)
    {filter_clause}
    ORDER BY region_code, release_cohort, follow_up_period
"""


def recidivism_person_level_query(include_unmatched_people: bool) -> str:
    """
    Returns a query template for doing person-level recidivism validation against an external dataset.

    Args:
        include_unmatched_people: (bool) Whether to include rows where the internal and external datasets disagree about
         whether this person was included in a given release cohort.
    """
    filter_clause = \
        "" if include_unmatched_people \
        else "WHERE external_data.person_external_id IS NOT NULL AND internal_data.person_external_id IS NOT NULL"
    return RECIDIVISM_PERSON_LEVEL_EXTERNAL_COMPARISON_QUERY_TEMPLATE.format(filter_clause=filter_clause)
