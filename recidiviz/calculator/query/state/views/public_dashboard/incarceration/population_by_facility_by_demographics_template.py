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
"""Shared code for views of most recent incarceration population by facility and demographics."""
from recidiviz.calculator.query import bq_utils
from recidiviz.calculator.query.state import (
    dataset_config,
    state_specific_query_strings,
)
from recidiviz.metrics.metric_big_query_view import MetricBigQueryViewBuilder

POPULATION_BY_FACILITY_BY_DEMOGRAPHICS_VIEW_QUERY_TEMPLATE = """
    /*{description}*/
    WITH facility_names AS (
        SELECT
        state_code,
        person_id,
        date_of_stay,
        COALESCE(facility_shorthand, facility, 'INTERNAL_UNKNOWN') as facility,
        {state_specific_race_or_ethnicity_groupings},
        gender,
        age_bucket
      FROM
        `{project_id}.{reference_views_dataset}.single_day_incarceration_population_for_spotlight_materialized`
      LEFT JOIN
        `{project_id}.{static_reference_dataset}.state_incarceration_facility_capacity`
      USING (state_code, facility)
      WHERE {facility_type_filter}
    ), facility_mapping AS (
        SELECT * REPLACE({state_specific_facility_mapping})
        FROM facility_names
    ), unnested_dimensions AS (
        SELECT
        state_code,
        person_id,
        date_of_stay,
        facility,
        race_or_ethnicity,
        gender,
        age_bucket
      FROM
        facility_mapping,
        {facility_dimension},
        {unnested_race_or_ethnicity_dimension},
        {gender_dimension},
        {age_dimension}
    )

    SELECT
      state_code,
      date_of_stay,
      facility,
      race_or_ethnicity,
      gender,
      age_bucket,
      COUNT(DISTINCT(person_id)) as total_population
    FROM unnested_dimensions
    WHERE (race_or_ethnicity != 'ALL' AND gender = 'ALL' AND age_bucket = 'ALL') -- Race breakdown
      OR (race_or_ethnicity = 'ALL' AND gender != 'ALL' AND age_bucket = 'ALL') -- Gender breakdown
      OR (race_or_ethnicity = 'ALL' AND gender = 'ALL' AND age_bucket != 'ALL') -- Age breakdown
      OR (facility != 'ALL' AND race_or_ethnicity = 'ALL' AND gender = 'ALL' AND age_bucket = 'ALL') -- Facility breakdown
      OR (facility = 'ALL' AND race_or_ethnicity = 'ALL' AND gender = 'ALL' AND age_bucket = 'ALL') -- State-wide count
    GROUP BY state_code, date_of_stay,  facility, race_or_ethnicity, gender, age_bucket
    ORDER BY state_code, date_of_stay, facility, race_or_ethnicity, gender, age_bucket
    """


def get_view_builder(
    view_id: str,
    description: str,
    facility_type: state_specific_query_strings.SpotlightFacilityType,
) -> MetricBigQueryViewBuilder:
    """Retrieves an incarceration population view builder filtered by facility type"""
    return MetricBigQueryViewBuilder(
        view_id=view_id,
        description=description,
        dataset_id=dataset_config.PUBLIC_DASHBOARD_VIEWS_DATASET,
        view_query_template=POPULATION_BY_FACILITY_BY_DEMOGRAPHICS_VIEW_QUERY_TEMPLATE,
        dimensions=(
            "state_code",
            "date_of_stay",
            "facility",
            "race_or_ethnicity",
            "gender",
            "age_bucket",
        ),
        static_reference_dataset=dataset_config.STATIC_REFERENCE_TABLES_DATASET,
        reference_views_dataset=dataset_config.REFERENCE_VIEWS_DATASET,
        unnested_race_or_ethnicity_dimension=bq_utils.unnest_column(
            "race_or_ethnicity", "race_or_ethnicity"
        ),
        gender_dimension=bq_utils.unnest_column("gender", "gender"),
        age_dimension=bq_utils.unnest_column("age_bucket", "age_bucket"),
        facility_dimension=bq_utils.unnest_column("facility", "facility"),
        state_specific_race_or_ethnicity_groupings=state_specific_query_strings.state_specific_race_or_ethnicity_groupings(
            race_or_ethnicity_column="prioritized_race_or_ethnicity"
        ),
        state_specific_facility_mapping=state_specific_query_strings.spotlight_state_specific_facility(),
        facility_type_filter=state_specific_query_strings.spotlight_state_specific_facility_filter(
            facility_type=facility_type
        ),
    )
