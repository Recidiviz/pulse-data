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
"""Incarceration facility population by age by day compared to the facility's goal capacity"""
# pylint: disable=trailing-whitespace
from recidiviz.big_query.big_query_view import (
    SimpleBigQueryViewBuilder,
    SimpleBigQueryViewBuilder,
)
from recidiviz.calculator.query import bq_utils
from recidiviz.calculator.query.state import (
    dataset_config,
    state_specific_query_strings,
)
from recidiviz.calculator.query.state.dataset_config import (
    STATE_BASE_DATASET,
    DATAFLOW_METRICS_MATERIALIZED_DATASET,
    STATIC_REFERENCE_TABLES_DATASET,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

FACILITY_POPULATION_BY_AGE_WITH_CAPACITY_BY_DAY_VIEW_NAME = (
    "facility_population_by_age_with_capacity_by_day"
)

FACILITY_POPULATION_BY_AGE_WITH_CAPACITY_BY_DAY_DESCRIPTION = """ Incarceration facility population 
by age by day compared to the facility's goal capacity"""

FACILITY_POPULATION_BY_AGE_WITH_CAPACITY_BY_DAY_QUERY_TEMPLATE = """
    /*{description}*/
    SELECT
      state_code,
      COALESCE(facility_shorthand, facility, 'INTERNAL_UNKNOWN') as facility,
      date_of_stay,
      pop_no_age,
      pop_0_19,
      pop_20_44,
      pop_45_54,
      pop_55_64,
      pop_65_74,
      pop_75_84,
      pop_over_85,
      pop_over_50,
      pop_over_60,
      total_population,
      capacity as goal_capacity,
      (IEEE_DIVIDE(total_population, capacity) * 100) as percent_goal_capacity
    FROM
        (SELECT
          state_code,
          facility,
          date_of_stay,
          COUNT(DISTINCT IF(age IS NULL, person_id, NULL)) as pop_no_age,
          COUNT(DISTINCT IF(age < 20, person_id, NULL)) as pop_0_19,
          COUNT(DISTINCT IF(age >= 20 and age < 45, person_id, NULL)) as pop_20_44,
          COUNT(DISTINCT IF(age >= 45 and age < 55, person_id, NULL)) as pop_45_54,
          COUNT(DISTINCT IF(age >= 55 and age < 65, person_id, NULL)) as pop_55_64,
          COUNT(DISTINCT IF(age >= 65 and age < 75, person_id, NULL)) as pop_65_74,
          COUNT(DISTINCT IF(age >= 75 and age < 85, person_id, NULL)) as pop_75_84,
          COUNT(DISTINCT IF(age >= 85, person_id, NULL)) as pop_over_85,
          COUNT(DISTINCT IF(age >= 50, person_id, NULL)) as pop_over_50,
          COUNT(DISTINCT IF(age >= 60, person_id, NULL)) as pop_over_60,
          COUNT(DISTINCT(person_id)) as total_population
        FROM
          (SELECT
            state_code,
            date_of_stay,
            facility,
            person_id,
            (DATE_DIFF(date_of_stay, birthdate, DAY)) / 365.25 as age
          FROM
            (SELECT
              state_code,
              person_id,
              facility,
              date_of_stay,
            FROM
              `{project_id}.{materialized_metrics_dataset}.most_recent_incarceration_population_metrics_materialized`
            WHERE {state_specific_facility_exclusion}
            AND date_of_stay > DATE_SUB(CURRENT_DATE(), INTERVAL 1 YEAR)),
          {facility_dimension}
          LEFT JOIN
            `{project_id}.{base_dataset}.state_person` 
            USING (state_code, person_id))
        GROUP BY state_code, facility, date_of_stay) 
    LEFT JOIN
      `{project_id}.{static_reference_dataset}.state_incarceration_facility_capacity`
    USING (state_code, facility)
    ORDER BY state_code, facility, date_of_stay
"""

FACILITY_POPULATION_BY_AGE_WITH_CAPACITY_BY_DAY_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=dataset_config.COVID_REPORT_DATASET,
    view_id=FACILITY_POPULATION_BY_AGE_WITH_CAPACITY_BY_DAY_VIEW_NAME,
    view_query_template=FACILITY_POPULATION_BY_AGE_WITH_CAPACITY_BY_DAY_QUERY_TEMPLATE,
    description=FACILITY_POPULATION_BY_AGE_WITH_CAPACITY_BY_DAY_DESCRIPTION,
    base_dataset=STATE_BASE_DATASET,
    materialized_metrics_dataset=DATAFLOW_METRICS_MATERIALIZED_DATASET,
    static_reference_dataset=STATIC_REFERENCE_TABLES_DATASET,
    reference_views_dataset=dataset_config.REFERENCE_VIEWS_DATASET,
    facility_dimension=bq_utils.unnest_column("facility", "facility"),
    state_specific_facility_exclusion=state_specific_query_strings.state_specific_facility_exclusion(),
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        FACILITY_POPULATION_BY_AGE_WITH_CAPACITY_BY_DAY_VIEW_BUILDER.build_and_print()
