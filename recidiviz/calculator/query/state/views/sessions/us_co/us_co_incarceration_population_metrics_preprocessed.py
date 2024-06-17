# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2022 Recidiviz, Inc.
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
"""State-specific preprocessing for joining with dataflow sessions"""

from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.state.dataset_config import (
    DATAFLOW_METRICS_MATERIALIZED_DATASET,
    SESSIONS_DATASET,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

US_CO_INCARCERATION_POPULATION_METRICS_PREPROCESSED_VIEW_NAME = (
    "us_co_incarceration_population_metrics_preprocessed"
)

US_CO_INCARCERATION_POPULATION_METRICS_PREPROCESSED_VIEW_DESCRIPTION = (
    """State-specific preprocessing for joining with dataflow sessions"""
)

US_CO_INCARCERATION_POPULATION_METRICS_PREPROCESSED_QUERY_TEMPLATE = """
    -- TODO(#15611): Remove preprocessing file when community confinement facilities are flagged in sessions
    WITH incarceration_population_cte AS (
        SELECT
            person_id,
            start_date_inclusive AS start_date,
            end_date_exclusive,
            metric_type AS metric_source,
            state_code,
            IF(included_in_state_population, 'INCARCERATION', 'INCARCERATION_NOT_INCLUDED_IN_STATE') AS compartment_level_1,
            COALESCE(purpose_for_incarceration, 'GENERAL') AS compartment_level_2,
            COALESCE(facility, 'EXTERNAL_UNKNOWN') AS compartment_location,
            COALESCE(facility, 'EXTERNAL_UNKNOWN') AS facility,
            CAST(NULL AS STRING) AS supervision_office,
            CAST(NULL AS STRING) AS supervision_district,
            custody_level AS correctional_level,
            custody_level_raw_text AS correctional_level_raw_text,
            housing_unit,
            housing_unit_category,
            housing_unit_type,
            housing_unit_type_raw_text,
            CAST(NULL AS STRING) AS supervising_officer_external_id,
            CAST(NULL AS STRING) AS case_type,
            prioritized_race_or_ethnicity,
            gender,
        FROM
            `{project_id}.{materialized_metrics_dataset}.most_recent_incarceration_population_span_metrics_materialized`
        WHERE
            state_code='US_CO'
    )
    SELECT
        pop.person_id,
        pop.start_date,
        pop.end_date_exclusive,
        pop.metric_source,
        pop.state_code,
        pop.compartment_level_1,
        IF(facilities.facility IS NULL, pop.compartment_level_2, 'COMMUNITY_CONFINEMENT')
            AS compartment_level_2,
        pop.compartment_location,
        pop.facility,
        pop.supervision_office,
        pop.supervision_district,
        pop.correctional_level,
        pop.correctional_level_raw_text,
        pop.housing_unit,
        pop.housing_unit_category,
        pop.housing_unit_type,
        pop.housing_unit_type_raw_text,
        pop.supervising_officer_external_id,
        pop.case_type,
        pop.prioritized_race_or_ethnicity,
        pop.gender,
    FROM incarceration_population_cte pop
    LEFT JOIN (
      --- TODO(#14601): Replace with common facility reference table
      SELECT DISTINCT facility,  state_code, "COMMUNITY_CORRECTIONS" as facility_type,
      FROM `{project_id}.normalized_state.state_incarceration_period`
      WHERE state_code = "US_CO" AND housing_unit LIKE '%COMMUNITY%' AND facility != 'ISP-INMATE'
    ) facilities
        ON pop.facility = facilities.facility AND pop.state_code = facilities.state_code
        AND pop.compartment_level_1 = 'INCARCERATION'
"""

US_CO_INCARCERATION_POPULATION_METRICS_PREPROCESSED_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=SESSIONS_DATASET,
    view_id=US_CO_INCARCERATION_POPULATION_METRICS_PREPROCESSED_VIEW_NAME,
    view_query_template=US_CO_INCARCERATION_POPULATION_METRICS_PREPROCESSED_QUERY_TEMPLATE,
    description=US_CO_INCARCERATION_POPULATION_METRICS_PREPROCESSED_VIEW_DESCRIPTION,
    materialized_metrics_dataset=DATAFLOW_METRICS_MATERIALIZED_DATASET,
    should_materialize=True,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        US_CO_INCARCERATION_POPULATION_METRICS_PREPROCESSED_VIEW_BUILDER.build_and_print()
