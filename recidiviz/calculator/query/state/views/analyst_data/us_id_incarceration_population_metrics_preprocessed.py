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
"""State-specific preprocessing for joining with dataflow sessions"""

from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.state.dataset_config import (
    ANALYST_VIEWS_DATASET,
    DATAFLOW_METRICS_MATERIALIZED_DATASET,
    STATIC_REFERENCE_TABLES_DATASET,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

US_ID_INCARCERATION_POPULATION_METRICS_PREPROCESSED_VIEW_NAME = (
    "us_id_incarceration_population_metrics_preprocessed"
)

US_ID_INCARCERATION_POPULATION_METRICS_PREPROCESSED_VIEW_DESCRIPTION = (
    """State-specific preprocessing for joining with dataflow sessions"""
)

US_ID_INCARCERATION_POPULATION_METRICS_PREPROCESSED_QUERY_TEMPLATE = """
    /*{description}*/
    WITH incarceration_population_cte AS (
        SELECT 
            DISTINCT
            person_id,
            date_of_stay AS date,
            metric_type AS metric_source,
            created_on,
            state_code,
            'INCARCERATION' as compartment_level_1,
            /* TODO(#6126): Investigate ID missing reason for incarceration */
            CASE 
                WHEN specialized_purpose_for_incarceration IN ('GENERAL','PAROLE_BOARD_HOLD','TREATMENT_IN_PRISON') 
                    THEN specialized_purpose_for_incarceration 
                ELSE COALESCE(specialized_purpose_for_incarceration, 'GENERAL') END as compartment_level_2,
            facility AS compartment_location,
            CAST(NULL AS STRING) AS correctional_level,
            CAST(NULL AS STRING) AS supervising_officer_external_id,
            CAST(NULL AS STRING) AS case_type
        FROM
            `{project_id}.{materialized_metrics_dataset}.most_recent_incarceration_population_metrics_included_in_state_population_materialized`
        WHERE state_code = 'US_ID'
    )
    SELECT
        pop.person_id,
        pop.date,
        pop.metric_source,
        pop.created_on,
        pop.state_code,
        CASE
            WHEN facilities.facility IS NULL THEN 'INCARCERATION_OUT_OF_STATE'
            ELSE pop.compartment_level_1
        END AS compartment_level_1,
        pop.compartment_level_2,
        pop.compartment_location,
        pop.correctional_level,
        pop.supervising_officer_external_id,
        pop.case_type
    FROM incarceration_population_cte pop
    LEFT JOIN `{project_id}.{static_reference_dataset}.state_incarceration_facilities` facilities
        ON pop.compartment_location = facilities.facility
"""

US_ID_INCARCERATION_POPULATION_METRICS_PREPROCESSED_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=ANALYST_VIEWS_DATASET,
    view_id=US_ID_INCARCERATION_POPULATION_METRICS_PREPROCESSED_VIEW_NAME,
    view_query_template=US_ID_INCARCERATION_POPULATION_METRICS_PREPROCESSED_QUERY_TEMPLATE,
    description=US_ID_INCARCERATION_POPULATION_METRICS_PREPROCESSED_VIEW_DESCRIPTION,
    materialized_metrics_dataset=DATAFLOW_METRICS_MATERIALIZED_DATASET,
    static_reference_dataset=STATIC_REFERENCE_TABLES_DATASET,
    should_materialize=True,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        US_ID_INCARCERATION_POPULATION_METRICS_PREPROCESSED_VIEW_BUILDER.build_and_print()
