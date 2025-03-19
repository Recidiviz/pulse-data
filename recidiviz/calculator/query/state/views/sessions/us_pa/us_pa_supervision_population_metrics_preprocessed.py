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
"""PA State-specific preprocessing for joining with dataflow sessions"""

from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.bq_utils import revert_nonnull_end_date_clause
from recidiviz.calculator.query.sessions_query_fragments import (
    create_sub_sessions_with_attributes,
)
from recidiviz.calculator.query.state.dataset_config import (
    DATAFLOW_METRICS_MATERIALIZED_DATASET,
    SESSIONS_DATASET,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

US_PA_SUPERVISION_POPULATION_METRICS_PREPROCESSED_VIEW_NAME = (
    "us_pa_supervision_population_metrics_preprocessed"
)

US_PA_SUPERVISION_POPULATION_METRICS_PREPROCESSED_VIEW_DESCRIPTION = """PA State-specific preprocessing for joining with dataflow sessions:
- Recategorizes CCC overlap with supervision as COMMUNITY_CONFINEMENT
- Recategorizes FAST district location as ABSCONSION
"""

US_PA_SUPERVISION_POPULATION_METRICS_PREPROCESSED_QUERY_TEMPLATE = rf"""
    -- TODO(#15613): Remove PA community confinement recategorization when hydrated in population metrics
    WITH overlapping_periods_cte AS
    (
    SELECT
        person_id,
        start_date_inclusive AS start_date,
        end_date_exclusive,
        metric_type AS metric_source,
        s.state_code,
        IF(included_in_state_population, 'SUPERVISION', 'SUPERVISION_OUT_OF_STATE') AS compartment_level_1,
        supervision_type as compartment_level_2,
        CONCAT(COALESCE(level_1_supervision_location_external_id,'EXTERNAL_UNKNOWN'),'|', COALESCE(level_2_supervision_location_external_id,'EXTERNAL_UNKNOWN')) AS compartment_location,
        CAST(NULL AS STRING) AS facility,
        COALESCE(level_1_supervision_location_external_id,'EXTERNAL_UNKNOWN') AS supervision_office,
        COALESCE(level_2_supervision_location_external_id,'EXTERNAL_UNKNOWN') AS supervision_district,
        supervision_level AS correctional_level,
        supervision_level_raw_text AS correctional_level_raw_text,
        CAST(NULL AS STRING) AS housing_unit,
        CAST(NULL AS STRING) AS housing_unit_category,
        CAST(NULL AS STRING) AS housing_unit_type,
        CAST(NULL AS STRING) AS housing_unit_type_raw_text,
        staff.external_id AS supervising_officer_external_id,
        case_type,
        case_type_raw_text,
        prioritized_race_or_ethnicity,
        gender,
        custodial_authority,
    FROM `{{project_id}}.{{materialized_metrics_dataset}}.most_recent_supervision_population_span_metrics_materialized` s
    LEFT JOIN
        `{{project_id}}.sessions.state_staff_id_to_legacy_supervising_officer_external_id_materialized` staff
    ON
        s.supervising_officer_staff_id = staff.staff_id
    WHERE s.state_code = 'US_PA'

    UNION ALL

    SELECT
        person_id,
        start_date_inclusive AS start_date,
        end_date_exclusive,
        CAST(NULL AS STRING) AS metric_source,
        state_code,
        CAST(NULL AS STRING) AS compartment_level_1,
        CAST(NULL AS STRING) AS compartment_level_2,
        CAST(NULL AS STRING) AS compartment_location,
        facility,
        CAST(NULL AS STRING) AS supervision_office,
        CAST(NULL AS STRING) AS supervision_district,
        CAST(NULL AS STRING) AS correctional_level,
        CAST(NULL AS STRING) AS correctional_level_raw_text,
        housing_unit,
        housing_unit_category,
        housing_unit_type,
        housing_unit_type_raw_text,
        CAST(NULL AS STRING) AS supervising_officer_external_id,
        CAST(NULL AS STRING) AS case_type,
        CAST(NULL AS STRING) AS case_type_raw_text,
        CAST(NULL AS STRING) AS prioritized_race_or_ethnicity,
        CAST(NULL AS STRING) AS gender,
        custodial_authority,
    FROM `{{project_id}}.{{materialized_metrics_dataset}}.most_recent_incarceration_population_span_metrics_materialized` s
    WHERE state_code = 'US_PA'
        AND NOT included_in_state_population
        AND REGEXP_CONTAINS(facility, "^[123]\\d\\d\\D*")
    )
    ,
    {create_sub_sessions_with_attributes(table_name='overlapping_periods_cte', use_magic_date_end_dates=True,
                                         end_date_field_name='end_date_exclusive')}
    ,
    sub_sessions_with_attributes_dedup AS
    (
    SELECT
        * EXCEPT(compartment_level_2, facility),
        IF(is_ccc_overlap, 'COMMUNITY_CONFINEMENT', compartment_level_2) AS compartment_level_2,
        IF(is_ccc_overlap, ccc_facility, NULL) AS facility,
    FROM
        (
        SELECT
            *,
            -- If we have a duplicate with a NULL value for metric_source it is a CCC overlap because this field is only NULL for the CCC sessions.
            LOGICAL_OR(metric_source IS NULL) OVER(PARTITION BY person_id, state_code, start_date, end_date_exclusive)
                AND LOGICAL_OR(compartment_level_1 = 'SUPERVISION') OVER(PARTITION BY person_id, state_code, start_date, end_date_exclusive) AS is_ccc_overlap,
            -- Get the name of the facility from CCC sessions
            FIRST_VALUE(facility) OVER(PARTITION BY person_id, state_code, start_date, end_date_exclusive ORDER BY IF(facility IS NULL,1,0)) AS ccc_facility,
        FROM sub_sessions_with_attributes
        )
    -- Drop rows representing time on CCC from the incarceration metric as the supervision metric row is now recategorized
    WHERE metric_source IS NOT NULL
    )
    SELECT
        person_id,
        start_date,
        {revert_nonnull_end_date_clause('end_date_exclusive')} AS end_date_exclusive,
        metric_source,
        state_code,
        compartment_level_1,
        compartment_level_2,
        compartment_location,
        facility,
        supervision_office,
        supervision_district,
        correctional_level,
        correctional_level_raw_text,
        housing_unit,
        housing_unit_category,
        housing_unit_type,
        housing_unit_type_raw_text,
        supervising_officer_external_id,
        case_type,
        case_type_raw_text,
        prioritized_race_or_ethnicity,
        gender,
        custodial_authority
    FROM sub_sessions_with_attributes_dedup
"""

US_PA_SUPERVISION_POPULATION_METRICS_PREPROCESSED_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=SESSIONS_DATASET,
    view_id=US_PA_SUPERVISION_POPULATION_METRICS_PREPROCESSED_VIEW_NAME,
    view_query_template=US_PA_SUPERVISION_POPULATION_METRICS_PREPROCESSED_QUERY_TEMPLATE,
    description=US_PA_SUPERVISION_POPULATION_METRICS_PREPROCESSED_VIEW_DESCRIPTION,
    materialized_metrics_dataset=DATAFLOW_METRICS_MATERIALIZED_DATASET,
    should_materialize=True,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        US_PA_SUPERVISION_POPULATION_METRICS_PREPROCESSED_VIEW_BUILDER.build_and_print()
