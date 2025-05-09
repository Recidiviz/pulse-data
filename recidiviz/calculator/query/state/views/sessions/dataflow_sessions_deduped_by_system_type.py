# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2025 Recidiviz, Inc.
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
"""Partially aggregated version of `dataflow_sessions` where sessions with the same compartment_level_1 are collapsed"""

from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.state.dataset_config import SESSIONS_DATASET
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

DATAFLOW_SESSIONS_DEDUPED_BY_SYSTEM_TYPE_VIEW_NAME = (
    "dataflow_sessions_deduped_by_system_type"
)

DATAFLOW_SESSIONS_DEDUPED_BY_SYSTEM_TYPE_VIEW_DESCRIPTION = """This is a view that creates an initial set of sub-sessions
from dataflow sessions with overlapping sessions with the same compartment type de-duplicated.
This view does the following:
1. Unnest attribute arrays in dataflow_sessions
2. Deduplicate overlapping periods with the same system type/compartment level 1
3. Re-aggregate the attribute array
"""

DATAFLOW_SESSIONS_DEDUPED_BY_SYSTEM_TYPE_QUERY_TEMPLATE = """
    WITH session_attributes_unnested AS
    (
    SELECT DISTINCT
        person_id,
        dataflow_session_id,
        state_code,
        start_date,
        end_date_exclusive,
        session_attributes.metric_source,
        session_attributes.compartment_level_1 AS compartment_level_1,
        session_attributes.compartment_level_2 AS compartment_level_2,
        session_attributes.supervising_officer_external_id AS supervising_officer_external_id,
        session_attributes.compartment_location AS compartment_location,
        session_attributes.facility,
        session_attributes.facility_name,
        session_attributes.supervision_office,
        session_attributes.supervision_office_name,
        session_attributes.supervision_district,
        session_attributes.supervision_district_name,
        session_attributes.supervision_region_name,
        session_attributes.correctional_level AS correctional_level,
        session_attributes.correctional_level_raw_text AS correctional_level_raw_text,
        session_attributes.housing_unit AS housing_unit,
        session_attributes.housing_unit_category AS housing_unit_category,
        session_attributes.housing_unit_type AS housing_unit_type,
        session_attributes.housing_unit_type_raw_text AS housing_unit_type_raw_text,
        session_attributes.case_type,
        session_attributes.case_type_raw_text,
        COALESCE(session_attributes.prioritized_race_or_ethnicity,'EXTERNAL_UNKNOWN') AS prioritized_race_or_ethnicity,
        session_attributes.gender,
        session_attributes.custodial_authority,
        last_day_of_data,
    FROM `{project_id}.{sessions_dataset}.dataflow_sessions_materialized`,
    UNNEST(session_attributes) session_attributes
    WHERE session_attributes.compartment_level_1 != 'INCARCERATION_NOT_INCLUDED_IN_STATE'
    )
    ,
    dual_recategorization_cte AS
    (
    SELECT DISTINCT
        person_id,
        dataflow_session_id,
        state_code,
        start_date,
        end_date_exclusive,
        compartment_level_1,
        IF(
            supervision_type_count > 1 AND compartment_level_1 = 'SUPERVISION',
            'DUAL',
            compartment_level_2
        ) AS compartment_level_2,
        supervising_officer_external_id,
        compartment_location,
        facility,
        facility_name,
        supervision_office,
        supervision_office_name,
        supervision_district,
        supervision_district_name,
        supervision_region_name,
        correctional_level,
        correctional_level_raw_text,
        housing_unit,
        housing_unit_category,
        housing_unit_type,
        housing_unit_type_raw_text,
        case_type,
        case_type_raw_text,
        prioritized_race_or_ethnicity,
        gender,
        custodial_authority,
        metric_source,
        last_day_of_data,
    FROM
        (
        SELECT
            *,
        COUNT(DISTINCT(
            IF(
                compartment_level_2 IN ('PAROLE', 'PROBATION'),
                compartment_level_2,
                NULL
            )
        )) OVER (PARTITION BY person_id, state_code, dataflow_session_id, compartment_level_1) AS supervision_type_count,
        FROM session_attributes_unnested
        )
    )
    ,
    dedup_compartment_level_2_cte AS
    (
    SELECT
        cte.person_id,
        cte.dataflow_session_id,
        cte.state_code,
        cte.metric_source,
        cte.compartment_level_1,
        cte.compartment_level_2,
        cte.supervising_officer_external_id,
        cte.compartment_location,
        cte.facility,
        cte.facility_name,
        cte.supervision_office,
        cte.supervision_office_name,
        cte.supervision_district,
        cte.supervision_district_name,
        cte.supervision_region_name,
        cte.correctional_level,
        cte.correctional_level_raw_text,
        cte.housing_unit,
        cte.housing_unit_category,
        cte.housing_unit_type,
        cte.housing_unit_type_raw_text,
        cte.case_type,
        cte.case_type_raw_text,
        cte.prioritized_race_or_ethnicity,
        cte.gender,
        cte.custodial_authority,
        cte.start_date,
        cte.end_date_exclusive,
        cte.last_day_of_data,
    FROM dual_recategorization_cte cte
    LEFT JOIN `{project_id}.{sessions_dataset}.compartment_level_2_dedup_priority` cl2_dedup
        USING(compartment_level_1, compartment_level_2)
    LEFT JOIN `{project_id}.{sessions_dataset}.supervision_level_dedup_priority` sl_dedup
        ON cte.correctional_level = sl_dedup.correctional_level
    QUALIFY ROW_NUMBER() OVER(PARTITION BY person_id, state_code, dataflow_session_id, compartment_level_1
        ORDER BY COALESCE(cl2_dedup.priority, 999),
                COALESCE(correctional_level_priority, 999),
                NULLIF(supervising_officer_external_id, 'EXTERNAL_UNKNOWN') NULLS LAST,
                NULLIF(compartment_location, 'EXTERNAL_UNKNOWN') NULLS LAST,
                NULLIF(case_type, 'EXTERNAL_UNKNOWN') NULLS LAST,
                NULLIF(case_type_raw_text, 'EXTERNAL_UNKNOWN') NULLS LAST,
                NULLIF(correctional_level_raw_text, 'EXTERNAL_UNKNOWN') NULLS LAST,
                NULLIF(housing_unit, 'EXTERNAL_UNKNOWN') NULLS LAST,
                NULLIF(housing_unit_category, 'EXTERNAL_UNKNOWN') NULLS LAST,
                NULLIF(housing_unit_type, 'EXTERNAL_UNKNOWN') NULLS LAST,
                NULLIF(housing_unit_type_raw_text, 'EXTERNAL_UNKNOWN') NULLS LAST,
                NULLIF(custodial_authority, 'EXTERNAL_UNKNOWN') NULLS LAST
                ) = 1
    )
    SELECT
        state_code,
        person_id,
        start_date,
        end_date_exclusive,
        dataflow_session_id,
        last_day_of_data,
        ARRAY_AGG(
            STRUCT(
                metric_source,
                compartment_level_1,
                compartment_level_2,
                compartment_location,
                facility,
                facility_name,
                supervision_office,
                supervision_office_name,
                supervision_district,
                supervision_district_name,
                supervision_region_name,
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
            )
            ORDER BY
                metric_source,
                compartment_level_1,
                compartment_level_2,
                compartment_location,
                facility,
                facility_name,
                supervision_office,
                supervision_office_name,
                supervision_district,
                supervision_district_name,
                supervision_region_name,
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
        ) AS session_attributes,
    FROM dedup_compartment_level_2_cte
    GROUP BY 1, 2, 3, 4, 5, 6
"""
DATAFLOW_SESSIONS_DEDUPED_BY_SYSTEM_TYPE_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=SESSIONS_DATASET,
    view_id=DATAFLOW_SESSIONS_DEDUPED_BY_SYSTEM_TYPE_VIEW_NAME,
    view_query_template=DATAFLOW_SESSIONS_DEDUPED_BY_SYSTEM_TYPE_QUERY_TEMPLATE,
    description=DATAFLOW_SESSIONS_DEDUPED_BY_SYSTEM_TYPE_VIEW_DESCRIPTION,
    sessions_dataset=SESSIONS_DATASET,
    clustering_fields=["state_code", "person_id"],
    should_materialize=True,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        DATAFLOW_SESSIONS_DEDUPED_BY_SYSTEM_TYPE_VIEW_BUILDER.build_and_print()
