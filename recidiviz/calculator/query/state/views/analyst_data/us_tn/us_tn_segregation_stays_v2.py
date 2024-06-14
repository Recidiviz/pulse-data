# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2024 Recidiviz, Inc.
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
"""
Snapshot view of everyone currently in Segregation in TDOC Facilities
"""

from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.sessions_query_fragments import (
    create_intersection_spans,
    create_sub_sessions_with_attributes,
)
from recidiviz.calculator.query.state.dataset_config import ANALYST_VIEWS_DATASET
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

US_TN_SEGREGATION_STAYS_VIEW_NAME_V2 = "us_tn_segregation_stays_v2"

US_TN_SEGREGATION_STAYS_VIEW_DESCRIPTION = (
    """Snapshot view of everyone currently in Segregation in TDOC Facilities"""
)

US_TN_SEGREGATION_STAYS_QUERY_TEMPLATE = f"""
    WITH segregation_cte AS (
        /* This CTE keeps all historic and current segregation periods with some cleaned up fields. 
        The reason for not filtering to only "current" periods here is because we first want to 
        stitch together continuous periods, many of which will be closed by now. However 
        periods are not closed out with high fidelity in this table. */
        SELECT
            pei.person_id,
            pei.external_id,
            pei.state_code,
            s.SiteID AS facility_id,
            DATE(s.StartDateTime) AS start_date,
            DATE(s.ActualEndDateTime) AS end_date, 
            DATE(s.ScheduleEndDateTime) AS scheduled_end_date,
            s.SegragationReason AS segregation_reason,
            s.SegregationStatus AS segregation_status,
            s.SegregationType AS segregation_type,
        FROM `{{project_id}}.us_tn_raw_data_up_to_date_views.Segregation_latest` s
        INNER JOIN `{{project_id}}.normalized_state.state_person_external_id` pei
            ON s.OffenderID = pei.external_id
            AND pei.state_code = 'US_TN'

    ),
    -- The vast majority of spans have an exclusive end date, but 16% dont. This cte constructs end_date_exclusive for
    -- those 16%
    fix_end_date AS (
        SELECT
            *,
            CASE WHEN DATE_ADD(end_date, INTERVAL 1 DAY) = next_start_date THEN DATE_ADD(end_date, INTERVAL 1 DAY)
                 ELSE end_date END AS end_date_exclusive,
    FROM 
        (
        SELECT 
            *, 
            LEAD(start_date) OVER(PARTITION BY person_id ORDER BY start_date) next_start_date
        FROM segregation_cte    
        )
    ),
    -- Unioning segregation raw data with ingested custody level information. MAXIMUM custody is also segregation and 
    -- we expect this data to be more reliable since periods are not always closed in segregation data.
    union_max_data AS (
        SELECT
            person_id,
            external_id,
            state_code,
            facility_id, 
            start_date,
            end_date_exclusive, 
            scheduled_end_date,
            segregation_reason,
            segregation_status,
            segregation_type,
        FROM fix_end_date

        UNION ALL 

        SELECT
            c.person_id,
            external_id,
            c.state_code,
            facility AS facility_id,
            start_date,
            end_date_exclusive,
            null AS scheduled_end_date,
            null AS segregation_reason,
            null AS segregation_status,
            correctional_level AS segregation_type,
        FROM `{{project_id}}.sessions.compartment_sub_sessions_materialized` c
        INNER JOIN `{{project_id}}.normalized_state.state_person_external_id`
            USING(person_id, state_code)
        WHERE c.state_code = 'US_TN'
            AND c.correctional_level = 'MAXIMUM'
            AND compartment_level_1 = 'INCARCERATION'
    ),
    -- sessions level information to determine if someone with an open period from 
    -- segregation table is actually still incarcerated. Taking all values for compartment 
    -- levels (incarcerated or not) because TN is using seg lists to clean up incorrectly open seg periods. 
    compartment_sessions AS (
        SELECT 
            state_code,
            person_id,
            start_date,
            end_date_exclusive,
            compartment_level_1, 
            compartment_level_2, 
            facility,
            housing_unit
        FROM `{{project_id}}.sessions.compartment_sub_sessions_materialized`
        WHERE state_code='US_TN' 
    ),
    -- Using create_intersection_spans because we know there are some overlapping periods within Segregation raw table and between Segregation and max custordy periods
    incarceration_spans AS (
        SELECT * 
        FROM ({create_intersection_spans(table_1_name='union_max_data',
                                    table_2_name='compartment_sessions',
                                    index_columns=['state_code', 'person_id'],
                                    table_1_columns=['external_id','segregation_type'],
                                    table_2_columns=['compartment_level_1','compartment_level_2', 'facility', 'housing_unit'],
                                    table_1_start_date_field_name='start_date',
                                    table_1_end_date_field_name='end_date_exclusive',)})
    ),
    -- Handles overlapping spans with differing data, like we see in Segregation and Max table
    {create_sub_sessions_with_attributes('incarceration_spans',
                                         end_date_field_name='end_date_exclusive')}
    ,
    -- Prioritizing where segregation_type is 'MAXIMUM' since that data is more reliable 
    -- than the segregation data. 
    dedup_on_max AS (
        SELECT
            state_code,
            person_id,
            external_id,
            facility,
            housing_unit,
            start_date,
            end_date_exclusive,
            segregation_type, 
            compartment_level_1,
            compartment_level_2
        FROM
            sub_sessions_with_attributes
        -- After priortizing MAX because of more reliable data, we prioritize SPND (special needs facility)
        -- because often times a spot is held at orginal facility while at SPND
        QUALIFY ROW_NUMBER() OVER(PARTITION BY person_id, state_code, start_date, end_date_exclusive
                                ORDER BY 
                                    CASE WHEN segregation_type = 'MAXIMUM' THEN 0 
                                         WHEN segregation_type = 'PUN' THEN 1 
                                         WHEN segregation_type = 'PCB' THEN 2 
                                         ELSE 3 END ASC) = 1
    )
    -- transforming raw segregation_types to solitary confinement enum values
    SELECT 
        person_id,
        state_code,
        facility,
        compartment_level_1,
        compartment_level_2,
        housing_unit,
        segregation_type AS housing_unit_type_raw_text,
        CASE 
          WHEN segregation_type IN ('ASE', 'MAXIMUM') THEN 'ADMINISTRATIVE_SOLITARY_CONFINEMENT'
          WHEN segregation_type IN ('HCE','INV', 'MET', 'SPD') THEN 'TEMPORARY_SOLITARY_CONFINEMENT'
          WHEN segregation_type IN ('IPT', 'MSG', 'QUA', 'SIP', 'TSD') THEN 'OTHER_SOLITARY_CONFINEMENT'
          WHEN segregation_type IN ('PCB') THEN 'PROTECTIVE_CUSTODY'
          WHEN segregation_type IN ('PUN') THEN 'DISCIPLINARY_SOLITARY_CONFINEMENT'
          WHEN segregation_type IN ('TSE') THEN 'MENTAL_HEALTH_SOLITARY_CONFINEMENT'
        ELSE 'GENERAL'
        END AS housing_unit_type,
        start_date,
        end_date_exclusive
    FROM dedup_on_max
"""

US_TN_SEGREGATION_STAYS_VIEW_BUILDER_V2 = SimpleBigQueryViewBuilder(
    dataset_id=ANALYST_VIEWS_DATASET,
    view_id=US_TN_SEGREGATION_STAYS_VIEW_NAME_V2,
    description=US_TN_SEGREGATION_STAYS_VIEW_DESCRIPTION,
    view_query_template=US_TN_SEGREGATION_STAYS_QUERY_TEMPLATE,
    should_materialize=True,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        US_TN_SEGREGATION_STAYS_VIEW_BUILDER_V2.build_and_print()
