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
from recidiviz.calculator.query.bq_utils import (
    today_between_start_date_and_nullable_end_date_exclusive_clause,
)
from recidiviz.calculator.query.sessions_query_fragments import (
    aggregate_adjacent_spans,
    create_sub_sessions_with_attributes,
)
from recidiviz.calculator.query.state.dataset_config import ANALYST_VIEWS_DATASET
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

US_TN_SEGREGATION_STAYS_VIEW_NAME = "us_tn_segregation_stays"

US_TN_SEGREGATION_STAYS_VIEW_DESCRIPTION = (
    """Snapshot view of everyone currently in Segregation in TDOC Facilities"""
)

US_TN_SEGREGATION_STAYS_QUERY_TEMPLATE = f"""
    WITH segregation_cte AS (
        /* This CTE keeps all historic and current segregation periods with some cleaned up fields
        and latest facility information. The reason for not filtering to only "current" periods here is because
        we first want to stitch together continuous periods, many of which will be closed by now. However because
         periods are not closed out with high fidelity in this table, we carry latest facility information through */
        --TODO(#26842): Explicitly add periods spent in max custody since not all might be entered in this table
        SELECT
            pei.person_id,
            pei.external_id,
            pei.state_code,
            s.SiteID AS seg_facility,
            DATE(s.StartDateTime) AS start_date,
            DATE(s.ActualEndDateTime) AS end_date,
            DATE(s.ScheduleEndDateTime) AS scheduled_end_date,
            s.SegragationReason AS segregation_reason,
            s.SegregationStatus AS segregation_status,
            s.SegregationType AS segregation_type,
            c.facility_id AS current_facility_id, 
            c.unit_id AS current_unit_id,
        FROM `{{project_id}}.us_tn_raw_data_up_to_date_views.Segregation_latest` s
        INNER JOIN `{{project_id}}.normalized_state.state_person_external_id` pei
            ON s.OffenderID = pei.external_id
            AND pei.state_code = 'US_TN'
        -- TODO(#27428): Remove this join when custody level information aligns with location information
        LEFT JOIN `{{project_id}}.analyst_data.us_tn_cellbed_assignment_raw_materialized` c
            USING(person_id, state_code)
        -- There are a very small number of duplicates on person id and start date in the segregation table
        QUALIFY ROW_NUMBER() OVER(PARTITION BY pei.person_id, start_date ORDER BY end_date DESC) = 1
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
    /* 8% of the rows in the segregation table have overlapping spans. This can be for various reasons (multiple counts
     of punitive segregation starting on different dates, not closing out periods, etc). This CTE corrects
     for that before we can collapse adjacent spans */
    {create_sub_sessions_with_attributes('fix_end_date',
                                         end_date_field_name='end_date_exclusive')}
   ,
   keep_attribute_arrays AS (
        -- Attributes of all currently "active" segregation spans where segregation facility matches current facility
        -- This provides information, for example, on all the Segregation Reasons that might be currently applicable
        SELECT
            person_id,
            state_code,
            MIN(start_date) AS earliest_start_date,
            MAX(end_date_exclusive) AS latest_end_date,
            ARRAY_AGG(DISTINCT segregation_reason) AS segregation_reason,
            ARRAY_AGG(DISTINCT segregation_status) AS segregation_status,
            ARRAY_AGG(DISTINCT segregation_type) AS segregation_type,
        FROM sub_sessions_with_attributes
        WHERE {today_between_start_date_and_nullable_end_date_exclusive_clause(
                start_date_column="start_date",
                end_date_column="end_date_exclusive"
            )}
            AND seg_facility = current_facility_id
        GROUP BY 1,2
   ),
   latest_attributes AS (
        -- Attributes of latest segregation span where segregation facility matches current facility
        SELECT *
        FROM fix_end_date
        WHERE {today_between_start_date_and_nullable_end_date_exclusive_clause(
                start_date_column="start_date",
                end_date_column="end_date_exclusive"
            )}
            AND seg_facility = current_facility_id
        QUALIFY ROW_NUMBER() OVER(PARTITION BY person_id ORDER BY start_date DESC) = 1
   ),
   deduplicate AS ( 
    SELECT DISTINCT
            person_id,
            state_code,
            start_date,
            end_date_exclusive,
    FROM sub_sessions_with_attributes
    ),
    -- Collapses adjacent spans. We dont create new spans if facility or segregation type changes. However, we're only
    -- keeping periods where the latest seg facility is the same as the current facility, to partially mitigate 
    -- open periods that weren't closed out
    sessionized_cte AS
    (
    {aggregate_adjacent_spans(table_name='deduplicate',
                       end_date_field_name='end_date_exclusive')}
    )
    -- Now that we have periods of continuous segregation, we bring in attributes from all active spans
    SELECT
        s.person_id,
        s.state_code,
        JSON_EXTRACT_SCALAR(full_name,'$.given_names') AS first_name,
        JSON_EXTRACT_SCALAR(full_name,'$.surname') AS last_name,
        s.start_date AS continuous_seg_start_date,
        s.end_date_exclusive AS continuous_seg_end_date,
        l.external_id,
        l.current_facility_id,
        l.current_unit_id,
        l.start_date AS latest_seg_start_date,
        l.scheduled_end_date AS latest_scheduled_end_date,
        l.segregation_type AS latest_segregation_type,
        l.segregation_reason AS latest_segregation_reason,
        l.segregation_status AS latest_segregation_status,
        DATE_DIFF(CURRENT_DATE('US/Pacific'), s.start_date, DAY) AS length_of_continuous_seg_stay,        
        k.segregation_reason AS segregation_reason_array,
        k.segregation_status AS segregation_status_array,
        k.segregation_type AS segregation_type_array,
    FROM sessionized_cte s
    LEFT JOIN latest_attributes l
        USING(person_id)
    LEFT JOIN `{{project_id}}.analyst_data.us_tn_max_stays_materialized` m
        USING(person_id)
    LEFT JOIN keep_attribute_arrays k
        USING(person_id)
    INNER JOIN `{{project_id}}.normalized_state.state_person` sp
        USING(person_id)
    WHERE {today_between_start_date_and_nullable_end_date_exclusive_clause(
                start_date_column="s.start_date",
                end_date_column="s.end_date_exclusive"
            )}
        -- Excludes people who show up as currently in max custody
        AND m.person_id IS NULL
        AND l.current_facility_id is not null

"""

US_TN_SEGREGATION_STAYS_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=ANALYST_VIEWS_DATASET,
    view_id=US_TN_SEGREGATION_STAYS_VIEW_NAME,
    description=US_TN_SEGREGATION_STAYS_VIEW_DESCRIPTION,
    view_query_template=US_TN_SEGREGATION_STAYS_QUERY_TEMPLATE,
    should_materialize=True,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        US_TN_SEGREGATION_STAYS_VIEW_BUILDER.build_and_print()
