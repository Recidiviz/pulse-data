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
"""Sessionization and deduplication of MO housing stays"""

from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.sessions_query_fragments import (
    aggregate_adjacent_spans,
    create_sub_sessions_with_attributes,
)
from recidiviz.calculator.query.state.dataset_config import SESSIONS_DATASET
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

US_MO_HOUSING_STAY_SESSIONS_VIEW_NAME = "us_mo_housing_stay_sessions"

US_MO_HOUSING_STAY_SESSIONS_VIEW_DESCRIPTION = (
    """Sessionization and deduplication of MO housing stays"""
)

US_MO_HOUSING_STAY_SESSIONS_QUERY_TEMPLATE = f"""
    --TODO(#14709): Templatize sub-sessionization query
    WITH confinement_type_priority_cte AS
    /*
    CTE that is used to deduplicate in cases where a person is listed in more than one confinement type for a period
    of time.
    */
    (
    SELECT
      *
    FROM UNNEST(['SOLITARY_CONFINEMENT','GENERAL','INTERNAL_UNKNOWN','EXTERNAL_UNKNOWN']) AS confinement_type
    WITH offset AS confinement_type_priority
    )
    ,
    stay_type_priority_cte AS 
    /*
    Secondary deduplication CTE that is used to deduplicate in cases where a person is listed in more than one stay type 
    within the same confinement type for a period of time.
    */
    (  
    SELECT
      *
    FROM UNNEST(['PERMANENT','TEMPORARY','INTERNAL_UNKNOWN','EXTERNAL_UNKNOWN']) AS stay_type
    WITH offset AS stay_type_priority
    )
    ,
    periods_with_priority_cte AS
    /*
    This CTE is the overlapping input periods, but with non-null end dates and with the deduplication priority columns
    added
    */
    (
    SELECT 
        c.*,
        COALESCE(confinement_type_priority,999) AS confinement_type_priority,
        COALESCE(stay_type_priority,999) AS stay_type_priority,
    FROM `{{project_id}}.{{sessions_dataset}}.us_mo_housing_stays_preprocessed` c
    LEFT JOIN confinement_type_priority_cte 
        USING(confinement_type)
    LEFT JOIN stay_type_priority_cte
        USING(stay_type)
    -- Remove data from before 2000 to reduce data quality issues
    WHERE start_date > "2000-01-01"
    )
    ,
    {create_sub_sessions_with_attributes(table_name='periods_with_priority_cte', use_magic_date_end_dates=True,
                                         end_date_field_name='end_date_exclusive')}
    ,
    sessions_with_attributes_dedup AS
    /*
    Duplicate spans are created any time that there is more than one attribute value that applies for a given date 
    range. In this specific case, we know that can happen where someone is listed as GENERAL and SOLITARY_CONFINEMENT
    simultaneously. This CTE deduplicates so that all date-ranges are unique and prioritizes the session based on 
    the deduplication priority defined in the first two CTEs.
    */
    (
    SELECT 
      *
    FROM sub_sessions_with_attributes
    QUALIFY ROW_NUMBER() OVER(PARTITION BY person_id, state_code, start_date, end_date_exclusive 
        ORDER BY confinement_type_priority, stay_type_priority, facility_code) = 1
    )
    ,
    sessionized_cte AS 
    (
    /*
    At this point we now have non-overlapping sessions and can just use our standard sessionization logic to aggregate
    together temporally adjacent sessions with identical attributes.
    */
    {aggregate_adjacent_spans(table_name='sessions_with_attributes_dedup',
                       attribute=['facility_code','stay_type','confinement_type'],
                       session_id_output_name='housing_stay_session_id',
                       end_date_field_name='end_date_exclusive')}
    )
    SELECT 
        *,
        end_date_exclusive AS end_date
    FROM sessionized_cte
"""

US_MO_HOUSING_STAY_SESSIONS_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=SESSIONS_DATASET,
    view_id=US_MO_HOUSING_STAY_SESSIONS_VIEW_NAME,
    sessions_dataset=SESSIONS_DATASET,
    description=US_MO_HOUSING_STAY_SESSIONS_VIEW_DESCRIPTION,
    view_query_template=US_MO_HOUSING_STAY_SESSIONS_QUERY_TEMPLATE,
    should_materialize=True,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        US_MO_HOUSING_STAY_SESSIONS_VIEW_BUILDER.build_and_print()
