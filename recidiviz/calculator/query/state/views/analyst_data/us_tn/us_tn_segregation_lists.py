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
from recidiviz.calculator.query.bq_utils import nonnull_end_date_exclusive_clause
from recidiviz.calculator.query.state.dataset_config import ANALYST_VIEWS_DATASET
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

US_TN_SEGREGATION_LISTS_VIEW_NAME = "us_tn_segregation_lists"

US_TN_SEGREGATION_LISTS_VIEW_DESCRIPTION = (
    """Snapshot view of everyone currently in Segregation in TDOC Facilities"""
)

US_TN_SEGREGATION_LISTS_QUERY_TEMPLATE = f"""
    WITH 
    -- all open periods from housing_unit_type_collapsed_solitary_sessions in TN
    current_seg_stays AS (
        SELECT 
          person_id,
          state_code,
          start_date, 
          end_date_exclusive,
          e.external_id,
          JSON_EXTRACT_SCALAR(full_name,'$.given_names') AS first_name,
          JSON_EXTRACT_SCALAR(full_name,'$.surname') AS last_name,
        FROM `{{project_id}}.sessions.housing_unit_type_collapsed_solitary_sessions_materialized`
        LEFT JOIN `{{project_id}}.normalized_state.state_person`
        USING(person_id, state_code)
        LEFT JOIN `{{project_id}}.normalized_state.state_person_external_id` e
        USING(person_id, state_code)
        WHERE state_code = "US_TN"
        AND CURRENT_DATE('US/Eastern') BETWEEN start_date and IFNULL(DATE_SUB(end_date_exclusive, INTERVAL 1 DAY), "9999-12-31")
    ), 
    -- get open periods from output of us_tn_segregation_stays_v2 view 
    latest_seg_stay AS (
        SELECT 
            person_id,
            external_id,
            state_code,
            facility,
            housing_unit,
            housing_unit_type,
            housing_unit_type_raw_text AS segregation_type,
            compartment_level_1,
            compartment_level_2,
            s.start_date,
            s.end_date_exclusive
        FROM current_seg_stays s
        LEFT JOIN `{{project_id}}.analyst_data.us_tn_segregation_stays_v2_materialized` a
        USING(person_id, state_code)
        WHERE CURRENT_DATE('US/Eastern') BETWEEN a.start_date and {nonnull_end_date_exclusive_clause('a.end_date_exclusive')}
    )
        SELECT 
            s.person_id,
            s.state_code,
            s.external_id,
            s.first_name,
            s.last_name,
            s.start_date, 
            s.end_date_exclusive, 
            compartment_level_1,
            compartment_level_2,
            segregation_type,
            housing_unit_type,
            l.facility AS current_facility, 
            l.housing_unit AS current_unit,
            open_segs.* EXCEPT(external_id),
            CASE WHEN housing_unit_type = 'ADMINISTRATIVE_SOLITARY_CONFINEMENT' AND DATE_DIFF(CURRENT_DATE('US/Eastern'), s.start_date, DAY) > 365 THEN TRUE
                 WHEN housing_unit_type != 'ADMINISTRATIVE_SOLITARY_CONFINEMENT' AND DATE_DIFF(CURRENT_DATE('US/Eastern'), s.start_date, DAY) > 180 THEN TRUE
                 ELSE FALSE END AS long_stay,
            CASE 
                WHEN compartment_level_1 != "INCARCERATION" THEN "no longer incarcerated"
                WHEN compartment_level_1 = "INCARCERATION" AND compartment_level_2 != 'GENERAL' THEN "incarcerated, not in TDOC facility"
                WHEN (compartment_level_1 = "INCARCERATION" 
                        AND facility NOT IN UNNEST(open_seg_periods_facilities))
                        AND ARRAY_LENGTH(open_seg_periods_facilities) > 0 
                        THEN "seg facility mismatch"
                WHEN count_distinct_facilities_open_periods > 1 THEN "multiple facility open seg periods"
                WHEN open_periods_latest_scheduled_end > CURRENT_DATE('US/Eastern') THEN "future scheduled end date"
                WHEN "DEA" IN UNNEST(open_seg_periods_reasons) THEN "death row"
                ELSE "high confidence"
            END AS data_category,
        FROM current_seg_stays s
        LEFT JOIN latest_seg_stay l 
        USING(person_id)
        LEFT JOIN (
            SELECT OffenderID as external_id, 
                    ARRAY_AGG(DISTINCT SiteID) open_seg_periods_facilities, 
                    ARRAY_AGG(DISTINCT SegregationType) AS open_seg_periods_types, 
                    ARRAY_AGG(DISTINCT SegragationReason) AS open_seg_periods_reasons,
                    MAX(DATE(ScheduleEndDateTime)) AS open_periods_latest_scheduled_end,
                    count(*) AS count_open_periods,
                    count(distinct SiteID) AS count_distinct_facilities_open_periods
                FROM `{{project_id}}.us_tn_raw_data_up_to_date_views.Segregation_latest` 
                WHERE ActualEndDateTime IS NULL
                GROUP BY 1
        ) open_segs
            ON s.external_id = open_segs.external_id
"""

US_TN_SEGREGATION_LISTS_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=ANALYST_VIEWS_DATASET,
    view_id=US_TN_SEGREGATION_LISTS_VIEW_NAME,
    description=US_TN_SEGREGATION_LISTS_VIEW_DESCRIPTION,
    view_query_template=US_TN_SEGREGATION_LISTS_QUERY_TEMPLATE,
    should_materialize=True,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        US_TN_SEGREGATION_LISTS_VIEW_BUILDER.build_and_print()
