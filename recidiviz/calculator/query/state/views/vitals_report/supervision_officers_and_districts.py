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
"""Supervision officer to district mapping"""
from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.bq_utils import nonnull_end_date_exclusive_clause
from recidiviz.calculator.query.state import (
    dataset_config,
    state_specific_query_strings,
)
from recidiviz.ingest.views.dataset_config import NORMALIZED_STATE_DATASET
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

SUPERVISION_OFFICERS_AND_DISTRICTS_VIEW_NAME = "supervision_officers_and_districts"

SUPERVISION_OFFICERS_AND_DISTRICTS_DESCRIPTION = """
Mapping of supervision officers and their districts
"""

enabled_states = ("US_ND", "US_ID", "US_IX")

SUPERVISION_OFFICERS_AND_DISTRICTS_QUERY_TEMPLATE = f"""
   WITH us_id_ix_roster AS (
        SELECT DISTINCT 
            'US_ID' AS state_code,
            external_id AS supervising_officer_external_id,
            district AS supervising_district_external_id,
        FROM `{{project_id}}.{{reference_views_dataset}}.product_roster_materialized` r
        WHERE state_code='US_ID'
        UNION ALL
        SELECT DISTINCT 
            'US_IX' AS state_code,
            external_id AS supervising_officer_external_id,
            district AS supervising_district_external_id,
        FROM `{{project_id}}.{{reference_views_dataset}}.product_roster_materialized` r
        # The users in the roster all have US_ID state code
        WHERE state_code='US_ID'
   )
   SELECT 
        sup_pop.state_code,
        IF(us_id_ix_roster.supervising_district_external_id IS NOT NULL, us_id_ix_roster.supervising_district_external_id, sup_pop.supervising_district_external_id) AS supervising_district_external_id,
        IF(us_id_ix_roster.supervising_officer_external_id IS NOT NULL, us_id_ix_roster.supervising_officer_external_id, sup_pop.supervising_officer_external_id) AS supervising_officer_external_id,
        {{vitals_state_specific_district_id}},
        {{vitals_state_specific_district_name}}
   FROM (
      SELECT
        metrics.*,
        staff.external_id AS supervising_officer_external_id,
      FROM
        `{{project_id}}.{{materialized_metrics_dataset}}.most_recent_supervision_population_span_metrics_materialized` metrics
      LEFT JOIN
        `{{project_id}}.sessions.state_staff_id_to_legacy_supervising_officer_external_id_materialized` staff
      ON
        metrics.supervising_officer_staff_id = staff.staff_id
      WHERE included_in_state_population
   ) sup_pop
   LEFT JOIN us_id_ix_roster 
      ON sup_pop.state_code = us_id_ix_roster.state_code
      AND sup_pop.supervising_officer_external_id = us_id_ix_roster.supervising_officer_external_id 
   LEFT JOIN `{{project_id}}.{{reference_views_dataset}}.supervision_location_ids_to_names_materialized` locations
        ON sup_pop.state_code = locations.state_code
        AND {{vitals_state_specific_join_with_supervision_location_ids}}
   LEFT JOIN `{{project_id}}.sessions.state_staff_id_to_legacy_supervising_officer_external_id_materialized` sid
        ON sup_pop.supervising_officer_staff_id = sid.staff_id
    LEFT JOIN `{{project_id}}.{{normalized_state_dataset}}.state_staff_role_period` rp
        ON sid.staff_id = rp.staff_id
   
   WHERE COALESCE(DATE_SUB(end_date_exclusive, INTERVAL 1 DAY), CURRENT_DATE('US/Eastern')) > DATE_SUB(CURRENT_DATE('US/Eastern'), INTERVAL 217 DAY) -- 217 = 210 days back for avgs + 7-day buffer for late data
        AND sup_pop.state_code in {enabled_states}
        AND (
            (us_id_ix_roster.supervising_officer_external_id IS NOT NULL 
                AND us_id_ix_roster.supervising_district_external_id = sup_pop.level_2_supervision_location_external_id
                AND sup_pop.state_code IN ('US_ID', 'US_IX'))
            OR (us_id_ix_roster.supervising_officer_external_id IS NULL AND sup_pop.state_code NOT IN ('US_ID', 'US_IX'))
        ) 
        -- Only include people who were active during the span of time we're looking at
        AND rp.role_type = 'SUPERVISION_OFFICER'
        AND rp.start_date < {nonnull_end_date_exclusive_clause('sup_pop.end_date_exclusive')}
        AND {nonnull_end_date_exclusive_clause('rp.end_date')} > sup_pop.start_date_inclusive
   GROUP BY 1,2,3,4,5
"""

SUPERVISION_OFFICERS_AND_DISTRICTS_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=dataset_config.VITALS_REPORT_DATASET,
    view_id=SUPERVISION_OFFICERS_AND_DISTRICTS_VIEW_NAME,
    view_query_template=SUPERVISION_OFFICERS_AND_DISTRICTS_QUERY_TEMPLATE,
    description=SUPERVISION_OFFICERS_AND_DISTRICTS_DESCRIPTION,
    materialized_metrics_dataset=dataset_config.DATAFLOW_METRICS_MATERIALIZED_DATASET,
    reference_views_dataset=dataset_config.REFERENCE_VIEWS_DATASET,
    normalized_state_dataset=NORMALIZED_STATE_DATASET,
    vitals_state_specific_district_id=state_specific_query_strings.vitals_state_specific_district_id(
        "sup_pop"
    ),
    vitals_state_specific_district_name=state_specific_query_strings.vitals_state_specific_district_name(
        "sup_pop"
    ),
    vitals_state_specific_join_with_supervision_location_ids=state_specific_query_strings.vitals_state_specific_join_with_supervision_location_ids(
        "sup_pop"
    ),
    should_materialize=True,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        SUPERVISION_OFFICERS_AND_DISTRICTS_VIEW_BUILDER.build_and_print()
