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
"""Supervisees eligible for supervision downgrade by PO by day."""
# pylint: disable=line-too-long


from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.state import (
    dataset_config,
    state_specific_query_strings,
)
from recidiviz.calculator.query.state.state_specific_query_strings import (
    VITALS_LEVEL_1_SUPERVISION_LOCATION_OPTIONS,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

# TODO(#8389) Templatize vital base-view generation

SUPERVISION_DOWNGRADE_OPPORTUNITIES_BY_PO_BY_DAY_VIEW_NAME = (
    "supervision_downgrade_opportunities_by_po_by_day"
)

SUPERVISION_DOWNGRADE_OPPORTUNITIES_BY_PO_BY_DAY_DESCRIPTION = """
Number of supervisees who can have their supervision level downgraded by PO by day
compared to the PO's total caseload
"""

SUPERVISION_DOWNGRADE_OPPORTUNITIES_BY_PO_BY_DAY_QUERY_TEMPLATE = """
    /*{description}*/
    WITH downgrade_opportunities AS (
        SELECT
            state_code,
            date_of_supervision,
            supervising_officer_external_id,
            level_1_supervision_location_external_id,
            level_2_supervision_location_external_id,
            COUNT (DISTINCT(IF(recommended_supervision_downgrade_level IS NOT NULL, person_id, NULL))) as total_downgrade_opportunities,
        FROM `{project_id}.{shared_metric_views_dataset}.supervision_mismatches_by_day_materialized`
        -- 210 is 6 months (180 days) for the 6 month time series chart + 30 days for monthly average on the first day
        WHERE date_of_supervision > DATE_SUB(CURRENT_DATE('US/Eastern'), INTERVAL 210 DAY)
        GROUP BY 1, 2, 3, 4, 5
    )

    {vitals_state_specific_supervision_location_exclusions}

    SELECT DISTINCT
        downgrade_opportunities.state_code,
        downgrade_opportunities.date_of_supervision,
        IFNULL(downgrade_opportunities.supervising_officer_external_id, 'UNKNOWN') as supervising_officer_external_id,
        sup_pop.district_id,
        sup_pop.district_name,
        total_downgrade_opportunities,
        sup_pop.people_under_supervision AS total_under_supervision,
        IFNULL(SAFE_DIVIDE((sup_pop.people_under_supervision - total_downgrade_opportunities), sup_pop.people_under_supervision), 1) * 100 AS timely_downgrade,
    FROM downgrade_opportunities_excluded_locations downgrade_opportunities
    INNER JOIN `{project_id}.{vitals_views_dataset}.supervision_population_by_po_by_day_materialized` sup_pop
        ON sup_pop.state_code = downgrade_opportunities.state_code
        AND sup_pop.date_of_supervision = downgrade_opportunities.date_of_supervision
        AND sup_pop.supervising_officer_external_id = downgrade_opportunities.supervising_officer_external_id
        AND {vitals_state_specific_join_with_supervision_population}
    WHERE downgrade_opportunities.level_1_supervision_location_external_id = 'ALL'
        OR downgrade_opportunities.state_code IN {vitals_level_1_state_codes}
        
    """

SUPERVISION_DOWNGRADE_OPPORTUNITIES_BY_PO_BY_DAY_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=dataset_config.VITALS_REPORT_DATASET,
    view_id=SUPERVISION_DOWNGRADE_OPPORTUNITIES_BY_PO_BY_DAY_VIEW_NAME,
    view_query_template=SUPERVISION_DOWNGRADE_OPPORTUNITIES_BY_PO_BY_DAY_QUERY_TEMPLATE,
    description=SUPERVISION_DOWNGRADE_OPPORTUNITIES_BY_PO_BY_DAY_DESCRIPTION,
    shared_metric_views_dataset=dataset_config.SHARED_METRIC_VIEWS_DATASET,
    vitals_views_dataset=dataset_config.VITALS_REPORT_DATASET,
    vitals_state_specific_join_with_supervision_population=state_specific_query_strings.vitals_state_specific_join_with_supervision_population(
        "downgrade_opportunities"
    ),
    vitals_level_1_state_codes=VITALS_LEVEL_1_SUPERVISION_LOCATION_OPTIONS,
    vitals_state_specific_supervision_location_exclusions=state_specific_query_strings.vitals_state_specific_supervision_location_exclusions(
        "downgrade_opportunities"
    ),
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        SUPERVISION_DOWNGRADE_OPPORTUNITIES_BY_PO_BY_DAY_VIEW_BUILDER.build_and_print()
