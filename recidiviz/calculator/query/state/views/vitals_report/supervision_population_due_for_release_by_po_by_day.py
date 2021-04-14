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
"""Supervisees due for release by PO by day."""


from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.state import dataset_config
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

SUPERVISION_POPULATION_DUE_FOR_RELEASE_BY_PO_BY_DAY_VIEW_NAME = (
    "supervision_population_due_for_release_by_po_by_day"
)

SUPERVISION_POPULATION_DUE_FOR_RELEASE_BY_PO_BY_DAY_DESCRIPTION = """
    Supervision population due for release by PO by day
 """

SUPERVISION_POPULATION_DUE_FOR_RELEASE_BY_PO_BY_DAY_QUERY_TEMPLATE = """
    /*{description}*/
   WITH due_for_release AS (
        SELECT
            state_code,
            date_of_supervision,
            IFNULL(supervising_officer_external_id, 'UNKNOWN') as supervising_officer_external_id,
            IFNULL(level_1_supervision_location_external_id, 'UNKNOWN') as level_1_supervision_location_external_id,
            COUNT (DISTINCT(IF(projected_end_date < date_of_supervision, person_id, NULL))) as due_for_release_count,
        FROM `{project_id}.{materialized_metrics_dataset}.most_recent_supervision_population_metrics_materialized`,
        UNNEST ([level_1_supervision_location_external_id, 'ALL']) AS level_1_supervision_location_external_id,
        UNNEST ([supervising_officer_external_id, 'ALL']) AS supervising_officer_external_id
        WHERE date_of_supervision > DATE_SUB(CURRENT_DATE('US/Pacific'), INTERVAL 365 DAY)
            AND projected_end_date IS NOT NULL
            AND state_code = "US_ND"
        GROUP BY state_code, date_of_supervision, supervising_officer_external_id, level_1_supervision_location_external_id
    )
    
    SELECT 
        due_for_release.state_code,
        due_for_release.date_of_supervision,
        due_for_release.supervising_officer_external_id,
        due_for_release.level_1_supervision_location_external_id as district_id,
        locations.level_1_supervision_location_name as district_name,
        due_for_release_count,
        sup_pop.people_under_supervision AS total_under_supervision,
        SAFE_DIVIDE((sup_pop.people_under_supervision - due_for_release_count), sup_pop.people_under_supervision) * 100 AS timely_discharge,
    FROM due_for_release
    LEFT JOIN `{project_id}.{reference_views_dataset}.supervision_location_ids_to_names` locations
        ON due_for_release.state_code = locations.state_code
            AND due_for_release.level_1_supervision_location_external_id = locations.level_1_supervision_location_external_id
    LEFT JOIN `{project_id}.{vitals_views_dataset}.supervision_population_by_po_by_day_materialized` sup_pop
        on sup_pop.state_code = due_for_release.state_code
        AND sup_pop.date_of_supervision = due_for_release.date_of_supervision
        AND sup_pop.supervising_officer_external_id = due_for_release.supervising_officer_external_id
        AND sup_pop.supervising_district_external_id = due_for_release.level_1_supervision_location_external_id
    
    """

SUPERVISION_POPULATION_DUE_FOR_RELEASE_BY_PO_BY_DAY_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=dataset_config.VITALS_REPORT_DATASET,
    view_id=SUPERVISION_POPULATION_DUE_FOR_RELEASE_BY_PO_BY_DAY_VIEW_NAME,
    view_query_template=SUPERVISION_POPULATION_DUE_FOR_RELEASE_BY_PO_BY_DAY_QUERY_TEMPLATE,
    description=SUPERVISION_POPULATION_DUE_FOR_RELEASE_BY_PO_BY_DAY_DESCRIPTION,
    materialized_metrics_dataset=dataset_config.DATAFLOW_METRICS_MATERIALIZED_DATASET,
    reference_views_dataset=dataset_config.REFERENCE_VIEWS_DATASET,
    vitals_views_dataset=dataset_config.VITALS_REPORT_DATASET,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        SUPERVISION_POPULATION_DUE_FOR_RELEASE_BY_PO_BY_DAY_VIEW_BUILDER.build_and_print()
