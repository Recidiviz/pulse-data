# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2023 Recidiviz, Inc.
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
"""Location name/type information for supervision and incarceration locations"""

from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.state.dataset_config import (
    DATAFLOW_METRICS_MATERIALIZED_DATASET,
    REFERENCE_VIEWS_DATASET,
    SESSIONS_DATASET,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

SESSION_LOCATION_NAMES_VIEW_NAME = "session_location_names"

SESSION_LOCATION_NAMES_VIEW_DESCRIPTION = (
    """Location name/type information for supervision and incarceration locations"""
)

SESSION_LOCATION_NAMES_QUERY_TEMPLATE = """
    WITH all_locations AS (
        -- Gather all incarceration and supervision location for each state from
        -- dataflow population spans
        SELECT DISTINCT
            state_code,
            COALESCE(facility,'EXTERNAL_UNKNOWN') AS facility,
            CAST(NULL AS STRING) AS supervision_office,
            CAST(NULL AS STRING) AS supervision_district,
        FROM `{project_id}.{materialized_metrics_dataset}.most_recent_incarceration_population_span_metrics_materialized`

        UNION ALL

        SELECT DISTINCT
            state_code,
            CAST(NULL AS STRING) AS facility,
            COALESCE(level_1_supervision_location_external_id,'EXTERNAL_UNKNOWN') AS supervision_office,
            COALESCE(level_2_supervision_location_external_id,'EXTERNAL_UNKNOWN') AS supervision_district,
        FROM `{project_id}.{materialized_metrics_dataset}.most_recent_supervision_population_span_metrics_materialized`
    )
    SELECT
        all_locations.state_code,
        facility,
        COALESCE(
            incarceration_level_1.level_1_incarceration_location_name,
            facility,
            "NOT_APPLICABLE"
        ) AS facility_name,
        supervision_office,
        COALESCE(
            supervision_level_1.level_1_supervision_location_name,
            supervision_office,
            "NOT_APPLICABLE"
        ) AS supervision_office_name,
        supervision_district,
        COALESCE(
            supervision_level_2.level_2_supervision_location_name,
            supervision_district,
            "NOT_APPLICABLE"
        ) AS supervision_district_name,
        IFNULL(supervision_level_2.level_3_supervision_location_name, "NOT_APPLICABLE") AS supervision_region_name,
    FROM all_locations
    # TODO(#18892): Replace with `reference_views.location_metadata` once that is hydrated
    LEFT JOIN `{project_id}.{reference_views_dataset}.supervision_location_ids_to_names_materialized` supervision_level_1
        ON supervision_level_1.level_2_supervision_location_external_id = all_locations.supervision_district
        AND supervision_level_1.level_1_supervision_location_external_id = all_locations.supervision_office
        AND supervision_level_1.state_code = all_locations.state_code
    LEFT JOIN (
        -- Join the supervision district and region separately to handle cases where
        -- the district has a known name but the supervision office does not
        SELECT
            state_code,
            level_2_supervision_location_external_id,
            level_2_supervision_location_name,
            level_3_supervision_location_name,
        FROM `{project_id}.{reference_views_dataset}.supervision_location_ids_to_names_materialized`
        -- Deduplicate to one row per level 2 location for the few districts that are
        -- mapped to more than one region
        QUALIFY ROW_NUMBER() OVER (
            PARTITION BY state_code, level_2_supervision_location_external_id
            ORDER BY level_3_supervision_location_name,
                level_2_supervision_location_name
        ) = 1
    ) supervision_level_2
        ON supervision_level_2.level_2_supervision_location_external_id = all_locations.supervision_district
        AND supervision_level_2.state_code = all_locations.state_code
    # TODO(#18892): Replace with `reference_views.location_metadata` once that is hydrated
    LEFT JOIN `{project_id}.{reference_views_dataset}.incarceration_location_ids_to_names` incarceration_level_1
        ON incarceration_level_1.level_1_incarceration_location_external_id = all_locations.facility
        AND incarceration_level_1.state_code = all_locations.state_code
    """

SESSION_LOCATION_NAMES_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=SESSIONS_DATASET,
    view_id=SESSION_LOCATION_NAMES_VIEW_NAME,
    view_query_template=SESSION_LOCATION_NAMES_QUERY_TEMPLATE,
    description=SESSION_LOCATION_NAMES_VIEW_DESCRIPTION,
    materialized_metrics_dataset=DATAFLOW_METRICS_MATERIALIZED_DATASET,
    reference_views_dataset=REFERENCE_VIEWS_DATASET,
    clustering_fields=["state_code"],
    should_materialize=True,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        SESSION_LOCATION_NAMES_VIEW_BUILDER.build_and_print()
