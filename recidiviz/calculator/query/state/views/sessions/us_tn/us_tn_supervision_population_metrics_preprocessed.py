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
"""TN State-specific preprocessing for joining with dataflow sessions"""

from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.state.dataset_config import (
    DATAFLOW_METRICS_MATERIALIZED_DATASET,
    SESSIONS_DATASET,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

US_TN_SUPERVISION_POPULATION_METRICS_PREPROCESSED_VIEW_NAME = (
    "us_tn_supervision_population_metrics_preprocessed"
)

US_TN_SUPERVISION_POPULATION_METRICS_PREPROCESSED_VIEW_DESCRIPTION = """TN State-specific preprocessing for joining with dataflow sessions:
- Recategorize INTERNAL_UNKNOWN supervision levels based on raw text codes
"""

US_TN_SUPERVISION_POPULATION_METRICS_PREPROCESSED_QUERY_TEMPLATE = """
    -- TODO(#12046): [Pathways] Remove TN-specific raw supervision-level mappings
    SELECT
        person_id,
        start_date_inclusive AS start_date,
        end_date_exclusive,
        metric_type AS metric_source,
        df.state_code,
        IF(included_in_state_population, 'SUPERVISION', 'SUPERVISION_OUT_OF_STATE') AS compartment_level_1,
        supervision_type AS compartment_level_2,
        CONCAT(COALESCE(df.level_1_supervision_location_external_id,'EXTERNAL_UNKNOWN'),'|', COALESCE(df.level_2_supervision_location_external_id,'EXTERNAL_UNKNOWN')) AS compartment_location,
        CAST(NULL AS STRING) AS facility,
        COALESCE(df.level_1_supervision_location_external_id,'EXTERNAL_UNKNOWN') AS supervision_office,
        COALESCE(df.level_2_supervision_location_external_id,'EXTERNAL_UNKNOWN') AS supervision_district,
        CASE
            WHEN COALESCE(supervision_level, "INTERNAL_UNKNOWN") != "INTERNAL_UNKNOWN" THEN supervision_level
            ELSE "INTERNAL_UNKNOWN"
        END AS correctional_level,
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
    FROM `{project_id}.{materialized_metrics_dataset}.most_recent_supervision_population_span_metrics_materialized` df
    LEFT JOIN
        `{project_id}.sessions.state_staff_id_to_legacy_supervising_officer_external_id_materialized` staff
    ON
        df.supervising_officer_staff_id = staff.staff_id
    WHERE df.state_code = 'US_TN'
"""

US_TN_SUPERVISION_POPULATION_METRICS_PREPROCESSED_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=SESSIONS_DATASET,
    view_id=US_TN_SUPERVISION_POPULATION_METRICS_PREPROCESSED_VIEW_NAME,
    view_query_template=US_TN_SUPERVISION_POPULATION_METRICS_PREPROCESSED_QUERY_TEMPLATE,
    description=US_TN_SUPERVISION_POPULATION_METRICS_PREPROCESSED_VIEW_DESCRIPTION,
    materialized_metrics_dataset=DATAFLOW_METRICS_MATERIALIZED_DATASET,
    should_materialize=True,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        US_TN_SUPERVISION_POPULATION_METRICS_PREPROCESSED_VIEW_BUILDER.build_and_print()
