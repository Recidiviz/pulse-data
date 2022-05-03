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
    /*{description}*/   
    SELECT DISTINCT
        person_id,
        date_of_supervision AS date,
        metric_type AS metric_source,
        created_on,       
        state_code,
        'SUPERVISION' AS compartment_level_1,
        supervision_type AS compartment_level_2,
        CONCAT(COALESCE(level_1_supervision_location_external_id,'EXTERNAL_UNKNOWN'),'|', COALESCE(level_2_supervision_location_external_id,'EXTERNAL_UNKNOWN')) AS compartment_location,
        CAST(NULL AS STRING) AS facility,
        COALESCE(level_1_supervision_location_external_id,'EXTERNAL_UNKNOWN') AS supervision_office,
        COALESCE(level_2_supervision_location_external_id,'EXTERNAL_UNKNOWN') AS supervision_district,
        CASE
            WHEN COALESCE(supervision_level, "INTERNAL_UNKNOWN") != "INTERNAL_UNKNOWN" THEN supervision_level
            WHEN supervision_level_raw_text IN ("9WR",  "ZWS") THEN "WARRANT"
            WHEN supervision_level_raw_text =  "9AB" THEN "ABSCONDED"
            WHEN supervision_level_raw_text =  "9IS" THEN "ICOTS_OUT"
            WHEN supervision_level_raw_text =  "9DT" THEN "DETAINER"
            ELSE "INTERNAL_UNKNOWN"
        END AS correctional_level,
        supervision_level_raw_text AS correctional_level_raw_text,
        supervising_officer_external_id,
        case_type,
        judicial_district_code,
    FROM `{project_id}.{materialized_metrics_dataset}.most_recent_supervision_population_metrics_materialized`
    WHERE state_code = 'US_TN'
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
