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
"""PA State-specific preprocessing for joining with dataflow sessions"""

from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.state.dataset_config import (
    DATAFLOW_METRICS_MATERIALIZED_DATASET,
    SESSIONS_DATASET,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

US_PA_SUPERVISION_POPULATION_METRICS_PREPROCESSED_VIEW_NAME = (
    "us_pa_supervision_population_metrics_preprocessed"
)

US_PA_SUPERVISION_POPULATION_METRICS_PREPROCESSED_VIEW_DESCRIPTION = """PA State-specific preprocessing for joining with dataflow sessions:
- Recategorizes CCC overlap with supervision as COMMUNITY_CONFINEMENT
"""

US_PA_SUPERVISION_POPULATION_METRICS_PREPROCESSED_QUERY_TEMPLATE = r"""
    /*{description}*/   
    SELECT DISTINCT
    s.person_id,
    s.date_of_supervision AS date,
    s.metric_type AS metric_source,
    s.created_on,       
    s.state_code,
    'SUPERVISION' AS compartment_level_1,
    IF(ccc.person_id IS NOT NULL, 'COMMUNITY_CONFINEMENT', s.supervision_type) AS compartment_level_2,
    CONCAT(COALESCE(s.level_1_supervision_location_external_id,'EXTERNAL_UNKNOWN'),'|', COALESCE(s.level_2_supervision_location_external_id,'EXTERNAL_UNKNOWN')) AS compartment_location,
    COALESCE(ccc.facility,'EXTERNAL_UNKNOWN') AS facility,
    COALESCE(s.level_1_supervision_location_external_id,'EXTERNAL_UNKNOWN') AS supervision_office,
    COALESCE(s.level_2_supervision_location_external_id,'EXTERNAL_UNKNOWN') AS supervision_district,
    s.supervision_level AS correctional_level,
    s.supervision_level_raw_text AS correctional_level_raw_text,
    s.supervising_officer_external_id,
    s.case_type,
    s.judicial_district_code,
FROM `{project_id}.{materialized_metrics_dataset}.most_recent_supervision_population_span_to_single_day_metrics_materialized` s
LEFT JOIN `{project_id}.{materialized_metrics_dataset}.most_recent_incarceration_population_span_metrics_materialized` ccc
    ON s.person_id = ccc.person_id
    AND s.state_code = ccc.state_code
    AND s.date_of_supervision BETWEEN ccc.start_date_inclusive AND DATE_SUB(COALESCE(ccc.end_date_exclusive,'9999-01-01'), INTERVAL 1 DAY)
    AND NOT ccc.included_in_state_population
    AND REGEXP_CONTAINS(ccc.facility, "^[123]\\d\\d\\D*")
WHERE s.state_code = 'US_PA' 
AND s.included_in_state_population
"""

US_PA_SUPERVISION_POPULATION_METRICS_PREPROCESSED_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=SESSIONS_DATASET,
    view_id=US_PA_SUPERVISION_POPULATION_METRICS_PREPROCESSED_VIEW_NAME,
    view_query_template=US_PA_SUPERVISION_POPULATION_METRICS_PREPROCESSED_QUERY_TEMPLATE,
    description=US_PA_SUPERVISION_POPULATION_METRICS_PREPROCESSED_VIEW_DESCRIPTION,
    materialized_metrics_dataset=DATAFLOW_METRICS_MATERIALIZED_DATASET,
    should_materialize=True,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        US_PA_SUPERVISION_POPULATION_METRICS_PREPROCESSED_VIEW_BUILDER.build_and_print()
