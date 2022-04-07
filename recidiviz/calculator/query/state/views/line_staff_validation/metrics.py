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
""" Revocation / absconsion data

To generate the BQ view, run:
    python -m recidiviz.calculator.query.state.views.line_staff_validation.metrics
"""

from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.state import dataset_config
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

METRICS_VIEW_NAME = "metrics"

METRICS_DESCRIPTION = """
"""

METRICS_QUERY_TEMPLATE = """
WITH revocations AS (
    SELECT
        state_code,
        person_id,
        IF(state_code = 'US_PA', secondary_person_external_id, person_external_id) AS external_id,
        admission_date AS event_date,
        CASE
            WHEN most_severe_violation_type = 'TECHNICAL' THEN 'TECHNICAL_REVOCATION'
            ELSE 'NEW_CRIME'
        END AS event_type,
        supervising_district_external_id AS district_id,
        supervising_officer_external_id,
    FROM `{project_id}.{materialized_metrics_dataset}.most_recent_incarceration_commitment_from_supervision_metrics_included_in_state_population_materialized`
    WHERE
        admission_date >= DATE_SUB(CURRENT_DATE('US/Eastern'), INTERVAL 90 DAY)
        AND most_severe_violation_type IN ('TECHNICAL', 'FELONY', 'MISDEMEANOR', 'MUNICIPAL', 'LAW')
), absconsions AS (
    SELECT
        state_code,
        person_id,
        external_id,
        end_date as event_date,
        "ABSCONSCION" AS event_type,
        compartment_location_end AS district_id,
        supervising_officer_external_id_end AS supervising_officer_external_id
    FROM `{project_id}.{sessions_dataset}.compartment_sessions_materialized`
    LEFT JOIN `{project_id}.{base_dataset}.state_person`
        USING (person_id, state_code)
    LEFT JOIN `{project_id}.{base_dataset}.state_person_external_id`
        USING (person_id, state_code)
    WHERE
        (
            (state_code = 'US_PA' AND id_type = 'US_PA_PBPP') OR
            (state_code = 'US_ND' AND id_type = 'US_ND_SID') OR
            (state_code NOT IN ('US_PA', 'US_ND'))
        )
        AND compartment_level_1 = 'SUPERVISION'
        AND end_reason = 'ABSCONSION'
        AND end_date >= DATE_SUB(CURRENT_DATE('US/Eastern'), INTERVAL 90 DAY)
), all_events AS (
    SELECT * FROM revocations
    UNION ALL
    SELECT * FROM absconsions
)

SELECT
    all_events.*,
    JSON_VALUE(full_name, '$.given_names') as given_names,
    JSON_VALUE(full_name, '$.surname') as surname,
FROM all_events
LEFT JOIN `{project_id}.{base_dataset}.state_person`
    USING (person_id, state_code)
ORDER BY state_code, event_date
    """

METRICS_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=dataset_config.LINESTAFF_DATA_VALIDATION,
    view_id=METRICS_VIEW_NAME,
    should_materialize=True,
    view_query_template=METRICS_QUERY_TEMPLATE,
    description=METRICS_DESCRIPTION,
    materialized_metrics_dataset=dataset_config.DATAFLOW_METRICS_MATERIALIZED_DATASET,
    sessions_dataset=dataset_config.SESSIONS_DATASET,
    base_dataset=dataset_config.STATE_BASE_DATASET,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        METRICS_VIEW_BUILDER.build_and_print()
