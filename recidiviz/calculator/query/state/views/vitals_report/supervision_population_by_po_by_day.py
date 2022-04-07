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
"""Supervision population by PO and day"""
from typing import Dict, Tuple

from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.bq_utils import hack_us_id_absconsions
from recidiviz.calculator.query.state import dataset_config
from recidiviz.calculator.query.state.views.dashboard.vitals_summaries.vitals_view_helpers import (
    state_specific_entity_filter,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

SUPERVISION_POPULATION_BY_PO_BY_DAY_VIEW_NAME = "supervision_population_by_po_by_day"

SUPERVISION_POPULATION_BY_PO_BY_DAY_DESCRIPTION = """
Supervision population by PO by day
"""

contact_population_by_state = {
    "US_ND": ("MINIMUM", "MEDIUM", "MAXIMUM"),
    "US_ID": (
        "MINIMUM",
        "MEDIUM",
        "HIGH",
        "MAXIMUM",
        "DIVERSION",
        "INTERSTATE_COMPACT",
        "INTERNAL_UNKNOWN",
    ),
}

risk_assessment_population_by_state = {
    "US_ND": ("MINIMUM", "MEDIUM", "MAXIMUM"),
    "US_ID": (
        "MINIMUM",
        "MEDIUM",
        "HIGH",
        "MAXIMUM",
        "DIVERSION",
        "INTERSTATE_COMPACT",
        "INTERNAL_UNKNOWN",
    ),
}

enabled_states = tuple(
    set(contact_population_by_state).union(set(risk_assessment_population_by_state))
)


def generate_state_specific_population(
    populations_by_state: Dict[str, Tuple[str, ...]], field: str
) -> str:
    """Generates a field selector which only counts people in the specified supervision groups in each state.
    Defaults to counting all supervised people for unlisted states.
    """
    state_clauses = "\n            ".join(
        f"WHEN '{state}' THEN COUNT(DISTINCT(IF(supervision_level in {populations}, person_id, null)))"
        for state, populations in populations_by_state.items()
    )
    return f"""
        CASE state_code
            {state_clauses}
            ELSE COUNT(DISTINCT(person_id))
        END as {field}"""


SUPERVISION_POPULATION_BY_PO_BY_DAY_QUERY_TEMPLATE = f"""
    /*{{description}}*/
    WITH supervision_population_metrics AS (
        {hack_us_id_absconsions('most_recent_supervision_population_metrics_materialized')}
    ),
    supervision_population AS (
        SELECT
            state_code,
            date_of_supervision,
            supervising_district_external_id,
            supervising_officer_external_id,
            CASE WHEN supervising_district_external_id = 'ALL' THEN 'ALL' ELSE district_id END AS district_id,
            CASE WHEN supervising_district_external_id = 'ALL' THEN 'ALL' ELSE district_name END AS district_name,
            COUNT(DISTINCT(person_id)) AS people_under_supervision,
            COUNT (DISTINCT IF(projected_end_date < date_of_supervision AND projected_end_date IS NOT NULL, person_id, NULL)) AS due_for_release_count,
            -- TODO(#7470): Expand contact population here once we process DIVERSION
            {generate_state_specific_population(contact_population_by_state, 'supervisees_requiring_contact')},
            {generate_state_specific_population(risk_assessment_population_by_state, 'supervisees_requiring_risk_assessment')},
        FROM supervision_population_metrics
        INNER JOIN `{{project_id}}.{{vitals_views_dataset}}.supervision_officers_and_districts_materialized` officers
            USING (state_code, supervising_officer_external_id),
        UNNEST ([officers.supervising_district_external_id, 'ALL']) AS supervising_district_external_id,
        UNNEST ([officers.supervising_officer_external_id, 'ALL']) AS supervising_officer_external_id
        WHERE date_of_supervision > DATE_SUB(CURRENT_DATE('US/Eastern'), INTERVAL 217 DAY) -- 217 = 210 days back for avgs + 7-day buffer for late data
            AND state_code in {enabled_states}
            AND {state_specific_entity_filter()}
        GROUP BY state_code, date_of_supervision, supervising_district_external_id, supervising_officer_external_id, district_id, district_name
    )
    
    SELECT 
        state_code,
        date_of_supervision,
        supervising_district_external_id,
        supervising_officer_external_id,
        district_id,
        district_name,
        people_under_supervision,
        due_for_release_count,
        supervisees_requiring_contact,
        supervisees_requiring_risk_assessment
    FROM supervision_population
    """

SUPERVISION_POPULATION_BY_PO_BY_DAY_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=dataset_config.VITALS_REPORT_DATASET,
    view_id=SUPERVISION_POPULATION_BY_PO_BY_DAY_VIEW_NAME,
    view_query_template=SUPERVISION_POPULATION_BY_PO_BY_DAY_QUERY_TEMPLATE,
    description=SUPERVISION_POPULATION_BY_PO_BY_DAY_DESCRIPTION,
    materialized_metrics_dataset=dataset_config.DATAFLOW_METRICS_MATERIALIZED_DATASET,
    vitals_views_dataset=dataset_config.VITALS_REPORT_DATASET,
    state_base_dataset=dataset_config.STATE_BASE_DATASET,
    should_materialize=True,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        SUPERVISION_POPULATION_BY_PO_BY_DAY_VIEW_BUILDER.build_and_print()
