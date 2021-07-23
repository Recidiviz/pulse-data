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
"""Daily summaries of vitals metrics, at the state, district, and PO level."""
# pylint: disable=trailing-whitespace,line-too-long
from typing import Dict, List, Optional

from recidiviz.calculator.query import bq_utils
from recidiviz.calculator.query.bq_utils import generate_district_id_from_district_name
from recidiviz.calculator.query.state import dataset_config
from recidiviz.calculator.query.state.state_specific_query_strings import (
    VITALS_LEVEL_1_SUPERVISION_LOCATION_OPTIONS,
    vitals_state_specific_district_display_name,
    vitals_state_specific_po_name,
)
from recidiviz.calculator.query.state.views.dashboard.vitals_summaries.vitals_view_helpers import (
    ENABLED_VITALS,
    make_enabled_states_filter_for_vital,
)
from recidiviz.metrics.metric_big_query_view import MetricBigQueryViewBuilder
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

VITALS_SUMMARIES_VIEW_NAME = "vitals_summaries"

VITALS_SUMMARIES_DESCRIPTION = """
Daily summaries of vitals metrics, at the state, district, and PO level.
"""


def generate_entity_summary_query(field: str, vitals_table: str) -> str:
    po_condition = "supervising_officer_external_id != 'ALL' AND district_id != 'ALL'"
    district_condition = (
        "supervising_officer_external_id = 'ALL' AND district_id != 'ALL'"
    )

    district_level_type = f"""
        IF(state_code in {VITALS_LEVEL_1_SUPERVISION_LOCATION_OPTIONS}, 'level_1_supervision_location', 'level_2_supervision_location')
    """

    return f"""
    SELECT
      state_code,
      most_recent_date_of_supervision,
      MAX(IF(date_of_supervision = most_recent_supervision_dates_per_state.most_recent_date_of_supervision, {field}, null)) as most_recent_{field},
      MAX(IF(date_of_supervision = DATE_SUB(most_recent_supervision_dates_per_state.most_recent_date_of_supervision, INTERVAL 30 DAY), {field}, null)) as {field}_30_days_before,
      MAX(IF(date_of_supervision = DATE_SUB(most_recent_supervision_dates_per_state.most_recent_date_of_supervision, INTERVAL 90 DAY), {field}, null)) as {field}_90_days_before,
      CASE
        WHEN {po_condition} THEN {bq_utils.clean_up_supervising_officer_external_id()}
        WHEN {district_condition} THEN {generate_district_id_from_district_name('district_name')}
        ELSE 'STATE_DOC'
      END as entity_id,
      CASE
        WHEN {po_condition} THEN {vitals_state_specific_po_name('state_code', 'supervising_officer_external_id')}
        WHEN {district_condition} THEN {vitals_state_specific_district_display_name('state_code', 'district_name')}
        ELSE 'STATE DOC'
      END as entity_name,
      IF( {po_condition}, {generate_district_id_from_district_name('district_name')}, 'STATE_DOC') as parent_entity_id,
      CASE 
        WHEN {po_condition} THEN 'po'
        WHEN {district_condition} THEN {district_level_type}
        ELSE 'state'
      END as entity_type,
    FROM
     `{{project_id}}.{{vitals_report_dataset}}.{vitals_table}` metric_table
    INNER JOIN
        most_recent_supervision_dates_per_state
    USING (state_code)
    WHERE (supervising_officer_external_id = 'ALL' OR district_id != 'ALL')
      AND date_of_supervision IN (
        most_recent_supervision_dates_per_state.most_recent_date_of_supervision,
        DATE_SUB(most_recent_supervision_dates_per_state.most_recent_date_of_supervision, INTERVAL 30 DAY),
        DATE_SUB(most_recent_supervision_dates_per_state.most_recent_date_of_supervision, INTERVAL 90 DAY))
      AND {make_enabled_states_filter_for_vital(field)}
    GROUP BY state_code, most_recent_date_of_supervision, entity_id, entity_name, parent_entity_id, supervising_officer_external_id, district_id
    """


def generate_overall_scores(
    enabled_vitals: Optional[Dict[str, List[str]]] = None,
) -> str:
    """
    Uses ENABLED_VITALS to generate the overall score field by averaging all of the enabled vitals per state.
    """
    # Example generated code:
    # enabled_vitals = {
    #    'US_XX': ['vital_1', 'vital_2'],
    #    'US_YY': ['vital_1', 'vital_3', 'vital_4']
    # }
    #
    # CASE state_code
    #   WHEN 'US_XX' THEN ROUND((most_recent_vital_1 + most_recent_vital_2) / 2, 0)
    #   WHEN 'US_YY' THEN ROUND((most_recent_vital_1 + most_recent_vital_3 + most_recent_vital_4) / 3, 0)
    # END as overall,
    #
    # CASE state_code
    #   WHEN 'US_XX' THEN ROUND((most_recent_vital_1 + most_recent_vital_2) / 2 - (vital_1_30_days_before + vital_2_30_days_before) / 2, 0)
    #   WHEN 'US_YY' THEN ROUND((most_recent_vital_1 + most_recent_vital_3 + most_recent_vital_4) / 3 - (vital_1_30_days_before + vital_3_30_days_before + vital_4_30_days_before) / 3, 0)
    # END as overall_30d,
    #
    # CASE state_code
    #   WHEN 'US_XX' THEN ROUND((most_recent_vital_1 + most_recent_vital_2) / 2 - (vital_1_90_days_before + vital_2_90_days_before) / 2, 0)
    #   WHEN 'US_YY' THEN ROUND((most_recent_vital_1 + most_recent_vital_3 + most_recent_vital_4) / 3 - (vital_1_90_days_before + vital_3_90_days_before + vital_4_90_days_before) / 3, 0)
    # END as overall_90d,

    if enabled_vitals is None:
        enabled_vitals = ENABLED_VITALS

    def most_recent_vitals_sum(vitals: List[str]) -> str:
        return f'({" + ".join(f"most_recent_{metric}" for metric in vitals)}) / {len(vitals)}'

    def historic_vitals_sum(vitals: List[str], days_past: int) -> str:
        return f'({" + ".join(f"{metric}_{days_past}_days_before" for metric in vitals)}) / {len(vitals)}'

    overall_cases = "\n        ".join(
        f"WHEN '{state}' THEN ROUND({most_recent_vitals_sum(vitals)}, 0)"
        for state, vitals in enabled_vitals.items()
    )
    most_recent_overall_query = f"""
      CASE state_code
        {overall_cases}
      END as overall,"""

    overall_30_cases = "\n        ".join(
        f"WHEN '{state}' THEN ROUND({most_recent_vitals_sum(vitals)} - {historic_vitals_sum(vitals, 30)}, 0)"
        for state, vitals in enabled_vitals.items()
    )
    overall_30_query = f"""
      CASE state_code
        {overall_30_cases}
      END as overall_30d,"""

    overall_90_cases = "\n        ".join(
        f"WHEN '{state}' THEN ROUND({most_recent_vitals_sum(vitals)} - {historic_vitals_sum(vitals, 90)}, 0)"
        for state, vitals in enabled_vitals.items()
    )
    overall_90_query = f"""
      CASE state_code
        {overall_90_cases}
      END as overall_90d,"""

    return most_recent_overall_query + overall_30_query + overall_90_query


VITALS_SUMMARIES_QUERY_TEMPLATE = f"""
    /*{{description}}*/
    WITH most_recent_supervision_dates_per_state AS (
    SELECT DISTINCT
        state_code,
        date_of_supervision as most_recent_date_of_supervision,
      FROM
        `{{project_id}}.{{materialized_metrics_dataset}}.most_recent_single_day_supervision_population_metrics_materialized`
    ), 
    timely_discharge AS (
     {generate_entity_summary_query('timely_discharge', 'supervision_population_due_for_release_by_po_by_day')}
    ),
    timely_risk_assessment AS (
     {generate_entity_summary_query('timely_risk_assessment', 'overdue_lsir_by_po_by_day')}
    ),
    timely_contact AS (
     {generate_entity_summary_query('timely_contact', 'timely_contact_by_po_by_day')}
    )

    SELECT 
      state_code,
      timely_risk_assessment.most_recent_date_of_supervision,
      timely_risk_assessment.entity_type,
      timely_risk_assessment.entity_id,
      timely_risk_assessment.entity_name,
      timely_risk_assessment.parent_entity_id,
      ROUND(most_recent_timely_discharge) as timely_discharge,
      ROUND(most_recent_timely_risk_assessment) as timely_risk_assessment,
      ROUND(most_recent_timely_contact) as timely_contact,
      {generate_overall_scores()}
    FROM timely_discharge
    FULL OUTER JOIN timely_risk_assessment
      USING (state_code, entity_id, parent_entity_id)
    FULL OUTER JOIN timely_contact
      USING (state_code, entity_id, parent_entity_id)
    WHERE entity_id is not null
      AND entity_id != 'UNKNOWN'
      AND ROUND(most_recent_timely_risk_assessment) != 0
    ORDER BY most_recent_date_of_supervision DESC
"""

VITALS_SUMMARIES_VIEW_BUILDER = MetricBigQueryViewBuilder(
    dataset_id=dataset_config.DASHBOARD_VIEWS_DATASET,
    view_id=VITALS_SUMMARIES_VIEW_NAME,
    view_query_template=VITALS_SUMMARIES_QUERY_TEMPLATE,
    dimensions=("entity_id", "entity_name", "parent_entity_id"),
    description=VITALS_SUMMARIES_DESCRIPTION,
    vitals_report_dataset=dataset_config.VITALS_REPORT_DATASET,
    materialized_metrics_dataset=dataset_config.DATAFLOW_METRICS_MATERIALIZED_DATASET,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VITALS_SUMMARIES_VIEW_BUILDER.build_and_print()
