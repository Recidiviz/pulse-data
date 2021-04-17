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
from recidiviz.calculator.query import bq_utils
from recidiviz.metrics.metric_big_query_view import MetricBigQueryViewBuilder
from recidiviz.calculator.query.state import dataset_config
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

VITALS_SUMMARIES_VIEW_NAME = "vitals_summaries"

VITALS_SUMMARIES_DESCRIPTION = """
Daily summaries of vitals metrics, at the state, district, and PO level.
"""

VITALS_SUMMARIES_QUERY_TEMPLATE = """
    /*{description}*/
    WITH most_recent_job_id AS (
    SELECT
        state_code,
        metric_date as most_recent_date_of_supervision,
        job_id,
        metric_type
      FROM
        `{project_id}.{materialized_metrics_dataset}.most_recent_daily_job_id_by_metric_and_state_code_materialized`
      WHERE metric_type = "SUPERVISION_POPULATION"
      AND state_code = "US_ND"
    ), 
    timely_discharge_po AS (
        SELECT
            due_for_release.state_code,
            MAX(IF(date_of_supervision = most_recent_job_id.most_recent_date_of_supervision, timely_discharge, null)) as most_recent_timely_discharge,
            MAX(IF(date_of_supervision = DATE_SUB(most_recent_job_id.most_recent_date_of_supervision, INTERVAL 7 DAY), timely_discharge, null)) as timely_discharge_7_days_before,
            MAX(IF(date_of_supervision = DATE_SUB(most_recent_job_id.most_recent_date_of_supervision, INTERVAL 28 DAY), timely_discharge, null)) as timely_discharge_28_days_before,
            {clean_up_supervising_officer_external_id} as entity_id,
            supervising_officer_external_id as entity_name,REPLACE(district_name, ' ', '_') as parent_entity_id,
            'po' as entity_type,
            most_recent_date_of_supervision,
        FROM `{project_id}.{vitals_report_dataset}.supervision_population_due_for_release_by_po_by_day` due_for_release
        INNER JOIN
            most_recent_job_id 
        ON
            due_for_release.state_code = most_recent_job_id.state_code
        WHERE date_of_supervision IN (
            most_recent_job_id.most_recent_date_of_supervision,
            DATE_SUB(most_recent_job_id.most_recent_date_of_supervision, INTERVAL 7 DAY),
            DATE_SUB(most_recent_job_id.most_recent_date_of_supervision, INTERVAL 28 DAY))
        AND supervising_officer_external_id != 'ALL' AND district_id != 'ALL'
        GROUP BY state_code, most_recent_date_of_supervision, entity_id, entity_name, parent_entity_id
    ),
    timely_risk_assessment_po AS (
        SELECT
            overdue_lsir.state_code,
            MAX(IF(date_of_supervision = most_recent_job_id.most_recent_date_of_supervision, timely_risk_assessment, 0)) as most_recent_timely_risk_assessment,
            MAX(IF(date_of_supervision = DATE_SUB(most_recent_job_id.most_recent_date_of_supervision, INTERVAL 7 DAY), timely_risk_assessment, 0)) as timely_risk_assessment_7_days_before,
            MAX(IF(date_of_supervision = DATE_SUB(most_recent_job_id.most_recent_date_of_supervision, INTERVAL 28 DAY), timely_risk_assessment, 0)) as timely_risk_assessment_28_days_before, 
            {clean_up_supervising_officer_external_id} as entity_id,
            supervising_officer_external_id as entity_name,
            REPLACE(district_name, ' ', '_') as parent_entity_id,
            'po' as entity_type,
            most_recent_date_of_supervision,
        FROM `{project_id}.{vitals_report_dataset}.overdue_lsir_by_po_by_day` overdue_lsir
        INNER JOIN
            most_recent_job_id 
        ON
            overdue_lsir.state_code = most_recent_job_id.state_code
        WHERE date_of_supervision IN (
            most_recent_job_id.most_recent_date_of_supervision,
            DATE_SUB(most_recent_job_id.most_recent_date_of_supervision, INTERVAL 7 DAY),
            DATE_SUB(most_recent_job_id.most_recent_date_of_supervision, INTERVAL 28 DAY))
        AND supervising_officer_external_id != 'ALL' AND district_id != 'ALL'
        GROUP BY state_code, most_recent_date_of_supervision, entity_id, entity_name, parent_entity_id
    ), timely_discharge_district AS (
        SELECT
            due_for_release.state_code,
            MAX(IF(date_of_supervision = most_recent_job_id.most_recent_date_of_supervision, timely_discharge, null)) as most_recent_timely_discharge,
            MAX(IF(date_of_supervision = DATE_SUB(most_recent_job_id.most_recent_date_of_supervision, INTERVAL 7 DAY), timely_discharge, null)) as timely_discharge_7_days_before,
            MAX(IF(date_of_supervision = DATE_SUB(most_recent_job_id.most_recent_date_of_supervision, INTERVAL 28 DAY), timely_discharge, null)) as timely_discharge_28_days_before,
            REPLACE(district_name, ' ', '_') as entity_id,district_name as entity_name,
            'STATE_DOC' as parent_entity_id,
            'level_1_supervision_location' as entity_type,
            most_recent_date_of_supervision,
        FROM `{project_id}.{vitals_report_dataset}.supervision_population_due_for_release_by_po_by_day` due_for_release
        INNER JOIN
            most_recent_job_id 
        ON
            due_for_release.state_code = most_recent_job_id.state_code
       WHERE date_of_supervision IN (
            most_recent_job_id.most_recent_date_of_supervision,
            DATE_SUB(most_recent_job_id.most_recent_date_of_supervision, INTERVAL 7 DAY),
            DATE_SUB(most_recent_job_id.most_recent_date_of_supervision, INTERVAL 28 DAY))
        AND supervising_officer_external_id = 'ALL' AND district_id != 'ALL'
        GROUP BY state_code, most_recent_date_of_supervision, entity_id, entity_name, parent_entity_id
    ),
    timely_risk_assessment_district AS (
        SELECT
            overdue_lsir.state_code,
            MAX(IF(date_of_supervision = most_recent_job_id.most_recent_date_of_supervision, timely_risk_assessment, 0)) as most_recent_timely_risk_assessment,
            MAX(IF(date_of_supervision = DATE_SUB(most_recent_job_id.most_recent_date_of_supervision, INTERVAL 7 DAY), timely_risk_assessment, 0)) as timely_risk_assessment_7_days_before,
            MAX(IF(date_of_supervision = DATE_SUB(most_recent_job_id.most_recent_date_of_supervision, INTERVAL 28 DAY), timely_risk_assessment, 0)) as timely_risk_assessment_28_days_before, 
            REPLACE(district_name, ' ', '_') as entity_id,district_name as entity_name,
            'STATE_DOC' as parent_entity_id,
            'district_level' as entity_type,
            most_recent_date_of_supervision,
        FROM `{project_id}.{vitals_report_dataset}.overdue_lsir_by_po_by_day` overdue_lsir
        INNER JOIN
            most_recent_job_id 
        ON
            overdue_lsir.state_code = most_recent_job_id.state_code
        WHERE date_of_supervision IN (
            most_recent_job_id.most_recent_date_of_supervision,
            DATE_SUB(most_recent_job_id.most_recent_date_of_supervision, INTERVAL 7 DAY),
            DATE_SUB(most_recent_job_id.most_recent_date_of_supervision, INTERVAL 28 DAY))
        AND supervising_officer_external_id = 'ALL' AND district_id != 'ALL'
        GROUP BY state_code, most_recent_date_of_supervision, entity_id, entity_name, parent_entity_id
    ), timely_discharge_state AS (
        SELECT
            due_for_release.state_code,
            MAX(IF(date_of_supervision = most_recent_job_id.most_recent_date_of_supervision, timely_discharge, null)) as most_recent_timely_discharge,
            MAX(IF(date_of_supervision = DATE_SUB(most_recent_job_id.most_recent_date_of_supervision, INTERVAL 7 DAY), timely_discharge, null)) as timely_discharge_7_days_before,
            MAX(IF(date_of_supervision = DATE_SUB(most_recent_job_id.most_recent_date_of_supervision, INTERVAL 28 DAY), timely_discharge, null)) as timely_discharge_28_days_before,
            'STATE_DOC' as entity_id,'STATE DOC' as entity_name,'STATE_DOC' as parent_entity_id,
            'state' as entity_type,
            most_recent_date_of_supervision,
        FROM `{project_id}.{vitals_report_dataset}.supervision_population_due_for_release_by_po_by_day` due_for_release
        INNER JOIN
            most_recent_job_id 
        ON
            due_for_release.state_code = most_recent_job_id.state_code
        WHERE date_of_supervision IN (
            most_recent_job_id.most_recent_date_of_supervision,
            DATE_SUB(most_recent_job_id.most_recent_date_of_supervision, INTERVAL 7 DAY),
            DATE_SUB(most_recent_job_id.most_recent_date_of_supervision, INTERVAL 28 DAY))
        AND supervising_officer_external_id = 'ALL' AND district_id = 'ALL'
        GROUP BY state_code, most_recent_date_of_supervision, entity_id, entity_name, parent_entity_id
    ),
    timely_risk_assessment_state AS (
        SELECT
            overdue_lsir.state_code,
            MAX(IF(date_of_supervision = most_recent_job_id.most_recent_date_of_supervision, timely_risk_assessment, 0)) as most_recent_timely_risk_assessment,
            MAX(IF(date_of_supervision = DATE_SUB(most_recent_job_id.most_recent_date_of_supervision, INTERVAL 7 DAY), timely_risk_assessment, 0)) as timely_risk_assessment_7_days_before,
            MAX(IF(date_of_supervision = DATE_SUB(most_recent_job_id.most_recent_date_of_supervision, INTERVAL 28 DAY), timely_risk_assessment, 0)) as timely_risk_assessment_28_days_before,
            'STATE_DOC' as entity_id,'STATE DOC' as entity_name,'STATE_DOC' as parent_entity_id,
            'state' as entity_type,
            most_recent_date_of_supervision,
        FROM `{project_id}.{vitals_report_dataset}.overdue_lsir_by_po_by_day` overdue_lsir
        INNER JOIN
            most_recent_job_id 
        ON
            overdue_lsir.state_code = most_recent_job_id.state_code
        WHERE date_of_supervision IN (
            most_recent_job_id.most_recent_date_of_supervision,
            DATE_SUB(most_recent_job_id.most_recent_date_of_supervision, INTERVAL 7 DAY),
            DATE_SUB(most_recent_job_id.most_recent_date_of_supervision, INTERVAL 28 DAY))
        AND supervising_officer_external_id = 'ALL' AND district_id = 'ALL'
        GROUP BY state_code, most_recent_date_of_supervision, entity_id, entity_name, parent_entity_id
    ), all_output AS (
        SELECT timely_discharge_po.state_code, timely_discharge_po.most_recent_date_of_supervision, timely_discharge_po.entity_type, entity_id, entity_name, parent_entity_id, most_recent_timely_discharge, timely_discharge_7_days_before, timely_discharge_28_days_before, most_recent_timely_risk_assessment, timely_risk_assessment_7_days_before, timely_risk_assessment_28_days_before
        FROM timely_discharge_po 
        LEFT JOIN timely_risk_assessment_po
        USING (entity_id, entity_name, parent_entity_id)

        UNION ALL 

        SELECT timely_discharge_district.state_code, timely_discharge_district.most_recent_date_of_supervision, timely_discharge_district.entity_type, entity_id, entity_name, parent_entity_id, most_recent_timely_discharge, timely_discharge_7_days_before, timely_discharge_28_days_before, most_recent_timely_risk_assessment, timely_risk_assessment_7_days_before, timely_risk_assessment_28_days_before
        FROM timely_discharge_district 
        LEFT JOIN timely_risk_assessment_district
        USING (entity_id, entity_name, parent_entity_id) 

        UNION ALL

        SELECT timely_discharge_state.state_code, timely_discharge_state.most_recent_date_of_supervision, timely_discharge_state.entity_type, entity_id, entity_name, parent_entity_id, most_recent_timely_discharge, timely_discharge_7_days_before, timely_discharge_28_days_before, most_recent_timely_risk_assessment, timely_risk_assessment_7_days_before, timely_risk_assessment_28_days_before
        FROM timely_discharge_state
        LEFT JOIN timely_risk_assessment_state
        USING (entity_id, entity_name, parent_entity_id)       
    )

    SELECT 
        state_code,
        most_recent_date_of_supervision,
        entity_type,
        entity_id,
        entity_name,
        parent_entity_id,
        ROUND((most_recent_timely_discharge + most_recent_timely_risk_assessment + 80) / 3, 0) as overall,
        ROUND(most_recent_timely_discharge, 0) as timely_discharge,
        ROUND(most_recent_timely_risk_assessment, 0) as timely_risk_assessment,
        # TODO(#6703): update once contact vitals are completed.
        80 as timely_contact,
        ROUND((most_recent_timely_discharge + most_recent_timely_risk_assessment + 80) / 3 - (timely_discharge_7_days_before + timely_risk_assessment_7_days_before  + 80) / 3, 0) as overall_7d,
        ROUND((most_recent_timely_discharge + most_recent_timely_risk_assessment + 80) / 3 - (timely_discharge_28_days_before + timely_risk_assessment_28_days_before + 80) / 3, 0) as overall_28d
    FROM all_output
    WHERE entity_id is not null
    AND entity_id != 'UNKNOWN'
    ORDER BY most_recent_date_of_supervision DESC
"""

VITALS_SUMMARIES_VIEW_BUILDER = MetricBigQueryViewBuilder(
    dataset_id=dataset_config.DASHBOARD_VIEWS_DATASET,
    view_id=VITALS_SUMMARIES_VIEW_NAME,
    view_query_template=VITALS_SUMMARIES_QUERY_TEMPLATE,
    dimensions=("entity_id", "entity_name", "parent_entity_id"),
    description=VITALS_SUMMARIES_DESCRIPTION,
    clean_up_supervising_officer_external_id=bq_utils.clean_up_supervising_officer_external_id(),
    vitals_report_dataset=dataset_config.VITALS_REPORT_DATASET,
    materialized_metrics_dataset=dataset_config.DATAFLOW_METRICS_MATERIALIZED_DATASET,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VITALS_SUMMARIES_VIEW_BUILDER.build_and_print()
