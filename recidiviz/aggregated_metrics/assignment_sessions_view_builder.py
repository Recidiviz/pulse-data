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
"""Generates view builder creating spans of assignment to a level of analysis for a specified population"""
from recidiviz.aggregated_metrics.dataset_config import AGGREGATED_METRICS_DATASET_ID
from recidiviz.aggregated_metrics.models.metric_population_type import (
    MetricPopulationType,
)
from recidiviz.aggregated_metrics.models.metric_unit_of_analysis_type import (
    MetricUnitOfAnalysis,
    MetricUnitOfAnalysisType,
)
from recidiviz.big_query.big_query_address import BigQueryAddress
from recidiviz.big_query.big_query_view import (
    BigQueryViewBuilder,
    SimpleBigQueryViewBuilder,
)
from recidiviz.calculator.query.bq_utils import (
    MAGIC_START_DATE,
    nonnull_end_date_clause,
    revert_nonnull_end_date_clause,
)
from recidiviz.calculator.query.sessions_query_fragments import (
    create_intersection_spans,
    create_sub_sessions_with_attributes,
    list_to_query_string,
)
from recidiviz.observations.metric_unit_of_observation import MetricUnitOfObservation
from recidiviz.observations.metric_unit_of_observation_type import (
    MetricUnitOfObservationType,
)
from recidiviz.observations.span_selector import SpanSelector
from recidiviz.segment.product_type import ProductType

# Dictionary of queries define periods of assignment of a unit of observation to a unit of analysis
_UNIT_OF_ANALYSIS_ASSIGNMENT_QUERIES_DICT: dict[
    tuple[MetricUnitOfObservationType, MetricUnitOfAnalysisType], str
] = {
    **{
        # Experiment variant assignments
        (
            metric_unit_of_observation_type,
            MetricUnitOfAnalysisType.EXPERIMENT_VARIANT,
        ): f"SELECT * FROM `{{project_id}}.experiments_metadata.experiment_assignments_{metric_unit_of_observation_type.short_name}_materialized`"
        for metric_unit_of_observation_type in MetricUnitOfObservationType
    },
    (
        MetricUnitOfObservationType.PERSON_ID,
        MetricUnitOfAnalysisType.PERSON_ID,
    ): "SELECT * FROM `{project_id}.sessions.system_sessions_materialized`",
    (
        MetricUnitOfObservationType.PERSON_ID,
        MetricUnitOfAnalysisType.SUPERVISION_OFFICER,
    ): """
SELECT *, supervising_officer_external_id AS officer_id
FROM `{project_id}.sessions.supervision_officer_sessions_materialized`
""",
    (
        MetricUnitOfObservationType.PERSON_ID,
        MetricUnitOfAnalysisType.SUPERVISION_OFFICER_OR_PREVIOUS_IF_TRANSITIONAL,
    ): """
SELECT *, supervising_officer_external_id AS officer_id
FROM `{project_id}.sessions.supervision_officer_or_previous_if_transitional_sessions_materialized`
""",
    (
        MetricUnitOfObservationType.PERSON_ID,
        MetricUnitOfAnalysisType.SUPERVISION_OFFICE,
    ): """SELECT
    *, 
    supervision_district AS district,
    supervision_office AS office,
FROM
    `{project_id}.sessions.location_sessions_materialized`
""",
    (
        MetricUnitOfObservationType.PERSON_ID,
        MetricUnitOfAnalysisType.SUPERVISION_DISTRICT,
    ): "SELECT *, supervision_district AS district, FROM `{project_id}.sessions.location_sessions_materialized`",
    (
        MetricUnitOfObservationType.PERSON_ID,
        MetricUnitOfAnalysisType.SUPERVISION_UNIT,
    ): "SELECT * FROM `{project_id}.sessions.supervision_unit_supervisor_sessions_materialized`",
    (
        MetricUnitOfObservationType.PERSON_ID,
        MetricUnitOfAnalysisType.WORKFLOWS_CASELOAD,
    ): "SELECT * FROM `{project_id}.sessions.person_caseload_location_sessions_materialized`",
    (
        MetricUnitOfObservationType.PERSON_ID,
        MetricUnitOfAnalysisType.LOCATION_DETAIL,
    ): "SELECT * FROM `{project_id}.sessions.person_caseload_location_sessions_materialized`",
    (
        MetricUnitOfObservationType.PERSON_ID,
        MetricUnitOfAnalysisType.LOCATION,
    ): "SELECT * FROM `{project_id}.sessions.person_caseload_location_sessions_materialized`",
    (
        MetricUnitOfObservationType.PERSON_ID,
        MetricUnitOfAnalysisType.STATE_CODE,
    ): "SELECT * FROM `{project_id}.sessions.compartment_sessions_materialized`",
    (
        MetricUnitOfObservationType.PERSON_ID,
        MetricUnitOfAnalysisType.FACILITY,
    ): "SELECT * FROM `{project_id}.sessions.location_sessions_materialized`",
    (
        MetricUnitOfObservationType.PERSON_ID,
        MetricUnitOfAnalysisType.FACILITY_COUNSELOR,
    ): """
SELECT 
    * EXCEPT (incarceration_staff_assignment_id),
    incarceration_staff_assignment_id AS facility_counselor_id,
FROM
    `{project_id}.sessions.incarceration_staff_assignment_sessions_preprocessed_materialized`
""",
    (
        MetricUnitOfObservationType.PERSON_ID,
        MetricUnitOfAnalysisType.INSIGHTS_CASELOAD_CATEGORY,
    ): """
SELECT * FROM `{project_id}.analyst_data.insights_caseload_category_sessions_materialized`
""",
    (
        MetricUnitOfObservationType.PERSON_ID,
        MetricUnitOfAnalysisType.ALL_STATES,
    ): """
SELECT *, TRUE AS in_signed_state FROM `{project_id}.sessions.system_sessions_materialized`
""",
    (
        MetricUnitOfObservationType.SUPERVISION_OFFICER,
        MetricUnitOfAnalysisType.SUPERVISION_OFFICER,
    ): """
SELECT * FROM `{project_id}.sessions.supervision_officer_caseload_count_spans_materialized`
""",
    (
        MetricUnitOfObservationType.SUPERVISION_OFFICER,
        MetricUnitOfAnalysisType.SUPERVISION_OFFICER_OR_PREVIOUS_IF_TRANSITIONAL,
    ): """
SELECT * FROM `{project_id}.sessions.supervision_staff_attribute_sessions_materialized`
WHERE is_supervision_officer
""",
    (
        MetricUnitOfObservationType.SUPERVISION_OFFICER,
        MetricUnitOfAnalysisType.SUPERVISION_OFFICE,
    ): """SELECT
    *, 
    supervision_office_id AS office,
    supervision_district_id AS district,
FROM
    `{project_id}.sessions.supervision_staff_attribute_sessions_materialized`
WHERE is_supervision_officer
""",
    (
        MetricUnitOfObservationType.SUPERVISION_OFFICER,
        MetricUnitOfAnalysisType.SUPERVISION_DISTRICT,
    ): """SELECT
    *, supervision_district_id AS district,
FROM
    `{project_id}.sessions.supervision_staff_attribute_sessions_materialized`
WHERE is_supervision_officer
""",
    (
        MetricUnitOfObservationType.SUPERVISION_OFFICER,
        MetricUnitOfAnalysisType.SUPERVISION_UNIT,
    ): """SELECT
    *, 
    supervisor_staff_id AS unit_supervisor,
FROM
    `{project_id}.sessions.supervision_staff_attribute_sessions_materialized`,
    UNNEST(supervisor_staff_id_array) AS supervisor_staff_id
WHERE is_supervision_officer
""",
    (
        MetricUnitOfObservationType.SUPERVISION_OFFICER,
        MetricUnitOfAnalysisType.STATE_CODE,
    ): """
SELECT * FROM `{project_id}.sessions.supervision_staff_attribute_sessions_materialized`
WHERE is_supervision_officer
""",
    (
        MetricUnitOfObservationType.SUPERVISION_OFFICER,
        MetricUnitOfAnalysisType.ALL_STATES,
    ): """
SELECT *, TRUE AS in_signed_state
FROM `{project_id}.sessions.supervision_staff_attribute_sessions_materialized`
WHERE is_supervision_officer
""",
    (
        MetricUnitOfObservationType.WORKFLOWS_PRIMARY_USER,
        MetricUnitOfAnalysisType.ALL_STATES,
    ): """SELECT
    state_code,
    email_address,
    start_date,
    end_date_exclusive,
    TRUE AS in_signed_state,
FROM
    `{project_id}.analyst_data.workflows_provisioned_user_registration_sessions_materialized`
WHERE
    is_registered
    AND is_primary_user""",
    (
        MetricUnitOfObservationType.WORKFLOWS_PRIMARY_USER,
        MetricUnitOfAnalysisType.STATE_CODE,
    ): """SELECT
    state_code,
    email_address,
    start_date,
    end_date_exclusive, 
FROM
    `{project_id}.analyst_data.workflows_provisioned_user_registration_sessions_materialized`
WHERE
    is_registered
    AND is_primary_user""",
    (
        MetricUnitOfObservationType.WORKFLOWS_PRIMARY_USER,
        MetricUnitOfAnalysisType.SUPERVISION_DISTRICT,
    ): """SELECT
        state_code,
        email_address,
        start_date,
        end_date_exclusive,
        location_id AS district,
    FROM
        `{project_id}.analyst_data.workflows_provisioned_user_registration_sessions_materialized`
    WHERE
        system_type = "SUPERVISION"
        AND is_registered
        AND is_primary_user
""",
    (
        MetricUnitOfObservationType.WORKFLOWS_PROVISIONED_USER,
        MetricUnitOfAnalysisType.SUPERVISION_OFFICE,
    ): """SELECT
        state_code,
        email_address,
        start_date,
        end_date_exclusive,
        state_staff_supervision_district AS district,
        state_staff_supervision_office AS office,
    FROM
        `{project_id}.analyst_data.workflows_provisioned_user_registration_sessions_materialized`
    WHERE
        system_type = "SUPERVISION"
""",
    (
        MetricUnitOfObservationType.WORKFLOWS_PRIMARY_USER,
        MetricUnitOfAnalysisType.SUPERVISION_OFFICE,
    ): """SELECT
        state_code,
        email_address,
        start_date,
        end_date_exclusive,
        state_staff_supervision_district AS district,
        state_staff_supervision_office AS office,
    FROM
        `{project_id}.analyst_data.workflows_provisioned_user_registration_sessions_materialized`
    WHERE
        system_type = "SUPERVISION"
        AND is_registered
        AND is_primary_user
""",
    (
        MetricUnitOfObservationType.WORKFLOWS_PRIMARY_USER,
        MetricUnitOfAnalysisType.FACILITY,
    ): """SELECT
        state_code,
        email_address,
        start_date,
        end_date_exclusive,
        location_id AS facility,
    FROM
        `{project_id}.analyst_data.workflows_provisioned_user_registration_sessions_materialized`
    WHERE
        system_type = "INCARCERATION"
        AND is_registered
        AND is_primary_user
""",
    (
        MetricUnitOfObservationType.WORKFLOWS_PRIMARY_USER,
        MetricUnitOfAnalysisType.LOCATION,
    ): """SELECT
    state_code,
    email_address,
    start_date,
    end_date_exclusive,
    location_name,
FROM
    `{project_id}.analyst_data.workflows_provisioned_user_registration_sessions_materialized`
WHERE
    is_registered
    AND is_primary_user""",
    (
        MetricUnitOfObservationType.WORKFLOWS_PRIMARY_USER,
        MetricUnitOfAnalysisType.SUPERVISION_OFFICER,
    ): """SELECT
    state_code,
    email_address,
    start_date,
    end_date_exclusive,
    staff_external_id AS officer_id,
FROM
    `{project_id}.analyst_data.workflows_provisioned_user_registration_sessions_materialized`
WHERE
    system_type = "SUPERVISION"
    AND is_registered
    AND is_primary_user""",
    (
        MetricUnitOfObservationType.WORKFLOWS_PRIMARY_USER,
        MetricUnitOfAnalysisType.SUPERVISION_OFFICER_OR_PREVIOUS_IF_TRANSITIONAL,
    ): """SELECT
    state_code,
    email_address,
    start_date,
    end_date_exclusive,
    staff_external_id AS officer_id,
FROM
    `{project_id}.analyst_data.workflows_provisioned_user_registration_sessions_materialized`
WHERE
    system_type = "SUPERVISION"
    AND is_registered
    AND is_primary_user""",
    (
        MetricUnitOfObservationType.WORKFLOWS_PRIMARY_USER,
        MetricUnitOfAnalysisType.FACILITY_COUNSELOR,
    ): """SELECT
    state_code,
    email_address,
    start_date,
    end_date_exclusive,
    staff_id AS facility_counselor_id,
FROM
    `{project_id}.analyst_data.workflows_provisioned_user_registration_sessions_materialized`
WHERE
    system_type = "INCARCERATION"
    AND is_registered
    AND is_primary_user""",
    (
        MetricUnitOfObservationType.WORKFLOWS_PROVISIONED_USER,
        MetricUnitOfAnalysisType.ALL_STATES,
    ): """SELECT
    state_code,
    email_address,
    start_date,
    end_date_exclusive, 
    TRUE AS in_signed_state,
FROM
    `{project_id}.analyst_data.workflows_provisioned_user_registration_sessions_materialized`
""",
    (
        MetricUnitOfObservationType.WORKFLOWS_PROVISIONED_USER,
        MetricUnitOfAnalysisType.STATE_CODE,
    ): """SELECT
    state_code,
    email_address,
    start_date,
    end_date_exclusive, 
FROM
    `{project_id}.analyst_data.workflows_provisioned_user_registration_sessions_materialized`
""",
    (
        MetricUnitOfObservationType.WORKFLOWS_PROVISIONED_USER,
        MetricUnitOfAnalysisType.SUPERVISION_DISTRICT,
    ): """SELECT
        state_code,
        email_address,
        start_date,
        end_date_exclusive,
        location_id AS district,
    FROM
        `{project_id}.analyst_data.workflows_provisioned_user_registration_sessions_materialized`
    WHERE
        system_type = "SUPERVISION"
""",
    (
        MetricUnitOfObservationType.WORKFLOWS_PROVISIONED_USER,
        MetricUnitOfAnalysisType.FACILITY,
    ): """SELECT
        state_code,
        email_address,
        start_date,
        end_date_exclusive,
        location_id AS facility,
    FROM
        `{project_id}.analyst_data.workflows_provisioned_user_registration_sessions_materialized`
    WHERE
        system_type = "INCARCERATION"
""",
    (
        MetricUnitOfObservationType.WORKFLOWS_PROVISIONED_USER,
        MetricUnitOfAnalysisType.LOCATION,
    ): """SELECT
    state_code,
    email_address,
    start_date,
    end_date_exclusive,
    location_name,
FROM
    `{project_id}.analyst_data.workflows_provisioned_user_registration_sessions_materialized`
""",
    (
        MetricUnitOfObservationType.WORKFLOWS_PROVISIONED_USER,
        MetricUnitOfAnalysisType.SUPERVISION_OFFICER,
    ): """SELECT
    state_code,
    email_address,
    start_date,
    end_date_exclusive,
    staff_external_id AS officer_id,
FROM
    `{project_id}.analyst_data.workflows_provisioned_user_registration_sessions_materialized`
WHERE
    system_type = "SUPERVISION"
""",
    (
        MetricUnitOfObservationType.WORKFLOWS_PROVISIONED_USER,
        MetricUnitOfAnalysisType.SUPERVISION_OFFICER_OR_PREVIOUS_IF_TRANSITIONAL,
    ): """SELECT
    state_code,
    email_address,
    start_date,
    end_date_exclusive,
    staff_external_id AS officer_id,
FROM
    `{project_id}.analyst_data.workflows_provisioned_user_registration_sessions_materialized`
WHERE
    system_type = "SUPERVISION"
""",
    (
        MetricUnitOfObservationType.WORKFLOWS_PROVISIONED_USER,
        MetricUnitOfAnalysisType.FACILITY_COUNSELOR,
    ): """SELECT
    state_code,
    email_address,
    start_date,
    end_date_exclusive,
    staff_id AS facility_counselor_id,
FROM
    `{project_id}.analyst_data.workflows_provisioned_user_registration_sessions_materialized`
WHERE
    system_type = "INCARCERATION"
""",
    (
        MetricUnitOfObservationType.WORKFLOWS_SURFACEABLE_CASELOAD,
        MetricUnitOfAnalysisType.ALL_STATES,
    ): "SELECT *, TRUE AS in_signed_state FROM `{project_id}.analyst_data.workflows_record_archive_surfaceable_caseload_sessions_materialized`",
    (
        MetricUnitOfObservationType.WORKFLOWS_SURFACEABLE_CASELOAD,
        MetricUnitOfAnalysisType.STATE_CODE,
    ): "SELECT * FROM `{project_id}.analyst_data.workflows_record_archive_surfaceable_caseload_sessions_materialized`",
    (
        MetricUnitOfObservationType.INSIGHTS_PRIMARY_USER,
        MetricUnitOfAnalysisType.ALL_STATES,
    ): """SELECT
    state_code,
    email_address,
    start_date,
    end_date_exclusive, 
    TRUE AS in_signed_state,
FROM
    `{project_id}.analyst_data.supervisor_homepage_outcomes_module_provisioned_user_registration_sessions_materialized`
WHERE
    is_registered
    AND is_primary_user""",
    (
        MetricUnitOfObservationType.INSIGHTS_PRIMARY_USER,
        MetricUnitOfAnalysisType.STATE_CODE,
    ): """SELECT
    state_code,
    email_address,
    start_date,
    end_date_exclusive, 
FROM
    `{project_id}.analyst_data.supervisor_homepage_outcomes_module_provisioned_user_registration_sessions_materialized`
WHERE
    is_registered
    AND is_primary_user""",
    (
        MetricUnitOfObservationType.INSIGHTS_PRIMARY_USER,
        MetricUnitOfAnalysisType.SUPERVISION_DISTRICT,
    ): """SELECT
        state_code,
        email_address,
        start_date,
        end_date_exclusive,
        location_id AS district,
    FROM
        `{project_id}.analyst_data.supervisor_homepage_outcomes_module_provisioned_user_registration_sessions_materialized`
    WHERE
        is_registered
        AND is_primary_user
""",
    (
        MetricUnitOfObservationType.INSIGHTS_PRIMARY_USER,
        MetricUnitOfAnalysisType.SUPERVISION_OFFICE,
    ): """SELECT
        state_code,
        email_address,
        start_date,
        end_date_exclusive,
        state_staff_supervision_district AS district,
        state_staff_supervision_office AS office,
    FROM
        `{project_id}.analyst_data.supervisor_homepage_outcomes_module_provisioned_user_registration_sessions_materialized`
    WHERE
        system_type = "SUPERVISION"
        AND is_registered
        AND is_primary_user
""",
    (
        MetricUnitOfObservationType.INSIGHTS_PROVISIONED_USER,
        MetricUnitOfAnalysisType.SUPERVISION_OFFICE,
    ): """SELECT
        state_code,
        email_address,
        start_date,
        end_date_exclusive,
        state_staff_supervision_district AS district,
        state_staff_supervision_office AS office,
    FROM
        `{project_id}.analyst_data.supervisor_homepage_outcomes_module_provisioned_user_registration_sessions_materialized`
    WHERE
        system_type = "SUPERVISION"
""",
    (
        MetricUnitOfObservationType.INSIGHTS_PRIMARY_USER,
        MetricUnitOfAnalysisType.SUPERVISION_UNIT,
    ): """SELECT
        state_code,
        email_address,
        start_date,
        end_date_exclusive,
        staff_id AS unit_supervisor,
    FROM
        `{project_id}.analyst_data.supervisor_homepage_outcomes_module_provisioned_user_registration_sessions_materialized`
    WHERE
        is_registered
        AND is_primary_user
""",
    (
        MetricUnitOfObservationType.INSIGHTS_PROVISIONED_USER,
        MetricUnitOfAnalysisType.ALL_STATES,
    ): """SELECT
    state_code,
    email_address,
    start_date,
    end_date_exclusive, 
    TRUE AS in_signed_state,
FROM
    `{project_id}.analyst_data.supervisor_homepage_outcomes_module_provisioned_user_registration_sessions_materialized`
""",
    (
        MetricUnitOfObservationType.INSIGHTS_PROVISIONED_USER,
        MetricUnitOfAnalysisType.STATE_CODE,
    ): """SELECT
    state_code,
    email_address,
    start_date,
    end_date_exclusive, 
FROM
    `{project_id}.analyst_data.supervisor_homepage_outcomes_module_provisioned_user_registration_sessions_materialized`
""",
    (
        MetricUnitOfObservationType.INSIGHTS_PROVISIONED_USER,
        MetricUnitOfAnalysisType.SUPERVISION_DISTRICT,
    ): """SELECT
        state_code,
        email_address,
        start_date,
        end_date_exclusive,
        location_id AS district,
    FROM
        `{project_id}.analyst_data.supervisor_homepage_outcomes_module_provisioned_user_registration_sessions_materialized`
""",
    (
        MetricUnitOfObservationType.INSIGHTS_PROVISIONED_USER,
        MetricUnitOfAnalysisType.SUPERVISION_UNIT,
    ): """SELECT
        state_code,
        email_address,
        start_date,
        end_date_exclusive,
        staff_id AS unit_supervisor,
    FROM
        `{project_id}.analyst_data.supervisor_homepage_outcomes_module_provisioned_user_registration_sessions_materialized`
""",
    (
        MetricUnitOfObservationType.INSIGHTS_PROVISIONED_USER,
        MetricUnitOfAnalysisType.SUPERVISION_OFFICER,
    ): """SELECT
        state_code,
        email_address,
        start_date,
        end_date_exclusive,
        staff_external_id AS officer_id,
    FROM
        `{project_id}.analyst_data.supervisor_homepage_outcomes_module_provisioned_user_registration_sessions_materialized`
""",
    (
        MetricUnitOfObservationType.INSIGHTS_PROVISIONED_USER,
        MetricUnitOfAnalysisType.INSIGHTS_PROVISIONED_USER,
    ): """SELECT
        state_code,
        email_address,
        start_date,
        end_date_exclusive,
    FROM
        `{project_id}.analyst_data.supervisor_homepage_outcomes_module_provisioned_user_registration_sessions_materialized`
""",
    (
        MetricUnitOfObservationType.INSIGHTS_PRIMARY_USER,
        MetricUnitOfAnalysisType.INSIGHTS_PROVISIONED_USER,
    ): """SELECT
        state_code,
        email_address,
        start_date,
        end_date_exclusive,
    FROM
        `{project_id}.analyst_data.supervisor_homepage_outcomes_module_provisioned_user_registration_sessions_materialized`
    WHERE
        is_registered
        AND is_primary_user
""",
    (
        MetricUnitOfObservationType.WORKFLOWS_PROVISIONED_USER,
        MetricUnitOfAnalysisType.WORKFLOWS_PROVISIONED_USER,
    ): """SELECT
    state_code,
    email_address,
    start_date,
    end_date_exclusive, 
FROM
    `{project_id}.analyst_data.workflows_provisioned_user_registration_sessions_materialized`
""",
    (
        MetricUnitOfObservationType.WORKFLOWS_PRIMARY_USER,
        MetricUnitOfAnalysisType.WORKFLOWS_PROVISIONED_USER,
    ): """SELECT
    state_code,
    email_address,
    start_date,
    end_date_exclusive, 
FROM
    `{project_id}.analyst_data.workflows_provisioned_user_registration_sessions_materialized`
WHERE
    is_registered
    AND is_primary_user
""",
    (
        MetricUnitOfObservationType.SUPERVISION_OFFICER,
        MetricUnitOfAnalysisType.WORKFLOWS_PROVISIONED_USER,
    ): """SELECT
    users.state_code,
    users.email_address,
    staff_external.supervising_officer_external_id AS officer_id,
    users.start_date,
    users.end_date_exclusive, 
FROM
    `{project_id}.analyst_data.workflows_provisioned_user_registration_sessions_materialized` users
INNER JOIN
    `{project_id}.normalized_state.state_staff` staff
ON
    users.email_address = staff.email
    AND users.state_code = staff.state_code
INNER JOIN
    `{project_id}.sessions.state_staff_id_to_legacy_supervising_officer_external_id_materialized` staff_external
ON
    staff.staff_id = staff_external.staff_id
""",
    (
        MetricUnitOfObservationType.PERSON_ID,
        MetricUnitOfAnalysisType.WORKFLOWS_PROVISIONED_USER,
    ): """SELECT * FROM `{project_id}.analyst_data.workflows_user_person_assignment_sessions_materialized`""",
    (
        MetricUnitOfObservationType.PERSON_ID,
        MetricUnitOfAnalysisType.INSIGHTS_PROVISIONED_USER,
    ): """SELECT * FROM `{project_id}.analyst_data.insights_user_person_assignment_sessions_materialized`""",
    (
        MetricUnitOfObservationType.SUPERVISION_OFFICER,
        MetricUnitOfAnalysisType.OFFICER_OUTLIER_USAGE_COHORT,
    ): """
SELECT
    state_code,
    officer_id,
    cohort_month_end_date,
    cohort_month_end_date AS start_date,
    CAST(NULL AS DATE) AS end_date_exclusive,
    metric_id,
    outlier_usage_cohort,
FROM
    `{project_id}.analyst_data.insights_officer_outlier_usage_cohort_materialized`""",
    (
        MetricUnitOfObservationType.PERSON_ID,
        MetricUnitOfAnalysisType.OFFICER_OUTLIER_USAGE_COHORT,
    ): f"""
SELECT
    cohort.state_code,
    sessions.person_id,
    cohort.cohort_month_end_date,
    GREATEST(cohort.cohort_month_end_date, sessions.start_date) AS start_date,
    sessions.end_date_exclusive AS end_date_exclusive,
    cohort.metric_id,
    cohort.outlier_usage_cohort,
FROM
    `{{project_id}}.analyst_data.insights_officer_outlier_usage_cohort_materialized` cohort
-- Get all persons supervised by the officer after the cohort month end date
LEFT JOIN
    `{{project_id}}.sessions.supervision_officer_sessions_materialized` sessions
ON
    cohort.officer_id = sessions.supervising_officer_external_id
    AND cohort.state_code = sessions.state_code
    AND {nonnull_end_date_clause("sessions.end_date_exclusive")} >= cohort.cohort_month_end_date
""",
    (
        MetricUnitOfObservationType.TASKS_PRIMARY_USER,
        MetricUnitOfAnalysisType.ALL_STATES,
    ): """SELECT
    state_code,
    email_address,
    start_date,
    end_date_exclusive,
    TRUE AS in_signed_state,
FROM
    `{project_id}.analyst_data.tasks_provisioned_user_registration_sessions_materialized`
WHERE
    is_registered
    AND is_primary_user""",
    (
        MetricUnitOfObservationType.TASKS_PRIMARY_USER,
        MetricUnitOfAnalysisType.STATE_CODE,
    ): """SELECT
    state_code,
    email_address,
    start_date,
    end_date_exclusive, 
FROM
    `{project_id}.analyst_data.tasks_provisioned_user_registration_sessions_materialized`
WHERE
    is_registered
    AND is_primary_user""",
    (
        MetricUnitOfObservationType.TASKS_PRIMARY_USER,
        MetricUnitOfAnalysisType.SUPERVISION_DISTRICT,
    ): """SELECT
        state_code,
        email_address,
        start_date,
        end_date_exclusive,
        location_id AS district,
    FROM
        `{project_id}.analyst_data.tasks_provisioned_user_registration_sessions_materialized`
    WHERE
        system_type = "SUPERVISION"
        AND is_registered
        AND is_primary_user
""",
    (
        MetricUnitOfObservationType.TASKS_PRIMARY_USER,
        MetricUnitOfAnalysisType.LOCATION,
    ): """SELECT
    state_code,
    email_address,
    start_date,
    end_date_exclusive,
    location_name,
FROM
    `{project_id}.analyst_data.tasks_provisioned_user_registration_sessions_materialized`
WHERE
    is_registered
    AND is_primary_user""",
    (
        MetricUnitOfObservationType.TASKS_PRIMARY_USER,
        MetricUnitOfAnalysisType.SUPERVISION_OFFICER,
    ): """SELECT
    state_code,
    email_address,
    start_date,
    end_date_exclusive,
    staff_external_id AS officer_id,
FROM
    `{project_id}.analyst_data.tasks_provisioned_user_registration_sessions_materialized`
WHERE
    system_type = "SUPERVISION"
    AND is_registered
    AND is_primary_user""",
    (
        MetricUnitOfObservationType.TASKS_PRIMARY_USER,
        MetricUnitOfAnalysisType.SUPERVISION_OFFICER_OR_PREVIOUS_IF_TRANSITIONAL,
    ): """SELECT
    state_code,
    email_address,
    start_date,
    end_date_exclusive,
    staff_external_id AS officer_id,
FROM
    `{project_id}.analyst_data.tasks_provisioned_user_registration_sessions_materialized`
WHERE
    system_type = "SUPERVISION"
    AND is_registered
    AND is_primary_user""",
    (
        MetricUnitOfObservationType.TASKS_PROVISIONED_USER,
        MetricUnitOfAnalysisType.ALL_STATES,
    ): """SELECT
    state_code,
    email_address,
    start_date,
    end_date_exclusive, 
    TRUE AS in_signed_state,
FROM
    `{project_id}.analyst_data.tasks_provisioned_user_registration_sessions_materialized`
""",
    (
        MetricUnitOfObservationType.TASKS_PROVISIONED_USER,
        MetricUnitOfAnalysisType.STATE_CODE,
    ): """SELECT
    state_code,
    email_address,
    start_date,
    end_date_exclusive, 
FROM
    `{project_id}.analyst_data.tasks_provisioned_user_registration_sessions_materialized`
""",
    (
        MetricUnitOfObservationType.TASKS_PROVISIONED_USER,
        MetricUnitOfAnalysisType.SUPERVISION_DISTRICT,
    ): """SELECT
        state_code,
        email_address,
        start_date,
        end_date_exclusive,
        location_id AS district,
    FROM
        `{project_id}.analyst_data.tasks_provisioned_user_registration_sessions_materialized`
    WHERE
        system_type = "SUPERVISION"
""",
    (
        MetricUnitOfObservationType.TASKS_PROVISIONED_USER,
        MetricUnitOfAnalysisType.SUPERVISION_OFFICE,
    ): """SELECT
        state_code,
        email_address,
        start_date,
        end_date_exclusive,
        state_staff_supervision_district AS district,
        state_staff_supervision_office AS office,
    FROM
        `{project_id}.analyst_data.tasks_provisioned_user_registration_sessions_materialized`
    WHERE
        system_type = "SUPERVISION"
""",
    (
        MetricUnitOfObservationType.TASKS_PRIMARY_USER,
        MetricUnitOfAnalysisType.SUPERVISION_OFFICE,
    ): """SELECT
        state_code,
        email_address,
        start_date,
        end_date_exclusive,
        state_staff_supervision_district AS district,
        state_staff_supervision_office AS office,
    FROM
        `{project_id}.analyst_data.tasks_provisioned_user_registration_sessions_materialized`
    WHERE
        system_type = "SUPERVISION"
        AND is_registered
        AND is_primary_user
""",
    (
        MetricUnitOfObservationType.TASKS_PROVISIONED_USER,
        MetricUnitOfAnalysisType.LOCATION,
    ): """SELECT
    state_code,
    email_address,
    start_date,
    end_date_exclusive,
    location_name,
FROM
    `{project_id}.analyst_data.tasks_provisioned_user_registration_sessions_materialized`
""",
    (
        MetricUnitOfObservationType.TASKS_PROVISIONED_USER,
        MetricUnitOfAnalysisType.SUPERVISION_OFFICER,
    ): """SELECT
    state_code,
    email_address,
    start_date,
    end_date_exclusive,
    staff_external_id AS officer_id,
FROM
    `{project_id}.analyst_data.tasks_provisioned_user_registration_sessions_materialized`
WHERE
    system_type = "SUPERVISION"
""",
    (
        MetricUnitOfObservationType.TASKS_PROVISIONED_USER,
        MetricUnitOfAnalysisType.SUPERVISION_OFFICER_OR_PREVIOUS_IF_TRANSITIONAL,
    ): """SELECT
    state_code,
    email_address,
    start_date,
    end_date_exclusive,
    staff_external_id AS officer_id,
FROM
    `{project_id}.analyst_data.tasks_provisioned_user_registration_sessions_materialized`
WHERE
    system_type = "SUPERVISION"
""",
    (
        MetricUnitOfObservationType.GLOBAL_PROVISIONED_USER,
        MetricUnitOfAnalysisType.ALL_STATES,
    ): """SELECT
    state_code,
    email_address,
    start_date,
    end_date_exclusive, 
    TRUE AS in_signed_state,
FROM
    `{project_id}.analyst_data.global_provisioned_user_sessions_materialized`
""",
    (
        MetricUnitOfObservationType.GLOBAL_PROVISIONED_USER,
        MetricUnitOfAnalysisType.STATE_CODE,
    ): """SELECT
    state_code,
    email_address,
    start_date,
    end_date_exclusive, 
FROM
    `{project_id}.analyst_data.global_provisioned_user_sessions_materialized`
""",
    (
        MetricUnitOfObservationType.GLOBAL_PROVISIONED_USER,
        MetricUnitOfAnalysisType.SUPERVISION_DISTRICT,
    ): """SELECT
        state_code,
        email_address,
        start_date,
        end_date_exclusive,
        location_id AS district,
    FROM
        `{project_id}.analyst_data.global_provisioned_user_sessions_materialized`
    WHERE
        system_type = "SUPERVISION"
""",
    (
        MetricUnitOfObservationType.GLOBAL_PROVISIONED_USER,
        MetricUnitOfAnalysisType.SUPERVISION_OFFICE,
    ): """SELECT
        state_code,
        email_address,
        start_date,
        end_date_exclusive,
        state_staff_supervision_district AS district,
        state_staff_supervision_office AS office,
    FROM
        `{project_id}.analyst_data.global_provisioned_user_sessions_materialized`
    WHERE
        system_type = "SUPERVISION"
""",
    (
        MetricUnitOfObservationType.GLOBAL_PROVISIONED_USER,
        MetricUnitOfAnalysisType.FACILITY,
    ): """SELECT
        state_code,
        email_address,
        start_date,
        end_date_exclusive,
        location_id AS facility,
    FROM
        `{project_id}.analyst_data.global_provisioned_user_sessions_materialized`
    WHERE
        system_type = "INCARCERATION"
""",
    (
        MetricUnitOfObservationType.GLOBAL_PROVISIONED_USER,
        MetricUnitOfAnalysisType.LOCATION,
    ): """SELECT
    state_code,
    email_address,
    start_date,
    end_date_exclusive,
    location_name,
FROM
    `{project_id}.analyst_data.global_provisioned_user_sessions_materialized`
""",
    (
        MetricUnitOfObservationType.GLOBAL_PROVISIONED_USER,
        MetricUnitOfAnalysisType.SUPERVISION_OFFICER,
    ): """SELECT
    state_code,
    email_address,
    start_date,
    end_date_exclusive,
    staff_external_id AS officer_id,
FROM
    `{project_id}.analyst_data.global_provisioned_user_sessions_materialized`
WHERE
    system_type = "SUPERVISION"
""",
    (
        MetricUnitOfObservationType.GLOBAL_PROVISIONED_USER,
        MetricUnitOfAnalysisType.SUPERVISION_OFFICER_OR_PREVIOUS_IF_TRANSITIONAL,
    ): """SELECT
    state_code,
    email_address,
    start_date,
    end_date_exclusive,
    staff_external_id AS officer_id,
FROM
    `{project_id}.analyst_data.global_provisioned_user_sessions_materialized`
WHERE
    system_type = "SUPERVISION"
""",
    (
        MetricUnitOfObservationType.GLOBAL_PROVISIONED_USER,
        MetricUnitOfAnalysisType.FACILITY_COUNSELOR,
    ): """SELECT
    state_code,
    email_address,
    start_date,
    end_date_exclusive,
    staff_id AS facility_counselor_id,
FROM
    `{project_id}.analyst_data.global_provisioned_user_sessions_materialized`
WHERE
    system_type = "INCARCERATION"
""",
    (
        MetricUnitOfObservationType.GLOBAL_PROVISIONED_USER,
        MetricUnitOfAnalysisType.GLOBAL_PROVISIONED_USER,
    ): """SELECT
    state_code,
    email_address,
    start_date,
    end_date_exclusive,
FROM
    `{project_id}.analyst_data.global_provisioned_user_sessions_materialized`
""",
    (
        MetricUnitOfObservationType.GLOBAL_PROVISIONED_USER,
        MetricUnitOfAnalysisType.PRODUCT_ACCESS,
    ): f"""SELECT
    state_code,
    email_address,
    start_date,
    end_date_exclusive,
    {list_to_query_string([f"is_provisioned_{product_type.pretty_name}" for product_type in ProductType])},
    {list_to_query_string([f"is_primary_user_{product_type.pretty_name}" for product_type in ProductType])},
FROM
    `{{project_id}}.analyst_data.global_provisioned_user_sessions_materialized`
""",
    (
        MetricUnitOfObservationType.JII_TABLET_APP_PROVISIONED_USER,
        MetricUnitOfAnalysisType.STATE_CODE,
    ): "SELECT * FROM `{project_id}.analyst_data.jii_tablet_app_provisioned_user_registration_sessions_materialized`",
    (
        MetricUnitOfObservationType.JII_TABLET_APP_PROVISIONED_USER,
        MetricUnitOfAnalysisType.FACILITY,
    ): """
SELECT state_code, person_id, start_date, end_date_exclusive, location_id AS facility 
FROM `{project_id}.analyst_data.jii_tablet_app_provisioned_user_registration_sessions_materialized`
""",
    (
        MetricUnitOfObservationType.JII_TABLET_APP_PROVISIONED_USER,
        MetricUnitOfAnalysisType.ALL_STATES,
    ): """
SELECT *, TRUE AS in_signed_state FROM `{project_id}.analyst_data.jii_tablet_app_provisioned_user_registration_sessions_materialized`
""",
}


def has_configured_assignment_query(
    unit_of_analysis_type: MetricUnitOfAnalysisType,
    unit_of_observation_type: MetricUnitOfObservationType,
) -> bool:
    """Returns True if this unit of analysis and unit of observation have a configured
    assignment query.
    """
    return (
        unit_of_observation_type,
        unit_of_analysis_type,
    ) in _UNIT_OF_ANALYSIS_ASSIGNMENT_QUERIES_DICT


def get_assignment_query_for_unit_of_analysis(
    unit_of_analysis_type: MetricUnitOfAnalysisType,
    unit_of_observation_type: MetricUnitOfObservationType,
) -> str | None:
    """Returns the assignment query that associates a unit of analysis with its assigned
    units of observation.
    """
    if not has_configured_assignment_query(
        unit_of_analysis_type,
        unit_of_observation_type,
    ):
        return None

    return _UNIT_OF_ANALYSIS_ASSIGNMENT_QUERIES_DICT[
        (unit_of_observation_type, unit_of_analysis_type)
    ]


def _get_metric_assignment_sessions_view_address(
    *,
    unit_of_analysis_type: MetricUnitOfAnalysisType,
    unit_of_observation_type: MetricUnitOfObservationType,
    population_type: MetricPopulationType,
) -> BigQueryAddress:
    """Gets the view address of the metric assignment sessions view for the given
    (population_type, unit_of_observation_type, unit_of_analysis_type) combination
    """
    unit_of_analysis_name = unit_of_analysis_type.short_name
    unit_of_observation_name = unit_of_observation_type.short_name

    population_name = population_type.population_name_short
    view_id = f"{population_name}_{unit_of_analysis_name}_metrics_{unit_of_observation_name}_assignment_sessions"
    return BigQueryAddress(
        # TODO(#29291): Change the dataset this is in to unit_of_analysis_assignments
        dataset_id=AGGREGATED_METRICS_DATASET_ID,
        # TODO(#29291): Change this view_id to follow the format
        #  population_unitofobservation_to_unitofanalysis_assignments. For example:
        #  supervision_person_to_supervision_unit_assignments.
        table_id=view_id,
    )


def get_metric_assignment_sessions_materialized_table_address(
    *,
    unit_of_analysis_type: MetricUnitOfAnalysisType,
    unit_of_observation_type: MetricUnitOfObservationType,
    population_type: MetricPopulationType,
) -> BigQueryAddress:
    """Gets the materialized address of the metric assignment sessions view for the
    given (population_type, unit_of_observation_type, unit_of_analysis_type) combination
    """
    view_address = _get_metric_assignment_sessions_view_address(
        unit_of_analysis_type=unit_of_analysis_type,
        unit_of_observation_type=unit_of_observation_type,
        population_type=population_type,
    )
    return BigQueryViewBuilder.build_standard_materialized_address(
        dataset_id=view_address.dataset_id,
        view_id=view_address.table_id,
    )


def generate_metric_assignment_sessions_view_builder(
    unit_of_analysis_type: MetricUnitOfAnalysisType,
    unit_of_observation_type: MetricUnitOfObservationType,
    population_type: MetricPopulationType,
    population_selector: SpanSelector,
) -> SimpleBigQueryViewBuilder:
    """
    Takes as input a unit of analysis (indicating the type of aggregation), a unit of observation
    (indicating the type of unit that is assigned to the unit of aggregation), and population.
    Returns a SimpleBigQueryViewBuilder where each row is a continuous time period during which
    a unit of observation is associated with the specified aggregation level indicated by the unit of analysis.
    """
    unit_of_analysis = MetricUnitOfAnalysis.for_type(unit_of_analysis_type)
    unit_of_observation = MetricUnitOfObservation(type=unit_of_observation_type)

    unit_of_analysis_name = unit_of_analysis_type.short_name
    unit_of_observation_name = unit_of_observation_type.short_name

    view_address = _get_metric_assignment_sessions_view_address(
        unit_of_observation_type=unit_of_observation_type,
        unit_of_analysis_type=unit_of_analysis_type,
        population_type=population_type,
    )

    view_description = f"""Query that extracts sessionized views of the relationship between units of analysis
for use in the {unit_of_analysis_name}_metrics table, based on {unit_of_observation_name} assignments to 
{unit_of_analysis_name}.
"""
    # list of all primary key columns from unit of observation that aren't already one of the primary key columns
    # of the unit of analysis
    child_primary_key_columns = [
        col
        for col in unit_of_observation.primary_key_columns_ordered
        if col not in unit_of_analysis.primary_key_columns
    ]
    child_primary_key_columns_query_string = (
        list_to_query_string(child_primary_key_columns) + ",\n"
        if child_primary_key_columns
        else ""
    )

    population_query = population_selector.generate_span_selector_query()

    query_template = f"""
WITH
-- define population in terms of unit of observation
sample AS (
    {population_query}
)
-- {unit_of_observation_name} assignments to {unit_of_analysis_name}
, assign AS (
    SELECT
        {child_primary_key_columns_query_string}{unit_of_analysis.get_primary_key_columns_query_string()},
        start_date,
        end_date_exclusive,
        1 AS dummy,
    FROM ({get_assignment_query_for_unit_of_analysis(unit_of_analysis_type, unit_of_observation_type)})
    WHERE
        CONCAT({unit_of_analysis.get_primary_key_columns_query_string()}) IS NOT NULL
)
-- if assigned unit not always in sample population, take intersection of exclusive periods
-- to determine the start and end dates of assignment
, potentially_adjacent_spans AS (
    {create_intersection_spans(
        table_1_name="assign", 
        table_2_name="sample", 
        index_columns=unit_of_observation.primary_key_columns_ordered,
        include_zero_day_intersections=True,
        table_1_columns=[col for col in unit_of_analysis.primary_key_columns if col not in unit_of_observation.primary_key_columns_ordered] + ["dummy"],
        table_2_columns=[]
    )}
)
,
{create_sub_sessions_with_attributes(
    table_name="potentially_adjacent_spans", 
    index_columns=unit_of_observation.primary_key_columns_ordered, 
    end_date_field_name="end_date_exclusive"
)}
, sub_sessions_with_attributes_distinct AS (
    SELECT DISTINCT *
    FROM sub_sessions_with_attributes
)
-- Re-sessionize all intersecting spans
, {unit_of_analysis_name}_assignments AS (
    SELECT
        {child_primary_key_columns_query_string}{unit_of_analysis.get_primary_key_columns_query_string()},
        session_id,
        MIN(start_date) AS assignment_date,
        MAX({nonnull_end_date_clause("end_date_exclusive")}) AS end_date_exclusive,
    FROM (
        SELECT
            * EXCEPT(date_gap),
            SUM(IF(date_gap, 1, 0)) OVER (
                PARTITION BY {child_primary_key_columns_query_string}{unit_of_analysis.get_primary_key_columns_query_string()}
                ORDER BY start_date, {nonnull_end_date_clause("end_date_exclusive")}
            ) AS session_id,
        FROM (
            SELECT
                *,
                IFNULL(
                    LAG(end_date_exclusive) OVER(
                        PARTITION BY {child_primary_key_columns_query_string}{unit_of_analysis.get_primary_key_columns_query_string()}
                        ORDER BY start_date, {nonnull_end_date_clause("end_date_exclusive")}
                    ) != start_date, TRUE
                ) AS date_gap,
            FROM
                sub_sessions_with_attributes_distinct
        )
    )
    GROUP BY {child_primary_key_columns_query_string}{unit_of_analysis.get_primary_key_columns_query_string()}, session_id
)
,
-- Identify dates that the unit of observation entered the population.
-- We will add a flag for assignment start dates that fall on this date.
population_start_dates AS (
    SELECT
        {list_to_query_string(unit_of_observation.primary_key_columns_ordered)},
        start_date AS assignment_date,
    FROM
        sample
    QUALIFY
        IFNULL(
            LAG(end_date_exclusive) OVER (PARTITION BY {list_to_query_string(unit_of_observation.primary_key_columns_ordered)} ORDER BY start_date),
            DATE("{MAGIC_START_DATE}")
        ) != start_date
)
SELECT 
    * EXCEPT(session_id, assignment_date, end_date_exclusive),
    -- Cast any DATETIME values to DATE
    DATE(assignment_date) AS assignment_date,
    DATE({revert_nonnull_end_date_clause("end_date_exclusive")}) AS end_date,
    DATE({revert_nonnull_end_date_clause("end_date_exclusive")}) AS end_date_exclusive,
    population_start_dates.assignment_date IS NOT NULL AS assignment_is_first_day_in_population,
FROM {unit_of_analysis_name}_assignments
LEFT JOIN
    population_start_dates
USING
    (assignment_date, {list_to_query_string(unit_of_observation.primary_key_columns_ordered)})

"""
    return SimpleBigQueryViewBuilder(
        dataset_id=view_address.dataset_id,
        view_id=view_address.table_id,
        view_query_template=query_template,
        description=view_description,
        clustering_fields=unit_of_observation.primary_key_columns_ordered,
        should_materialize=True,
    )
