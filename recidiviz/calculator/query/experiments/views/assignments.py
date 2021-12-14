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
"""Creates the view builder and view for metrics from case triage."""
from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.experiments.dataset_config import EXPERIMENTS_DATASET
from recidiviz.calculator.query.state.dataset_config import (
    SESSIONS_DATASET,
    STATE_BASE_DATASET,
    STATIC_REFERENCE_TABLES_DATASET,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

US_ID_RAW_DATASET = "us_id_raw_data_up_to_date_views"
US_PA_RAW_DATASET = "us_pa_raw_data_up_to_date_views"

ASSIGNMENTS_VIEW_NAME = "assignments"

ASSIGNMENTS_VIEW_DESCRIPTION = (
    "Tracks assignments of experiment/policy/program variants to subjects in each "
    "experiment."
)

ASSIGNMENTS_QUERY_TEMPLATE = """
-- last day data observable in sessions
WITH last_day_of_data AS (
    SELECT
        state_code,
        MIN(last_day_of_data) AS last_day_of_data,
    FROM
        `{project_id}.{sessions_dataset}.compartment_sessions_materialized`
    GROUP BY 1
)

-- Dummy "treatment" to get all persons when first observed in sessions
, first_observed AS (
    SELECT
        "FIRST_OBSERVED" AS experiment_id,
        state_code as state_code,
        CAST(person_id AS STRING) AS subject_id,
        "person_id" as id_type,
        compartment_level_1 AS variant_id,
        MIN(start_date) AS variant_date,
    FROM
        `{project_id}.{sessions_dataset}.compartment_sessions_materialized`
    GROUP BY 1, 2, 3, 4, 5
)

-- When clients referred to dosage probation in ID
, dosage_probation AS (
    SELECT
        "DOSAGE_PROBATION" AS experiment_id,
        "US_ID" as state_code,
        CAST(person_id AS STRING) AS subject_id,
        "person_id" as id_type,
        COALESCE(dsg_asgnmt, "TREATED_INTERNAL_UNKNOWN") AS variant_id,
        DATE(prgrm_strt_dt) AS variant_date,
    FROM
        `{project_id}.{us_id_raw_dataset}.DoPro_Participant_latest` a
    INNER JOIN
        `{project_id}.{state_base_dataset}.state_person_external_id` b
    ON
        a.ofndr_num = b.external_id
    WHERE
        b.state_code = "US_ID" AND
        -- Remove IDOC's test subject
        ofndr_num NOT IN ("30054")
)

-- When clients referred to GEO CIS in ID
, geo_cis_referral AS (
    SELECT
        "GEO_CIS_REFERRAL" AS experiment_id,
        "US_ID" as state_code,
        CAST(person_id AS STRING) AS subject_id,
        "person_id" as id_type,
        "REFERRED" AS variant_id,
        DATE(start_date) AS variant_date,
    FROM
        `{project_id}.us_id_raw_data_up_to_date_views.geo_cis_participants_latest` a
    INNER JOIN
        `{project_id}.{state_base_dataset}.state_person_external_id` b
    ON
        a.person_external_id = b.external_id
    WHERE
        b.state_code = "US_ID" AND
        -- Remove IDOC's test subject
        person_external_id NOT IN ("30054")
)

-- When officers first given access to Case Triage
, case_triage_access AS (
    SELECT
        "CASE_TRIAGE_ACCESS" AS experiment_id,
        state_code as state_code,
        officer_external_id AS subject_id,
        "officer_external_id" as id_type,
        "RECEIVED_ACCESS" AS variant_id,
        received_access AS variant_date,
        -- cluster_id = district code, if rolled out consistently at district level
        -- block_id = state_code once in more than one state
    FROM
        `{project_id}.{static_reference_dataset}.case_triage_users`
)

-- When officers first given access to monthly reports
, monthly_report_access AS (
    SELECT
        "MONTHLY_REPORT_ACCESS" AS experiment_id,
        state_code as state_code,
        officer_external_id AS subject_id,
        "officer_external_id" as id_type,
        "RECEIVED_ACCESS" AS variant_id,
        received_access AS variant_date,
        -- cluster_id = district code, if rolled out consistently at district level
        -- block_id = state_code once in more than one state
    FROM
        `{project_id}.{static_reference_dataset}.case_triage_users`
)

-- Covid-related CPP cohort in ND
, us_nd_community_placement_program AS (
    SELECT 
        "COVID_EARLY_RELEASE" AS experiment_id,
        "US_ND" AS state_code,
        CAST(person_id AS STRING) AS subject_id,
        "person_id" AS id_type,
        "COMMUNITY_PLACEMENT_PROGRAM" AS variant_id,
        MIN(start_date) AS variant_date,
    FROM 
        `{project_id}.{sessions_dataset}.compartment_sessions_materialized` a
    WHERE
        compartment_level_2 = "COMMUNITY_PLACEMENT_PROGRAM"
    GROUP BY 1, 2, 3, 4, 5
)

-- Covid-related reprieve cohort in PA
, us_pa_covid_reprieves AS (
    SELECT 
        "COVID_EARLY_RELEASE" AS experiment_id,
        "US_PA" AS state_code,
        CAST(person_id AS STRING) AS subject_id,
        "person_id" AS id_type,
        "TEMPORARY_REPRIEVE" AS variant_id,
        reprieve_date AS variant_date,
    FROM 
        `{project_id}.{static_reference_dataset}.us_pa_temporary_reprieves` a
    INNER JOIN
        `{project_id}.{state_base_dataset}.state_person_external_id` b
    ON
        a.external_id = b.external_id
        AND b.id_type = "US_PA_INMATE"
)

-- Covid-related furlough cohort in PA
, us_pa_covid_furloughs AS (
    SELECT 
        "COVID_EARLY_RELEASE" AS experiment_id,
        "US_PA" AS state_code,
        CAST(person_id AS STRING) AS subject_id,
        "person_id" AS id_type,
        "FURLOUGH" AS variant_id,
        -- Define first marked movement as treatment
        MIN(DATE(Status_Dt)) AS variant_date,
    FROM 
        `{project_id}.{us_pa_raw_dataset}.dbo_vwCCISAllMvmt_latest` a
    INNER JOIN (
        SELECT
            CCISMvmt_Id,
        FROM
            `{project_id}.{us_pa_raw_dataset}.dbo_vwCCISAllProgDtls_latest`
        WHERE
            -- covid-related furloughs only
            Program_Id = "70"
        )
    USING
        (CCISMvmt_Id)
    INNER JOIN
        `{project_id}.{state_base_dataset}.state_person_external_id` b
    ON
        a.Inmate_Number = b.external_id
        AND b.id_type = "US_PA_INMATE"
    GROUP BY 1, 2, 3, 4, 5
)

-- State-level experiments from static reference table
-- Generally, these are state-wide policies or other state-level changes
, state_assignments AS (
    SELECT
        experiment_id,
        state_code,
        state_code AS subject_id,
        "state_code" as id_type,
        variant_id,
        CAST(variant_date AS DATETIME) AS variant_time,
    FROM
        `{project_id}.{static_reference_dataset}.experiment_state_assignments_materialized`
)

-- Union all assignment subqueries
, stacked AS (
    SELECT *
    FROM first_observed
    UNION ALL
    SELECT *
    FROM dosage_probation
    UNION ALL
    SELECT *
    FROM geo_cis_referral
    UNION ALL
    SELECT *
    FROM case_triage_access
    UNION ALL
    SELECT *
    FROM monthly_report_access
    UNION ALL
    SELECT *
    FROM us_nd_community_placement_program
    UNION ALL
    SELECT *
    FROM us_pa_covid_reprieves
    UNION ALL
    SELECT *
    FROM us_pa_covid_furloughs
    UNION ALL
    SELECT *
    FROM state_assignments
)

-- Add state-level last day data observed
SELECT *
FROM stacked
INNER JOIN last_day_of_data USING(state_code)
"""

ASSIGNMENTS_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=EXPERIMENTS_DATASET,
    view_id=ASSIGNMENTS_VIEW_NAME,
    view_query_template=ASSIGNMENTS_QUERY_TEMPLATE,
    description=ASSIGNMENTS_VIEW_DESCRIPTION,
    us_id_raw_dataset=US_ID_RAW_DATASET,
    us_pa_raw_dataset=US_PA_RAW_DATASET,
    sessions_dataset=SESSIONS_DATASET,
    state_base_dataset=STATE_BASE_DATASET,
    static_reference_dataset=STATIC_REFERENCE_TABLES_DATASET,
    should_materialize=True,
    clustering_fields=["experiment_id"],
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        ASSIGNMENTS_VIEW_BUILDER.build_and_print()
