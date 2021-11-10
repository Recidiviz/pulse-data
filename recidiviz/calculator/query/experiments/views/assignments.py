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
    STATE_BASE_DATASET,
    STATIC_REFERENCE_TABLES_DATASET,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

# TODO(#9985): uncomment this after dopro_participants exists in us_id_raw
# US_ID_RAW_DATASET = "us_id_raw_data_up_to_date_views"

ASSIGNMENTS_VIEW_NAME = "assignments"

ASSIGNMENTS_VIEW_DESCRIPTION = (
    "Tracks assignments of experiment/policy/program variants to subjects in each "
    "experiment."
)

# after #9650 lands, swap dataset reference to us_id_raw_data_up_to_date_views in place
# of experiments_scratch. Until that lands, big_query_view_dag_walker_test will fail.

ASSIGNMENTS_QUERY_TEMPLATE = """
WITH dosage_probation AS (
    SELECT
        "DOSAGE_PROBATION" AS experiment_id,
        "US_ID" as state_code,
        CAST(person_id AS STRING) AS subject_id,
        "person_id" as id_type,
        COALESCE(dsg_asgnmt, "TREATED_INTERNAL_UNKNOWN") AS variant_id,
        CAST(DATE(prgrm_strt_dt) AS DATETIME) AS variant_time,
        "0" as cluster_id,
        "0" as block_id,
    FROM
        `{project_id}.{us_id_raw_dataset}.dopro_participant` a
    INNER JOIN
        `{project_id}.{state_base_dataset}.state_person_external_id` b
    ON
        CAST(a.ofndr_num AS STRING) = b.external_id
    WHERE
        b.state_code = "US_ID" AND
        -- Remove IDOC's test subject
        ofndr_num NOT IN (30054)
)

, geo_cis_referral AS (
    SELECT
        "GEO_CIS_REFERRAL" AS experiment_id,
        "US_ID" as state_code,
        CAST(person_id AS STRING) AS subject_id,
        "person_id" as id_type,
        "referred" AS variant_id,
        CAST(DATE(start_date) AS DATETIME) AS variant_time,
        "0" as cluster_id,
        "0" as block_id,
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

, case_triage_access AS (
    SELECT
        "CASE_TRIAGE_ACCESS" AS experiment_id,
        state_code as state_code,
        officer_external_id AS subject_id,
        "officer_external_id" as id_type,
        "received_access" AS variant_id,
        CAST(received_access AS DATETIME) AS variant_time,
        "0" as cluster_id,  -- district code, if rolled out consistently at district level
        "0" as block_id,  -- state_code once in more than one state
    FROM
        `{project_id}.{static_reference_dataset}.case_triage_users`
)

, monthly_report_access AS (
    SELECT
        "MONTHLY_REPORT_ACCESS" AS experiment_id,
        state_code as state_code,
        officer_external_id AS subject_id,
        "officer_external_id" as id_type,
        "received_access" AS variant_id,
        CAST(received_access AS DATETIME) AS variant_time,
        "0" as cluster_id,  -- district code, if rolled out consistently at district level
        "0" as block_id,  -- state_code once in more than one state
    FROM
        `{project_id}.{static_reference_dataset}.case_triage_users`
)

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

"""

ASSIGNMENTS_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=EXPERIMENTS_DATASET,
    view_id=ASSIGNMENTS_VIEW_NAME,
    view_query_template=ASSIGNMENTS_QUERY_TEMPLATE,
    description=ASSIGNMENTS_VIEW_DESCRIPTION,
    # TODO(#9985): SWAP IN US_ID_RAW_DATASET HERE
    us_id_raw_dataset=STATIC_REFERENCE_TABLES_DATASET,
    state_base_dataset=STATE_BASE_DATASET,
    static_reference_dataset=STATIC_REFERENCE_TABLES_DATASET,
    should_materialize=True,
    clustering_fields=["experiment_id"],
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        ASSIGNMENTS_VIEW_BUILDER.build_and_print()
