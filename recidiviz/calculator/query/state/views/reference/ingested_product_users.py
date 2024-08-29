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
"""View containing users that may have access to Polaris products that we receive via ingest."""

from recidiviz.big_query.selected_columns_big_query_view import (
    SelectedColumnsBigQueryViewBuilder,
)
from recidiviz.calculator.query.bq_utils import list_to_query_string
from recidiviz.calculator.query.state import dataset_config
from recidiviz.common.constants.auth import RosterPredefinedRoles
from recidiviz.common.constants.states import StateCode
from recidiviz.ingest.direct.dataset_config import raw_latest_views_dataset_for_region
from recidiviz.ingest.direct.types.direct_ingest_instance import DirectIngestInstance
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

INGESTED_PRODUCT_USERS_VIEW_NAME = "ingested_product_users"

INGESTED_PRODUCT_USERS_DESCRIPTION = """View containing users that may have access to Polaris products
that we receive via ingest (instead of uploaded CSVs). This view is only used for exporting and
should not have any downstream views: use `product_roster` instead."""

INGESTED_PRODUCT_USERS_QUERY_TEMPLATE = f"""
    WITH
    mo_users AS (
        SELECT
            'US_MO' AS state_code,
            LOWER(email) AS email_address,
            IF(STRING_AGG(DISTINCT district, ',') IS NOT NULL, 'supervision_staff', 'leadership_role') as roles,
            -- the old dashboard_user_restrictions views used an empty string for no district
            -- instead of NULL, so keep that behavior here.
            IFNULL(STRING_AGG(DISTINCT district, ','), '') AS district,
            emp_info.BDGNO as external_id,
            ARRAY_AGG(first_name ORDER BY record_date DESC)[SAFE_OFFSET(0)] AS first_name,
            ARRAY_AGG(last_name ORDER BY record_date DESC)[SAFE_OFFSET(0)] AS last_name,
            CAST(NULL AS STRING) AS pseudonymized_id,
        FROM `{{project_id}}.{{us_mo_raw_data_up_to_date_dataset}}.LANTERN_DA_RA_LIST_latest`
        LEFT JOIN `{{project_id}}.{{us_mo_raw_data_up_to_date_dataset}}.LBCMDATA_APFX90_latest` emp_info
            ON UPPER(lname) LIKE '%' || UPPER(last_name) || '%'
            AND UPPER(fname) LIKE '%' || UPPER(first_name) || '%'
            AND ENDDTE='0'
        WHERE email IS NOT NULL
        GROUP BY LOWER(email), emp_info.BDGNO
    ),
    state_staff_users AS (
        SELECT
            state_code,
            LOWER(email) AS email_address,
            CASE
                WHEN is_supervision_officer_supervisor THEN "{RosterPredefinedRoles.SUPERVISION_OFFICER_SUPERVISOR.value}"
                WHEN is_supervision_officer THEN "{RosterPredefinedRoles.SUPERVISION_LINE_STAFF.value}"
                ELSE "{RosterPredefinedRoles.UNKNOWN.value}"
            END AS roles,
            district,
            external_id,
            given_names AS first_name,
            surname AS last_name,
            pseudonymized_id
        FROM `{{project_id}}.reference_views.current_staff_materialized`
        WHERE state_code IN ({{state_staff_states}})
        AND email IS NOT NULL
    ),
    all_users AS (
        SELECT * FROM mo_users
        UNION ALL
        SELECT * FROM state_staff_users
        -- Exclude D20 users because we're trying not to make any changes to them
        -- TODO(#25566): Add them back in
        WHERE NOT (state_code = "US_TN" AND district = "20")
    )
    SELECT {{columns}} FROM all_users
"""


INGESTED_PRODUCT_USERS_VIEW_BUILDER = SelectedColumnsBigQueryViewBuilder(
    dataset_id=dataset_config.REFERENCE_VIEWS_DATASET,
    view_id=INGESTED_PRODUCT_USERS_VIEW_NAME,
    view_query_template=INGESTED_PRODUCT_USERS_QUERY_TEMPLATE,
    description=INGESTED_PRODUCT_USERS_DESCRIPTION,
    us_mo_raw_data_up_to_date_dataset=raw_latest_views_dataset_for_region(
        state_code=StateCode.US_MO, instance=DirectIngestInstance.PRIMARY
    ),
    state_staff_states=list_to_query_string(
        [
            "US_AR",
            "US_CA",
            "US_ME",
            "US_MI",
            "US_ND",
            "US_PA",
        ],
        quoted=True,
    ),
    columns=[
        "state_code",
        "email_address",
        "external_id",
        "roles",
        # TODO(#26245): Rename to district_id or district_name (or supervision_[one of those]?)
        "district",
        "first_name",
        "last_name",
        "pseudonymized_id",
    ],
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        INGESTED_PRODUCT_USERS_VIEW_BUILDER.build_and_print()
