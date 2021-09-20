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
"""View pulling from raw data in US_ID that captures supervision contacts, not limited to actual contacts with a client"""
from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.state.dataset_config import (
    ANALYST_VIEWS_DATASET,
    STATE_BASE_DATASET,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

US_ID_RAW_SUPERVISION_CONTACTS_VIEW_NAME = "us_id_raw_supervision_contacts"

US_ID_RAW_DATASET = "us_id_raw_data_up_to_date_views"

US_ID_RAW_SUPERVISION_CONTACTS_VIEW_DESCRIPTION = "View pulling from raw data in US_ID that captures supervision contacts, not limited to actual contacts with a client"

US_ID_RAW_SUPERVISION_CONTACTS_QUERY_TEMPLATE = """
    /*{description}*/
    -- View cleans raw contacts data. This pre-processing is similar to what's done for state_supervision_contact
    -- but those are limited to actual contacts with a client (i.e. Face to Face or Virtual Contacts). The supervision contact data
    -- has a lot of other contacts (e.g. COLLATERAL) that can be useful for identifying, for example, treatment referrals
    # TODO(#9230): Eventually deprecate this table and build off of state_supervision_contact
    SELECT 
        person_id, 
        sprvsn_cntc_id,
        ofndr_num,
        cntc_loc_desc AS location_raw_text,
        cntc_rslt_desc AS status_raw_text,
        cntc_typ_desc AS contact_type_raw_text,
        cntc_title_desc AS contact_reason_raw_text,
        DATE(SAFE.PARSE_DATETIME("%Y-%m-%d %X", cntc_dt)) AS contact_date 
    FROM `{project_id}.{raw_dataset}.sprvsn_cntc_latest` 
    LEFT JOIN `{project_id}.{raw_dataset}.cntc_loc_cd_latest`
        USING(cntc_loc_cd)
    LEFT JOIN `{project_id}.{raw_dataset}.cntc_rslt_cd_latest`
        USING(cntc_rslt_cd)
    LEFT JOIN `{project_id}.{raw_dataset}.cntc_typ_cd_latest`
        USING(cntc_typ_cd)
    LEFT JOIN `{project_id}.{raw_dataset}.cntc_title_cd_latest`
        USING(cntc_title_cd)
    INNER JOIN `{project_id}.{base_dataset}.state_person_external_id` pei
        ON pei.external_id = ofndr_num
        AND pei.state_code = "US_ID"
    """

US_ID_RAW_SUPERVISION_CONTACTS_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=ANALYST_VIEWS_DATASET,
    view_id=US_ID_RAW_SUPERVISION_CONTACTS_VIEW_NAME,
    view_query_template=US_ID_RAW_SUPERVISION_CONTACTS_QUERY_TEMPLATE,
    description=US_ID_RAW_SUPERVISION_CONTACTS_VIEW_DESCRIPTION,
    raw_dataset=US_ID_RAW_DATASET,
    base_dataset=STATE_BASE_DATASET,
    should_materialize=True,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        US_ID_RAW_SUPERVISION_CONTACTS_VIEW_BUILDER.build_and_print()
