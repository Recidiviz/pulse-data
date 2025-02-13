# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2025 Recidiviz, Inc.
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
"""Materialized view for Contact Codes in TN relevant for opportunity forms."""

from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.state.dataset_config import (
    ANALYST_VIEWS_DATASET,
    SESSIONS_DATASET,
)
from recidiviz.common.constants.states import StateCode
from recidiviz.ingest.direct.dataset_config import raw_latest_views_dataset_for_region
from recidiviz.ingest.direct.types.direct_ingest_instance import DirectIngestInstance
from recidiviz.ingest.views.dataset_config import NORMALIZED_STATE_DATASET
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

US_TN_RELEVANT_CONTACT_CODES_VIEW_NAME = "us_tn_relevant_contact_codes"

US_TN_RELEVANT_CONTACT_CODES_QUERY_TEMPLATE = """
    WITH latest_system_session AS (
        -- get latest system session date to bring in relevant codes only for this time on supervision
        SELECT
            person_id,
            start_date AS latest_system_session_start_date,
        FROM `{project_id}.{sessions_dataset}.system_sessions_materialized`
        WHERE state_code = 'US_TN'
        QUALIFY ROW_NUMBER() OVER(PARTITION BY person_id ORDER BY system_session_id DESC) = 1
    )
    SELECT 
        person_id,
        CAST(CAST(ContactNoteDateTime AS DATETIME) AS DATE) AS contact_date,
        contact.ContactNoteType AS contact_type,
    FROM `{project_id}.{us_tn_raw_data_up_to_date_dataset}.ContactNoteType_latest` contact
    INNER JOIN `{project_id}.{normalized_state_dataset}.state_person_external_id` pei
        ON contact.OffenderID = pei.external_id
        AND pei.state_code = 'US_TN'
    LEFT JOIN latest_system_session
        USING (person_id)
    WHERE CAST(CAST(ContactNoteDateTime AS DATETIME) AS DATE) >= latest_system_session_start_date
        AND (
            ContactNoteType LIKE '%PSE%'
            OR ContactNoteType IN ('VRPT','VWAR','COHC','ARRP','PWAR')
            OR ContactNoteType LIKE "%ABS%"
            OR ContactNoteType IN ('DRUN','DRUP','DRUM','DRUX')
            OR ContactNoteType LIKE "FSW%"
            OR ContactNoteType LIKE "%EMP%"
            OR ContactNoteType IN ("SPEC", "SPET", "XSPE")
            OR ContactNoteType LIKE 'VRR%'
            OR ContactNoteType IN ("FAC1","FAC2")
            OR ContactNoteType LIKE 'FEE%'
            OR ContactNoteType = 'TEPE'
            OR ContactNoteType IN ('NCAC','NCAF')
            OR ContactNoteType IN ('BBNN', 'BBNP')
            OR ContactNoteType IN ('ARRN', 'ARRP')
        )
"""

US_TN_RELEVANT_CONTACT_CODES_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=ANALYST_VIEWS_DATASET,
    view_id=US_TN_RELEVANT_CONTACT_CODES_VIEW_NAME,
    description=__doc__,
    view_query_template=US_TN_RELEVANT_CONTACT_CODES_QUERY_TEMPLATE,
    normalized_state_dataset=NORMALIZED_STATE_DATASET,
    sessions_dataset=SESSIONS_DATASET,
    us_tn_raw_data_up_to_date_dataset=raw_latest_views_dataset_for_region(
        state_code=StateCode.US_TN,
        instance=DirectIngestInstance.PRIMARY,
    ),
    should_materialize=True,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        US_TN_RELEVANT_CONTACT_CODES_VIEW_BUILDER.build_and_print()
