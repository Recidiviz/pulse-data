# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2024 Recidiviz, Inc.
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
"""Sessions for incarceration staff caseloads in ME"""

from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.state import dataset_config
from recidiviz.calculator.query.state.dataset_config import SESSIONS_DATASET
from recidiviz.common.constants.states import StateCode
from recidiviz.ingest.direct.dataset_config import raw_latest_views_dataset_for_region
from recidiviz.ingest.direct.types.direct_ingest_instance import DirectIngestInstance
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

US_ME_INCARCERATION_STAFF_ASSIGNMENT_SESSIONS_PREPROCESSED_VIEW_NAME = (
    "us_me_incarceration_staff_assignment_sessions_preprocessed"
)

US_ME_INCARCERATION_STAFF_ASSIGNMENT_SESSIONS_PREPROCESSED_VIEW_DESCRIPTION = """Sessions for incarceration staff caseloads in ME.
    
    Note that there are apparently some stray non-incarceration (supervision) caseloads in this dataset."""

US_ME_INCARCERATION_STAFF_ASSIGNMENT_SESSIONS_PREPROCESSED_QUERY_TEMPLATE = """
    SELECT DISTINCT
        "US_ME" AS state_code,
        pei.person_id,
        Cis_100_Client_Id as person_external_id,
        DATE(SAFE_CAST(Assignment_Date AS DATETIME)) AS start_date,
        DATE(SAFE_CAST(sp.Supervision_End_Date AS DATETIME)) AS end_date_exclusive,
        -- US_ME doesn't have staff internal id's yet
        NULL AS incarceration_staff_assignment_id,
        IFNULL(ids.external_id_mapped, Cis_900_Employee_Id) AS incarceration_staff_assignment_external_id,
        "INCARCERATION_STAFF" AS incarceration_staff_assignment_role_type,
        -- US_ME doesn't have role subtypes yet
        CAST(NULL AS STRING) AS incarceration_staff_assignment_role_subtype,
        ROW_NUMBER() OVER (PARTITION BY Cis_100_Client_Id 
                    /* Prioritize cases in the following order 
                            1) NULL Supervision_End_Date
                            2) Case Workers (1) and Correctional Care and Treatment Worker (4)
                            3) Latest assignment date
                            4) Status of officer: primary, secondary, temporary
                            5) Latest supervision end date */
                                ORDER BY
                                        IF(sp.Supervision_End_Date IS NULL, 0, 1),
                                        IF(Cis_1240_Supervision_Type_Cd IN ('1', '4'), 0, 1) , 
                                        Assignment_Date DESC,
                                        Cis_1241_Super_Status_Cd,
                                        sp.Supervision_End_Date DESC) AS case_priority
    FROM `{project_id}.{us_me_raw_data_up_to_date_dataset}.CIS_124_SUPERVISION_HISTORY_latest` sp
    LEFT JOIN `{project_id}.{static_reference_dataset}.agent_multiple_ids_map` ids
        ON sp.Cis_900_Employee_Id = ids.external_id_to_map AND ids.state_code = 'US_ME' 
    LEFT JOIN `{project_id}.normalized_state.state_person_external_id` pei
        ON sp.Cis_100_Client_Id = pei.external_id AND pei.state_code = 'US_ME'
    WHERE
        -- Ignore assignments from the future
        SAFE_CAST(Assignment_Date AS DATETIME) <= CURRENT_DATE('US/Eastern')
"""

US_ME_INCARCERATION_STAFF_ASSIGNMENT_SESSIONS_PREPROCESSED_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=SESSIONS_DATASET,
    view_id=US_ME_INCARCERATION_STAFF_ASSIGNMENT_SESSIONS_PREPROCESSED_VIEW_NAME,
    description=US_ME_INCARCERATION_STAFF_ASSIGNMENT_SESSIONS_PREPROCESSED_VIEW_DESCRIPTION,
    view_query_template=US_ME_INCARCERATION_STAFF_ASSIGNMENT_SESSIONS_PREPROCESSED_QUERY_TEMPLATE,
    us_me_raw_data_up_to_date_dataset=raw_latest_views_dataset_for_region(
        state_code=StateCode.US_ME, instance=DirectIngestInstance.PRIMARY
    ),
    static_reference_dataset=dataset_config.STATIC_REFERENCE_TABLES_DATASET,
    should_materialize=False,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        US_ME_INCARCERATION_STAFF_ASSIGNMENT_SESSIONS_PREPROCESSED_VIEW_BUILDER.build_and_print()
