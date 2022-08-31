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
"""Query containing the treatment referrals and completion date to hydrate program assignment.
"""
from recidiviz.ingest.direct.views.direct_ingest_big_query_view_types import (
    DirectIngestPreProcessedIngestViewBuilder,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

VIEW_QUERY_TEMPLATE = """
    WITH TrtClassCode_trim AS (
        # The TrtClassCodes were shortened in the raw data file for simplification purposes so we trim the codes to only include codes (not subcodes)
        # in order to join the raw descriptions table. The TrtClassCode is maintained in the program_id field in order to allow for downstream work to 
        # parse based on subcodes as needed. 
    SELECT 
        TrtClassCode,
        CASE WHEN CHAR_LENGTH(TrtClassCode) = 5 THEN LEFT(TrtClassCode,3)
            WHEN CHAR_LENGTH(TrtClassCode) = 4 THEN LEFT(TrtClassCode,2) END AS classification_code
    FROM {dbo_Treatment}
    ),
    TrtClassCode_decode AS (
        SELECT 
            DISTINCT
            TrtClassCode_trim.TrtClassCode,
            TrtClassCode_trim.classification_code,
            ref.classification_code,
            ref.classification_description
        FROM TrtClassCode_trim
        LEFT JOIN {treatment_classification_codes} ref USING (classification_code)
    )
    SELECT 
        ParoleNumber, 
        ParoleCountID, 
        TreatmentID,
        TrtStatusCode,
        TrtClassCode,
        TrtClassCode_decode.classification_description,
        TrtProgramDescription,
        TrtProgramCode,
        TrtDo,
        TrtCounty,
        TrtStartDateYear,
        TrtStartDateMonth,
        TrtStartDateDay,
        TrtEndDateYear,
        TrtEndDateMonth,
        TrtEndDateDay
    FROM {dbo_Treatment}
    LEFT JOIN TrtClassCode_decode USING (TrtClassCode)
    WHERE TrtProgramCode IN ('REF','REFO','SEXO')
"""


VIEW_BUILDER = DirectIngestPreProcessedIngestViewBuilder(
    region="us_pa",
    ingest_view_name="program_assignment",
    view_query_template=VIEW_QUERY_TEMPLATE,
    order_by_cols="ParoleNumber ASC",
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
