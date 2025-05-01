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
"""Query containing person information."""

from recidiviz.ingest.direct.views.direct_ingest_view_query_builder import (
    DirectIngestViewQueryBuilder,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

VIEW_QUERY_TEMPLATE = """
WITH 
-- This CTE gets the phone numbers associated with the most recent address for each JII
most_recent_phone_number AS (
    SELECT
        OffenderCd,
        CellPhoneNo,
        ResidencePhoneNo
    FROM {IA_DOC_Addresses}
    QUALIFY ROW_NUMBER() OVER (PARTITION BY OffenderCd ORDER BY COALESCE(CAST(AddressEndDt AS DATETIME), DATE(9999,9,9)) DESC, CAST(AddressStartDt AS DATETIME) DESC, CAST(EnteredDt AS DATETIME) DESC) = 1
)
SELECT 
    OffenderCd,
    OffenderFirstNm,
    OffenderMiddleNm,
    OffenderLastNm,
    Sex,
    Race,
    EthnicOrigin,
    DATE(BirthDt) AS BirthDt,
    COALESCE(CellPhoneNo, ResidencePhoneNo) AS PhoneNo
FROM {IA_DOC_Offender_Details}
LEFT JOIN most_recent_phone_number USING(OffenderCd)
"""

VIEW_BUILDER = DirectIngestViewQueryBuilder(
    region="us_ia",
    ingest_view_name="state_person",
    view_query_template=VIEW_QUERY_TEMPLATE,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
