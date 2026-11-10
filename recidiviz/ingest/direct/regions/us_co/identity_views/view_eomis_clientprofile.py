# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2026 Recidiviz, Inc.
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
"""Identity ingest view for US_CO eomis_clientprofile (supervision) JII
identity information.

One row per person keyed on OFFENDERID, the table's primary key. Every row also
carries an ADCNUMBER (the DOC number), and the two ids are one-to-one in the
source, so each fragment emits both. eomis_inmateprofile covers the same people
from the incarceration side; clustering joins the two sources' fragments on the
shared ids, and a few hundred supervision-only people appear only here.
"""
from recidiviz.ingest.direct.views.direct_ingest_view_query_builder import (
    DirectIngestViewQueryBuilder,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

VIEW_QUERY_TEMPLATE = """
SELECT
    OFFENDERID,
    ADCNUMBER,
    CLIENTFIRSTNAME AS FIRST_NAME,
    CLIENTMIDDLENAME AS MIDDLE_NAME,
    CLIENTLASTNAME AS LAST_NAME,
    CLIENTNAMESUFFIX AS NAME_SUFFIX,
    -- The raw datetime column surfaces as a datetime string; only the date part
    -- is a birthdate. Out-of-bounds birthdates (source year typos like
    -- 0198-06-01) pass through here; the mapping omits them via
    -- reasonable_birthdate_or_null.
    CAST(CAST(CLIENTDATEOFBIRTH AS DATETIME) AS DATE) AS BIRTH_DATE,
    CLIENTSEX AS SEX_CODE,
    CLIENTRACE AS RACE_CODE
FROM {eomis_clientprofile}
"""

VIEW_BUILDER = DirectIngestViewQueryBuilder(
    region="us_co",
    ingest_view_name="eomis_clientprofile",
    view_query_template=VIEW_QUERY_TEMPLATE,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
