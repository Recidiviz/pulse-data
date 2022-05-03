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
"""Query containing incarcerated person information."""

from recidiviz.ingest.direct.views.direct_ingest_big_query_view_types import (
    DirectIngestPreProcessedIngestViewBuilder,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

VIEW_QUERY_TEMPLATE = """
    WITH normalized_rows AS (
        SELECT 
            I.OFFENDERID, 
            ADCNUMBER,
            INMATEFIRSTNAME, 
            INMATEMIDDLENAME, 
            INMATELASTNAME, 
            INMATENAMESUFFIX, 
            INMATEDATEOFBIRTH, 
            INMATESEXCODE, 
            INMATERACECODE, 
            ROW_NUMBER() OVER (PARTITION BY I.OFFENDERID ORDER BY I.DATELASTUPDATE DESC, I.TIMELASTUPDATE DESC) AS recency_rank
        FROM {eomis_inmateprofile} I
    ) 
    SELECT 
        OFFENDERID, 
        ADCNUMBER,
        INMATEFIRSTNAME, 
        INMATEMIDDLENAME, 
        INMATELASTNAME, 
        INMATENAMESUFFIX, 
        INMATEDATEOFBIRTH, 
        INMATESEXCODE, 
        INMATERACECODE, 
    FROM normalized_rows
    WHERE recency_rank = 1
"""

VIEW_BUILDER = DirectIngestPreProcessedIngestViewBuilder(
    region="us_co",
    ingest_view_name="StatePerson",
    view_query_template=VIEW_QUERY_TEMPLATE,
    order_by_cols="OFFENDERID",
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
