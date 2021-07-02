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
"""Query containing the supervision contacts made during a person's supervision period(s).
"""

from recidiviz.ingest.direct.views.direct_ingest_big_query_view_types import (
    DirectIngestPreProcessedIngestViewBuilder,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

VIEW_QUERY_TEMPLATE = """
SELECT
    FACT_PAROLEE_CNTC_SUMRY_ID as contact_id,
    PERSON_ID as person_id,
    PAROLE_NUMBER as parole_number,
    CAST(LEFT(START_DATE, 10) AS DATE) AS contact_start_date,
    CAST(LEFT(END_DATE, 10) AS DATE) AS contact_end_date,
    DURATION_MINS as duration,
    CONTACT_TYPE as contact_type,
    METHOD as method,
    ATTEMPTED as contact_attempt,
    COLLATERAL_TYPE as collateral_type,
    ASSISTED as assisted,
    PRL_AGNT_EMPL_NO as agent_number,
    PRL_AGNT_FIRST_NAME as agent_first_name,
    PRL_AGNT_LAST_NAME as agent_last_name,
    PRL_AGNT_ORG_NAME as parole_org
  FROM
    {dbo_PRS_FACT_PAROLEE_CNTC_SUMRY}
"""

VIEW_BUILDER = DirectIngestPreProcessedIngestViewBuilder(
    region="us_pa",
    ingest_view_name="supervision_contacts",
    view_query_template=VIEW_QUERY_TEMPLATE,
    order_by_cols="parole_number ASC, contact_id ASC",
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
