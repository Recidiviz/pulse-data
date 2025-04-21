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
"""
Ingest view for charges that led to convictions, but did not lead to commitments
to ADCRR custody. We ingest these charges onto placeholder sentences so that we can
have a full picture of a person's prior convictions when determining eligibility.

We cannot access any additional information about the sentences these charges led to
because that information is not maintained at the state level. Probation is run at the 
county level in AZ, and the state does not keep independent data about probation or 
county jails.

We prefix sentence external IDs created by this view with "ARREST-" to make them easily 
distinguishable from sentences that are actual ADCRR commitments.
"""
from recidiviz.common.constants.reasonable_dates import (
    STANDARD_DATE_FIELD_REASONABLE_LOWER_BOUND,
)
from recidiviz.ingest.direct.views.direct_ingest_view_query_builder import (
    DirectIngestViewQueryBuilder,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

VIEW_QUERY_TEMPLATE = f"""
SELECT 
    CLASS_ARREST_HISTORY_ID, 
    -- All but 19 DOC_ID values in this table are associated with a PERSON_ID in DOC_EPISODE.
    PERSON_ID, 
    CAST(OFFENSE_DATE AS DATETIME) AS OFFENSE_DATE,
    c.DESCRIPTION as county, 
    l.DESCRIPTION as disposition, 
    ARS_CODE, 
    ars.DESCRIPTION as charge_description,
    SEX_OFFENCE
FROM {{DOC_CLASS_ARREST_HISTORY}} a
LEFT JOIN {{LOOKUPS}}  l
ON (DISPOSITION_ID = l.LOOKUP_ID)
LEFT JOIN {{LOOKUPS}}  s
ON (STATE_ID = s.LOOKUP_ID)
LEFT JOIN {{LOOKUPS}}  c
ON (COUNTY_ID = c.LOOKUP_ID)
LEFT JOIN  {{AZ_DOC_SC_ARS_CODE}} ars 
ON(CAST(CAST(CAST(a.ARS_ID AS DECIMAL) AS INT64) AS STRING) = ars.ARS_ID)
JOIN {{DOC_EPISODE}} 
USING(DOC_ID)
WHERE ENTERED_IN_ERROR_FLAG = 'N'
-- Since we are creating placeholder sentences, and a sentence requires at least one CONVICTED
-- charge, we filter out charges that did not lead to a conviction. This is also reasonable
-- because charges that did not lead to a conviction do not factor into eligibility for 
-- opportunities.
AND (upper(l.DESCRIPTION) NOT IN (
    "NO CHARGES FILED", 
    "PENDING CHARGES", 
    "DISCIPLINE VIOLATION", 
    "NOT PROCESSED", 
    "SET ASIDE", 
    "VACATE", 
    "EXPUNGED", 
    "COMMUTED", 
    "PARDON", 
    "DISMISSED", 
    "UNKNOWN", 
    "NOT GUILTY", 
    "DRUG COURT", 
    "PENDING CHARGES"
    )
    OR l.DESCRIPTION IS NULL)
    AND CAST(OFFENSE_DATE AS DATETIME) BETWEEN '{STANDARD_DATE_FIELD_REASONABLE_LOWER_BOUND}' and @update_timestamp
"""

VIEW_BUILDER = DirectIngestViewQueryBuilder(
    region="us_az",
    ingest_view_name="nonstate_sentence",
    view_query_template=VIEW_QUERY_TEMPLATE,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
