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
"""Query for state sentences.

This view will need to be revisited once we have more information on the most reliable 
sources for key dates."""
from recidiviz.ingest.direct.views.direct_ingest_view_query_builder import (
    DirectIngestViewQueryBuilder,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

VIEW_QUERY_TEMPLATE = """
SELECT
    off.OFFENSE_ID AS external_id,
    CONCAT(off.COMMITMENT_ID, '-', doc.DOC_ID) AS sentence_group_external_id,
    CASE
        WHEN life.DESCRIPTION IN ('Life (35 to Life)', 'Life (25 to Life)', 'Life', 'Natural Life')
        THEN 1
        ELSE 0
    END AS is_life,
    CASE
        WHEN life.DESCRIPTION = 'Death'
        THEN 1
        ELSE 0
    END AS is_capital,
    CASE
        WHEN parole.DESCRIPTION = 'Yes'
        THEN 1
        ELSE 0
    END AS parole_possible,
    CAST(commit.SENTENCED_DTM AS DATETIME) AS imposed_date,
    county.DESCRIPTION AS county_code,
    doc.PERSON_ID AS person_id,
    IF(off.NUM_JAIL_CREDIT_DAYS = 'NULL', '0', off.NUM_JAIL_CREDIT_DAYS) AS jail_credit_days
FROM {AZ_DOC_SC_OFFENSE} off
LEFT JOIN {LOOKUPS} life ON life.LOOKUP_ID = off.LIFE_OR_DEATH_ID
LEFT JOIN {LOOKUPS} parole ON parole.LOOKUP_ID = off.PAROLE_ELIGIBILITY_ID
LEFT JOIN {AZ_DOC_SC_COMMITMENT} commit ON off.COMMITMENT_ID = commit.COMMITMENT_ID
LEFT JOIN {AZ_DOC_SC_EPISODE} sc_episode ON commit.SC_EPISODE_ID = sc_episode.SC_EPISODE_ID
LEFT JOIN {DOC_EPISODE} doc ON sc_episode.DOC_ID = doc.DOC_ID
LEFT JOIN {LOOKUPS} county ON county.LOOKUP_ID = commit.COUNTY_ID
-- very rare edge case that breaks ingest
WHERE doc.PERSON_ID IS NOT NULL
"""
VIEW_BUILDER = DirectIngestViewQueryBuilder(
    region="us_az",
    ingest_view_name="state_sentence",
    view_query_template=VIEW_QUERY_TEMPLATE,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
