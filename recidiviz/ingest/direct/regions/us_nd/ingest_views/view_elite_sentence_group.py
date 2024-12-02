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
"""Query containing incarceration sentence group information from the ELITE system.

Incarceration sentence groups are interpreted at the booking level. Each booking can 
be comprised of one to many sentences. 
"""

from recidiviz.ingest.direct.views.direct_ingest_view_query_builder import (
    DirectIngestViewQueryBuilder,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

VIEW_QUERY_TEMPLATE = """
SELECT DISTINCT
    REPLACE(REPLACE(aggs.OFFENDER_BOOK_ID,',',''), '.00', '') AS OFFENDER_BOOK_ID,
    PAROLE_DATE,
    FINAL_SENT_EXP_DATE,
    COALESCE(OVR_POS_REL_DATE, CALC_POS_REL_DATE) AS RELEASE_DATE,
    COALESCE(aggs.MODIFY_DATETIME, aggs.CREATE_DATETIME) AS GROUP_UPDT_DTM,
FROM {elite_offendersentenceaggs@ALL} aggs
-- Do not make sentence groups for booking IDs that appear in sentence aggs, but never
-- actually led to a booking. It is unclear why these appear in sentence aggs when 
-- they do not have any component sentences in elite_offendersentences.
-- As of 11/19/2024 this only excludes 52 OFFENDER_BOOK_IDs.
INNER JOIN {elite_offendersentences} sentences
    ON(REPLACE(REPLACE(sentences.OFFENDER_BOOK_ID,',',''), '.00', '') = REPLACE(REPLACE(aggs.OFFENDER_BOOK_ID,',',''), '.00', ''))
"""

VIEW_BUILDER = DirectIngestViewQueryBuilder(
    region="us_nd",
    ingest_view_name="elite_sentence_group",
    view_query_template=VIEW_QUERY_TEMPLATE,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
