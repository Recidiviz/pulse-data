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
"""Identity ingest view for US_ND Elite (facilities) staff identity information.

Some rows are program-referral placeholders that carry only a STAFF_ID and no
name; the mapping produces an external-id-only fragment for those.
"""

from recidiviz.ingest.direct.regions.us_nd.identity_views.name_unwrapping import (
    coalesce_trailing_name_suffix_sql,
    strip_trailing_name_suffix_sql,
    unwrap_primary_name_sql,
)
from recidiviz.ingest.direct.regions.us_nd.identity_views.template_elite_staff import (
    ALL_STAFF_CTE_NAME,
    ELITE_STAFF_ROSTER_TEMPLATE,
)
from recidiviz.ingest.direct.views.direct_ingest_view_query_builder import (
    DirectIngestViewQueryBuilder,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

# Elite staff names carry data-entry junk (digit-prefixed surnames like 00NAME,
# pure-digit placeholders like 00, underscores, stray #). Cleaning recovers the
# real name and nulls a name that is entirely junk. The view lifts a trailing
# generational suffix (JR, SR, II, ...) glued onto the given name or surname
# into NAME_SUFFIX. No alias extraction: these are placeholder artifacts, not
# genuine alternate names.
VIEW_QUERY_TEMPLATE = f"""
{ELITE_STAFF_ROSTER_TEMPLATE},
-- Cleaned name parts; the outer SELECT below lifts out the trailing
-- generational suffix.
cleaned AS (
    SELECT
        STAFF_ID,
        {unwrap_primary_name_sql('FIRST_NAME')} AS FIRST_NAME,
        {unwrap_primary_name_sql('MIDDLE_NAME')} AS MIDDLE_NAME,
        {unwrap_primary_name_sql('LAST_NAME')} AS LAST_NAME
    FROM {ALL_STAFF_CTE_NAME}
)
SELECT
    STAFF_ID,
    {strip_trailing_name_suffix_sql('FIRST_NAME')} AS FIRST_NAME,
    MIDDLE_NAME,
    {strip_trailing_name_suffix_sql('LAST_NAME')} AS LAST_NAME,
    {coalesce_trailing_name_suffix_sql('LAST_NAME', 'FIRST_NAME')} AS NAME_SUFFIX
FROM cleaned
"""

VIEW_BUILDER = DirectIngestViewQueryBuilder(
    region="us_nd",
    ingest_view_name="elite_staff",
    view_query_template=VIEW_QUERY_TEMPLATE,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
