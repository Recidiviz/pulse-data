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
"""Identity ingest view for US_ND Docstars (supervision) staff identity information.

The view lifts a trailing generational suffix (JR, SR, II, ...) glued onto the
given name or the surname into NAME_SUFFIX, after splitting the
specialized-caseload suffix (" -IC" / " -OS") off the surname.
"""

from recidiviz.ingest.direct.regions.us_nd.identity_views.name_unwrapping import (
    build_alias_list_sql,
    coalesce_trailing_name_suffix_sql,
    extract_alias_name_part_sql,
    strip_trailing_name_suffix_sql,
    unwrap_primary_name_sql,
)
from recidiviz.ingest.direct.regions.us_nd.identity_views.template_docstars_staff import (
    DOCSTARS_STAFF_ROSTER_TEMPLATE,
    LATEST_NAMES_CTE_NAME,
)
from recidiviz.ingest.direct.views.direct_ingest_view_query_builder import (
    DirectIngestViewQueryBuilder,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

# Surname with the specialized-caseload suffix (" -IC" / " -OS") stripped, done
# before the standard name cleaning so the suffix does not survive as a name.
_LASTNAME_BASE = (
    "IF(LastName LIKE '% -%', SPLIT(LastName, ' -')[SAFE_OFFSET(0)], LastName)"
)

# JSON array of alternate names embedded in the raw name columns (a parenthetical
# nickname in FirstName, etc.), one partial-alias entry per column that has one;
# the mapping expands these into IdentityAlias rows.
_ALIAS_LIST_SQL = build_alias_list_sql(
    field_to_expression={
        "given_name": extract_alias_name_part_sql("FirstName"),
        "surname": extract_alias_name_part_sql(_LASTNAME_BASE),
    }
)

VIEW_QUERY_TEMPLATE = f"""
{DOCSTARS_STAFF_ROSTER_TEMPLATE},
-- Cleaned name parts; the outer SELECT below lifts out the trailing
-- generational suffix.
cleaned AS (
  SELECT
    OFFICER,
    {unwrap_primary_name_sql(_LASTNAME_BASE)} AS surname,
    {unwrap_primary_name_sql('FirstName')} AS given_name,
    {_ALIAS_LIST_SQL} AS ALIAS_LIST,
    EMAIL
  FROM {LATEST_NAMES_CTE_NAME}
)
SELECT
  OFFICER,
  {strip_trailing_name_suffix_sql('surname')} AS LAST_NAME,
  {strip_trailing_name_suffix_sql('given_name')} AS FIRST_NAME,
  {coalesce_trailing_name_suffix_sql('surname', 'given_name')} AS NAME_SUFFIX,
  ALIAS_LIST,
  EMAIL
FROM cleaned
"""

VIEW_BUILDER = DirectIngestViewQueryBuilder(
    region="us_nd",
    ingest_view_name="docstars_staff",
    view_query_template=VIEW_QUERY_TEMPLATE,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
