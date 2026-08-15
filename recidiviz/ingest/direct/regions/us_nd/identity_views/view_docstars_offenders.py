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
"""Identity ingest view for US_ND Docstars (supervision) JII identity information.

The view lifts a trailing generational suffix (JR, SR, II, ...) glued onto the
given name or the surname into NAME_SUFFIX so the name parts stay clean. In the
Docstars data the suffix lands on the given name far more often than the surname.
"""

from recidiviz.ingest.direct.regions.us_nd.identity_views.name_unwrapping import (
    build_alias_list_sql,
    coalesce_trailing_name_suffix_sql,
    extract_alias_name_part_sql,
    strip_trailing_name_suffix_sql,
    unwrap_primary_name_sql,
)
from recidiviz.ingest.direct.views.direct_ingest_view_query_builder import (
    DirectIngestViewQueryBuilder,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

# JSON array of the alternate names embedded in the raw name columns, one entry
# per column that has an alternate. Each entry is a partial alias holding just
# the part it came from (a FIRST alternate fills given_name, etc.); the mapping
# expands these into IdentityAlias rows with $split_json/$foreach.
_ALIAS_LIST_SQL = build_alias_list_sql(
    field_to_expression={
        "given_name": extract_alias_name_part_sql("FIRST"),
        "middle_name": extract_alias_name_part_sql("MIDDLE"),
        "surname": extract_alias_name_part_sql("LAST_NAME"),
    }
)

VIEW_QUERY_TEMPLATE = f"""
WITH
-- Cleaned name parts and demographics, one row per raw record. The trailing
-- generational suffix is lifted out of the name parts in the outer SELECT.
cleaned AS (
    SELECT
        SID,
        ITAGROOT_ID,
        {unwrap_primary_name_sql('FIRST')} AS given_name,
        {unwrap_primary_name_sql('MIDDLE')} AS MIDDLE_NAME,
        {unwrap_primary_name_sql('LAST_NAME')} AS surname,
        {_ALIAS_LIST_SQL} AS ALIAS_LIST,
        CAST(CAST(DOB AS DATETIME) AS DATE) AS DOB,
        SEX,
        RACE,
        EMAIL,
        PHONE,
        PHONE2
    FROM {{docstars_offenders}}
)
SELECT
    SID,
    ITAGROOT_ID,
    {strip_trailing_name_suffix_sql('given_name')} AS FIRST_NAME,
    MIDDLE_NAME,
    {strip_trailing_name_suffix_sql('surname')} AS LAST_NAME,
    {coalesce_trailing_name_suffix_sql('surname', 'given_name')} AS NAME_SUFFIX,
    ALIAS_LIST,
    DOB,
    SEX,
    RACE,
    EMAIL,
    PHONE,
    PHONE2
FROM cleaned
"""

VIEW_BUILDER = DirectIngestViewQueryBuilder(
    region="us_nd",
    ingest_view_name="docstars_offenders",
    view_query_template=VIEW_QUERY_TEMPLATE,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
