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
"""Identity ingest view for US_ND Elite (facilities) JII identity information.

One row per person keyed on the cleaned Elite ROOT_OFFENDER_ID. The primary
name comes from elite_offenders; alternate names come from the elite_alias
table (tagged with their raw ALIAS_NAME_TYPE, excluding gang monikers) plus any
alternates pulled out of the elite_offenders name itself, JSON-aggregated into
ALIAS_LIST for the mapping to expand into IdentityAlias rows. Aliases that
duplicate the primary name pass through here; cluster resolution drops them
(see resolve_cluster_attributes).

For both the primary name and each alias, the view lifts a trailing
generational suffix (JR, SR, II, ...) out of the given name or the surname
into its own suffix column. The elite sources record suffixes inconsistently
-- sometimes in the dedicated SUFFIX column, sometimes glued onto a name part
-- so extracting the embedded form keeps the name parts clean and lets
otherwise-identical names deduplicate.
"""

from recidiviz.ingest.direct.regions.us_nd.identity_views.name_unwrapping import (
    coalesce_trailing_name_suffix_sql,
    extract_alias_name_part_sql,
    extract_trailing_name_suffix_sql,
    strip_trailing_name_suffix_sql,
    unwrap_name_suffix_sql,
    unwrap_primary_name_sql,
)
from recidiviz.ingest.direct.views.direct_ingest_view_query_builder import (
    DirectIngestViewQueryBuilder,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

# Strip commas and trailing decimals/zeroes so the id matches the US_ND_ELITE id
# used elsewhere and joins across elite_offenders and elite_alias.
_ROOT_ID_CLEAN = r"REGEXP_REPLACE(ROOT_OFFENDER_ID, r'\.00$|,', '')"

# elite_alias rows with this type are gang monikers: affiliation data rather than
# alternate names, and frequently placeholders like "NO GANG NAME". The view
# excludes them from identity aliases entirely.
_GANG_ALIAS_TYPE = "GNG"

# elite_alias rows with this type are nicknames. The source records a lone
# nickname in LAST_NAME with FIRST_NAME empty, but a nickname is a given-style
# name, so the view routes it to given_name.
_NICKNAME_ALIAS_TYPE = "N"

VIEW_QUERY_TEMPLATE = f"""
WITH
-- Per-row primary record from elite_offenders: the cleaned canonical name plus
-- any alternate embedded in that name, alongside demographics.
elite_primary AS (
    SELECT
        {_ROOT_ID_CLEAN} AS ROOT_OFFENDER_ID,
        {unwrap_primary_name_sql('FIRST_NAME')} AS given_name,
        {unwrap_primary_name_sql('LAST_NAME')} AS surname,
        {extract_alias_name_part_sql('FIRST_NAME')} AS alt_given,
        {extract_alias_name_part_sql('LAST_NAME')} AS alt_surname,
        BIRTH_DATE,
        SEX_CODE,
        RACE_CODE
    FROM {{elite_offenders}}
),
-- One row per person: primary name, demographics, and race aggregation. This
-- CTE lifts a trailing generational suffix glued onto the given name or the
-- surname into NAME_SUFFIX, and picks all per-person scalars from one source
-- row (the ordered ARRAY_AGG ... LIMIT 1 below, which sorts the group's rows
-- by every field so the pick is deterministic) so the name parts, suffix, and
-- demographics can never mix values from different rows of a group.
person AS (
    SELECT
        ROOT_OFFENDER_ID,
        {strip_trailing_name_suffix_sql('picked.given_name')} AS FIRST_NAME,
        {strip_trailing_name_suffix_sql('picked.surname')} AS LAST_NAME,
        {coalesce_trailing_name_suffix_sql('picked.surname', 'picked.given_name')} AS NAME_SUFFIX,
        -- Alternates pulled out of the primary name, one alias-typed ('A') entry
        -- per name part that carries one, with the surname's trailing suffix
        -- lifted into its own column. The alias assembly below unnests this so
        -- it scans this per-person CTE once rather than once per extracted part.
        ARRAY_CONCAT(
            IF(picked.alt_given IS NOT NULL,
               [STRUCT('A' AS alias_type, picked.alt_given AS given_name, CAST(NULL AS STRING) AS middle_name, CAST(NULL AS STRING) AS surname, CAST(NULL AS STRING) AS name_suffix)],
               []),
            IF(picked.alt_surname IS NOT NULL,
               [STRUCT('A' AS alias_type, CAST(NULL AS STRING) AS given_name, CAST(NULL AS STRING) AS middle_name, {strip_trailing_name_suffix_sql('picked.alt_surname')} AS surname, {extract_trailing_name_suffix_sql('picked.alt_surname')} AS name_suffix)],
               [])
        ) AS extracted_alt_aliases,
        CAST(CAST(picked.BIRTH_DATE AS DATETIME) AS DATE) AS BIRTH_DATE,
        picked.SEX_CODE AS SEX_CODE,
        RACE_CODE_LIST,
        HISPANIC_INDICATOR
    FROM (
        SELECT
            ROOT_OFFENDER_ID,
            ARRAY_AGG(STRUCT(
                given_name, surname, alt_given, alt_surname, BIRTH_DATE, SEX_CODE
            ) ORDER BY given_name, surname, alt_given, alt_surname, BIRTH_DATE, SEX_CODE
              LIMIT 1)[OFFSET(0)] AS picked,
            STRING_AGG(DISTINCT RACE_CODE, ',' ORDER BY RACE_CODE) AS RACE_CODE_LIST,
            IF(LOGICAL_OR(RACE_CODE = 'HIS'), 'HISPANIC', 'OTHER') AS HISPANIC_INDICATOR
        FROM elite_primary
        GROUP BY ROOT_OFFENDER_ID
    )
),
-- Alternate names from the elite_alias table (may be several per person),
-- cleaned and tagged with their raw type for the mapping to resolve to a use.
-- The IF expressions below route a nickname stored as a lone surname to
-- given_name.
alias_rows AS (
    SELECT
        ROOT_OFFENDER_ID,
        alias_type,
        IF(is_lone_nickname,
           surname, {strip_trailing_name_suffix_sql('given_name')})
            AS given_name,
        middle_name,
        IF(is_lone_nickname,
           NULL, {strip_trailing_name_suffix_sql('surname')})
            AS surname,
        -- Prefer the dedicated SUFFIX column; fall back to a suffix embedded in
        -- the surname or given name. For a nickname the suffix stays NULL (its
        -- surname column holds the nickname).
        IF(is_lone_nickname,
           NULL,
           COALESCE(name_suffix, {coalesce_trailing_name_suffix_sql('surname', 'given_name')}))
            AS name_suffix
    FROM (
        SELECT
            {_ROOT_ID_CLEAN} AS ROOT_OFFENDER_ID,
            ALIAS_NAME_TYPE AS alias_type,
            {unwrap_primary_name_sql('FIRST_NAME')} AS given_name,
            {unwrap_primary_name_sql('MIDDLE_NAME')} AS middle_name,
            {unwrap_primary_name_sql('LAST_NAME')} AS surname,
            {unwrap_name_suffix_sql('SUFFIX')} AS name_suffix,
            -- A nickname (type N) the source stored as a lone surname, which the
            -- IF expressions above route to given_name instead.
            {unwrap_primary_name_sql('FIRST_NAME')} IS NULL
                AND ALIAS_NAME_TYPE = '{_NICKNAME_ALIAS_TYPE}' AS is_lone_nickname
        FROM {{elite_alias}}
        WHERE ALIAS_NAME_TYPE IS DISTINCT FROM '{_GANG_ALIAS_TYPE}'
    )
),
-- Union the elite_alias rows with alternates extracted from the primary name.
-- The extracted alternates were assembled on the person CTE and are unnested
-- here in a single scan; they are alias-typed ('A') since the source has no type.
all_aliases AS (
    SELECT ROOT_OFFENDER_ID, alias_type, given_name, middle_name, surname, name_suffix
    FROM alias_rows
    UNION ALL
    SELECT
        ROOT_OFFENDER_ID, alt.alias_type, alt.given_name, alt.middle_name,
        alt.surname, alt.name_suffix
    FROM person, UNNEST(extracted_alt_aliases) AS alt
),
-- Deduplicate and JSON-aggregate the aliases into one ALIAS_LIST per person.
-- Redundant entries survive here on purpose: the identity mapping delegate
-- drops entries with no name parts at mapping time (see
-- IdentityIngestViewManifestCompilerDelegate.get_filter_if_all_null_fields),
-- and cluster resolution drops entries identical to the resolved primary name
-- (see resolve_cluster_attributes).
alias_list AS (
    SELECT
        ROOT_OFFENDER_ID,
        TO_JSON_STRING(ARRAY_AGG(
            STRUCT(
                alias_type AS alias_type,
                given_name AS given_name,
                middle_name AS middle_name,
                surname AS surname,
                name_suffix AS name_suffix
            )
            ORDER BY alias_type, given_name, middle_name, surname, name_suffix
        )) AS ALIAS_LIST
    FROM (
        SELECT DISTINCT
            ROOT_OFFENDER_ID, alias_type, given_name, middle_name, surname, name_suffix
        FROM all_aliases
    )
    GROUP BY ROOT_OFFENDER_ID
)
SELECT
    p.ROOT_OFFENDER_ID,
    p.FIRST_NAME,
    p.LAST_NAME,
    p.NAME_SUFFIX,
    p.BIRTH_DATE,
    p.SEX_CODE,
    p.RACE_CODE_LIST,
    p.HISPANIC_INDICATOR,
    IFNULL(al.ALIAS_LIST, '[]') AS ALIAS_LIST
FROM person p
LEFT JOIN alias_list al USING (ROOT_OFFENDER_ID)
"""

VIEW_BUILDER = DirectIngestViewQueryBuilder(
    region="us_nd",
    ingest_view_name="elite_offenders",
    view_query_template=VIEW_QUERY_TEMPLATE,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
