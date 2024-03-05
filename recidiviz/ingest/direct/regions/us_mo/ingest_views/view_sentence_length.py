# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2023 Recidiviz, Inc.
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
"""Query produces a view for sentence length information as it changes over time.
This includes:
  - Sentence length in days
  - Earned time credit
  - Projected dates

These data are valid from the length_update_datetime of a StateSentenceLength
entity until the length_update_datetime of the next chronological StateSentenceLength
entity.

Raw data files include:
  - LBAKRDTA_TAK020 is the crosswalk between individual sentences and eligibility data
  - LBAKRDTA_TAK022 has the base information for incarceration sentences
  - LBAKRDTA_TAK023 has detailed information for incarceration sentences
  - LBAKRDTA_TAK044 has information for parole eligibility
"""
from recidiviz.ingest.direct.views.direct_ingest_view_query_builder import (
    DirectIngestViewQueryBuilder,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override


def table_ids(prefix: str) -> str:
    return f"""
    CONCAT({prefix}_DOC, '-', {prefix}_CYC, '-', {prefix}_SEO) AS sentence_id,
    CONCAT({prefix}_DOC, '-', {prefix}_CYC) AS sentence_group_id
    """


def parsed_datetime_columns(prefix: str) -> str:
    """Generates SQL to parse the row creation date and time, row update date and time, and system update datetime.

    We use SAFE.PARSE_DATETIME because a handful of _TCR and _TLU fields are erroneous. So,
    external_create and external_update will be NULL if a date/time field does not exist, is '0',
    or in an unexpected format.
    """
    return f"""
    SAFE.PARSE_DATETIME(
        "%y%j%H%M%S",
        CONCAT(
            RIGHT({prefix}_DCR, 5),
            LPAD({prefix}_TCR, 6, '0')
        )
    ) AS external_create,
    SAFE.PARSE_DATETIME(
        "%y%j%H%M%S",
        CONCAT(
            RIGHT({prefix}_DLU, 5),
            LPAD({prefix}_TLU, 6, '0')
        )
    ) AS external_update,
    update_datetime
    """


def fill_value(col: str) -> str:
    return f"""
    LAST_VALUE({col} IGNORE NULLS) 
        OVER(PARTITION BY sentence_id ORDER BY critical_date, src ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
    AS {col}
    """


def critical_date_column() -> str:
    """SQL to set a critical_date column as the most recent external date.
    It defaults to the system update_datetime if no external date exists.
    """
    return """
    CASE 
        WHEN external_update IS NOT NULL THEN external_update
        WHEN external_update IS NULL AND external_create IS NOT NULL THEN external_create
        ELSE update_datetime
    END AS critical_date
    """


def bad_dates() -> str:
    return str(
        ("0", "19000000", "20000000", "66666666", "77777777", "88888888", "99999999")
    )


# Creates a table with a unique key of sentence_id, sentence_group_id, critical_date
# Columns BS_PD, CG_MD, BT_SLY, BT_SLM, BT_SLD, BT_SCT, BT_PC
# And then grouped columns group_BS_PD, group_CG_MD, group_BT_SLY, group_BT_SLM, group_BT_SLD, group_BT_SCT, group_BT_PC
# The unique key of sentence_id, critical_date is needed to union data from different tables together.
# The sentence_group_id, critical_date pairing is needed to make data for StateSentenceGroup

# TODO(#26620) Include supervision sentence data.
VIEW_QUERY_TEMPLATE = f"""
WITH _bs_cleaned AS (
    SELECT 
        {table_ids('BS')},
        {parsed_datetime_columns('BS')},
        BS_PD
    FROM 
        {{LBAKRDTA_TAK022@ALL}}
    WHERE
        BS_PD NOT IN {bad_dates()}
),
_bs_to_union AS (
    SELECT DISTINCT
        'BS' AS src,
        sentence_id,
        sentence_group_id,
        {critical_date_column()},
        BS_PD,
        CAST(NULL AS string) AS CG_MD, 
        CAST(NULL AS string) AS BT_SLY, 
        CAST(NULL AS string) AS BT_SLM, 
        CAST(NULL AS string) AS BT_SLD, 
        CAST(NULL AS string) AS BT_SCT, 
        CAST(NULL AS string) AS BT_PC
    FROM
        _bs_cleaned
),
_bt_cleaned AS (
    SELECT 
        {table_ids('BT')},
        {parsed_datetime_columns('BT')},
        CASE WHEN BT_SLY = '9999' THEN NULL ELSE BT_SLY END AS BT_SLY, -- sentence length, years
        CASE WHEN BT_SLM = '99'   THEN NULL ELSE BT_SLM END AS BT_SLM, -- sentence length, months
        CASE WHEN BT_SLD = '99'   THEN NULL ELSE BT_SLD END AS BT_SLD, -- sentence length, days
        BT_SCT, -- earned time
        CASE WHEN BT_PC NOT IN {bad_dates()} THEN BT_PC ELSE NULL END AS BT_PC
    FROM 
        {{LBAKRDTA_TAK023@ALL}}
),
_bt_to_union AS (
    SELECT DISTINCT
        'BT' AS src,
        sentence_id, 
        sentence_group_id,
        {critical_date_column()},
        CAST(NULL AS string) AS BS_PD,
        CAST(NULL AS string) AS CG_MD, 
        BT_SLY, 
        BT_SLM, 
        BT_SLD, 
        BT_SCT, 
        BT_PC
    FROM
        _bt_cleaned
),
_cg_date AS (
    SELECT 
        CG_DOC, 
        CG_CYC, 
        CG_ESN, 
        CG_MD,
        {parsed_datetime_columns('CG')}
    FROM 
        {{LBAKRDTA_TAK044@ALL}}
    WHERE 
        CG_MD NOT IN {bad_dates()}
),
_cg_cleaned AS (
    SELECT DISTINCT
        CG_DOC, 
        CG_CYC, 
        CG_ESN, 
        CG_MD,
        {critical_date_column()}
    FROM 
        _cg_date
),
_bq_cleaned AS (
    SELECT DISTINCT 
        BQ_DOC, 
        BQ_CYC, 
        BQ_SEO, 
        BQ_ESN,
        {table_ids('BQ')}
    FROM 
        {{LBAKRDTA_TAK020@ALL}}
),
_cg_bq_to_union AS (
    SELECT DISTINCT
        'CQ' AS src,
        sentence_id,
        sentence_group_id,
        critical_date, 
        CAST(NULL AS string) AS BS_PD,
        CG_MD, 
        CAST(NULL AS string) AS BT_SLY, 
        CAST(NULL AS string) AS BT_SLM, 
        CAST(NULL AS string) AS BT_SLD, 
        CAST(NULL AS string) AS BT_SCT, 
        CAST(NULL AS string) AS BT_PC
    FROM 
        _bq_cleaned
    JOIN 
        _cg_cleaned
    ON 
        BQ_DOC = CG_DOC AND 
        BQ_CYC = CG_CYC AND 
        BQ_ESN = CG_ESN
),
unioned_rows AS (
    SELECT * FROM _bs_to_union
    UNION ALL 
    SELECT * FROM _cg_bq_to_union
    UNION ALL 
    SELECT * FROM _bt_to_union
), 
sentence_length_values AS (
    SELECT
        -- regex should have double slash in BigQuery
        REGEXP_EXTRACT(sentence_id, '^(\\\\d+)-') AS person_ext_id,
        sentence_id AS sentence_ext_id,
        critical_date,
        {fill_value('BS_PD')},
        {fill_value('CG_MD')},
        {fill_value('BT_SLY')},
        {fill_value('BT_SLM')},
        {fill_value('BT_SLD')},
        {fill_value('BT_SCT')},
        {fill_value('BT_PC')}
    FROM 
        unioned_rows
    -- Filters down to only the last row per PK for each date
    WHERE true -- needs a WHERE clause to be valid in the emulator query planner/analyzer
    QUALIFY ROW_NUMBER() OVER (PARTITION BY sentence_id, critical_date ORDER BY src DESC) = 1
)
-- DirectIngestViewQueryBuilder didn't like the QUALIFY at the end.
SELECT * FROM sentence_length_values
"""

VIEW_BUILDER = DirectIngestViewQueryBuilder(
    region="us_mo",
    ingest_view_name="sentence_length",
    view_query_template=VIEW_QUERY_TEMPLATE,
    order_by_cols="sentence_ext_id, critical_date",
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
