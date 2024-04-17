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
  - LBAKRDTA_TAK022 has the base information for charges
  - LBAKRDTA_TAK023 has detailed information for incarceration sentences
  - LBAKRDTA_TAK024 has detailed information for supervision sentences
  - LBAKRDTA_TAK025 has relates sentences to their status code information
  - LBAKRDTA_TAK026 has detailed information for sentence status codes
  - LBAKRDTA_TAK044 has information for parole eligibility

The critical dates for StateSentenceLength arise from a few places:
  - A create/update to charge level information (so could affect several sentences)
        - It's important to note we build critical dates for charge level information, but
          also inner join to sentences so that we do not accidentally create non-existent sentences.
  - A create/update to sentence information, like projected dates
  - A create/update to parole eligibility

Supervision and incarceration sentences are simply stacked together via UNION ALL
after getting common critical dates from their charges. We filter down to unique rows based on sentence_external_id
and critical date, allowing us to get changes in sentence length data from each source.
"""
from recidiviz.ingest.direct.regions.us_mo.ingest_views.templates_sentences import (
    FROM_BU_BS_BV_BW_WHERE_NOT_PRETRIAL,
    MAGIC_DATES,
)
from recidiviz.ingest.direct.views.direct_ingest_view_query_builder import (
    DirectIngestViewQueryBuilder,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override


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
        OVER(PARTITION BY sentence_ext_id ORDER BY critical_date, src ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
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


PROJECTED_COMPLETION_DATES = f"""
SELECT DISTINCT
    BS_DOC AS DOC,
    BS_CYC AS CYC,
    BS_SEO AS SEO,
    {critical_date_column()},
    BS_PD, -- projected completion_date
    CAST(NULL AS STRING) AS CG_MD
FROM (
    SELECT 
        BS_DOC,
        BS_CYC,
        BS_SEO,
        {parsed_datetime_columns('BS')},
        BS_PD
    FROM 
        {{LBAKRDTA_TAK022@ALL}}
    WHERE
        BS_PD NOT IN {MAGIC_DATES}
) AS _bs
"""

ELIGIBILITY_DATES = f"""
-- We will use the critical dates from the CG file, and not the crosswalk file
SELECT
    CG.CG_DOC AS DOC,
    CG.CG_CYC AS CYC,
    BQ.BQ_SEO AS SEO,
    {critical_date_column()},
    CAST(NULL AS STRING) AS BS_PD,
    CG.CG_MD  -- Eligibility Minimum Release Date
FROM (
    SELECT
         CG_DOC,
         CG_CYC,
         CG_ESN,
         CG_MD,
        {parsed_datetime_columns('CG')}
    FROM
    {{LBAKRDTA_TAK044@ALL}}
) AS CG
JOIN (
    SELECT
        BQ_DOC,
        BQ_CYC,
        BQ_SEO,
        BQ_ESN
    FROM
        {{LBAKRDTA_TAK020}}
) AS BQ
ON
    BQ_DOC = CG_DOC AND
    BQ_CYC = CG_CYC AND
    BQ_ESN = CG_ESN
WHERE
    CG_MD NOT IN {MAGIC_DATES}
"""

INCARCERATION_SENTENCES = f"""
SELECT DISTINCT
    'BT' AS src,
    person_ext_id,
    sentence_ext_id, 
    {critical_date_column()},
    BT_SLY AS sentence_length_years, 
    BT_SLM AS sentence_length_months, 
    BT_SLD AS sentence_length_days, 
    BT_SCT, 
    BT_PC
FROM (
    SELECT 
        BT_DOC AS person_ext_id,
        CONCAT(BT_DOC, '-', BT_CYC, '-', BT_SEO, '-', 'INCARCERATION') AS sentence_ext_id,
        {parsed_datetime_columns('BT')},
        CASE WHEN BT_SLY IN ('9999', '8888', '6666') THEN NULL ELSE BT_SLY END AS BT_SLY, -- sentence length, years
        CASE WHEN BT_SLM IN ('99', '88', '66') THEN NULL ELSE BT_SLM END AS BT_SLM, -- sentence length, months
        CASE WHEN BT_SLD IN ('99', '88', '66') THEN NULL ELSE BT_SLD END AS BT_SLD, -- sentence length, days
        BT_SCT, -- earned time
        CASE WHEN BT_PC NOT IN {MAGIC_DATES} THEN BT_PC ELSE NULL END AS BT_PC
    FROM 
        {{LBAKRDTA_TAK023@ALL}} AS bt
    JOIN
        {{LBAKRDTA_TAK022}} AS bs
    ON
        bt.BT_DOC = bs.BS_DOC AND
        bt.BT_CYC = bs.BS_CYC AND
        bt.BT_SEO = bs.BS_SEO
    WHERE
        BT_SD NOT IN {MAGIC_DATES}
) AS _bt
"""

_NOT_PRETRIAL_OR_BOND_SUPERVISION_SENTENCES = f"""
SELECT DISTINCT
    BU.BU_DOC, -- unique for each person
    BU.BU_CYC, -- unique for each sentence group
    BU.BU_SEO -- unique for each charge
{FROM_BU_BS_BV_BW_WHERE_NOT_PRETRIAL}
"""

SUPERVISION_SENTENCES = f"""
SELECT DISTINCT
    'BU' AS src,
    person_ext_id,    
    sentence_ext_id, 
    {critical_date_column()},
    BU_SBY AS sentence_length_years, 
    BU_SBM AS sentence_length_months, 
    BU_SBD AS sentence_length_days, 
    CAST(NULL AS STRING) AS BT_SCT, 
    CAST(NULL AS STRING) AS BT_PC
FROM (
    SELECT 
        -- recall that a sentence is updated with BU_FSO, so the ID is just, DOC, CYC, and SEO
        BU_ALL.BU_DOC AS person_ext_id,
        CONCAT(BU_ALL.BU_DOC, '-', BU_ALL.BU_CYC, '-', BU_ALL.BU_SEO, '-', 'SUPERVISION') AS sentence_ext_id,
        {parsed_datetime_columns('BU')},
        CASE WHEN BU_ALL.BU_SBY IN ('9999', '8888', '6666') THEN NULL ELSE BU_SBY END AS BU_SBY, -- probation length, years
        CASE WHEN BU_ALL.BU_SBM IN ('99', '88', '66') THEN NULL ELSE BU_SBM END AS BU_SBM, -- probation length, months
        CASE WHEN BU_ALL.BU_SBD IN ('99', '88', '66') THEN NULL ELSE BU_SBD END AS BU_SBD -- probation length, days
    FROM 
        {{LBAKRDTA_TAK024@ALL}} AS BU_ALL
    JOIN 
        ({_NOT_PRETRIAL_OR_BOND_SUPERVISION_SENTENCES}) AS not_pretrial
    USING (BU_DOC, BU_CYC, BU_SEO)
) AS _bu
"""

VIEW_QUERY_TEMPLATE = f"""
WITH 
    projected_completion_dates AS ({PROJECTED_COMPLETION_DATES}),
    eligibility_dates AS ({ELIGIBILITY_DATES}),
    incarceration_only_ledger AS ({INCARCERATION_SENTENCES}),
    supervision_only_ledger AS ({SUPERVISION_SENTENCES}),
    -- Build charge level ledger with projected_completion_date and eligibility_date
    charge_level_ledger AS (
        SELECT * FROM projected_completion_dates
        UNION ALL
        SELECT * FROM eligibility_dates
    ),
    -- Build a ledger with critical dates from charges and sentences having incarceration data
    incarceration_ledger AS (
        SELECT
            'CHARGE' AS src,
            DOC AS person_ext_id,
            sentence_ext_id,
            critical_date,
            BS_PD,  -- Charge level, projected completion date
            CG_MD,  -- Charge level, eligibility minimum release date
            CAST(NULL AS STRING) AS sentence_length_years, 
            CAST(NULL AS STRING) AS sentence_length_months, 
            CAST(NULL AS STRING) AS sentence_length_days, 
            CAST(NULL AS STRING) AS BT_SCT, 
            CAST(NULL AS STRING) AS BT_PC
        FROM
            charge_level_ledger 
        JOIN (
            SELECT sentence_ext_id
              FROM incarceration_only_ledger
        ) AS _inc_sentence_ids
        ON
            CONCAT(DOC, '-', CYC, '-', SEO, '-', 'INCARCERATION') = _inc_sentence_ids.sentence_ext_id
        UNION ALL
        SELECT
            src,
            person_ext_id,
            sentence_ext_id, 
            critical_date,
            CAST(NULL AS STRING) AS BS_PD,  -- Charge level, projected completion date
            CAST(NULL AS STRING) AS CG_MD,  -- Charge level, eligibility minimum release date
            sentence_length_years, 
            sentence_length_months, 
            sentence_length_days, 
            BT_SCT, 
            BT_PC
        FROM
            incarceration_only_ledger
    ),
    -- Build a ledger with critical dates from charges and sentences having supervision data
    supervision_ledger AS (
        SELECT
            'CHARGE' AS src,
            DOC AS person_ext_id,
            CONCAT(DOC, '-', CYC, '-', SEO, '-', 'SUPERVISION') AS sentence_ext_id,
            critical_date,
            BS_PD,  -- Charge level, projected completion date
            CG_MD,  -- Charge level, eligibility minimum release date
            CAST(NULL AS STRING) AS sentence_length_years, 
            CAST(NULL AS STRING) AS sentence_length_months, 
            CAST(NULL AS STRING) AS sentence_length_days, 
            CAST(NULL AS STRING) AS BT_SCT, 
            CAST(NULL AS STRING) AS BT_PC
        FROM
            charge_level_ledger 
        JOIN (
            SELECT sentence_ext_id
              FROM supervision_only_ledger
        ) AS _sup_sentence_ids
        ON
            CONCAT(DOC, '-', CYC, '-', SEO, '-', 'SUPERVISION') = _sup_sentence_ids.sentence_ext_id
        UNION ALL
        SELECT
            src,
            person_ext_id, 
            sentence_ext_id, 
            critical_date,
            CAST(NULL AS STRING) AS BS_PD,  -- Charge level, projected completion date
            CAST(NULL AS STRING) AS CG_MD,  -- Charge level, eligibility minimum release date
            sentence_length_years, 
            sentence_length_months, 
            sentence_length_days, 
            BT_SCT, 
            BT_PC
        FROM
            supervision_only_ledger
    ), unioned_rows AS (
        SELECT * FROM incarceration_ledger
        UNION ALL
        SELECT * FROM supervision_ledger
    ),
    sentence_length_values AS (
        SELECT
            person_ext_id,
            sentence_ext_id,
            critical_date,
            {fill_value('BS_PD')},
            {fill_value('CG_MD')},
            {fill_value('sentence_length_years')},
            {fill_value('sentence_length_months')},
            {fill_value('sentence_length_days')},
            {fill_value('BT_SCT')},
            {fill_value('BT_PC')}
        FROM 
            unioned_rows
        WHERE 
            -- needs a WHERE clause to be valid in the emulator query planner/analyzer   
            true 
        QUALIFY 
            -- Filters down to only the last row per PK for each date
            ROW_NUMBER() OVER (PARTITION BY sentence_ext_id, critical_date ORDER BY src DESC) = 1
    )
    -- DirectIngestViewQueryBuilder didn't like the QUALIFY at the end.
    SELECT * FROM sentence_length_values
"""

VIEW_BUILDER = DirectIngestViewQueryBuilder(
    region="us_mo",
    ingest_view_name="sentence_length",
    view_query_template=VIEW_QUERY_TEMPLATE,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
