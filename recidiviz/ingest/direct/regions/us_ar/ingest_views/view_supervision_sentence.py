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
"""Query containing supervision sentence information."""

from recidiviz.ingest.direct.views.direct_ingest_view_query_builder import (
    DirectIngestViewQueryBuilder,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

VIEW_QUERY_TEMPLATE = """
WITH sc_cleaned AS (
    -- Probation and parole sentences are recorded in the SENTENCECOMPONENT table with
    -- details that are true at the time of imposition, including some details that 
    -- are predicted/intended yet subject to change (such as effective date). We do an
    -- inner join with SUPVTIMELINE in order to limit the results to concrete, real-world
    -- sentences, and use dates from SUPVTIMELINE to ensure that ingested sentencing dates
    -- reflect actual events. This is especially important for parole cases, since incarceration
    -- terms automatically get parole data appended in the SENTENCECOMPONENT table, regardless
    -- of if the sentence actually has an associated parole term (which not all do).
    SELECT 
        OFFENDERID,
        COMMITMENTPREFIX,
        SENTENCECOMPONENT,
        COMPSTATUSCODE,
        NULLIF(COMPSTATUSDATE,'1000-01-01 00:00:00') AS COMPSTATUSDATE,
        NULLIF(OFFENSEDATE,'1000-01-01 00:00:00') AS OFFENSEDATE,
        NULLIF(SENTENCEIMPOSEDDATE,'1000-01-01 00:00:00') AS SENTENCEIMPOSEDDATE,
        PROBATIONTERMY,
        PROBATIONTERMM,
        PROBATIONTERMD,
        EXTENDEDTERMY,
        EXTENDEDTERMM,
        EXTENDEDTERMD,
        SERIOUSNESSLEVEL,
        STATUTE1,
        STATUTE2,
        STATUTE3,
        STATUTE4,
        NUMBERCOUNTS,
        UPPER(FELONYMISDCLASS) AS FELONYMISDCLASS,

        NULLIF(SUPVPERIODBEGINDATE,'1000-01-01 00:00:00') AS SUPVPERIODBEGINDATE,
        NULLIF(SUPVSTARTDATE,'1000-01-01 00:00:00') AS SUPVSTARTDATE,
        NULLIF(SUPVTERMDATE,'1000-01-01 00:00:00') AS SUPVTERMDATE,
        LENGTHPAROLEYEAR,
        LENGTHPAROLEMONTH,
        LENGTHPAROLEDAY
    FROM {SENTENCECOMPONENT}
    INNER JOIN {SUPVTIMELINE} 
    USING(OFFENDERID,COMMITMENTPREFIX,SENTENCECOMPONENT)
    WHERE REGEXP_CONTAINS(OFFENDERID, r'^[[:digit:]]+$') 
)

-- Some sentence components have both parole and probation sub-components (i.e., rows
-- with non-zero values in the LENGTHPAROLE columns as well as the PROBATIONTERM columns).
-- The union below allows us to ingest these sub-components separately, setting SUPVTYPE to
-- either PAROLE or PROBATION so that the mapping knows which of the sentence length columns
-- should be used.

SELECT 
    *, 
    'PAROLE' AS SUPVTYPE 
FROM sc_cleaned 
WHERE GREATEST(
    LENGTHPAROLEYEAR,
    LENGTHPAROLEMONTH,
    LENGTHPAROLEDAY
) != '0'
UNION ALL (
    SELECT
        *, 
        'PROBATION' AS SUPVTYPE 
    FROM sc_cleaned 
    WHERE GREATEST(
        PROBATIONTERMY,
        PROBATIONTERMM,
        PROBATIONTERMD,
        EXTENDEDTERMY,
        EXTENDEDTERMM,
        EXTENDEDTERMD
    ) != '0'
) 
"""

VIEW_BUILDER = DirectIngestViewQueryBuilder(
    region="us_ar",
    ingest_view_name="supervision_sentence",
    view_query_template=VIEW_QUERY_TEMPLATE,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
