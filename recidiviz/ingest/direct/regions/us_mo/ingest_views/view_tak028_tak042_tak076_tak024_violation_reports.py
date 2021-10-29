# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2020 Recidiviz, Inc.
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
"""Query containing violation reports information."""

from recidiviz.ingest.direct.regions.us_mo.ingest_views.us_mo_view_query_fragments import (
    ALL_OFFICERS_FRAGMENT,
    NON_INVESTIGATION_SUPERVISION_SENTENCES_FRAGMENT,
    TAK142_FINALLY_FORMED_DOCUMENT_FRAGMENT,
)
from recidiviz.ingest.direct.views.direct_ingest_big_query_view_types import (
    DirectIngestPreProcessedIngestViewBuilder,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override
from recidiviz.utils.string import StrictStringFormatter

FINALLY_FORMED_VIOLATIONS_E6 = StrictStringFormatter().format(
    TAK142_FINALLY_FORMED_DOCUMENT_FRAGMENT, document_type_code="XIF"
)

OFFICERS_WITH_MOST_RECENT_ROLE_FRAGMENT = f"""
    {ALL_OFFICERS_FRAGMENT},
    officers_with_role_recency_ranks AS(
        -- Officers with their roles ranked from most recent to least recent.
        SELECT
            BDGNO,
            CLSTTL,
            LNAME,
            FNAME,
            MINTL,
            ROW_NUMBER() OVER (PARTITION BY BDGNO ORDER BY STRDTE DESC) AS recency_rank
        FROM
            normalized_all_officers),
    officers_with_recent_role AS (
        -- Officers with their most recent role only
        SELECT
            BDGNO,
            CLSTTL,
            LNAME,
            FNAME,
            MINTL,
        FROM
            officers_with_role_recency_ranks
        WHERE
            officers_with_role_recency_ranks.recency_rank = 1
            AND officers_with_role_recency_ranks.CLSTTL != ''
            AND officers_with_role_recency_ranks.CLSTTL IS NOT NULL)
    """

VIEW_QUERY_TEMPLATE = f"""
    WITH
    {NON_INVESTIGATION_SUPERVISION_SENTENCES_FRAGMENT},
    {OFFICERS_WITH_MOST_RECENT_ROLE_FRAGMENT},
    conditions_violated_cf AS (
    -- An updated version of TAK042 that only has one row per citation.
        SELECT
            conditions_cf.CF_DOC,
            conditions_cf.CF_CYC,
            conditions_cf.CF_VSN,
            STRING_AGG(DISTINCT conditions_cf.CF_VCV, ',' ORDER BY conditions_cf.CF_VCV) AS VIOLATED_CONDITIONS,
            MAX(COALESCE(conditions_cf.CF_DCR, '0')) as CREATE_DT,
            MAX(COALESCE(conditions_cf.CF_DLU, '0')) as UPDATE_DT
        FROM
            {{LBAKRDTA_TAK042}} AS conditions_cf
        GROUP BY
            conditions_cf.CF_DOC,
            conditions_cf.CF_CYC,
            conditions_cf.CF_VSN
    ),
    valid_sentences_cz AS (
    -- Only keeps rows in TAK076 which refer to either
    -- IncarcerationSentences or non-INV SupervisionSentences
        SELECT
            sentence_xref_with_probation_info_cz_bu.CZ_DOC,
            sentence_xref_with_probation_info_cz_bu.CZ_CYC,
            sentence_xref_with_probation_info_cz_bu.CZ_VSN,
            sentence_xref_with_probation_info_cz_bu.CZ_SEO,
            sentence_xref_with_probation_info_cz_bu.CZ_FSO,
            sentence_xref_with_probation_info_cz_bu.CZ_DCR,
            sentence_xref_with_probation_info_cz_bu.CZ_TCR,
            sentence_xref_with_probation_info_cz_bu.CZ_DLU,
            sentence_xref_with_probation_info_cz_bu.CZ_TLU
        FROM (
            SELECT
                *
            FROM
                {{LBAKRDTA_TAK076}} sentence_xref_cz
            LEFT JOIN
                non_investigation_supervision_sentences_bu
            ON
                sentence_xref_cz.CZ_DOC = non_investigation_supervision_sentences_bu.BU_DOC
                AND sentence_xref_cz.CZ_CYC = non_investigation_supervision_sentences_bu.BU_CYC
                AND sentence_xref_cz.CZ_SEO = non_investigation_supervision_sentences_bu.BU_SEO
                AND sentence_xref_cz.CZ_FSO = non_investigation_supervision_sentences_bu.BU_FSO
            WHERE sentence_xref_cz.CZ_FSO = '0' OR
                non_investigation_supervision_sentences_bu.BU_DOC IS NOT NULL
        ) sentence_xref_with_probation_info_cz_bu
    ),
    finally_formed_violations_e6 AS(
        -- Finally formed violation reports. As we've filtered for just
        -- violation reports, DOS in this table is equivalent to VSN in other
        -- tables.
        {FINALLY_FORMED_VIOLATIONS_E6})
    SELECT
        BY_DOC,BY_CYC,BY_VSN,BY_VE,BY_VWI,BY_VRT,BY_VSI,BY_VPH,BY_VBG,BY_VA,BY_VIC,BY_DAX,BY_VC,BY_VD,BY_VIH,BY_VIM,
        BY_VIL,BY_VOR,BY_PIN,BY_PLN,BY_PON,BY_RCA,BY_VTY,BY_DV,BY_UID,BY_UIU,VIOLATED_CONDITIONS,CZ_DOC,CZ_CYC,CZ_SEO,
        CZ_FSO,FINAL_FORMED_CREATE_DATE,FINAL_FORMED_UPDATE_DATE,BDGNO,CLSTTL,LNAME,FNAME,MINTL
    FROM
        {{LBAKRDTA_TAK028}} violation_reports_by
    LEFT JOIN
        conditions_violated_cf
    ON
        violation_reports_by.BY_DOC = conditions_violated_cf.CF_DOC
        AND violation_reports_by.BY_CYC = conditions_violated_cf.CF_CYC
        AND violation_reports_by.BY_VSN = conditions_violated_cf.CF_VSN
    JOIN
        valid_sentences_cz
    ON
        violation_reports_by.BY_DOC = valid_sentences_cz.CZ_DOC
        AND violation_reports_by.BY_CYC = valid_sentences_cz.CZ_CYC
        AND violation_reports_by.BY_VSN = valid_sentences_cz.CZ_VSN
    LEFT JOIN
        finally_formed_violations_e6
    ON
        violation_reports_by.BY_DOC = finally_formed_violations_e6.E6_DOC
        AND violation_reports_by.BY_CYC = finally_formed_violations_e6.E6_CYC
        AND violation_reports_by.BY_VSN = finally_formed_violations_e6.E6_DOS
    LEFT JOIN
        officers_with_recent_role
    ON
        violation_reports_by.BY_PON = officers_with_recent_role.BDGNO
    """

VIEW_BUILDER = DirectIngestPreProcessedIngestViewBuilder(
    region="us_mo",
    ingest_view_name="tak028_tak042_tak076_tak024_violation_reports",
    view_query_template=VIEW_QUERY_TEMPLATE,
    order_by_cols="BY_DOC, BY_CYC, BY_VSN",
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
