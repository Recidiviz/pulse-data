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
"""Query containing citation information."""

from recidiviz.ingest.direct.views.direct_ingest_big_query_view_types import \
    DirectIngestPreProcessedIngestViewBuilder
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override
from recidiviz.ingest.direct.regions.us_mo.ingest_views.us_mo_view_query_fragments import \
    NON_INVESTIGATION_SUPERVISION_SENTENCES_FRAGMENT, TAK142_FINALLY_FORMED_DOCUMENT_FRAGMENT

FINALLY_FORMED_CITATIONS_E6 = \
    TAK142_FINALLY_FORMED_DOCUMENT_FRAGMENT.format(document_type_code='XIT')

VIEW_QUERY_TEMPLATE = f"""
    WITH
    {NON_INVESTIGATION_SUPERVISION_SENTENCES_FRAGMENT},
    valid_sentences_js AS (
    -- Only keeps rows in TAK291 which refer to either
    -- IncarcerationSentences or non-INV SupervisionSentences
        SELECT
            sentence_xref_with_probation_info_js_bu.JS_DOC,
            sentence_xref_with_probation_info_js_bu.JS_CYC,
            sentence_xref_with_probation_info_js_bu.JS_CSQ,
            sentence_xref_with_probation_info_js_bu.JS_SEO,
            sentence_xref_with_probation_info_js_bu.JS_FSO,
            sentence_xref_with_probation_info_js_bu.JS_DCR,
            sentence_xref_with_probation_info_js_bu.JS_DLU
        FROM (
            SELECT
                *
            FROM
                {{LBAKRDTA_TAK291}} sentence_xref_js
            LEFT JOIN
                non_investigation_supervision_sentences_bu
            ON
                sentence_xref_js.JS_DOC = non_investigation_supervision_sentences_bu.BU_DOC
                AND sentence_xref_js.JS_CYC = non_investigation_supervision_sentences_bu.BU_CYC
                AND sentence_xref_js.JS_SEO = non_investigation_supervision_sentences_bu.BU_SEO
                AND sentence_xref_js.JS_FSO = non_investigation_supervision_sentences_bu.BU_FSO
            WHERE sentence_xref_js.JS_FSO = '0' OR
                non_investigation_supervision_sentences_bu.BU_DOC IS NOT NULL
        ) sentence_xref_with_probation_info_js_bu
    ),
    citations_with_multiple_violations_jt AS (
    -- An updated version of TAK292 that only has one row per citation.
        SELECT
            citations_jt.JT_DOC,
            citations_jt.JT_CYC,
            citations_jt.JT_CSQ,
            STRING_AGG(DISTINCT citations_jt.JT_VCV, ',' ORDER BY citations_jt.JT_VCV) AS VIOLATED_CONDITIONS,
            MAX(COALESCE(citations_jt.JT_VG, '0')) AS MAX_DATE
        FROM
            {{LBAKRDTA_TAK292}} citations_jt
        GROUP BY
            citations_jt.JT_DOC,
            citations_jt.JT_CYC,
            citations_jt.JT_CSQ
    ),
    finally_formed_citations_e6 AS(
        -- Finally formed citations. As we've filtered for just citations
        -- DOS in this table is equivalent to CSQ in other tables.
        {FINALLY_FORMED_CITATIONS_E6})
    SELECT
        JT_DOC,JT_CYC,JT_CSQ,VIOLATED_CONDITIONS,MAX_DATE,JS_DOC,JS_CYC,JS_SEO,JS_FSO,FINAL_FORMED_CREATE_DATE
    FROM
        citations_with_multiple_violations_jt
    JOIN
        valid_sentences_js
    ON
        citations_with_multiple_violations_jt.JT_DOC = valid_sentences_js.JS_DOC
        AND citations_with_multiple_violations_jt.JT_CYC = valid_sentences_js.JS_CYC
        AND citations_with_multiple_violations_jt.JT_CSQ = valid_sentences_js.JS_CSQ
    LEFT JOIN
        finally_formed_citations_e6
    ON
        citations_with_multiple_violations_jt.JT_DOC = finally_formed_citations_e6.E6_DOC
        AND citations_with_multiple_violations_jt.JT_CYC = finally_formed_citations_e6.E6_CYC
        AND citations_with_multiple_violations_jt.JT_CSQ = finally_formed_citations_e6.E6_DOS
"""

VIEW_BUILDER = DirectIngestPreProcessedIngestViewBuilder(
    region='us_mo',
    ingest_view_name='tak291_tak292_tak024_citations',
    view_query_template=VIEW_QUERY_TEMPLATE,
    order_by_cols='JT_DOC, JT_CYC, JT_CSQ',
)

if __name__ == '__main__':
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
