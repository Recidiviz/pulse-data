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
"""Query containing incarceration period from sentence information."""

from recidiviz.ingest.direct.views.direct_ingest_big_query_view_types import (
    DirectIngestPreProcessedIngestViewBuilder,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override
from recidiviz.ingest.direct.regions.us_mo.ingest_views.us_mo_view_query_fragments import (
    INCARCERATION_SUB_SUBCYCLE_SPANS_FRAGMENT,
    STATUSES_BY_DATE_FRAGMENT,
    MOST_RECENT_STATUS_UPDATES_FRAGMENT,
)

VIEW_QUERY_TEMPLATE = f"""
    WITH {INCARCERATION_SUB_SUBCYCLE_SPANS_FRAGMENT},
    {STATUSES_BY_DATE_FRAGMENT},
    {MOST_RECENT_STATUS_UPDATES_FRAGMENT},
    incarceration_subcycle_from_incarceration_sentence AS (
        SELECT
            sentence_inst_ids.BT_DOC,
            sentence_inst_ids.BT_CYC,
            sentence_inst_ids.BT_SEO,
            body_status_f1.* EXCEPT(F1_TCR, F1_DCR)
        FROM (
            SELECT BT_DOC, BT_CYC, BT_SEO
            FROM {{LBAKRDTA_TAK023}} sentence_inst_bt
            GROUP BY BT_DOC, BT_CYC, BT_SEO
        ) sentence_inst_ids
        LEFT OUTER JOIN
            {{LBAKRDTA_TAK158}} body_status_f1
        ON
            sentence_inst_ids.BT_DOC = body_status_f1.F1_DOC AND
            sentence_inst_ids.BT_CYC = body_status_f1.F1_CYC AND
            sentence_inst_ids.BT_SEO = body_status_f1.F1_SEO
        WHERE body_status_f1.F1_DOC IS NOT NULL
            AND body_status_f1.F1_SST = 'I'
    ),
    incarceration_periods_from_incarceration_sentence AS (
        SELECT *
        FROM
            incarceration_subcycle_from_incarceration_sentence
        LEFT OUTER JOIN
            sub_subcycle_spans
        ON
            incarceration_subcycle_from_incarceration_sentence.F1_DOC = sub_subcycle_spans.DOC AND
            incarceration_subcycle_from_incarceration_sentence.F1_CYC = sub_subcycle_spans.CYC AND
            incarceration_subcycle_from_incarceration_sentence.F1_SQN = sub_subcycle_spans.SQN
        )
    SELECT
        incarceration_periods_from_incarceration_sentence.* EXCEPT(SQN),
        ROW_NUMBER() OVER (
            PARTITION BY DOC, CYC 
            ORDER BY SUB_SUBCYCLE_START_DT, SUB_SUBCYCLE_END_DT, SQN
        ) AS SQN,
        start_codes.STATUS_CODES AS START_SCD_CODES,
        end_codes.STATUS_CODES AS END_SCD_CODES,
        most_recent_status_updates.MOST_RECENT_SENTENCE_STATUS_DATE 
    FROM
        incarceration_periods_from_incarceration_sentence
    LEFT OUTER JOIN
        all_scd_codes_by_date start_codes
    ON
        incarceration_periods_from_incarceration_sentence.F1_DOC = start_codes.BW_DOC AND
        incarceration_periods_from_incarceration_sentence.F1_CYC = start_codes.BW_CYC AND
        incarceration_periods_from_incarceration_sentence.SUB_SUBCYCLE_START_DT = start_codes.STATUS_DATE
    LEFT OUTER JOIN
        all_scd_codes_by_date end_codes
    ON
        incarceration_periods_from_incarceration_sentence.F1_DOC = end_codes.BW_DOC AND
        incarceration_periods_from_incarceration_sentence.F1_CYC = end_codes.BW_CYC AND
        incarceration_periods_from_incarceration_sentence.SUB_SUBCYCLE_END_DT = end_codes.STATUS_DATE
    LEFT OUTER JOIN
      most_recent_status_updates
    ON
        incarceration_periods_from_incarceration_sentence.F1_DOC = most_recent_status_updates.BW_DOC AND
        incarceration_periods_from_incarceration_sentence.F1_CYC = most_recent_status_updates.BW_CYC 
    """

VIEW_BUILDER = DirectIngestPreProcessedIngestViewBuilder(
    region="us_mo",
    ingest_view_name="tak158_tak023_tak026_incarceration_period_from_incarceration_sentence",
    view_query_template=VIEW_QUERY_TEMPLATE,
    order_by_cols="BT_DOC, BT_CYC, BT_SEO, F1_SQN",
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
