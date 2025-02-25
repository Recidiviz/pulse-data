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
"""Query for transfer_to_supervision_from_incarceration state task deadlines."""
from recidiviz.ingest.direct.views.direct_ingest_view_query_builder import (
    DirectIngestViewQueryBuilder,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

VIEW_QUERY_TEMPLATE = """
     WITH
        SentenceBase AS (
        SELECT  
            sent.SentenceId, 
            sent.OffenderId,
            detail.SegmentPED,
            detail.update_datetime
        FROM {scl_Sentence} sent
            LEFT JOIN {scl_SentenceLink} link 
                ON sent.SentenceId = link.SentenceId
            LEFT JOIN {scl_SentenceLinkOffense} linkoffense 
                ON link.SentenceLinkId = linkoffense.SentenceLinkId
            LEFT JOIN {scl_Offense} off 
                ON linkoffense.OffenseId = off.OffenseId
            LEFT JOIN {scl_SentenceOrder} ord 
                ON off.SentenceOrderId = ord.SentenceOrderId
            LEFT JOIN {scl_SentenceOrderType} ord_type 
                ON ord.SentenceOrderTypeId = ord_type.SentenceOrderTypeId
            LEFT JOIN {scl_SentenceDetail@ALL} detail 
                ON sent.SentenceId = detail.SentenceId
        WHERE ord_type.SentenceOrderCategoryId = '2'
            AND ord.SentenceOrderEventTypeId IN ('1', '2', '3', '5')
            AND link.SentenceLinkClassId = '1'
        ),
    lag_cte AS (
        SELECT
            SentenceId,
            OffenderId, 
            SegmentPED,
            update_datetime,
            LAG(SegmentPED) OVER (PARTITION BY SentenceId ORDER BY update_datetime) AS PREV_SegmentPED,
            LAG(SentenceId) OVER (PARTITION BY SentenceID ORDER BY update_datetime) AS PREV_SentenceId
        FROM SentenceBase
    )
     SELECT DISTINCT
        SentenceId,
        OffenderId, 
        SegmentPED,
        update_datetime,
    FROM lag_cte
    WHERE (SegmentPED != PREV_SegmentPED  OR (PREV_SegmentPED IS NULL and SegmentPED IS NOT NULL)
    OR (SegmentPED IS NULL and PREV_SentenceId IS NULL)) AND update_datetime IS NOT NULL
"""
VIEW_BUILDER = DirectIngestViewQueryBuilder(
    region="us_ix",
    ingest_view_name="transfer_to_supervision_deadline",
    view_query_template=VIEW_QUERY_TEMPLATE,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
