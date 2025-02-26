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
"""Query for supervision sentences."""
from recidiviz.ingest.direct.regions.us_ix.ingest_views.template_sentence import (
    sentence_view_template,
)
from recidiviz.ingest.direct.views.direct_ingest_big_query_view_types import (
    DirectIngestPreProcessedIngestViewBuilder,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

sentences_base = sentence_view_template()

supervision_addon_CTE = """
    SELECT 
        SentenceOrderId,
        EndDate
    FROM {scl_ProbationSupervision}
"""

VIEW_QUERY_TEMPLATE = f"""
    WITH ProbationSupervision as ({supervision_addon_CTE})
    SELECT
        SentenceId, 
        OffenderId, 
        CountyId,
        (DATE(EffectiveDate)) as EffectiveDate, 
        (DATE(SentenceDate)) as SentenceDate,
        SentenceOrderTypeCode,
        (DATE(EndDate)) as EndDate,
        TermStatusDesc,
        SentenceOrderEventTypeName,
        _parentsentenceid,
        Sequence,
        ChargeId
    FROM ({sentences_base}) sent
    LEFT JOIN ProbationSupervision prob_sup ON sent.SentenceOrderId = prob_sup.SentenceOrderId
    WHERE SentenceOrderCategoryId = '1'
    AND SentenceOrderEventTypeId IN ('1', '2', '5') -- keep "Initial", "Amendment", and "Linked Event" sentences
"""
VIEW_BUILDER = DirectIngestPreProcessedIngestViewBuilder(
    region="us_ix",
    ingest_view_name="supervision_sentence",
    view_query_template=VIEW_QUERY_TEMPLATE,
    order_by_cols="OffenderId, SentenceId",
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
