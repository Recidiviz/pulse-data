# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2024 Recidiviz, Inc.
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
"""Query for incarceration sentences v2."""
from recidiviz.ingest.direct.regions.us_ix.ingest_views.template_sentence import (
    sentence_view_template,
)
from recidiviz.ingest.direct.views.direct_ingest_view_query_builder import (
    DirectIngestViewQueryBuilder,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

sentences_base = sentence_view_template()

VIEW_QUERY_TEMPLATE = f"""
    WITH
        {sentences_base}
        SELECT
            SentenceId,
            OffenderId, 
            CountyId,
            SentenceDate,
            EffectiveDate,
            DpedApprovedDate,
            SentenceOrderEventTypeName,
            Sequence,
            ChargeId,
            FtrdApprovedDate,
            CorrectionsCompactEndDate,
            relationships,
            SegmentMaxYears,
            SegmentMaxMonths,
            SegmentMaxDays,
            SegmentPED,
            SegmentSatisfactionDate,
            SegmentStartDate,
            SegmentEndDate,
            SegmentYears,
            SegmentMonths,
            SegmentDays,
            OffenseSentenceTypeId,
            SentenceStatusId,
            OffenseSortingOrder
        FROM final_sentences
        WHERE SentenceOrderCategoryId = '2'
        AND SentenceOrderEventTypeId IN ('1', '2', '3', '5') -- keep "Initial", "Amendment", and "Error Correction" sentences
"""
VIEW_BUILDER = DirectIngestViewQueryBuilder(
    region="us_ix",
    ingest_view_name="incarceration_sentence_v2",
    view_query_template=VIEW_QUERY_TEMPLATE,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
