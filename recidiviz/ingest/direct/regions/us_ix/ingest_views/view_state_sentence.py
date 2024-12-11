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
"""Query for state sentences for US_IX."""
from recidiviz.ingest.direct.regions.us_ix.ingest_views.template_sentence import (
    new_sentence_view_template,
)
from recidiviz.ingest.direct.views.direct_ingest_view_query_builder import (
    DirectIngestViewQueryBuilder,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

sentences_base = new_sentence_view_template()

# TODO(#32140) Update state_sentence view in US_IX so that all consecutive sentence exist.
VIEW_QUERY_TEMPLATE = f"""
    WITH
        {sentences_base}
        SELECT
            SentenceId,
            OffenderId, 
            CountyId,
            SentenceDate,
            CorrectionsCompactEndDate,
            CorrectionsCompactStartDate,
            TermId,
            OffenseSentenceTypeName,
            SentenceOrderTypeId,
            inState
        FROM final_sentences
        WHERE SentenceOrderEventTypeId IN ('1', '2', '3', '5') -- keep "Initial", "Amendment", and "Error Correction" sentences
        -- Enforces that there is always a start date to a sentence
        AND (SentenceDate IS NOT NULL OR CorrectionsCompactStartDate IS NOT NULL)
"""
VIEW_BUILDER = DirectIngestViewQueryBuilder(
    region="us_ix",
    ingest_view_name="state_sentence",
    view_query_template=VIEW_QUERY_TEMPLATE,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
