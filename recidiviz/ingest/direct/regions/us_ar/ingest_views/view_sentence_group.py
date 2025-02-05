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
"""Query containing sentence group information."""

from recidiviz.ingest.direct.regions.us_ar.ingest_views.us_ar_view_query_fragments import (
    ALL_SENTENCES_FRAGMENT,
)
from recidiviz.ingest.direct.views.direct_ingest_view_query_builder import (
    DirectIngestViewQueryBuilder,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

VIEW_QUERY_TEMPLATE = f"""
WITH 
-- This view query uses the same data (ALL_SENTENCES_FRAGMENT) as the sentence view,
-- but uses a SELECT DISTINCT to get the unique combinations of OFFENDERID, COMMITMENTPREFIX,
-- and sentence_category. It's important to include sentence_category here so that if a commitment
-- includes components with prison time followed by supervision, or separate prison and supervision
-- components, the prison components and supervision components each get their own sentence group.
{ALL_SENTENCES_FRAGMENT}
SELECT DISTINCT 
    OFFENDERID,
    COMMITMENTPREFIX,
    sentence_category
FROM all_sentences_with_parents
"""

VIEW_BUILDER = DirectIngestViewQueryBuilder(
    region="us_ar",
    ingest_view_name="sentence_group",
    view_query_template=VIEW_QUERY_TEMPLATE,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
