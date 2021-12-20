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
"""Query containing offender sentence term information."""

from recidiviz.ingest.direct.views.direct_ingest_big_query_view_types import (
    DirectIngestPreProcessedIngestViewBuilder,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

# TODO(#10389): Consider updating the exclusion of suspended sentences when we
#  redesign our sentencing model
VIEW_QUERY_TEMPLATE = """
SELECT * 
FROM {elite_offendersentenceterms}
-- We avoid bringing in "SUSPENDED" sentences that are placeholders in case someone's 
-- probation is revoked. We get no info about these sentences other than a length (
-- start/end dates are null).
WHERE SENTENCE_TERM_CODE != 'SUSP'
"""

VIEW_BUILDER = DirectIngestPreProcessedIngestViewBuilder(
    region="us_nd",
    ingest_view_name="elite_offendersentenceterms",
    view_query_template=VIEW_QUERY_TEMPLATE,
    order_by_cols="OFFENDER_BOOK_ID",
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
