# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2021 Recidiviz, Inc.
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
"""Query containing incarceration period information extracted from the output of the `OffenderMovementSharedQuery`."""

from recidiviz.ingest.direct.regions.us_tn.ingest_views.OffenderMovementSharedQuery import (
    ALL_PERIODS_QUERY,
)
from recidiviz.ingest.direct.views.direct_ingest_big_query_view_types import (
    DirectIngestPreProcessedIngestViewBuilder,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

VIEW_QUERY_TEMPLATE = f"""
WITH all_periods AS ( {ALL_PERIODS_QUERY} )
SELECT 
    OffenderID,
    StartDateTime,
    EndDateTime,
    Site,
    StartMovementType,
    StartMovementReason,
    EndMovementType,
    EndMovementReason,
    ROW_NUMBER() OVER (PARTITION BY OffenderID ORDER BY MovementSequenceNumber ASC) AS IncarcerationSequenceNumber
FROM all_periods
WHERE InferredPeriodType = 'INCARCERATION'
"""

VIEW_BUILDER = DirectIngestPreProcessedIngestViewBuilder(
    region="us_tn",
    ingest_view_name="OffenderMovementIncarcerationPeriod",
    view_query_template=VIEW_QUERY_TEMPLATE,
    order_by_cols="OffenderID ASC",
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
