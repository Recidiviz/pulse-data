# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2022 Recidiviz, Inc.
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
"""Query containing charge and court information from the elite tables."""

from recidiviz.ingest.direct.views.direct_ingest_big_query_view_types import (
    DirectIngestPreProcessedIngestViewBuilder,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

VIEW_QUERY_TEMPLATE = """
    SELECT
        OFFENDER_BOOK_ID,
        ORDER_ID,
        charges.CHARGE_SEQ,
        charges.CHARGE_STATUS,
        charges.INITIAL_COUNTS,
        charges.OFFENCE_CODE,
        charges.OFFENCE_TYPE,
        charges.OFFENSE_DATE,
        charges.COMMENT_TEXT,
        codes.description as CODE_DESCRIPTION, 
        codes.severity_ranking as SEVERITY_RANKING,
        orders.CONVICTION_DATE,
        orders.COUNTY_CODE,
        orders.COURT_DATE,
        orders.JUDGE_NAME,
        orders.ORDER_STATUS,
        orders.ISSUING_AGY_LOC_ID
    FROM {elite_offenderchargestable} charges
    LEFT JOIN {elite_orderstable} orders
    USING (OFFENDER_BOOK_ID, ORDER_ID)
    LEFT JOIN {RECIDIVIZ_REFERENCE_offense_codes} codes
    USING (OFFENCE_CODE)
"""

VIEW_BUILDER = DirectIngestPreProcessedIngestViewBuilder(
    region="us_nd",
    ingest_view_name="elite_state_charges",
    view_query_template=VIEW_QUERY_TEMPLATE,
    order_by_cols="OFFENDER_BOOK_ID, ORDER_ID",
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
