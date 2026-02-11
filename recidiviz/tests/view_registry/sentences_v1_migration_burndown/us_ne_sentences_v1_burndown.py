# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2025 Recidiviz, Inc.
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
"""US_NE exemptions for deprecated sentence v1 view references in product views."""

from recidiviz.big_query.big_query_address import BigQueryAddress
from recidiviz.calculator.query.state.views.meetings.clients import (
    MEETINGS_CLIENTS_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.platform_data_for_cpa.views_for_export.jii_data import (
    JII_CPA_DATA_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.sessions.supervision_projected_completion_date_spans import (
    SUPERVISION_PROJECTED_COMPLETION_DATE_SPANS_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.workflows.firestore.client_record import (
    CLIENT_RECORD_VIEW_BUILDER,
)

# For each US_NE metric export, for each product view in that export, a mapping of
#   deprecated v1 sentences views that are a) referenced directly without going through
#   sentence_sessions/sentence_sessions_v2_all and are b) part of the ancestor graph of
#   this product view, with all places they are referenced.
US_NE_SENTENCE_V1_PRODUCT_USAGE_EXEMPTIONS: dict[
    str, dict[BigQueryAddress, dict[BigQueryAddress, set[BigQueryAddress]]]
] = {
    "WORKFLOWS_FIRESTORE": {
        CLIENT_RECORD_VIEW_BUILDER.address: {
            SUPERVISION_PROJECTED_COMPLETION_DATE_SPANS_VIEW_BUILDER.address: {
                CLIENT_RECORD_VIEW_BUILDER.address,
            },
        },
    },
    "MEETINGS": {
        # Meetings client view doesn't pull sentence information, but does pull other information
        # from the workflows client record for simplicity.
        MEETINGS_CLIENTS_VIEW_BUILDER.address: {
            SUPERVISION_PROJECTED_COMPLETION_DATE_SPANS_VIEW_BUILDER.address: {
                CLIENT_RECORD_VIEW_BUILDER.address,
            }
        }
    },
    "CPA": {
        JII_CPA_DATA_VIEW_BUILDER.address: {
            SUPERVISION_PROJECTED_COMPLETION_DATE_SPANS_VIEW_BUILDER.address: {
                CLIENT_RECORD_VIEW_BUILDER.address,
            },
        },
    },
}
