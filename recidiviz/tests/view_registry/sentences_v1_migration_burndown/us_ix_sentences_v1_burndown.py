# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2026 Recidiviz, Inc.
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
"""US_IX exemptions for deprecated sentence v1 view references in product views."""

from recidiviz.big_query.big_query_address import BigQueryAddress
from recidiviz.calculator.query.state.views.sentencing.case_disposition import (
    SENTENCING_CASE_DISPOSITION_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.sentencing.us_ix.us_ix_sentencing_case_disposition_preprocessed import (
    US_IX_SENTENCING_CASE_DISPOSITION_PREPROCESSED_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.sessions.compartment_sessions_closest_sentence_imposed_group import (
    COMPARTMENT_SESSIONS_CLOSEST_SENTENCE_IMPOSED_GROUP_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.sessions.sentence_imposed_group_summary import (
    SENTENCE_IMPOSED_GROUP_SUMMARY_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.sessions.sentences_preprocessed import (
    SENTENCES_PREPROCESSED_VIEW_BUILDER,
)

# For each US_IX metric export, for each product view in that export, a mapping of
#   deprecated v1 sentences views that are a) referenced directly without going through
#   sentence_sessions/sentence_sessions_v2_all and are b) part of the ancestor graph of
#   this product view, with all places they are referenced.
US_IX_SENTENCE_V1_PRODUCT_USAGE_EXEMPTIONS: dict[
    str, dict[BigQueryAddress, dict[BigQueryAddress, set[BigQueryAddress]]]
] = {
    "SENTENCING": {
        SENTENCING_CASE_DISPOSITION_VIEW_BUILDER.address: {
            COMPARTMENT_SESSIONS_CLOSEST_SENTENCE_IMPOSED_GROUP_VIEW_BUILDER.address: {
                US_IX_SENTENCING_CASE_DISPOSITION_PREPROCESSED_VIEW_BUILDER.address,
            },
            SENTENCE_IMPOSED_GROUP_SUMMARY_VIEW_BUILDER.address: {
                US_IX_SENTENCING_CASE_DISPOSITION_PREPROCESSED_VIEW_BUILDER.address,
            },
            SENTENCES_PREPROCESSED_VIEW_BUILDER.address: {
                US_IX_SENTENCING_CASE_DISPOSITION_PREPROCESSED_VIEW_BUILDER.address,
            },
        },
    },
}
