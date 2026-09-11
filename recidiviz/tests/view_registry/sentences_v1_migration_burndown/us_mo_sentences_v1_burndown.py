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
"""US_MO exemptions for deprecated sentence v1 view references in product views."""

from recidiviz.big_query.big_query_address import BigQueryAddress
from recidiviz.calculator.query.state.views.sentencing.case_disposition import (
    SENTENCING_CASE_DISPOSITION_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.sentencing.sentence_cohort import (
    SENTENCE_COHORT_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.sessions.compartment_sessions_closest_sentence_imposed_group import (
    COMPARTMENT_SESSIONS_CLOSEST_SENTENCE_IMPOSED_GROUP_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.sessions.sentence_imposed_group_summary import (
    SENTENCE_IMPOSED_GROUP_SUMMARY_VIEW_BUILDER,
)

# US_MO's sentencing_case_disposition doesn't reference v1 sentence views directly.
# It reaches them through the shared sentence_cohort view, which still uses v1 data
# for US_IX. These edges can be removed once US_IX migrates to v2 infra.
# TODO(#33402): Remove once US_IX is migrated to v2 sentences infra.
US_MO_SENTENCE_V1_PRODUCT_USAGE_EXEMPTIONS: dict[
    str, dict[BigQueryAddress, dict[BigQueryAddress, set[BigQueryAddress]]]
] = {
    "SENTENCING": {
        SENTENCING_CASE_DISPOSITION_VIEW_BUILDER.address: {
            COMPARTMENT_SESSIONS_CLOSEST_SENTENCE_IMPOSED_GROUP_VIEW_BUILDER.address: {
                SENTENCE_COHORT_VIEW_BUILDER.address,
            },
            SENTENCE_IMPOSED_GROUP_SUMMARY_VIEW_BUILDER.address: {
                SENTENCE_COHORT_VIEW_BUILDER.address,
            },
        },
    },
}
