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
"""Reference views used by other views."""
from typing import List

from recidiviz.big_query.big_query_view import BigQueryViewBuilder
from recidiviz.calculator.query.state.views.practices.client_record_archive import (
    CLIENT_RECORD_ARCHIVE_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.practices.clients_latest_referral_status import (
    CLIENTS_LATEST_REFERRAL_STATUS_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.practices.clients_referral_completed import (
    CLIENTS_REFERRAL_COMPLETED_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.practices.clients_referral_form_viewed import (
    CLIENTS_REFERRAL_FORM_VIEWED_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.practices.clients_surfaced import (
    CLIENTS_SURFACED_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.practices.firestore.firestore_views import (
    FIRESTORE_VIEW_BUILDERS,
)

PRACTICES_VIEW_BUILDERS: List[BigQueryViewBuilder] = [
    *FIRESTORE_VIEW_BUILDERS,
    CLIENT_RECORD_ARCHIVE_VIEW_BUILDER,
    CLIENTS_SURFACED_VIEW_BUILDER,
    CLIENTS_REFERRAL_FORM_VIEWED_VIEW_BUILDER,
    CLIENTS_LATEST_REFERRAL_STATUS_VIEW_BUILDER,
    CLIENTS_REFERRAL_COMPLETED_VIEW_BUILDER,
]
