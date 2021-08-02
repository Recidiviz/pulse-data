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
"""Configuration for views in externally_shared_views."""

from typing import Sequence

from recidiviz.big_query.big_query_view import BigQueryViewBuilder
from recidiviz.calculator.query.externally_shared_views.views.sessions import (
    PARTNER_SHARED_SESSIONS_VIEW_BUILDERS,
)
from recidiviz.calculator.query.externally_shared_views.views.state_person_external_id import (
    PARTNER_SHARED_STATE_PERSON_EXTERNAL_ID_VIEW_BUILDERS,
)

VIEW_BUILDERS_FOR_VIEWS_TO_UPDATE: Sequence[BigQueryViewBuilder] = (
    PARTNER_SHARED_SESSIONS_VIEW_BUILDERS
    + PARTNER_SHARED_STATE_PERSON_EXTERNAL_ID_VIEW_BUILDERS
)
