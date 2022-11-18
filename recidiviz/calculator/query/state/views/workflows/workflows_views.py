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
from recidiviz.calculator.query.state.views.workflows.client_record_archive import (
    CLIENT_RECORD_ARCHIVE_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.workflows.clients_latest_referral_status import (
    CLIENTS_LATEST_REFERRAL_STATUS_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.workflows.clients_latest_referral_status_extended import (
    CLIENTS_LATEST_REFERRAL_STATUS_EXTENDED_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.workflows.clients_opportunity_previewed import (
    CLIENTS_OPPORTUNITY_PREVIEWED_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.workflows.clients_referral_completed import (
    CLIENTS_REFERRAL_COMPLETED_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.workflows.clients_referral_form_viewed import (
    CLIENTS_REFERRAL_FORM_VIEWED_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.workflows.clients_referral_implemented import (
    CLIENTS_REFERRAL_IMPLEMENTED_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.workflows.clients_surfaced import (
    CLIENTS_SURFACED_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.workflows.compliant_reporting_referral_record_archive import (
    COMPLIANT_REPORTING_REFERRAL_RECORD_ARCHIVE_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.workflows.firestore.firestore_views import (
    FIRESTORE_VIEW_BUILDERS,
)
from recidiviz.calculator.query.state.views.workflows.person_id_to_external_id import (
    PERSON_ID_TO_EXTERNAL_ID_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.workflows.reidentified_dashboard_users import (
    REIDENTIFIED_DASHBOARD_USERS_VIEW_BUILDER,
)

WORKFLOWS_VIEW_BUILDERS: List[BigQueryViewBuilder] = [
    *FIRESTORE_VIEW_BUILDERS,
    PERSON_ID_TO_EXTERNAL_ID_VIEW_BUILDER,
    COMPLIANT_REPORTING_REFERRAL_RECORD_ARCHIVE_VIEW_BUILDER,
    CLIENT_RECORD_ARCHIVE_VIEW_BUILDER,
    CLIENTS_SURFACED_VIEW_BUILDER,
    CLIENTS_OPPORTUNITY_PREVIEWED_VIEW_BUILDER,
    CLIENTS_REFERRAL_FORM_VIEWED_VIEW_BUILDER,
    CLIENTS_LATEST_REFERRAL_STATUS_VIEW_BUILDER,
    CLIENTS_LATEST_REFERRAL_STATUS_EXTENDED_VIEW_BUILDER,
    CLIENTS_REFERRAL_COMPLETED_VIEW_BUILDER,
    CLIENTS_REFERRAL_IMPLEMENTED_VIEW_BUILDER,
    REIDENTIFIED_DASHBOARD_USERS_VIEW_BUILDER,
]
