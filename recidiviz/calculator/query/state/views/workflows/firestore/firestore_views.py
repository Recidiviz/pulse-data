#  Recidiviz - a data platform for criminal justice reform
#  Copyright (C) 2025 Recidiviz, Inc.
#
#  This program is free software: you can redistribute it and/or modify
#  it under the terms of the GNU General Public License as published by
#  the Free Software Foundation, either version 3 of the License, or
#  (at your option) any later version.
#
#  This program is distributed in the hope that it will be useful,
#  but WITHOUT ANY WARRANTY; without even the implied warranty of
#  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#  GNU General Public License for more details.
#
#  You should have received a copy of the GNU General Public License
#  along with this program.  If not, see <https://www.gnu.org/licenses/>.
#  =============================================================================
"""Views to which will be exported to a GCP bucket to be ETL'd into Firestore."""
from typing import List

from recidiviz.big_query.big_query_view import BigQueryViewBuilder
from recidiviz.calculator.query.state.views.reference.workflows_opportunity_configs import (
    WORKFLOWS_OPPORTUNITY_CONFIGS,
)
from recidiviz.calculator.query.state.views.workflows.firestore.client_record import (
    CLIENT_RECORD_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.workflows.firestore.incarceration_staff_record import (
    INCARCERATION_STAFF_RECORD_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.workflows.firestore.location_record import (
    LOCATION_RECORD_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.workflows.firestore.resident_record import (
    RESIDENT_RECORD_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.workflows.firestore.supervision_staff_record import (
    SUPERVISION_STAFF_RECORD_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.workflows.firestore.us_ix_supervision_tasks_record import (
    US_IX_SUPERVISION_TASKS_RECORD_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.workflows.firestore.us_ix_transfer_to_limited_supervision_jii_record import (
    US_IX_TRANSFER_TO_LIMITED_SUPERVISION_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.workflows.firestore.us_nd_supervision_tasks_record import (
    US_ND_SUPERVISION_TASKS_RECORD_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.workflows.firestore.us_tx_supervision_tasks_record import (
    US_TX_SUPERVISION_TASKS_RECORD_VIEW_BUILDER,
)

FIRESTORE_VIEW_BUILDERS: List[BigQueryViewBuilder] = [
    *[config.view_builder for config in WORKFLOWS_OPPORTUNITY_CONFIGS],
    CLIENT_RECORD_VIEW_BUILDER,
    RESIDENT_RECORD_VIEW_BUILDER,
    INCARCERATION_STAFF_RECORD_VIEW_BUILDER,
    SUPERVISION_STAFF_RECORD_VIEW_BUILDER,
    LOCATION_RECORD_VIEW_BUILDER,
    US_IX_SUPERVISION_TASKS_RECORD_VIEW_BUILDER,
    US_IX_TRANSFER_TO_LIMITED_SUPERVISION_VIEW_BUILDER,  # Not currently configured as a workflow
    US_ND_SUPERVISION_TASKS_RECORD_VIEW_BUILDER,
    US_TX_SUPERVISION_TASKS_RECORD_VIEW_BUILDER,
    # ...add any other non-workflow Firestore view builders here
]
