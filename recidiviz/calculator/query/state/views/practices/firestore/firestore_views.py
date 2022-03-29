#  Recidiviz - a data platform for criminal justice reform
#  Copyright (C) 2022 Recidiviz, Inc.
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
from recidiviz.calculator.query.state.views.practices.firestore.client_record import (
    CLIENT_RECORD_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.practices.firestore.compliant_reporting_referral_record import (
    COMPLIANT_REPORTING_REFERRAL_RECORD_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.practices.firestore.staff_record import (
    STAFF_RECORD_VIEW_BUILDER,
)

FIRESTORE_VIEW_BUILDERS: List[BigQueryViewBuilder] = [
    CLIENT_RECORD_VIEW_BUILDER,
    STAFF_RECORD_VIEW_BUILDER,
    COMPLIANT_REPORTING_REFERRAL_RECORD_VIEW_BUILDER,
]
