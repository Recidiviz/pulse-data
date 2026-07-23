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
"""The Edovo earned-time credit-calculation consumer: pool stored completions and
hand the per-person totals to a ``PendingCreditSink``.

TODO(OBT-22951): wire this into a scheduled environment against the real eOMIS
sink. Nothing invokes it in production yet.
"""
from recidiviz.big_query.big_query_client import BigQueryClientImpl
from recidiviz.case_triage.edovo.credit_calculator import (
    EdovoCreditCalculation,
    calculate_all_pending_credits,
)
from recidiviz.case_triage.edovo.pending_credit_sink import PendingCreditSink
from recidiviz.persistence.database.session import Session


def run_credit_calculation(
    *,
    session: Session,
    bq_client: BigQueryClientImpl,
    sink: PendingCreditSink,
) -> EdovoCreditCalculation:
    """Pools stored completions into per-person credits, records them to |sink|, and
    returns the full calculation (including unresolved external ids)."""
    calculation = calculate_all_pending_credits(session=session, bq_client=bq_client)
    sink.record_pending_credits(calculation.person_credits)
    return calculation
