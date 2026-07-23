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
"""Destination seam for the computed per-person credit totals — the eOMIS handoff.

The concrete destination (ledger/sync CDOC reconciles in eOMIS) is owned by
OBT-22951; only an in-memory impl exists for tests. The real sink owns
handoff-level idempotency (not re-posting credits already sent); the calculation
itself is idempotent — see ``credit_calculator``.
"""
from abc import ABC, abstractmethod

from recidiviz.case_triage.edovo.credit_calculator import PersonCreditResult


class PendingCreditSink(ABC):
    """Destination for the pooled per-person credit totals produced by calculation."""

    @abstractmethod
    def record_pending_credits(self, results: list[PersonCreditResult]) -> None:
        """Hands the full set of per-person totals to the destination, once per run."""


class InMemoryPendingCreditSink(PendingCreditSink):
    """Retains the most recent run's results in memory, for tests."""

    def __init__(self) -> None:
        self.latest_results: list[PersonCreditResult] = []
        self.run_count: int = 0

    def record_pending_credits(self, results: list[PersonCreditResult]) -> None:
        self.latest_results = list(results)
        self.run_count += 1
