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
"""The contract between the shared eOMIS writeback runner and per-state flows.

A flow is one writeback use case (e.g. AR GED program referrals, CO sentence
credits): it knows which source to read candidates from, which eOMIS screens
to drive, which payloads to post, and how to verify the result. Everything a
flow does NOT have to implement — login, sessions, pacing, circuit breakers,
audit recording — lives in the shared runner and client.
"""
from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass
from enum import Enum
from typing import Generic, TypeVar

from recidiviz.common.constants.states import StateCode
from recidiviz.tools.eomis.client import EomisClient

# Shared action labels with runner-visible meaning. All other action labels
# are flow-defined.
SKIP_ACTION = "skip"
READ_ACTION = "read"


class ResultStatus(Enum):
    READ = "read"
    DRY_RUN = "dry-run"
    SUCCESS = "success"
    SKIPPED = "skipped"
    ERROR = "error"


@dataclass(frozen=True)
class Candidate:
    """One unit of work: a person whose eOMIS records may need a write."""

    offender_id: str
    action: str
    reason: str

    @property
    def is_actionable(self) -> bool:
        return self.action != SKIP_ACTION

    def display_fields(self) -> dict[str, str]:
        """Extra flow-specific columns for the run-plan table, in display
        order."""
        return {}


@dataclass(frozen=True)
class WriteResult:
    candidate: Candidate
    status: ResultStatus
    detail: str


CandidateT = TypeVar("CandidateT", bound=Candidate)


class EomisWritebackFlow(ABC, Generic[CandidateT]):
    """A single eOMIS writeback use case.

    Contract for implementations:
      - run_candidate must read the current eOMIS state before writing, so a
        re-run of the same candidates becomes skips (idempotency).
      - SUCCESS means the write was verified by reading the record back —
        never that a POST returned HTTP 200. eOMIS can return 200 on
        application errors.
    """

    @property
    @abstractmethod
    def state_code(self) -> StateCode:
        """The state this flow writes to. Creds and audit rows are scoped to
        this — never shared across states."""

    @property
    @abstractmethod
    def flow_name(self) -> str:
        """Human-readable name used in run output and audit records."""

    @property
    @abstractmethod
    def max_writes_per_run(self) -> int:
        """Volume circuit breaker: the runner refuses to start a commit run
        with more actionable candidates than this, so a bad source-view
        refresh cannot mass-write into a state system."""

    @abstractmethod
    def load_candidates(self) -> list[CandidateT]:
        """Loads and classifies candidates from this flow's production
        source."""

    @abstractmethod
    def run_candidate(
        self, client: EomisClient, candidate: CandidateT, *, commit: bool
    ) -> WriteResult:
        """Reads current eOMIS state for one candidate and, when commit is
        set, performs and verifies the write."""
