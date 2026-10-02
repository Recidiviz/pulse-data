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
"""Tests for the import's stale-candidate cleanup. retire_stale_candidates is
called directly with candidate rows inserted at known detection times; the
import-level step that runs it after every pass is covered in
import_processor_test.py."""
import datetime
import uuid

import pytest

from recidiviz.common.constants.identity import CandidateStatus
from recidiviz.common.constants.tenants import Tenant
from recidiviz.persistence.database.schema.identity import schema
from recidiviz.persistence.database.session_factory import SessionFactory
from recidiviz.services.identity.import_processing.stale_candidate_cleanup_pass import (
    retire_stale_candidates,
)
from recidiviz.tests.services.identity.test_utils import (
    IdentityDatabaseTestCase,
    insert_identity,
    insert_merge_candidate,
    insert_split_candidate,
    insert_update_attribute_candidate,
)

# A prior run's detection time, before this run started. A candidate stamped
# with it was not re-detected this run.
_STALE_TIME = datetime.datetime(2026, 9, 15, 12, 0, tzinfo=datetime.timezone.utc)
# This run's start time, and so the detected_at_utc a flagger stamps on a
# candidate it files or refreshes this run.
_RUN_TIME = datetime.datetime(2026, 9, 16, 12, 0, tzinfo=datetime.timezone.utc)


@pytest.mark.uses_db
class RetireStaleCandidatesTest(IdentityDatabaseTestCase):
    """Tests for retire_stale_candidates: PENDING candidates not re-detected
    this run are marked STALE, while re-detected, resolved, already-stale, and
    other-tenant candidates are left alone."""

    def _retire(self, *, tenant: Tenant = Tenant.US_OZ) -> None:
        """Runs the cleanup the way one import run does: a single session, the
        given tenant, and the run's start time."""
        with SessionFactory.using_database(self.database_key) as session:
            retire_stale_candidates(
                session=session, tenant=tenant, run_started_at=_RUN_TIME
            )

    def _new_identity(self, *, tenant: Tenant = Tenant.US_OZ) -> uuid.UUID:
        """Inserts a bare identity so a candidate's recidiviz_id foreign key
        resolves, returning its recidiviz_id."""
        recidiviz_id = uuid.uuid4()
        insert_identity(recidiviz_id=recidiviz_id, tenant=tenant)
        return recidiviz_id

    def _update_candidate_statuses(self) -> list[CandidateStatus]:
        with SessionFactory.using_database(self.database_key) as session:
            return [
                candidate.status
                for candidate in session.query(schema.UpdateAttributeCandidate)
                .order_by(schema.UpdateAttributeCandidate.id)
                .all()
            ]

    def _merge_candidate_statuses(self) -> list[CandidateStatus]:
        with SessionFactory.using_database(self.database_key) as session:
            return [
                candidate.status
                for candidate in session.query(schema.MergeCandidate)
                .order_by(schema.MergeCandidate.id)
                .all()
            ]

    def _split_candidate_statuses(self) -> list[CandidateStatus]:
        with SessionFactory.using_database(self.database_key) as session:
            return [
                candidate.status
                for candidate in session.query(schema.SplitCandidate)
                .order_by(schema.SplitCandidate.id)
                .all()
            ]

    def test_not_redetected_candidates_are_marked_stale(self) -> None:
        insert_update_attribute_candidate(
            recidiviz_id=self._new_identity(), detected_at_utc=_STALE_TIME
        )
        insert_merge_candidate(
            recidiviz_ids=[self._new_identity()], detected_at_utc=_STALE_TIME
        )
        insert_split_candidate(
            recidiviz_id=self._new_identity(), detected_at_utc=_STALE_TIME
        )

        self._retire()

        self.assertEqual([CandidateStatus.STALE], self._update_candidate_statuses())
        self.assertEqual([CandidateStatus.STALE], self._merge_candidate_statuses())
        self.assertEqual([CandidateStatus.STALE], self._split_candidate_statuses())

    def test_redetected_candidates_are_kept_pending(self) -> None:
        # A candidate refreshed this run carries detected_at_utc == the run's
        # start; the strict comparison in the cleanup must keep it.
        insert_update_attribute_candidate(
            recidiviz_id=self._new_identity(), detected_at_utc=_RUN_TIME
        )
        insert_merge_candidate(
            recidiviz_ids=[self._new_identity()], detected_at_utc=_RUN_TIME
        )
        insert_split_candidate(
            recidiviz_id=self._new_identity(), detected_at_utc=_RUN_TIME
        )

        self._retire()

        self.assertEqual([CandidateStatus.PENDING], self._update_candidate_statuses())
        self.assertEqual([CandidateStatus.PENDING], self._merge_candidate_statuses())
        self.assertEqual([CandidateStatus.PENDING], self._split_candidate_statuses())

    def test_resolved_and_stale_candidates_are_left_untouched(self) -> None:
        # A reviewer has already acted on these (or an earlier run retired them),
        # so even though this run did not re-detect them, the cleanup must not
        # re-stale a RESOLVED row or touch an already-STALE one.
        insert_update_attribute_candidate(
            recidiviz_id=self._new_identity(),
            status=CandidateStatus.RESOLVED,
            detected_at_utc=_STALE_TIME,
        )
        insert_merge_candidate(
            recidiviz_ids=[self._new_identity()],
            status=CandidateStatus.STALE,
            detected_at_utc=_STALE_TIME,
        )
        insert_split_candidate(
            recidiviz_id=self._new_identity(),
            status=CandidateStatus.STALE,
            detected_at_utc=_STALE_TIME,
        )

        self._retire()

        self.assertEqual([CandidateStatus.RESOLVED], self._update_candidate_statuses())
        self.assertEqual([CandidateStatus.STALE], self._merge_candidate_statuses())
        self.assertEqual([CandidateStatus.STALE], self._split_candidate_statuses())

    def test_retiring_stamps_status_updated_at(self) -> None:
        # Two candidates differing only in detected_at_utc: the stale one gets
        # the run's start time stamped on retirement, the re-detected one stays
        # PENDING with no stamp.
        insert_update_attribute_candidate(
            recidiviz_id=self._new_identity(), detected_at_utc=_STALE_TIME
        )
        insert_update_attribute_candidate(
            recidiviz_id=self._new_identity(), detected_at_utc=_RUN_TIME
        )

        self._retire()

        with SessionFactory.using_database(self.database_key) as session:
            status_stamps = [
                candidate.status_updated_at_utc
                for candidate in session.query(schema.UpdateAttributeCandidate)
                .order_by(schema.UpdateAttributeCandidate.id)
                .all()
            ]
        self.assertEqual([_RUN_TIME, None], status_stamps)

    def test_retired_candidate_ids_are_logged(self) -> None:
        """The paper trail: an accidental mass retirement must be traceable to
        the exact rows, so the cleanup logs each table's retired ids."""
        insert_split_candidate(
            recidiviz_id=self._new_identity(), detected_at_utc=_STALE_TIME
        )
        with SessionFactory.using_database(self.database_key) as session:
            (stale_id,) = session.query(schema.SplitCandidate.id).one()

        with self.assertLogs(level="INFO") as log_capture:
            self._retire()

        self.assertIn(
            f"Marking [1] rows of [split_candidates] STALE for tenant [US_OZ]: "
            f"ids [{stale_id}].",
            [record.getMessage() for record in log_capture.records],
        )

    def test_cleanup_is_scoped_to_the_tenant(self) -> None:
        # Another tenant's stale PENDING candidates must survive a US_OZ run.
        insert_update_attribute_candidate(
            recidiviz_id=self._new_identity(tenant=Tenant.US_ND),
            tenant=Tenant.US_ND,
            detected_at_utc=_STALE_TIME,
        )
        insert_merge_candidate(
            recidiviz_ids=[self._new_identity(tenant=Tenant.US_ND)],
            tenant=Tenant.US_ND,
            detected_at_utc=_STALE_TIME,
        )
        insert_split_candidate(
            recidiviz_id=self._new_identity(tenant=Tenant.US_ND),
            tenant=Tenant.US_ND,
            detected_at_utc=_STALE_TIME,
        )

        self._retire(tenant=Tenant.US_OZ)

        self.assertEqual([CandidateStatus.PENDING], self._update_candidate_statuses())
        self.assertEqual([CandidateStatus.PENDING], self._merge_candidate_statuses())
        self.assertEqual([CandidateStatus.PENDING], self._split_candidate_statuses())
