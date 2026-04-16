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
"""Tests for ``recidiviz/tools/auth/dry_run_import_ingested_users.py``.

These tests verify two properties of the dry-run script:
  1. It logs the changes that would be made to ``Roster`` and ``UserOverride``
     (CSV upsert phase + post-processing reconciliation).
  2. It commits *nothing* — the database state before and after a dry-run is
     byte-for-byte identical.
"""
import os
from datetime import datetime, timedelta
from typing import Any
from unittest import TestCase
from unittest.mock import patch

import pytest
from dateutil.tz import tzlocal

from recidiviz.cloud_storage.gcsfs_factory import GcsfsFactory
from recidiviz.cloud_storage.gcsfs_path import GcsfsFilePath
from recidiviz.common.io.local_file_contents_handle import LocalFileContentsHandle
from recidiviz.persistence.database.schema.case_triage.schema import (
    Roster,
    UserOverride,
)
from recidiviz.persistence.database.schema_type import SchemaType
from recidiviz.persistence.database.session_factory import SessionFactory
from recidiviz.persistence.database.sqlalchemy_database_key import SQLAlchemyDatabaseKey
from recidiviz.persistence.database.sqlalchemy_engine_manager import (
    SQLAlchemyEngineManager,
)
from recidiviz.tests.auth.helpers import (
    add_entity_to_database_session,
    generate_fake_default_permissions,
    generate_fake_rosters,
    generate_fake_user_overrides,
)
from recidiviz.tests.cloud_storage.fake_gcs_file_system import FakeGCSFileSystem
from recidiviz.tools.auth.dry_run_import_ingested_users import _run_dry_run
from recidiviz.tools.postgres import local_persistence_helpers, local_postgres_helpers
from recidiviz.tools.postgres.local_postgres_helpers import OnDiskPostgresLaunchResult

_FIXTURE_PATH = os.path.abspath(
    os.path.join(
        os.path.dirname(os.path.realpath(__file__)),
        "../../auth/fixtures/",
    )
)
_LOCAL_CSV_FIXTURE = os.path.join(
    _FIXTURE_PATH, "us_xx_ingested_incarceration_and_supervision_product_users.csv"
)
_GCS_BUCKET = "test-project-product-user-import"
_GCS_PATH = (
    f"gs://{_GCS_BUCKET}/US_XX/"
    "ingested_incarceration_and_supervision_product_users.csv"
)

# All emails present in the fixture CSV (see
# us_xx_ingested_incarceration_and_supervision_product_users.csv).
_CSV_EMAILS = {
    "supervision_staff@testdomain.com",
    "user@testdomain.com",
    "facilities_staff@testdomain.com",
    "facilities_leadership@testdomain.com",
    "user4@testdomain.com",
    "supervision_leadership@testdomain.com",
}

_SUPERVISION_STAFF = "supervision_line_staff"
_FACILITIES_STAFF = "facilities_line_staff"
_SUPERVISION_LEADERSHIP = "supervision_leadership"
_FACILITIES_LEADERSHIP = "facilities_leadership"


def _row_to_dict(row: Roster | UserOverride) -> dict[str, Any]:
    """Returns a hashable-friendly dict for snapshot comparison. Excludes the
    auto-managed created/updated timestamps."""
    return {
        "state_code": row.state_code,
        "email_address": row.email_address,
        "external_id": row.external_id,
        "roles": list(row.roles) if row.roles is not None else None,
        "district": row.district,
        "first_name": row.first_name,
        "last_name": row.last_name,
        "user_hash": row.user_hash,
        "pseudonymized_id": row.pseudonymized_id,
        "blocked_on": getattr(row, "blocked_on", None),
    }


@pytest.mark.uses_db
class DryRunImportIngestedUsersTest(TestCase):
    """Tests that ``_run_dry_run`` logs would-be changes and never commits."""

    postgres_launch_result: OnDiskPostgresLaunchResult

    @classmethod
    def setUpClass(cls) -> None:
        cls.postgres_launch_result = (
            local_postgres_helpers.start_on_disk_postgresql_database()
        )

    @classmethod
    def tearDownClass(cls) -> None:
        local_postgres_helpers.stop_and_clear_on_disk_postgresql_database(
            cls.postgres_launch_result
        )

    def setUp(self) -> None:
        self.maxDiff = None
        self.database_key = SQLAlchemyDatabaseKey.for_schema(SchemaType.CASE_TRIAGE)
        self.overridden_env_vars = (
            local_persistence_helpers.update_local_sqlalchemy_postgres_env_vars(
                self.postgres_launch_result
            )
        )
        engine = SQLAlchemyEngineManager.init_engine_for_postgres_instance(
            database_key=self.database_key,
            db_url=self.postgres_launch_result.url(),
        )
        self.database_key.declarative_meta.metadata.create_all(engine)

        # Stub out GCS so the dry-run can read the local fixture as if it were
        # a real gs:// object.
        self.fs = FakeGCSFileSystem()
        self.fs.upload_from_contents_handle_stream(
            path=GcsfsFilePath.from_absolute_path(_GCS_PATH),
            contents_handle=LocalFileContentsHandle(
                local_file_path=_LOCAL_CSV_FIXTURE, cleanup_file=False
            ),
            content_type="text/csv",
        )
        self.fs_patcher = patch.object(GcsfsFactory, "build", return_value=self.fs)
        self.fs_patcher.start()

        # Every CSV row's role(s) must exist in StateRolePermissions for the
        # state, otherwise ``_upsert_user_rows`` raises ValueError before we
        # get a chance to log anything.
        add_entity_to_database_session(
            self.database_key,
            [
                generate_fake_default_permissions(
                    state="US_XX", role=_SUPERVISION_STAFF
                ),
                generate_fake_default_permissions(
                    state="US_XX", role=_FACILITIES_STAFF
                ),
                generate_fake_default_permissions(
                    state="US_XX", role=_SUPERVISION_LEADERSHIP
                ),
                generate_fake_default_permissions(
                    state="US_XX", role=_FACILITIES_LEADERSHIP
                ),
            ],
        )

    def tearDown(self) -> None:
        self.fs_patcher.stop()
        local_postgres_helpers.restore_local_env_vars(self.overridden_env_vars)
        local_persistence_helpers.teardown_on_disk_postgresql_database(
            self.database_key
        )

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def _snapshot_db(self) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
        """Returns a (Roster, UserOverride) snapshot of the on-disk DB ordered
        by email so equality comparison is order-independent."""
        with SessionFactory.using_database(
            self.database_key, autocommit=False
        ) as session:
            roster = sorted(
                (_row_to_dict(r) for r in session.query(Roster).all()),
                key=lambda r: r["email_address"],
            )
            overrides = sorted(
                (_row_to_dict(r) for r in session.query(UserOverride).all()),
                key=lambda r: r["email_address"],
            )
        return roster, overrides

    def _run_dry_run_capturing_logs(self) -> list[str]:
        """Runs ``_run_dry_run`` against the test DB inside a fresh
        non-autocommit session and returns the captured INFO log lines."""
        with self.assertLogs(level="INFO") as log_ctx:
            with SessionFactory.using_database(
                self.database_key, autocommit=False
            ) as session:
                _run_dry_run(
                    session=session,
                    state_code="US_XX",
                    gcs_path=_GCS_PATH,
                )
        return log_ctx.output

    def _assert_db_unchanged(
        self,
        roster_before: list[dict[str, Any]],
        overrides_before: list[dict[str, Any]],
    ) -> None:
        roster_after, overrides_after = self._snapshot_db()
        self.assertEqual(
            roster_before,
            roster_after,
            "dry-run modified Roster — should be a no-op",
        )
        self.assertEqual(
            overrides_before,
            overrides_after,
            "dry-run modified UserOverride — should be a no-op",
        )

    def _section_lines(
        self, log_output: list[str], section_header_substring: str
    ) -> list[str]:
        """Returns the slice of ``log_output`` that begins at the line
        containing ``section_header_substring`` and ends just before the next
        section header (a line containing ``=== `` and ` diff:``). Used to
        assert that a CREATE/UPDATE/DELETE entry shows up under the right
        section header (e.g. "Roster (CSV upsert)" vs
        "UserOverride (CSV upsert)")."""
        start: int | None = None
        for i, line in enumerate(log_output):
            if section_header_substring in line:
                start = i
                break
        if start is None:
            self.fail(
                f"Section header containing {section_header_substring!r} not "
                f"found in logs:\n{log_output}"
            )
        end = len(log_output)
        for j in range(start + 1, len(log_output)):
            if "=== " in log_output[j] and " diff:" in log_output[j]:
                end = j
                break
        return log_output[start:end]

    def _assert_csv_upsert_change(
        self,
        log_output: list[str],
        table: str,
        action: str,
        email: str,
    ) -> None:
        """Asserts that the CSV-upsert-phase log section for ``table``
        ("Roster" or "UserOverride") contains a ``<action> <email>`` line.
        ``action`` is one of "CREATE", "UPDATE", "DELETE"."""
        section = self._section_lines(log_output, f"=== {table} (CSV upsert) diff:")
        self.assertTrue(
            any(f"{action} {email}" in line for line in section),
            f"Expected {action} {email} under '{table} (CSV upsert)' section; "
            f"got:\n" + "\n".join(section),
        )

    def _assert_post_processing_line(
        self, log_output: list[str], required_substrings: list[str]
    ) -> None:
        """Asserts that at least one line in ``log_output`` contains *all*
        of the substrings in ``required_substrings``. Useful for matching
        post-processing log lines like
        ``[DRY RUN] would delete N Roster row(s): [..., 'foo@bar.com', ...]``
        where the table name is part of the line text."""
        self.assertTrue(
            any(
                all(substring in line for substring in required_substrings)
                for line in log_output
            ),
            f"Expected a single log line containing all of {required_substrings}; "
            f"got:\n" + "\n".join(log_output),
        )

    # ------------------------------------------------------------------
    # Tests
    # ------------------------------------------------------------------

    def test_dry_run_logs_creates_when_roster_empty(self) -> None:
        """Empty Roster + a CSV with 6 users should produce CREATE log lines
        for all 6 users in the CSV-upsert-phase Roster diff and zero commits."""
        roster_before, overrides_before = self._snapshot_db()
        self.assertEqual([], roster_before)
        self.assertEqual([], overrides_before)

        log_output = self._run_dry_run_capturing_logs()

        # Verify the CSV-upsert-phase header is present.
        self.assertTrue(
            any("CSV upsert phase changes" in line for line in log_output),
            f"Expected 'CSV upsert phase changes' header in logs:\n{log_output}",
        )
        # Verify each CSV email shows up in a CREATE log line under the
        # Roster (CSV upsert) section specifically — not UserOverride.
        for email in _CSV_EMAILS:
            self._assert_csv_upsert_change(log_output, "Roster", "CREATE", email)

        # Post-upsert reconciliation should run with dry_run=True.
        self.assertTrue(
            any("Post-upsert reconciliation" in line for line in log_output)
        )
        self.assertTrue(any("[DRY RUN]" in line for line in log_output))

        # Final rollback should always log.
        self.assertTrue(any("Rolling back all changes" in line for line in log_output))

        self._assert_db_unchanged(roster_before, overrides_before)

    def test_dry_run_logs_updates_when_roster_has_existing(self) -> None:
        """A pre-existing Roster row whose fields differ from the CSV should
        produce an UPDATE log line under the Roster (CSV upsert) section
        listing the field-level changes; nothing should be persisted."""
        # Pre-populate Roster with one of the CSV users but with stale data.
        add_entity_to_database_session(
            self.database_key,
            [
                generate_fake_rosters(
                    email="supervision_staff@testdomain.com",
                    region_code="US_XX",
                    external_id="OLD_ID",
                    roles=[_SUPERVISION_STAFF],
                    district="OLD_DISTRICT",
                    first_name="Old",
                    last_name="Name",
                    pseudonymized_id="OLD_PSEUDO",
                ),
            ],
        )

        roster_before, overrides_before = self._snapshot_db()
        self.assertEqual(1, len(roster_before))

        log_output = self._run_dry_run_capturing_logs()

        # The pre-existing user should appear in an UPDATE entry within the
        # Roster (CSV upsert) section — NOT the UserOverride section.
        roster_section = self._section_lines(
            log_output, "=== Roster (CSV upsert) diff:"
        )
        update_index = next(
            (
                i
                for i, line in enumerate(roster_section)
                if "UPDATE supervision_staff@testdomain.com" in line
            ),
            None,
        )
        if update_index is None:
            self.fail(
                "Expected UPDATE supervision_staff under Roster (CSV upsert) "
                "section; got:\n" + "\n".join(roster_section)
            )
        # The next several log lines should contain at least one column->value
        # change. We sniff for any of the obviously stale fields.
        following = "\n".join(roster_section[update_index : update_index + 10])
        self.assertIn("OLD_DISTRICT", following)

        # And confirm there is NOT a parallel UPDATE in the UserOverride
        # (CSV upsert) section — UserOverride was empty pre-run, so the CSV
        # upsert phase should not have touched it.
        override_section = self._section_lines(
            log_output, "=== UserOverride (CSV upsert) diff:"
        )
        self.assertFalse(
            any(
                "supervision_staff@testdomain.com" in line for line in override_section
            ),
            "Did not expect supervision_staff in UserOverride (CSV upsert) "
            "section; got:\n" + "\n".join(override_section),
        )

        self._assert_db_unchanged(roster_before, overrides_before)

    def test_dry_run_logs_post_processing_for_non_ingested_user(self) -> None:
        """A Roster user that is *not* in the CSV should be flagged by the
        post-upsert reconciliation as a would-be Roster delete AND a would-be
        UserOverride create — and the email must appear specifically on the
        Roster-delete line and the UserOverride-create line, not just somewhere
        in the log. Nothing is persisted."""
        # This user is not in the fixture CSV.
        add_entity_to_database_session(
            self.database_key,
            [
                generate_fake_rosters(
                    email="not_in_csv@testdomain.com",
                    region_code="US_XX",
                    external_id="9999",
                    roles=[_SUPERVISION_STAFF],
                    district="D9",
                    first_name="Stale",
                    last_name="User",
                ),
            ],
        )

        roster_before, overrides_before = self._snapshot_db()

        log_output = self._run_dry_run_capturing_logs()

        # The Roster delete line lists the row's email in a sorted email list.
        self._assert_post_processing_line(
            log_output,
            ["[DRY RUN]", "would delete", "Roster row(s)", "not_in_csv@testdomain.com"],
        )

        # The UserOverride creates are emitted as a header line followed by
        # one "  + <email> -> {...}" line per row. The "+ <email>" entry must
        # follow a "would create ... UserOverride" header and precede the next
        # post-processing header (e.g. "would update", "would delete", "would
        # clear", "would leave"). Both header and per-row lines start with
        # "[DRY RUN] " because _log_dry_run prefixes everything.
        ucreate_header_idx = next(
            (
                i
                for i, line in enumerate(log_output)
                if "would create" in line and "UserOverride row(s)" in line
            ),
            None,
        )
        if ucreate_header_idx is None:
            self.fail(
                "Expected '[DRY RUN] would create N UserOverride row(s)' header; "
                "got:\n" + "\n".join(log_output)
            )
        next_header_keywords = (
            "would update",
            "would delete",
            "would clear",
            "would leave",
            "would set",
        )
        section_end = len(log_output)
        for j in range(ucreate_header_idx + 1, len(log_output)):
            if any(kw in log_output[j] for kw in next_header_keywords):
                section_end = j
                break
        create_section = log_output[ucreate_header_idx + 1 : section_end]
        self.assertTrue(
            any("+ not_in_csv@testdomain.com" in line for line in create_section),
            "Expected '+ not_in_csv@testdomain.com' line under the UserOverride "
            "create header; got:\n" + "\n".join(create_section),
        )

        self._assert_db_unchanged(roster_before, overrides_before)

    def test_dry_run_logs_unblock_for_blocked_ingested_user(self) -> None:
        """A future-blocked UserOverride for a user that is in the CSV should
        be flagged by the post-upsert reconciliation as a would-be unblock.
        The email must appear on the unblock line specifically, not just
        somewhere else in the log."""
        future_block = datetime.now(tzlocal()) + timedelta(days=5)
        add_entity_to_database_session(
            self.database_key,
            [
                generate_fake_user_overrides(
                    email="user@testdomain.com",
                    region_code="US_XX",
                    blocked_on=future_block,
                ),
            ],
        )

        roster_before, overrides_before = self._snapshot_db()

        log_output = self._run_dry_run_capturing_logs()

        # The unblock log line is a single line that lists matching emails.
        self._assert_post_processing_line(
            log_output,
            [
                "[DRY RUN]",
                "would clear future blocked_on",
                "UserOverride row(s)",
                "user@testdomain.com",
            ],
        )

        self._assert_db_unchanged(roster_before, overrides_before)

    def test_dry_run_does_not_commit_anything_with_complex_state(self) -> None:
        """End-to-end no-commit verification with a mixed pre-state covering
        all branches: pre-existing Roster row that overlaps the CSV (will be
        UPDATE-ed in upsert), pre-existing Roster row not in CSV (post-processing
        delete + UserOverride carryover), and a UserOverride that would be
        unblocked."""
        future_block = datetime.now(tzlocal()) + timedelta(days=5)
        add_entity_to_database_session(
            self.database_key,
            [
                # CSV-overlapping Roster row that will get an UPDATE.
                generate_fake_rosters(
                    email="user@testdomain.com",
                    region_code="US_XX",
                    external_id="OLD",
                    roles=[_SUPERVISION_STAFF],
                    district="OLD",
                    first_name="Old",
                    last_name="Name",
                ),
                # Roster row not in CSV — would be moved to UserOverride and
                # then deleted by post-processing.
                generate_fake_rosters(
                    email="not_in_csv@testdomain.com",
                    region_code="US_XX",
                    external_id="9999",
                    roles=[_SUPERVISION_STAFF],
                    district="D9",
                ),
                # UserOverride for an ingested user with a future block.
                generate_fake_user_overrides(
                    email="supervision_staff@testdomain.com",
                    region_code="US_XX",
                    blocked_on=future_block,
                ),
            ],
        )

        roster_before, overrides_before = self._snapshot_db()
        self.assertEqual(2, len(roster_before))
        self.assertEqual(1, len(overrides_before))

        log_output = self._run_dry_run_capturing_logs()

        # Each branch should be logged under the correct section, not just
        # "somewhere" in the output.
        # Branch 1: CSV-overlapping Roster row -> UPDATE under Roster (CSV upsert).
        self._assert_csv_upsert_change(
            log_output, "Roster", "UPDATE", "user@testdomain.com"
        )
        # Branch 2: Non-CSV Roster row -> "would delete ... Roster" line listing
        # the email.
        self._assert_post_processing_line(
            log_output,
            ["[DRY RUN]", "would delete", "Roster row(s)", "not_in_csv@testdomain.com"],
        )
        # Branch 3: Future-blocked UserOverride for an ingested email -> the
        # post-processing unblock line listing the email.
        self._assert_post_processing_line(
            log_output,
            [
                "[DRY RUN]",
                "would clear future blocked_on",
                "UserOverride row(s)",
                "supervision_staff@testdomain.com",
            ],
        )
        # And the structural log markers.
        self.assertTrue(any("CSV upsert phase changes" in line for line in log_output))
        self.assertTrue(
            any("Post-upsert reconciliation" in line for line in log_output)
        )
        self.assertTrue(any("Rolling back all changes" in line for line in log_output))

        # The most important assertion: the DB is byte-for-byte unchanged.
        self._assert_db_unchanged(roster_before, overrides_before)

    def test_dry_run_restores_session_commit_after_run(self) -> None:
        """The script monkey-patches ``session.commit`` to ``session.flush``
        for the duration of the upsert phase. After the dry-run finishes (or
        raises), the original ``commit`` should be restored on the session
        (i.e. it should NOT be aliased to ``flush``)."""
        with SessionFactory.using_database(
            self.database_key, autocommit=False
        ) as session:
            _run_dry_run(
                session=session,
                state_code="US_XX",
                gcs_path=_GCS_PATH,
            )
            # Bound methods create a new object on every attribute access, so
            # `is` comparison is unreliable; check the underlying function
            # identity instead.
            self.assertIsNot(
                session.commit.__func__,  # type: ignore[attr-defined]
                session.flush.__func__,  # type: ignore[attr-defined]
                "session.commit should be restored after _run_dry_run finishes",
            )
