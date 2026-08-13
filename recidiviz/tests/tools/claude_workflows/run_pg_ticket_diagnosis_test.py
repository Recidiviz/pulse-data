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
"""Tests for pg_ticket_diagnosis/run_pg_ticket_diagnosis.py.

The Cloud Build container ships run_pg_ticket_diagnosis.py flat alongside
claude_agent.py and pii_doc_parser_utils.py, so the script imports those by
bare module name. Reproduce that layout here by putting both directories on
sys.path before importing the module under test.
"""
import os
import sys
import unittest
from unittest import mock

from recidiviz.github.github_issue import GithubIssue
from recidiviz.issue_tracking.linear.linear_client import (
    LinearApiError,
    LinearEquivalentIssueGroup,
)
from recidiviz.issue_tracking.linear.linear_issue import LinearIssue
from recidiviz.tools.claude_workflows import claude_agent as _claude_agent_pkg
from recidiviz.tools.claude_workflows.pg_ticket_diagnosis import (
    pii_doc_parser_utils as _pii_doc_parser_pkg,
)

sys.path.insert(0, os.path.dirname(_claude_agent_pkg.__file__))
sys.path.insert(0, os.path.dirname(_pii_doc_parser_pkg.__file__))

# run_pg_ticket_diagnosis is a bare (non-recidiviz) module imported only after
# the sys.path setup above, so it must sit below the first-party imports.
# pylint: disable=wrong-import-position,wrong-import-order
import run_pg_ticket_diagnosis as run_pg  # type: ignore[import-not-found]  # noqa: E402

_MODULE = "run_pg_ticket_diagnosis"
_ISSUE = GithubIssue(repo="Recidiviz/pulse-data", number=88494)

_DOC_ID = "16Oce011Zebihlmas8xMKmzj6FQEuDg2JtX6vceALeJU"
_BODY_WITH_BANNER = (
    "> 🔒 **Private PII doc for this issue → [Open PII doc]"
    f"(https://docs.google.com/document/d/{_DOC_ID}/edit)** · Put officer/client "
    "details and screenshots here, not in this issue.\n\n"
    "<!-- pii-doc-linked -->\n\n#### What is the issue?\n"
)
_BODY_WITHOUT_BANNER = "#### What is the issue?\n\nThe task never cleared."


def _doc_from_lines(lines: list[str]) -> dict:
    """Build a minimal Docs API response whose text is `lines`."""
    return {
        "body": {
            "content": [
                {"paragraph": {"elements": [{"textRun": {"content": f"{line}\n"}}]}}
                for line in lines
            ]
        }
    }


class TestResolvePiiDocId(unittest.TestCase):
    """Tests for resolve_pii_doc_id — finding the ticket's own PII doc.

    The doc is created (and the banner prepended) seconds after the issue, so the
    live body is authoritative and the webhook payload body is not.
    """

    @staticmethod
    def _mock_get_issue(mock_client: mock.MagicMock) -> mock.MagicMock:
        """Returns the mocked repo.get_issue() that resolve_pii_doc_id calls.

        resolve_pii_doc_id reads the body via
        github_helperbot_client().get_repo(repo).get_issue(number).body, so tests
        drive it by setting return_value/side_effect on that innermost call.
        """
        return mock_client.return_value.get_repo.return_value.get_issue

    @mock.patch(f"{_MODULE}.github_helperbot_client")
    def test_returns_doc_id_from_live_body(self, mock_client: mock.MagicMock) -> None:
        get_issue = self._mock_get_issue(mock_client)
        get_issue.return_value = mock.MagicMock(body=_BODY_WITH_BANNER)
        # The payload body predates the banner — the live body must win.
        doc_id, body = run_pg.resolve_pii_doc_id(_ISSUE, _BODY_WITHOUT_BANNER)
        self.assertEqual(doc_id, _DOC_ID)
        self.assertEqual(body, _BODY_WITH_BANNER)
        mock_client.return_value.get_repo.assert_called_once_with(_ISSUE.repo)
        get_issue.assert_called_once_with(_ISSUE.number)

    @mock.patch(f"{_MODULE}.time.sleep")
    @mock.patch(f"{_MODULE}.github_helperbot_client")
    def test_retries_then_gives_up_when_no_banner(
        self, mock_client: mock.MagicMock, mock_sleep: mock.MagicMock
    ) -> None:
        get_issue = self._mock_get_issue(mock_client)
        get_issue.return_value = mock.MagicMock(body=_BODY_WITHOUT_BANNER)
        doc_id, body = run_pg.resolve_pii_doc_id(_ISSUE, _BODY_WITHOUT_BANNER)
        self.assertIsNone(doc_id)
        self.assertEqual(body, _BODY_WITHOUT_BANNER)
        self.assertEqual(get_issue.call_count, run_pg.PII_DOC_LOOKUP_ATTEMPTS)
        # Sleeps between attempts, not after the last one.
        self.assertEqual(mock_sleep.call_count, run_pg.PII_DOC_LOOKUP_ATTEMPTS - 1)

    @mock.patch(f"{_MODULE}.time.sleep")
    @mock.patch(f"{_MODULE}.github_helperbot_client")
    def test_stops_retrying_once_the_banner_appears(
        self, mock_client: mock.MagicMock, mock_sleep: mock.MagicMock
    ) -> None:
        get_issue = self._mock_get_issue(mock_client)
        get_issue.side_effect = [
            mock.MagicMock(body=_BODY_WITHOUT_BANNER),
            mock.MagicMock(body=_BODY_WITH_BANNER),
        ]
        doc_id, _ = run_pg.resolve_pii_doc_id(_ISSUE, _BODY_WITHOUT_BANNER)
        self.assertEqual(doc_id, _DOC_ID)
        self.assertEqual(get_issue.call_count, 2)
        mock_sleep.assert_called_once()

    @mock.patch(f"{_MODULE}.github_helperbot_client")
    def test_falls_back_to_payload_body_when_github_read_fails(
        self, mock_client: mock.MagicMock
    ) -> None:
        # A manual workflow_dispatch run can pass a body that already has the
        # banner, so the payload is still worth reading.
        self._mock_get_issue(mock_client).side_effect = RuntimeError("GitHub is down")
        doc_id, body = run_pg.resolve_pii_doc_id(_ISSUE, _BODY_WITH_BANNER)
        self.assertEqual(doc_id, _DOC_ID)
        self.assertEqual(body, _BODY_WITH_BANNER)

    @mock.patch(f"{_MODULE}.time.sleep")
    @mock.patch(f"{_MODULE}.github_helperbot_client")
    def test_issue_with_no_body_is_treated_as_empty(
        self, mock_client: mock.MagicMock, _mock_sleep: mock.MagicMock
    ) -> None:
        # PyGithub returns None for an issue filed with an empty body.
        self._mock_get_issue(mock_client).return_value = mock.MagicMock(body=None)
        doc_id, body = run_pg.resolve_pii_doc_id(_ISSUE, _BODY_WITHOUT_BANNER)
        self.assertIsNone(doc_id)
        self.assertEqual(body, "")


class TestFetchPiiForIssue(unittest.TestCase):
    """Tests for fetch_pii_for_issue — reads the per-ticket AND the shared doc."""

    # A per-ticket doc that was never filled in: template prose, no IDs.
    UNFILLED_TICKET_DOC = [
        "Client / Resident (PII)",
        "Client/resident IDs. (Hugo's helper bot reads IDs from here.)",
    ]
    # The shared doc's entry for issue 88494, with a usable ID under it.
    SHARED_DOC = [
        "#88494",
        "User: test-officer",
        "Resident/Client: TEST-CLIENT-ID-1",
        "#OBT-36212",
        "Resident/Client: someone else",
    ]

    def setUp(self) -> None:
        self.ctx = run_pg.DiagnosisContext()

    @staticmethod
    def _docs(
        *, ticket: list[str] | None = None, shared: list[str] | None = None
    ) -> mock.MagicMock:
        """Returns a _fetch_doc stub that answers per doc_id.

        fetch_pii_for_issue reads both sources, so a single return_value would
        hand the same doc to both and hide which one supplied the PII.
        """

        def fetch(  # pylint: disable=unused-argument
            *, doc_id: str, sa_email: str
        ) -> dict:
            if doc_id == run_pg.GITHUB_PII_DOC_ID:
                return _doc_from_lines(shared or [])
            return _doc_from_lines(ticket or [])

        return mock.MagicMock(side_effect=fetch)

    def _fetch(self, pii_doc_id: str | None, linear_id: str | None = None) -> str:
        return run_pg.fetch_pii_for_issue(
            issue_number="88494",
            linear_id=linear_id,
            pii_doc_id=pii_doc_id,
            sa_email="sa@recidiviz-staging.iam.gserviceaccount.com",
            ctx=self.ctx,
        )

    def test_reads_the_per_ticket_doc_verbatim(self) -> None:
        # The doc is handed to the agent as-is — no template parsing, so a
        # reworded heading can't change what the agent gets. [IMAGE] survives so
        # the agent can see the doc holds screenshots.
        lines = ["Client / Resident (PII)", "TDCJ: TEST-CLIENT-ID-2", "[IMAGE]"]
        with mock.patch.object(run_pg, "_fetch_doc", self._docs(ticket=lines)):
            pii = self._fetch(_DOC_ID)
        self.assertIn("\n".join(lines), pii)
        self.assertIn(run_pg.TICKET_PII_DOC_LABEL, pii)
        self.assertTrue(self.ctx.pii_fetched)

    def test_reads_both_docs_when_both_have_content(self) -> None:
        with mock.patch.object(
            run_pg,
            "_fetch_doc",
            self._docs(ticket=["TDCJ: TEST-CLIENT-ID-3"], shared=self.SHARED_DOC),
        ):
            pii = self._fetch(_DOC_ID)
        self.assertIn(run_pg.TICKET_PII_DOC_LABEL, pii)
        self.assertIn(run_pg.SHARED_PII_DOC_LABEL, pii)
        self.assertIn("TDCJ: TEST-CLIENT-ID-3", pii)
        self.assertIn("TEST-CLIENT-ID-1", pii)
        # The next ticket's section must not bleed in.
        self.assertNotIn("someone else", pii)

    def test_shared_doc_rescues_an_unfilled_ticket_doc(self) -> None:
        # The issue #95175 shape: the ticket has its own doc but nobody filled it
        # in, while the reporter did write the client's ID into the shared doc.
        # Picking one source would drop the only usable ID we have.
        with mock.patch.object(
            run_pg,
            "_fetch_doc",
            self._docs(ticket=self.UNFILLED_TICKET_DOC, shared=self.SHARED_DOC),
        ):
            pii = self._fetch(_DOC_ID)
        self.assertIn("TEST-CLIENT-ID-1", pii)

    def test_reads_shared_doc_without_a_doc_id(self) -> None:
        with mock.patch.object(
            run_pg, "_fetch_doc", self._docs(shared=self.SHARED_DOC)
        ) as mock_fetch_doc:
            pii = self._fetch(None)
        self.assertIn("TEST-CLIENT-ID-1", pii)
        # With no per-ticket doc there is only one fetch to make.
        self.assertEqual(mock_fetch_doc.call_count, 1)

    def test_raises_when_neither_source_has_anything(self) -> None:
        with mock.patch.object(
            run_pg,
            "_fetch_doc",
            self._docs(ticket=[], shared=["#99999", "Resident/Client: 12345"]),
        ):
            with self.assertRaisesRegex(run_pg.PIINotFoundError, "No PII found"):
                self._fetch(_DOC_ID)
        self.assertFalse(self.ctx.pii_fetched)

    def test_header_only_shared_section_counts_as_nothing(self) -> None:
        with mock.patch.object(
            run_pg, "_fetch_doc", self._docs(shared=["#88494", "", "#OBT-36212"])
        ):
            with self.assertRaisesRegex(run_pg.PIINotFoundError, "No PII found"):
                self._fetch(None)

    def test_one_source_failing_does_not_hide_the_other(self) -> None:
        # A 403 on the per-ticket doc must not lose a usable shared-doc entry.
        def fetch(  # pylint: disable=unused-argument
            *, doc_id: str, sa_email: str
        ) -> dict:
            if doc_id == run_pg.GITHUB_PII_DOC_ID:
                return _doc_from_lines(self.SHARED_DOC)
            raise run_pg.PIIFetchError("403 on the per-ticket doc")

        with mock.patch.object(run_pg, "_fetch_doc", side_effect=fetch):
            pii = self._fetch(_DOC_ID)
        self.assertIn("TEST-CLIENT-ID-1", pii)

    def test_raises_fetch_error_when_every_source_errors(self) -> None:
        with mock.patch.object(
            run_pg, "_fetch_doc", side_effect=run_pg.PIIFetchError("boom")
        ):
            with self.assertRaisesRegex(run_pg.PIIFetchError, "boom"):
                self._fetch(_DOC_ID)


@mock.patch.dict(
    os.environ,
    {
        "GCP_PROJECT_ID": "recidiviz-staging",
        "BUILD_ID": "test-build",
        "ISSUE_NUMBER": str(_ISSUE.number),
        "ISSUE_REPO": _ISSUE.repo,
        "ISSUE_TITLE": "[US_TX] Virtual Contacts Aren't Clearing",
        "ISSUE_BODY": _BODY_WITH_BANNER,
        "PRODUCT_AREAS": "tasks",
    },
)
@mock.patch(f"{_MODULE}.get_secret", mock.MagicMock(return_value="fake-api-key"))
@mock.patch(f"{_MODULE}.issue_has_marker", mock.MagicMock(return_value=False))
@mock.patch(
    f"{_MODULE}.resolve_pii_doc_id",
    mock.MagicMock(return_value=(_DOC_ID, _BODY_WITH_BANNER)),
)
class TestMainRefusesUnresolvedDiagnosis(unittest.TestCase):
    """Tests the post-loop guard in main().

    Aborts raised inside a tool handler can't catch an agent that reads a doc,
    finds nothing usable, and writes a state-wide diagnosis anyway — which is
    what happened on issue #95175. main() must refuse to post that.
    """

    DIAGNOSIS = "# TLDR\n\nUS_TX contact data is fresh and ingesting normally."

    @mock.patch(f"{_MODULE}._post_marked_comment")
    @mock.patch(f"{_MODULE}.run_agent")
    def test_discards_diagnosis_when_no_person_ids_resolved(
        self, mock_run_agent: mock.MagicMock, mock_post: mock.MagicMock
    ) -> None:
        # The agent produced a plausible-looking diagnosis but never resolved the
        # ticket's PII to a person, so it isn't about the reported client.
        mock_run_agent.return_value = run_pg.AgentResult(text=self.DIAGNOSIS)

        run_pg.main()

        posted = mock_post.call_args.args[1]
        self.assertIn("never resolved this ticket's PII to a person", posted)
        self.assertNotIn("ingesting normally", posted)

    @mock.patch(f"{_MODULE}.scrub_pii_from_comment", side_effect=lambda text, _: text)
    @mock.patch(f"{_MODULE}._post_marked_comment")
    @mock.patch(f"{_MODULE}.run_agent")
    def test_posts_diagnosis_when_person_ids_were_resolved(
        self,
        mock_run_agent: mock.MagicMock,
        mock_post: mock.MagicMock,
        _mock_scrub: mock.MagicMock,
    ) -> None:
        def _resolve_and_diagnose(*args: object) -> object:
            # ctx is the 7th positional arg run_agent receives.
            ctx = args[6]
            assert isinstance(ctx, run_pg.DiagnosisContext)
            ctx.person_ids_resolved = True
            return run_pg.AgentResult(text=self.DIAGNOSIS)

        mock_run_agent.side_effect = _resolve_and_diagnose

        run_pg.main()

        posted = mock_post.call_args.args[1]
        self.assertIn("ingesting normally", posted)

    @mock.patch(f"{_MODULE}._post_marked_comment")
    @mock.patch(f"{_MODULE}.run_agent")
    def test_keeps_the_specific_message_when_the_loop_already_failed(
        self, mock_run_agent: mock.MagicMock, mock_post: mock.MagicMock
    ) -> None:
        # A DiagnosisFailure abort already names which doc was unusable — the
        # guard must not overwrite it with its own generic message.
        mock_run_agent.return_value = run_pg.AgentResult(
            text=run_pg.PIINotFoundError("No PII found for issue 88494").user_message(),
            failed=True,
        )

        with mock.patch(
            f"{_MODULE}.scrub_pii_from_comment", side_effect=lambda text, _: text
        ):
            run_pg.main()

        posted = mock_post.call_args.args[1]
        self.assertIn("No PII found for issue 88494", posted)
        self.assertNotIn("never resolved this ticket's PII to a person", posted)


class TestResolveLinearIdForIssue(unittest.TestCase):
    """Tests for resolve_linear_id_for_issue — best-effort Linear resolution.

    TODO(OBT-44025): delete this class, plus the shared-doc fallback cases in
    TestFetchPiiForIssue, when the legacy go/github-pii path is removed.
    """

    @mock.patch(f"{_MODULE}.linear_client_from_secret")
    def test_returns_identifier_when_synced(
        self, mock_build_client: mock.MagicMock
    ) -> None:
        mock_client = mock_build_client.return_value
        mock_client.get_equivalent_issue_group_for_github_issue.return_value = (
            LinearEquivalentIssueGroup(
                linear_issue=LinearIssue.from_string("OBT-36212"),
                previous_issues=set(),
                github_issue=_ISSUE,
            )
        )
        self.assertEqual(run_pg.resolve_linear_id_for_issue(_ISSUE), "OBT-36212")
        mock_client.get_equivalent_issue_group_for_github_issue.assert_called_once_with(
            _ISSUE
        )

    @mock.patch(f"{_MODULE}.linear_client_from_secret")
    def test_returns_none_when_not_synced(
        self, mock_build_client: mock.MagicMock
    ) -> None:
        mock_build_client.return_value.get_equivalent_issue_group_for_github_issue.return_value = (
            None
        )
        self.assertIsNone(run_pg.resolve_linear_id_for_issue(_ISSUE))

    @mock.patch(f"{_MODULE}.linear_client_from_secret")
    def test_degrades_to_none_on_linear_api_error(
        self, mock_build_client: mock.MagicMock
    ) -> None:
        mock_build_client.return_value.get_equivalent_issue_group_for_github_issue.side_effect = LinearApiError(
            "boom"
        )
        self.assertIsNone(run_pg.resolve_linear_id_for_issue(_ISSUE))

    @mock.patch(f"{_MODULE}.linear_client_from_secret")
    def test_degrades_to_none_on_credential_failure(
        self, mock_build_client: mock.MagicMock
    ) -> None:
        # A missing-secret failure surfacing from client construction must not
        # abort the diagnosis (GitHub-number lookup still works for pre-Linear
        # tickets).
        mock_build_client.side_effect = KeyError("no linear api key secret")
        self.assertIsNone(run_pg.resolve_linear_id_for_issue(_ISSUE))
