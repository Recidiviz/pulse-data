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
"""Implements tests for the CaseNoteSearchRecordInterface class."""
from sqlalchemy import select

from recidiviz.persistence.database.async_session_factory import AsyncSessionFactory
from recidiviz.prototypes.case_note_search.case_note_search_record import (
    CaseNoteSearchRecord,
    CaseNoteSearchRecordInterface,
)
from recidiviz.tests.prototypes.utils.utils import PrototypesDatabaseTestCase


class TestCaseNoteSearchRecordInterface(PrototypesDatabaseTestCase):
    """Implements tests for the CaseNoteSearchRecordInterface class."""

    async def test_write_record(self) -> None:
        """Test writing a search record to the database."""
        async with AsyncSessionFactory.using_database(self.database_key) as session:
            test_data = {
                "user_external_id": "user_123",
                "client_external_id": "client_456",
                "state_code": "CA",
                "query": "test search",
                "page_size": 10,
                "with_snippet": False,
                "filter_conditions": {"status": "open"},
                "max_extractive_answer_count": 3,
                "max_snippet_count": 5,
                "results": {"results": [{"document_id": "11111"}], "error": None},
            }
            await CaseNoteSearchRecordInterface.write_record(
                session=session, **test_data  # type: ignore[arg-type]
            )

            result = await session.execute(
                select(CaseNoteSearchRecord).filter_by(user_external_id="user_123")
            )
            records = result.scalars().all()
            self.assertEqual(len(records), 1)

            record = records[0]
            self.assertEqual(record.user_external_id, test_data["user_external_id"])
            self.assertEqual(record.client_external_id, test_data["client_external_id"])
            self.assertEqual(record.state_code, test_data["state_code"])
            self.assertEqual(record.query, test_data["query"])
            self.assertEqual(record.page_size, test_data["page_size"])
            self.assertEqual(record.with_snippet, test_data["with_snippet"])
            self.assertEqual(record.filter_conditions, test_data["filter_conditions"])
            self.assertEqual(
                record.max_extractive_answer_count,
                test_data["max_extractive_answer_count"],
            )
            self.assertEqual(record.max_snippet_count, test_data["max_snippet_count"])
            self.assertEqual(record.results, test_data["results"])
