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
"""Implements the CaseNoteSearchRecordInterface class."""
import datetime
from typing import Dict, List, Optional

from sqlalchemy.ext.asyncio import AsyncSession

from recidiviz.persistence.database.schema.workflows.schema import CaseNoteSearchRecord


class CaseNoteSearchRecordInterface:
    """Contains methods for working with case note search records."""

    @staticmethod
    async def write_record(
        session: AsyncSession,
        user_external_id: Optional[str] = None,
        client_external_id: Optional[str] = None,
        state_code: Optional[str] = None,
        search_term: Optional[str] = None,
        page_size: Optional[int] = None,
        filter_conditions: Optional[Dict] = None,
        max_extractive_answer_count: Optional[int] = None,
        max_snippet_count: Optional[int] = None,
        summary_result_count: Optional[int] = None,
        case_note_ids: Optional[List[str]] = None,
        extractive_answer: Optional[str] = None,
        snippet: Optional[str] = None,
        summary: Optional[str] = None,
    ) -> None:
        """Write case note search record entries."""
        if filter_conditions is None:
            filter_conditions = {}
        if case_note_ids is None:
            case_note_ids = []

        async with session:
            new_record = CaseNoteSearchRecord(
                user_external_id=user_external_id,
                client_external_id=client_external_id,
                state_code=state_code,
                timestamp=datetime.datetime.now(tz=datetime.timezone.utc),
                search_term=search_term,
                page_size=page_size,
                filter_conditions=filter_conditions,
                max_extractive_answer_count=max_extractive_answer_count,
                max_snippet_count=max_snippet_count,
                summary_result_count=summary_result_count,
                case_note_ids=case_note_ids,
                extractive_answer=extractive_answer,
                snippet=snippet,
                summary=summary,
            )
            session.add(new_record)
            await session.commit()
