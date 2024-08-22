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
from typing import Any, Dict, Optional

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
        query: Optional[str] = None,
        page_size: Optional[int] = None,
        with_snippet: Optional[bool] = None,
        filter_conditions: Optional[Dict] = None,
        max_extractive_answer_count: Optional[int] = None,
        max_snippet_count: Optional[int] = None,
        results: Optional[Dict[str, Any]] = None,
    ) -> None:
        """Write case note search record entries."""
        if filter_conditions is None:
            filter_conditions = {}

        async with session:
            new_record = CaseNoteSearchRecord(
                user_external_id=user_external_id,
                client_external_id=client_external_id,
                state_code=state_code,
                timestamp=datetime.datetime.now(tz=datetime.timezone.utc),
                query=query,
                page_size=page_size,
                with_snippet=with_snippet,
                filter_conditions=filter_conditions,
                max_extractive_answer_count=max_extractive_answer_count,
                max_snippet_count=max_snippet_count,
                results=results,
            )
            session.add(new_record)
            await session.commit()
