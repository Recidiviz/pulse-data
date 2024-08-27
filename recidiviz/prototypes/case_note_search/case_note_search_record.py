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
from typing import Any, Dict, List, Optional

from sqlalchemy.ext.asyncio import AsyncSession

from recidiviz.persistence.database.async_session_factory import AsyncSessionFactory
from recidiviz.persistence.database.schema.workflows.schema import CaseNoteSearchRecord
from recidiviz.persistence.database.schema_type import SchemaType
from recidiviz.persistence.database.sqlalchemy_async_engine_manager import (
    SQLAlchemyAsyncEngineManager,
)
from recidiviz.persistence.database.sqlalchemy_database_key import SQLAlchemyDatabaseKey
from recidiviz.tools.prototypes.discovery_engine import (
    MAX_EXTRACTIVE_ANSWER_COUNT,
    MAX_SNIPPET_COUNT,
)


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


def nullify_case_note_field(results: Dict[str, Any]) -> Dict[str, Any]:
    """Nullify all case note fields within a result. We don't want to store the full
    case note in the results. We can access the case note using the document_id as a
    post-processing step if we want to reference the case note later.
    """
    if results.get("results", None) is None:
        return results
    for result in results["results"]:
        if "case_note" in result:
            result["case_note"] = None
    return results


async def record_case_note_search(
    user_external_id: Optional[str],
    client_external_id: Optional[str],
    state_code: str,
    query: Optional[str],
    page_size: Optional[int],
    filter_conditions: Optional[Dict[str, List[str]]],
    with_snippet: Optional[bool],
    results: Dict[str, Any],
) -> None:
    """Sets up an AsyncSession and uses the CaseNoteSearchRecordInterface to write to
    the case note search record table."""
    db_key = SQLAlchemyDatabaseKey(SchemaType.WORKFLOWS, db_name=state_code.lower())
    # initialize engine to connect to the database.
    await SQLAlchemyAsyncEngineManager.init_async_engine(db_key)
    async with AsyncSessionFactory.using_database(db_key) as session:
        await CaseNoteSearchRecordInterface.write_record(
            session=session,
            user_external_id=user_external_id,
            client_external_id=client_external_id,
            state_code=state_code,
            query=query,
            page_size=page_size,
            with_snippet=with_snippet,
            filter_conditions=filter_conditions,
            max_extractive_answer_count=MAX_EXTRACTIVE_ANSWER_COUNT,
            max_snippet_count=MAX_SNIPPET_COUNT,
            results=nullify_case_note_field(results),
        )
    # Teardown database.
    await SQLAlchemyAsyncEngineManager.teardown_async_engine_for_database_key(
        database_key=db_key
    )
