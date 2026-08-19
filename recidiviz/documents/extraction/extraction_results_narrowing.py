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
"""Restricts a read of one extractor's results table to the rows a single extraction run
produced.

An extractor's results table accumulates every result it has ever written. The parsed
results views read all of them and dedupe to the latest result per document, which is what
a consumer of current data wants. A caller who wants one run's output instead builds a
narrowing and passes it to the query, which filters the results table before the dedup, so
the row that survives is the latest within that run.

An eval sample is the case this exists for. Annotations are only meaningful against the
exact extractor version a human labeled, so the sample has to come from that version rather
than from whatever ran most recently.
"""
import attr

from recidiviz.common import attr_validators
from recidiviz.documents.extraction.extraction_results_columns import (
    EXTRACTION_JOB_ID_COLUMN_NAME,
    EXTRACTOR_VERSION_ID_COLUMN_NAME,
)
from recidiviz.utils.string_formatting import fix_indent


@attr.define(frozen=True, kw_only=True)
class ExtractionResultsNarrowing:
    """Restricts a read of one extractor's results table to the rows a single extraction run
    produced, and renders the WHERE clause that enforces it.
    """

    extractor_version_id: str = attr.ib(validator=attr_validators.is_non_empty_str)
    """The extractor version whose results to keep. Always set, because results are only
    comparable within a single version.
    """

    extraction_job_id: str | None = attr.ib(
        validator=attr_validators.is_opt_non_empty_str
    )
    """A single extraction job to narrow to further, or None to keep every job the version
    ran. One version usually spans several jobs, since a retry or a batched backfill each
    open their own.
    """

    def build_where_clause_sql(self) -> str:
        """Returns the WHERE clause restricting an extraction results table read to this
        narrowing's rows. For version "abc123" and job "def456" it returns

        WHERE extractor_version_id = 'abc123'
            AND extraction_job_id = 'def456'

        and omits the second condition when extraction_job_id is None.
        """
        version_condition = (
            f"{EXTRACTOR_VERSION_ID_COLUMN_NAME} = '{self.extractor_version_id}'"
        )
        if self.extraction_job_id is None:
            return f"WHERE {version_condition}"
        return fix_indent(
            f"""
            WHERE {version_condition}
                AND {EXTRACTION_JOB_ID_COLUMN_NAME} = '{self.extraction_job_id}'
            """,
            indent_level=0,
        )
