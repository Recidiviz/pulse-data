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
"""Shared rules for matching an Edovo-submitted person_external_id to a person.

The capture endpoint's existence check and the credit calculator's person
resolution both compare against ``state_person_external_id``, and must agree — or
the endpoint accepts a submission that later resolves to nobody and silently
earns no credit.

Per our agreement with Edovo (2026-04-24), either system may zero-pad, so both
comparisons strip leading zeros from both sides. Comparison only: the submitted
value is always stored verbatim, since the eOMIS writeback and the
no-double-credit constraint both depend on it.
"""

from recidiviz.big_query.big_query_address import BigQueryAddress

PERSON_EXTERNAL_ID_ADDRESS = BigQueryAddress(
    dataset_id="normalized_state", table_id="state_person_external_id"
)


def zero_stripped(column_or_param: str) -> str:
    """Returns SQL stripping leading zeros from |column_or_param| for comparison.

    Only *leading* zeros are stripped: ``TRIM`` would also strip trailing zeros,
    wrongly matching e.g. '100' to '1000'.

    An id that is entirely zeros is left alone rather than stripped to an empty
    string. Normalizing must only add the matches the Edovo agreement intends —
    ones that differ by padding — so it must not collapse every all-zero id to a
    single value, which would both match distinct ids to each other and let an
    empty id match a real stored one. US_CO has one such stored id, and it
    compared fine before this normalization existed.
    """
    return f"COALESCE(NULLIF(LTRIM({column_or_param}, '0'), ''), {column_or_param})"


def strip_leading_zeros(external_id: str) -> str:
    """Returns |external_id| with leading zeros stripped — the Python equivalent
    of ``zero_stripped``.

    The credit calculator strips the stored side in SQL and the submitted side
    here, so it can pass the latter as a query parameter. The two MUST agree;
    ``test_sql_and_python_stripping_agree`` checks that against the emulator.
    """
    return stripped if (stripped := external_id.lstrip("0")) else external_id
