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
"""Custom parser functions for use in ingest_view_file_parser_test.py."""


def ssn_from_parts(ssn_first_three: str, ssn_next_two: str, ssn_last_four: str) -> str:
    ssn = (
        int(ssn_first_three) * 1000000 + int(ssn_next_two) * 10000 + int(ssn_last_four)
    )
    return str(ssn)


def normalize_address_roads(full_address: str, is_valid_address: bool) -> str:
    if not is_valid_address:
        return "INVALID"
    parts = full_address.split(", ")
    parts[0] = parts[0].replace("Street", "St").replace("Road", "Rd")
    return ", ".join(parts)
