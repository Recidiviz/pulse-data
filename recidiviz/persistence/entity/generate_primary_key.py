# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2023 Recidiviz, Inc.
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
"""Utilities for generating primary keys for entities."""
from hashlib import sha256

from recidiviz.big_query.big_query_utils import MAX_BQ_INT
from recidiviz.common.constants.states import StateCode

PrimaryKey = int


def generate_primary_key(str_rep: str, state_code: StateCode) -> PrimaryKey:
    """Generate a primary key for an entity based on some string representation of that
    entity.

    We first generate a hex digest from the SHA256 hash of the string representation of
    external IDs. We then take the first 56 bits of the 64bit integer representation of
    the hex digest. Then, we apply a mask with the state fips code.

    The primary key hash collision probability is generated by the following formula:
        probability_of_hash_collision = 1 - e^(-k(k-1)/2^56) wher k is the number of elements.
        For k = 30000000, the probability of a hash collision is ~1%.
    """
    int_64_bits = generate_64_int_from_hex_digest(str_rep)
    # Shift down 8 bits to create a 56 bit integer
    int_56_bits = int_64_bits >> 8
    # Generate integer that is fips code with 17 0s trailing (a 56 bit integer is no
    # longer than 17 decimal places).
    fips_code_mask = state_code.get_state_fips_mask(places=17)
    primary_key = fips_code_mask + int_56_bits
    if primary_key > MAX_BQ_INT:
        raise ValueError(
            f"Primary key {primary_key} is greater than the maximum integer supported by BigQuery"
        )
    return fips_code_mask + int_56_bits


def generate_64_int_from_hex_digest(str_rep: str) -> int:
    """Generate a 64 bit integer from a hex digest."""
    hex_digest_64_bits = sha256(str_rep.encode()).hexdigest()[
        :16
    ]  # 16 hex chars = 64-bits
    return int.from_bytes(bytes.fromhex(hex_digest_64_bits), "little")
