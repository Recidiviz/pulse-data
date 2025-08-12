# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2024 Recidiviz, Inc.
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
"""Utilities for working with crc32c checksums."""
import struct
from typing import List, Tuple

import google_crc32c

MAX_CRC32C_ZERO_ARRAY_SIZE = 4 * 1024 * 1024


# TODO(#29093): replace with package implementation for crc32c_combine
def digest_ordered_checksum_and_size_pairs(
    checksum_and_size_pairs: List[Tuple[int, int]],
) -> bytes:
    """THIS FUNCTION WAS COPY-PASTED FROM:
    google.cloud.storage.transfer_manager._digest_ordered_checksum_and_size_pairs
    see https://github.com/googleapis/python-storage/blob/6ed22ee91d4596dd27338ff8b35076d1238a603c/google/cloud/storage/transfer_manager.py#L1337
    for original implementation. function was moved here so as not to call private function
    that might change in any version update w/o backwards compatibility.

    Takes in an ordered list of (checksum, size of checksumed bytes) and generates a
    single checksum. Is useful for generating a single checksum for a full file based on
    the checksums of a series of chunks that make up that file.
    """
    base_crc = None
    zeroes = bytes(MAX_CRC32C_ZERO_ARRAY_SIZE)
    for part_crc, size in checksum_and_size_pairs:
        if base_crc is None:
            base_crc = part_crc
        else:
            base_crc ^= 0xFFFFFFFF  # precondition

            # Zero pad base_crc32c. To conserve memory, do so with only
            # MAX_CRC32C_ZERO_ARRAY_SIZE at a time. Reuse the zeroes array where
            # possible.
            padded = 0
            while padded < size:
                desired_zeroes_size = min((size - padded), MAX_CRC32C_ZERO_ARRAY_SIZE)
                base_crc = google_crc32c.extend(base_crc, zeroes[:desired_zeroes_size])
                padded += desired_zeroes_size

            base_crc ^= 0xFFFFFFFF  # postcondition
            base_crc ^= part_crc
    crc_digest = struct.pack(
        ">L", base_crc
    )  # https://cloud.google.com/storage/docs/json_api/v1/objects#crc32c
    return crc_digest
