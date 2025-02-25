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
"""Tests for crc32c.py"""
import base64
import typing
import unittest

import google_crc32c

from recidiviz.utils.crc32c import digest_ordered_checksum_and_size_pairs


class DigestOrderedPairsTest(unittest.TestCase):
    """Tests for digest_ordered_checksum_and_size_pairs"""

    def run_digest_pairs(self, chunks: typing.List[bytes]) -> None:
        # checksum w/ all bytes
        full_crc32c = google_crc32c.Checksum()
        for chunk in chunks:
            full_crc32c.update(chunk)
        full_checksum = base64.b64encode(full_crc32c.digest()).decode("utf-8")

        # checksum in parts
        chunk_digests = [(google_crc32c.value(chunk), len(chunk)) for chunk in chunks]
        chunk_combined_digest = digest_ordered_checksum_and_size_pairs(chunk_digests)
        chunk_combined_checksum = base64.b64encode(chunk_combined_digest).decode(
            "utf-8"
        )

        assert full_checksum == chunk_combined_checksum

    def test_empty(self) -> None:
        chunks = [b""]
        self.run_digest_pairs(chunks)

    def test_single(self) -> None:
        chunks = [b"somethign something something"]
        self.run_digest_pairs(chunks)

    def test_multiple(self) -> None:
        chunks = [
            b"something somethingggggggggggggggggggggggggggggggggggggggggggg",
            b"how may checksums can aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
            b"checksum summmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmm",
            b"something something elseeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee",
        ]

        self.run_digest_pairs(chunks)
