# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2025 Recidiviz, Inc.
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
"""Tests for product_type.py"""
import unittest

from recidiviz.segment.product_type import ProductType


class ProductTypeTest(unittest.TestCase):
    """Tests ProductType functions"""

    def test_context_page_keyword(self) -> None:
        """Check that context_page_keyword is defined for every ProductType enum"""
        for product_type in ProductType:
            self.assertIsNotNone(product_type.context_page_keyword)
