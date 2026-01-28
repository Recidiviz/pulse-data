#  Recidiviz - a data platform for criminal justice reform
#  Copyright (C) 2025 Recidiviz, Inc.
#
#  This program is free software: you can redistribute it and/or modify
#  it under the terms of the GNU General Public License as published by
#  the Free Software Foundation, either version 3 of the License, or
#  (at your option) any later version.
#
#  This program is distributed in the hope that it will be useful,
#  but WITHOUT ANY WARRANTY; without even the implied warranty of
#  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#  GNU General Public License for more details.
#
#  You should have received a copy of the GNU General Public License
#  along with this program.  If not, see <https://www.gnu.org/licenses/>.
#  =============================================================================
"""Tests for raw_file_config_utils.py"""
import unittest
from typing import List, Optional

import attr

from recidiviz.ingest.direct.raw_data.raw_file_config_utils import (
    LIST_ITEM_IDENTIFIER_TAG,
    validate_list_item_identifiers,
)


class TestValidateListItemIdentifiers(unittest.TestCase):
    """Tests for validate_list_item_identifiers"""

    def test_multiple_list_identifiers(self) -> None:
        @attr.s(auto_attribs=True)
        class Element:
            id: int = attr.ib(metadata={LIST_ITEM_IDENTIFIER_TAG: True})
            other_id: int = attr.ib(metadata={LIST_ITEM_IDENTIFIER_TAG: True})

        @attr.s(auto_attribs=True)
        class Container:
            elements: List[Element]

        with self.assertRaises(ValueError) as cm:
            validate_list_item_identifiers(Container)
        self.assertIn("has multiple fields with metadata", str(cm.exception))

    def test_nested_list_identifier(self) -> None:
        @attr.s(auto_attribs=True)
        class SubElement:
            sub_id: int = attr.ib(metadata={LIST_ITEM_IDENTIFIER_TAG: True})

        @attr.s(auto_attribs=True)
        class Element:
            id: int = attr.ib(metadata={LIST_ITEM_IDENTIFIER_TAG: True})
            sub_elements: List[SubElement]

        @attr.s(auto_attribs=True)
        class Container:
            elements: List[Element]

        # Should not raise
        validate_list_item_identifiers(Container)

    def test_nested_missing_identifier(self) -> None:
        @attr.s(auto_attribs=True)
        class SubElement:
            sub_id: int

        @attr.s(auto_attribs=True)
        class Element:
            id: int = attr.ib(metadata={LIST_ITEM_IDENTIFIER_TAG: True})
            # Should handle optional fields
            sub_elements: Optional[List[SubElement]] = None

        @attr.s(auto_attribs=True)
        class Container:
            elements: List[Element]

        with self.assertRaises(ValueError) as cm:
            validate_list_item_identifiers(Container)
        self.assertIn(
            "has no field marked with metadata LIST_ITEM_IDENTIFIER_TAG: True",
            str(cm.exception),
        )

    def test_ignore_excluded_tag(self) -> None:
        @attr.s(auto_attribs=True)
        class SubElement:
            sub_id: int

        @attr.s(auto_attribs=True)
        class Element:
            id: int = attr.ib(metadata={LIST_ITEM_IDENTIFIER_TAG: True})
            excluded_field: list[SubElement] = attr.ib(metadata={"exclude": True})

        @attr.s(auto_attribs=True)
        class Container:
            elements: List[Element]

        # Should not raise, as the excluded field is ignored
        validate_list_item_identifiers(Container, excluded_tag="exclude")

    def test_non_list_child_class(self) -> None:
        @attr.s(auto_attribs=True)
        class SubElement:
            sub_id: int

        @attr.s(auto_attribs=True)
        class Element:
            id: int
            excluded_field: list[SubElement]

        @attr.s(auto_attribs=True)
        class Container:
            elements: Element

        with self.assertRaises(ValueError) as cm:
            validate_list_item_identifiers(Container)
        self.assertIn(
            "has no field marked with metadata LIST_ITEM_IDENTIFIER_TAG: True",
            str(cm.exception),
        )
