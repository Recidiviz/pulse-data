# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2019 Recidiviz, Inc.
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

"""A module which takes any HTML content and extracts from it key/value pairs
that a user might care about. The extracted information is put into the ingest
data model and returned.

This DataExtractor implementation works over HTML content by iterating over the
table cells (<td> and <tr> elements). If a cell matches  a key, we attempt to
find the value in an adjacent cell (to the right or below). We expect multi-key
values to exist in columns, i.e. a number of cells in the same position in rows
below a given key cell.

The |search_for_keys| flag in extract_and_populate_data, which defaults to True,
tells the extractor to search for HTML elements that look like keys but are not
in table cells, and converts those elements to cells. See _key_element_to_cell
for the HTML patterns we search over.
"""

import copy
import logging
from collections import defaultdict
from typing import Dict, Iterator, List, Optional, Set, Union

from lxml.html import HtmlElement, tostring

from recidiviz.ingest.extractor.data_extractor import DataExtractor
from recidiviz.ingest.models.ingest_info import IngestInfo


class HtmlDataExtractor(DataExtractor):
    """Data extractor for HTML pages."""

    def __init__(self, key_mapping_file):
        super().__init__(key_mapping_file)

        self.css_keys = self.manifest.get("css_key_mappings", {})
        self.keys_to_ignore = self.manifest.get("keys_to_ignore", [])

        self.keys.update(self.css_keys)

        self.all_keys = (
            set(self.keys.keys())
            | set(self.multi_keys.keys())
            | set(self.keys_to_ignore)
        )

    def _set_all_cells(self, content: HtmlElement, search_for_keys: bool) -> None:
        """Finds all leaf cells on a page and sets them.

        Args:
            content: the html_tree we are searching.
        """
        for key in self.keys.keys():
            if key in self.css_keys:
                self._css_key_to_cell(content, key)
            elif search_for_keys:
                self._convert_key_to_cells(content, key)

        all_cells = content.xpath("//*[self::th or self::td]")
        self.cells = [cell for cell in all_cells if self._is_leaf_cell(cell)]

    def _is_leaf_cell(self, e: HtmlElement) -> bool:
        """
        Args:
            e: an Element with tag 'th' or 'td'
        Returns:
            True if the Element does not contain any children with tag
            'th' or 'td'
        """
        return not e.findall(".//th") and not e.findall(".//td")

    # pylint: disable=arguments-differ
    def extract_and_populate_data(
        self,
        content: HtmlElement,
        ingest_info: IngestInfo = None,
        search_for_keys: bool = True,
    ) -> IngestInfo:
        """This function does all the work of taking the users yaml file
        and content and returning a populated data class.  This function
        iterates through every cell on the page and builds a model based on
        the keys that it sees.

        Args:
            content: An already parsed html data structure
            ingest_info: An IngestInfo object to use, if None we create a new
                one by default
            search_for_keys: Flag to allow searching for keys outside of
            table cells (<td> and <tr> elements).

        Returns:
            A populated ingest data model for a scrape.
        """
        content_copy = copy.deepcopy(content)
        HtmlDataExtractor._process_html(content_copy)
        self._set_all_cells(content_copy, search_for_keys)
        if ingest_info is None:
            ingest_info = IngestInfo()
        seen_map: Dict[int, Set[str]] = defaultdict(set)

        # We use this set to keep track of keys we have seen, by the end of this
        # function it should be the empty set.  If not we throw an error to let
        # the user know we have a problem.
        needed_keys = set(self.keys.keys()) | set(self.multi_keys.keys())

        for cell in self.cells:
            # This is a tiny hack to avoid an O(n) search over the keys list for
            # every cell.
            # An alternative approach is to force the user to give the exact key
            # with a semi colon in the yaml file, but that might be confusing.
            # Finally, we could preprocess the keys mapping to include multiple
            # keys that map to the same value ('hi' and 'hi:' both map to the
            # same thing) but that is a more expensive preprocessing calculation
            cell_val = self._normalize_cell(cell)
            lookup_keys = self.keys.get(cell_val) or self.multi_keys.get(cell_val)
            if not lookup_keys:
                # Users can specify a key with no value associated and then use
                # |get_value()| later. We shouldn't warn, even though we
                # won't find values for these keys.
                if cell_val in needed_keys:
                    needed_keys.remove(cell_val)
                continue
            values: List[Optional[str]] = []
            if cell_val in self.keys:
                values = [self._get_value_cell(cell)]
            elif cell_val in self.multi_keys:
                values = self._get_values_below_cell(cell)
            if values:
                self._set_or_create_object(ingest_info, lookup_keys, values, seen_map)
                if cell_val in needed_keys:
                    needed_keys.remove(cell_val)
        # If at the end of everything there are some keys we haven't found on
        # page we should complain.
        if needed_keys:
            logging.debug("The following keys could not be found: %s", needed_keys)
        return ingest_info.prune()

    def get_value(
        self, key: str, multiple: bool = False
    ) -> Union[List[Optional[str]], Optional[str]]:
        """
        This function iterates through every cell on the page, searching for a
        value that matches the provided |key|. Returns the string value if
        present, otherwise returns None

        Args:
            key: A key on the web page, whose value should be returned
            multiple: True if a list of values (below a cell) should be returned

        Returns:
            The found value(s) (if present), otherwise None.
        """
        for cell in self.cells:
            cell_val = self._normalize_cell(cell)
            if cell_val == key:
                if multiple:
                    return self._get_values_below_cell(cell)
                return self._get_value_cell(cell)
        return None

    @staticmethod
    def _process_html(content: HtmlElement) -> None:
        """Cleans up the provided content."""
        _remove_from_content(content, "//script")
        _remove_from_content(content, "//comment()")

        # Format line breaks as newlines
        for br in content.xpath("//br"):
            br.tail = "\n" + br.tail if br.tail else "\n"

    def _convert_key_to_cells(self, content: HtmlElement, key: str) -> None:
        """Searches for elements in |content| that match a |key| and converts
        those elements, along with their adjacent text, as table cells.

        Args:
            content: (HtmlElement) to be modified
            key: (string) to search for
        """
        matches = content.xpath(
            ".//*[starts-with("
            f'normalize-space(translate(text(),"\xA0"," ")),"{key}")]'
        )
        # results from the xpath call are references, so modifying them changes
        # |content|.
        for match in matches:
            # the xpath query above matches on links as well as regular html
            # elements. Therefore, we need to check text_content() as well as
            # text for the matching string.
            text = self._get_text_from_element(match)
            if text in self.keys_to_ignore:
                continue

            # Only convert elements that are not already table cells.
            if match.tag == "td" or match.tag == "th":
                continue
            # Ensure no individual words in |content| was split when matching.
            if text:
                remaining = " ".join(text.split()).replace(key, "")
                if not remaining or not remaining[0].isalpha():
                    if self._key_element_to_cell(key, match):
                        logging.debug(
                            "Converting element matching key %s: %s",
                            key,
                            tostring(match),
                        )

    def _css_key_to_cell(self, content: HtmlElement, css_key: str) -> None:
        matches = content.cssselect(css_key)

        for match in matches:
            logging.debug(
                "Adding cell [%s] which matches css key [%s]", tostring(match), css_key
            )
            key_cell = HtmlElement(css_key)
            key_cell.tag = "td"
            match.tag = "td"
            match.addprevious(key_cell)

    def _get_text_from_element(self, element: HtmlElement) -> Optional[str]:
        if element.text and element.text.strip():
            return element.text.strip()
        if element.text_content() and element.text_content().strip():
            return element.text_content().strip()
        return None

    def _key_element_to_cell(self, key: str, key_element: HtmlElement) -> bool:
        """Converts a |key_element| Element to a table cell and tries to modify
        the corresponding value to a cell.

        Args:
            key: (string) the key that |key_element| represents
            key_element: (HtmlElement) the element to be modified
        Returns:
            True if a modification was made and False otherwise.
        """

        # <foo><bar>key</bar>value</foo>
        # Create a new td element containing the following-sibling's text and
        # add it after the key cell.
        following_siblings = key_element.xpath("following-sibling::text()")
        if following_siblings:
            following_text = following_siblings[0].strip()
            if following_text:
                key_element.tag = "td"
                following_cell = HtmlElement(following_text)
                following_cell.tag = "td"
                key_element.addnext(following_cell)
                return True

        # <foo>key</foo><bar>value</bar>
        # The key and value are already adjacent, so just make them both cells.
        if key_element.getnext() is not None:
            key_element.tag = "td"
            key_element.getnext().tag = "td"
            return True

        # <foo><bar/><baz></baz>key: value</foo>
        # Create new td elements for the key and the value and insert them.
        for child in key_element:
            if child.tail and child.tail.startswith(key):
                if self._insert_cells_from_text(key, child.tail, key_element):
                    return True

        # <foo>key<bar>value</bar></foo>
        # Create a new td element containing the key and add it before the
        # value cell.
        if len(key_element) == 1:
            key_cell = HtmlElement(key)
            key_cell.tag = "td"
            value_cell = key_element[0]
            value_cell.tag = "td"
            value_cell.addprevious(key_cell)
            return True

        # <foo>key : value</foo>
        # Create new td elements for the key and the value and insert them.
        text = self._get_text_from_element(key_element)
        if text and text.startswith(key):
            if self._insert_cells_from_text(key, text, key_element):
                return True

        return False

    def _insert_cells_from_text(self, key: str, text: str, container) -> bool:
        """Given a |text| string in the format '<key>: <value>', inserts
        corresponding key/value td cells into |container|. Returns True if
        insertion is performed, False otherwise."""
        remaining = text[len(key) :].strip().strip(":").strip()
        if remaining:
            key_cell = HtmlElement(key)
            key_cell.tag = "td"
            value_cell = HtmlElement(remaining)
            value_cell.tag = "td"
            container.insert(0, key_cell)
            container.insert(1, value_cell)
            return True
        return False

    def _below(self, cell: HtmlElement) -> Iterator[HtmlElement]:
        """Yields all cells below the given cell and breaks if it finds a key.

        Args:
            cell: the <th> or <td> to traverse below
        Returns:
            a generator that yields the cells below |cell|
        """
        parent = cell.getparent()
        if parent is None:
            return
        index = parent.index(cell)
        next_row = parent.getnext()

        grand_parent = parent.getparent()
        # If |cell| is inside a <thead>, the |next_row| is inside a <tbody>.
        if grand_parent is not None and grand_parent.tag == "thead":
            tbody = grand_parent.getnext()
            if tbody is not None:
                next_row = next(tbody.iterchildren(), None)

        while next_row is not None:
            if next_row.tag == "tr" and index < len(next_row):
                if self._element_contains_key_descendant(next_row[index]):
                    break
                yield next_row[index]
            next_row = next_row.getnext()

    def _get_below(self, cell: HtmlElement) -> Optional[HtmlElement]:
        """Gets the cell below the given |cell|.

        Args:
            cell: the leaf cell we are analyzing
        Returns:
            The cell below or None.
        """
        return next(self._below(cell), None)

    def _get_all_below(self, cell: HtmlElement) -> List[HtmlElement]:
        """Gets all the cells below the given |cell|.

        Args:
            cell: the leaf cell we are analyzing
        Returns:
            The cells below or None.
        """
        return list(self._below(cell))

    def _get_value_cell(self, cell: HtmlElement) -> Optional[str]:
        """Tries to find a value of a given cell.

        Args:
            cell: the cell value we are trying to find

        Returns:
            A string representing the value of the cell, or None if it can't
            be found.
        """
        while cell is not None:
            adjacent_value = self._get_value_from_cell(cell)
            if adjacent_value is not None:
                return adjacent_value
            cell = cell.getparent()
        return None

    def _get_values_below_cell(self, cell: HtmlElement) -> List[Optional[str]]:
        """Tries to find a list of values given cell.

        Args:
            key: The cell we are trying to find.
        """
        while cell is not None:
            below_cells = self._get_all_below(cell)
            if below_cells:
                values = [cell.text_content().strip() for cell in below_cells]
                if values:
                    return values
            cell = cell.getparent()
        return []

    def _get_value_from_cell(self, cell: HtmlElement) -> Optional[str]:
        """Gets the value for a given |cell| by checking the cells to the right
        and below.

        Args:
            cell: an Element
        Returns:
            A str representing the value from an adjacent cell.
        """
        if cell is None:
            return None
        if cell.tag == "th":
            below_cell = self._get_below(cell)
            if below_cell is not None:
                below_text = below_cell.text_content().strip()
                if below_text:
                    return below_text

        right = cell.getnext()
        if self._is_viable(right):
            return right.text_content().strip()

        below = self._get_below(cell)
        if below is not None and self._is_viable(below):
            return below.text_content().strip()

        return None

    def _normalize_cell(self, cell: HtmlElement) -> str:
        """Given a cell, normalize the text content to compare to key mappings.

        Args:
            cell: the html element for a table cell.
        """
        return cell.text_content().strip().strip(":").strip()

    def _element_contains_key_descendant(self, e: HtmlElement) -> bool:
        """Returns True if Element |e| or a descendant has a key as its text
        content.

        Args:
            e: the Element to search in
        """
        for descendant in e.iter():
            if self._normalize_cell(descendant) in self.all_keys:
                return True
        return False

    def _is_viable(self, value_cell: HtmlElement) -> bool:
        """Returns True if the text in |value_cell| could be the value for a
        field. The text should be non-empty and the cell should not contain a
        key in any of its descendants.
        Args:
            value_cell: the candidate Element
        """
        if value_cell is None:
            return False
        value_text = value_cell.text_content().strip()
        if value_text is None:
            return False
        if self._element_contains_key_descendant(value_cell):
            return False
        return True


def _remove_from_content(content, xpath: str) -> None:
    for elem in content.xpath(xpath):
        parent = elem.getparent()
        if parent is not None:
            logging.debug("Removing <%s> element", elem.tag)
            parent.remove(elem)
