# Recidiviz - a platform for tracking granular recidivism metrics in real time
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

"""A module which takes any content and extracts from it key/value pairs that
a user might care about.  The extracted information is put into the ingest data
model and returned.
"""

import copy
import warnings
from lxml.html import HtmlElement

from recidiviz.ingest.extractor.data_extractor import DataExtractor
from recidiviz.ingest.models.ingest_info import IngestInfo


class HtmlDataExtractor(DataExtractor):
    """Data extractor for HTML pages."""

    def __init__(self, key_mapping_file=None):
        super().__init__(key_mapping_file)

        self.css_keys = self.manifest.get('css_key_mappings', {})
        keys_to_ignore = self.manifest.get('keys_to_ignore', [])

        self.keys.update(self.css_keys)

        self.all_keys = set(self.keys.keys()) | \
            set(self.multi_keys.keys()) | set(keys_to_ignore)

    def _set_all_cells(self, content):
        """Finds all leaf cells on a page and sets them.

        Args:
            content: the html_tree we are searching.
        """
        for key in self.keys.keys():
            if key in self.css_keys:
                self._css_key_to_cell(content, key)
            else:
                self._convert_key_to_cells(content, key)

        all_cells = content.xpath('//*[self::th or self::td]')
        self.cells = filter(self._is_leaf_cell, all_cells)

    def _is_leaf_cell(self, e):
        """
        Args:
            e: an Element with tag 'th' or 'td'
        Returns:
            True if the Element does not contain any children with tag
            'th' or 'td'
        """
        return not e.findall('.//th') and not e.findall('.//td')

    def extract_and_populate_data(self, content, ingest_info=None):
        """This function does all the work of taking the users yaml file
        and content and returning a populated data class.  This function
        iterates through every cell on the page and builds a model based on
        the keys that it sees.

        Args:
            content: An already parsed html data structure
            ingest_info: An IngestInfo object to use, if None we create a new
                one by default

        Returns:
            A populated ingest data model for a scrape.
        """
        content_copy = copy.deepcopy(content)
        HtmlDataExtractor._process_html(content_copy)
        self._set_all_cells(content_copy)
        if ingest_info is None:
            ingest_info = IngestInfo()

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
            values = None
            if cell_val in self.keys:
                values = [self._get_value_cell(cell)]
            elif cell_val in self.multi_keys:
                values = self._get_values_below_cell(cell)
            if values:
                lookup_keys = (self.keys.get(cell_val) or
                               self.multi_keys.get(cell_val))
                self._set_or_create_object(ingest_info, lookup_keys, values)
                if cell_val in needed_keys:
                    needed_keys.remove(cell_val)
        # If at the end of everything there are some keys we haven't found on
        # page we should complain.
        if needed_keys:
            # TODO 183: actually have real warning codes
            warnings.warn("The following keys could not be found: %s" %
                          needed_keys)
        return ingest_info.prune()

    @staticmethod
    def _process_html(content):
        """Cleans up the provided content."""
        # Remove <script> elements
        for script in content.xpath('//script'):
            parent = script.getparent()
            if parent is not None:
                parent.remove(script)

        # Format line breaks as newlines
        for br in content.xpath('//br'):
            br.tail = '\n' + br.tail if br.tail else '\n'

    def _convert_key_to_cells(self, content, key):
        """Searches for elements in |content| that match a |key| and converts
        those elements, along with their adjacent text, as table cells.

        Args:
            content: (HtmlElement) to be modified
            key: (string) to search for
        """
        matches = content.xpath(
            './/*[starts-with(normalize-space(text()),"%s")]' % key)
        # results from the xpath call are references, so modifying them changes
        # |content|.
        for match in matches:
            # Only convert elements that are not already table cells.
            if match.tag == 'td' or match.tag == 'th':
                continue
            # Ensure no individual words in |content| was split when matching.
            remaining = ' '.join(match.text.split()).replace(key, '')
            if not remaining or not remaining[0].isalpha():
                self._key_element_to_cell(key, match)

    def _css_key_to_cell(self, content, css_key):
        matches = content.cssselect(css_key)

        for match in matches:
            key_cell = HtmlElement(css_key)
            key_cell.tag = 'td'
            match.tag = 'td'
            match.addprevious(key_cell)

    def _key_element_to_cell(self, key, key_element):
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
        following_siblings = key_element.xpath('following-sibling::text()')
        if following_siblings:
            following_text = following_siblings[0].strip()
            if following_text:
                key_element.tag = 'td'
                following_cell = HtmlElement(following_text)
                following_cell.tag = 'td'
                key_element.addnext(following_cell)
                return True

        # <foo>key</foo><bar>value</bar>
        # The key and value are already adjacent, so just make them both cells.
        if key_element.getnext() is not None:
            key_element.tag = 'td'
            key_element.getnext().tag = 'td'
            return True

        # <foo>key<bar>value</bar></foo>
        # Create a new td element containing the key and add it before the
        # value cell.
        if len(key_element) == 1:
            key_cell = HtmlElement(key)
            key_cell.tag = 'td'
            value_cell = key_element[0]
            value_cell.tag = 'td'
            value_cell.addprevious(key_cell)
            return True

        # <foo>key : value</foo>
        # Create new td elements for the key and the value and insert them.
        text = key_element.text.strip()
        if text.startswith(key):
            text = text[len(key):].strip().strip(':').strip()
            if text != '':
                key_cell = HtmlElement(key)
                key_cell.tag = 'td'
                value_cell = HtmlElement(text)
                value_cell.tag = 'td'
                key_element.insert(0, key_cell)
                key_element.insert(1, value_cell)
                return True

        return False

    def _below(self, cell):
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
        if grand_parent is not None and grand_parent.tag == 'thead':
            next_row = grand_parent.getnext()[0]

        while next_row is not None:
            if next_row.tag == 'tr' and index < len(next_row):
                if self._element_contains_key_descendant(next_row[index]):
                    break
                yield next_row[index]
            next_row = next_row.getnext()

    def _get_below(self, cell):
        """Gets the cell below the given |cell|.

        Args:
            cell: the leaf cell we are analyzing
        Returns:
            The cell below or None.
        """
        return next(self._below(cell), None)

    def _get_all_below(self, cell):
        """Gets all the cells below the given |cell|.

        Args:
            cell: the leaf cell we are analyzing
        Returns:
            The cells below or None.
        """
        return list(self._below(cell))

    def _get_value_cell(self, cell):
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

    def _get_values_below_cell(self, cell):
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

    def _get_value_from_cell(self, cell):
        """Gets the value for a given |cell| by checking the cells to the right
        and below.

        Args:
            cell: an Element
        Returns:
            A str representing the value from an adjacent cell.
        """
        if cell is None:
            return None
        if cell.tag == 'th':
            below_cell = self._get_below(cell)
            if below_cell is not None:
                below_text = below_cell.text_content().strip()
                if below_text:
                    return below_text

        right = cell.getnext()
        if self._is_viable(right):
            return right.text_content().strip()

        below = self._get_below(cell)
        if self._is_viable(below):
            return below.text_content().strip()

        return None

    def _normalize_cell(self, cell):
        """ Given a cell, normalize the text content to compare to key mappings.

        Args:
            cell: the html element for a table cell.
        """
        return cell.text_content().strip().strip(':').strip()

    def _element_contains_key_descendant(self, e):
        """Returns True if Element |e| or a descendant has a key as its text
        content.

        Args:
            e: the Element to search in
        """
        for descendant in e.iter():
            if self._normalize_cell(descendant) in self.all_keys:
                return True
        return False

    def _is_viable(self, value_cell):
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
