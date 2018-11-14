# Recidiviz - a platform for tracking granular recidivism metrics in real time
# Copyright (C) 2018 Recidiviz, Inc.
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

TODO: Add more details here in implementation logic when it is done.
"""

import itertools
import yaml

from recidiviz.ingest.models.ingest_info import IngestInfo


class DataExtractor(object):
    """Data extractor for pages with information about a single person."""
    def __init__(self, key_mapping_file=None):
        """The init of the data extractor.

        Args:
            key_mapping_file: a yaml file defining the mappings that the
                data extractor uses to find relevant keys.
        """
        if key_mapping_file:
            with open(key_mapping_file, 'r') as ymlfile:
                manifest = yaml.load(ymlfile)
            key_mappings = manifest['key_mappings']
            keys_to_ignore = []
            multi_key_mapping = {}
            if 'keys_to_ignore' in manifest:
                keys_to_ignore = manifest['keys_to_ignore']
            if 'multi_key_mapping' in manifest:
                multi_key_mapping = manifest['multi_key_mapping']
            self.initialize_extractor(
                key_mappings, multi_key_mapping, keys_to_ignore)

    def initialize_extractor(self, keys, multi_keys, ignored_keys):
        """Initializes the data extractor with the keys that we want to
        search for in the html content.

        Args:
            keys: a dict mapping the keys on the page to our internal keys.
            multi_keys: a dict mapping the keys on the page that have multiple
                values to the internal keys
            ignored_keys: keys that exist on the page that we ignore
        """
        self.keys = {k.strip():v for k, v in keys.iteritems()}
        self.multi_keys = {k.strip():v for k, v in multi_keys.iteritems()}
        self.all_keys = self.keys.keys() + self.multi_keys.keys() + ignored_keys

    def _set_all_cells(self, content):
        """Finds all leaf cells on a page and sets them.

        Args:
            content: the html_tree we are searching.
        """
        all_cells = itertools.chain(
            content.findall('.//th'), content.findall('.//td'))
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
        and content and returning a populated data class.

        Args:
            content: An already parsed html data structure
            ingest_info: An IngestInfo object to use, if None we create a new
                one by default

        Returns:
            A populated ingest data model for a scrape.
        """
        self._set_all_cells(content)
        if not ingest_info:
            ingest_info = IngestInfo()

        for key, ingest_key in self.keys.iteritems():
            value = self.get_value(key)
            self._set_value(ingest_info, ingest_key, value)

        charges = self._extract_rows() if self.multi_keys else []

        if not ingest_info.people[0].bookings:
            ingest_info.people[0].create_booking()

        booking = ingest_info.people[0].bookings[0]

        for charge_dict in charges:
            charge = booking.create_charge()
            self._populate_charge(charge, charge_dict)

        return ingest_info

    def _extract_rows(self):
        """Extracts values from |self.multi_keys|. Should only be called from
        |extract_and_populate_data| since that method sets up parsing the
        content. Raises a ValueError if the keys do not all have the same
        number of rows.

        Returns:
            A list of dictionaries, where each dictionary has keys from the
            schema that map to values.
        """
        lists_of_values = []
        for key, schema_key in self.multi_keys.iteritems():
            values_below = self._get_values_below(key)
            values = [
                {schema_key: value} for value in values_below
            ]
            lists_of_values.append(values)

        charges = []
        lengths = set(len(l) for l in lists_of_values)
        # The charges columns should all have the same length
        if len(lengths) == 1:
            for i in range(lengths.pop()):
                charge = {}
                for field_list in lists_of_values:
                    charge.update(field_list[i])
                charges.append(charge)
        else:
            raise ValueError('the keys from |multi_key_mapping| do not all ' +
                             'have the same number of values.')

        return charges

    def _set_value(self, ingest_info, ingest_key, value):
        """This function intelligently sets the value in the ingest_info
        object if it can.

        Args:
            ingest_info: The ingest_info object to set the value on.
            ingest_key: The key we wish to set.
            value: The value to set the key to
        """
        value_classes = ingest_key.split('.')
        ingest_key = value_classes[1]
        class_to_set = value_classes[0]
        if class_to_set == 'person':
            # Create a person if one has not been created.
            if not ingest_info.people:
                ingest_info.create_person()
            obj = ingest_info.people[0]
        elif class_to_set == 'booking':
            # Create a booking if needed.
            if not ingest_info.people:
                ingest_info.create_person()
            if not ingest_info.people[0].bookings:
                ingest_info.people[0].create_booking()
            obj = ingest_info.people[0].bookings[0]
        elif class_to_set == 'arrest':
            if not ingest_info.people:
                ingest_info.create_person()
            if not ingest_info.people[0].bookings:
                ingest_info.people[0].create_booking()
            if not ingest_info.people[0].bookings[0].arrest:
                ingest_info.people[0].bookings[0].create_arrest()
            obj = ingest_info.people[0].bookings[0].arrest
        else:
            raise ValueError("can't set value on unknown class %s"
                             % class_to_set)

        if not hasattr(obj, ingest_key):
            raise AttributeError("can't set unknown value %s on class %s"
                                 % (ingest_key, class_to_set))

        setattr(obj, ingest_key, value)

    def _populate_charge(self, charge, charge_dict):
        bond = charge.create_bond()
        for key, value in charge_dict.iteritems():
            class_to_set, ingest_key = key.split('.')
            if class_to_set == 'charge':
                obj = charge
            elif class_to_set == 'bond':
                obj = bond
            else:
                raise ValueError("can't set value on unknown class '%s'"
                                 % class_to_set)

            if not hasattr(obj, ingest_key):
                raise AttributeError("can't set unknown value '%s' on class %s"
                                     % (ingest_key, class_to_set))
            setattr(obj, ingest_key, value)

    def _find_cell(self, key):
        """
        Args:
            key: str
        Returns:
            The first cell Element that matches the given |key|
        """
        for cell in self.cells:
            if key in cell.text_content().strip():
                #TODO: handle multiple key cells
                return cell
        raise ValueError('couldn\'t find key: %s' % key)

    def _below(self, cell):
        """Yields the cells below the given cell.

        Args:
            cell: the <th> or <td> to traverse below
        Returns:
            a generator that yields the cells below |cell|
        """
        parent = cell.getparent()
        index = parent.index(cell)
        next_row = parent.getnext()

        grand_parent = parent.getparent()
        # If |cell| is inside a <thead>, the |next_row| is inside a <tbody>.
        if grand_parent is not None and grand_parent.tag == 'thead':
            next_row = grand_parent.getnext()[0]

        while next_row is not None:
            if next_row.tag == 'tr' and index < len(next_row):
                yield next_row[index]
            next_row = next_row.getnext()

    def _get_below(self, cell):
        """Gets the cell below the given |cell|.

        Args:
            cell: the leaf cell we are analyzing
        Returns:
            The cell below or None.
        """
        return self._below(cell).next()

    def _get_all_below(self, cell):
        """Gets all the cells below the given |cell|.

        Args:
            cell: the leaf cell we are analyzing
        Returns:
            The cells below or None.
        """
        return list(self._below(cell))

    def get_value(self, key):
        """Tries to find a value of a given key.

        Args:
            key: the key we are trying to find

        Returns:
            A string representing the value of the key, or None if it can't
            be found.
        """
        cell = self._find_cell(key)
        while cell is not None and key in cell.text_content().strip():
            adjacent_value = self._get_value_from_cell(cell)
            if adjacent_value:
                return adjacent_value
            cell = cell.getparent()
        return None

    def _get_values_below(self, key):
        """Tries to find a list of values given key.

        Args:
            key: The key we are trying to find.
        """
        cell = self._find_cell(key)
        while cell is not None and cell.text_content().strip() == key:
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
            # TODO(#181): fall back to manual search for this value
            return None
        if cell.tag == 'th':
            below_text = self._get_below(cell).text_content().strip()
            if below_text:
                return below_text

        right = cell.getnext()
        if self._is_viable(right):
            return right.text_content().strip()

        below = self._get_below(cell)
        if self._is_viable(below):
            return below.text_content().strip()

        return None

    def _element_contains_key_descendant(self, e):
        """Returns True if Element |e| or a descendant has a key as its text
        content.

        Args:
            e: the Element to search in
        """
        for descendant in e.iter():
            if descendant.text_content().strip() in self.all_keys:
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
