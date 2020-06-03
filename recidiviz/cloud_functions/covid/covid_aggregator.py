# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2020 Recidiviz, Inc.
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

"""Logic to combine the individual COVID data sources into a single output file
"""


import csv
import datetime
from io import StringIO
import logging
import re
import requests
import xlrd


# Source for facility info mapping
FACILITY_INFO_MAPPING_URL = 'https://docs.google.com/spreadsheets/d/e/2PACX-1vT95DUfwcHbauuuMScd1Jb9u3vLCdfCcieXrRthNowoSbrmeWF3ibv06LkfcDxl1Vd97S5aujvnHdZX/pub?gid=1112897899&single=true&output=csv' # pylint:disable=line-too-long

OUTPUT_DATE_FORMAT = '%Y-%m-%d'
PRISON_DATE_FORMAT = '%Y-%m-%d'
UCLA_DATE_FORMAT = '%m/%d/%Y'
RECIDIVIZ_DATE_FORMAT = '%m/%d/%Y'

# Date column values that mean a row has no date value
MISSING_DATE_VALUES = ['', 'NA']

# Column names in output file
DATE_COLUMN = 'Date'
FACILITY_TYPE_COLUMN = 'Facility Type'
STATE_COLUMN = 'State'
FACILITY_NAME_COLUMN = 'Canonical Facility Name'
POP_TESTED_COLUMN = 'Pop Tested'
POP_TESTED_POSITIVE_COLUMN = 'Pop Tested Positive'
POP_TESTED_NEGATIVE_COLUMN = 'Pop Tested Negative'
POP_DEATHS_COLUMN = 'Pop Deaths'
STAFF_TESTED_COLUMN = 'Staff Tested'
STAFF_TESTED_POSITIVE_COLUMN = 'Staff Tested Postitive'
STAFF_TESTED_NEGATIVE_COLUMN = 'Staff Tested Negative'
STAFF_DEATHS_COLUMN = 'Staff Deaths'
SOURCE_COLUMN = 'Source'
COMPILATION_COLUMN = 'Compilation'
NOTES_COLUMN = 'Notes'

OUTPUT_COLUMN_ORDER = [
    DATE_COLUMN,
    FACILITY_TYPE_COLUMN,
    STATE_COLUMN,
    FACILITY_NAME_COLUMN,
    POP_TESTED_COLUMN,
    POP_TESTED_POSITIVE_COLUMN,
    POP_TESTED_NEGATIVE_COLUMN,
    POP_DEATHS_COLUMN,
    STAFF_TESTED_COLUMN,
    STAFF_TESTED_POSITIVE_COLUMN,
    STAFF_TESTED_NEGATIVE_COLUMN,
    STAFF_DEATHS_COLUMN,
    SOURCE_COLUMN,
    COMPILATION_COLUMN,
    NOTES_COLUMN
]


def aggregate(prison_csv_reader, ucla_workbook, recidiviz_csv_reader):
    """Aggregates all COVID data source files into a single output file

    Args:
        prison_csv_reader: prison file as a CSV DictReader
        ucla_workbook: UCLA file as an XLRD Book
        recidiviz_csv_reader: Recidiviz file as a CSV DictReader
    """
    if not (prison_csv_reader and ucla_workbook and recidiviz_csv_reader):
        raise RuntimeError(
            ('COVID aggregator source missing: Prison - {}, UCLA - {}, '
             + 'Recidiviz - {}')
            .format(prison_csv_reader, ucla_workbook, recidiviz_csv_reader))

    prison_data = _parse_prison_csv(prison_csv_reader)
    ucla_data = _parse_ucla_workbook(ucla_workbook)
    recidiviz_data = _parse_recidiviz_csv(recidiviz_csv_reader)

    facility_info_mapping = _fetch_facility_info_mapping()

    mapped_prison_data = _map_by_canonical_facility_info(
        prison_data, facility_info_mapping)
    mapped_ucla_data = _map_by_canonical_facility_info(
        ucla_data, facility_info_mapping)
    mapped_recidiviz_data = _map_by_canonical_facility_info(
        recidiviz_data, facility_info_mapping)

    aggregated_data = _combine_by_facility({
        'covidprisondata.com': mapped_prison_data,
        'UCLA Law Behind Bars': mapped_ucla_data,
        'Recidiviz': mapped_recidiviz_data
        })
    aggregated_csv = _to_csv_string(aggregated_data)

    return aggregated_csv


def _parse_prison_csv(prison_csv_reader):
    """Parses the prison data CSV"""
    data = []

    for raw_row in prison_csv_reader:
        row = {k.strip(): v.strip() for (k, v) in raw_row.items()}

        date = row['scrape_date']
        # Rows with missing dates should be ignored, since they can't be used
        if date in MISSING_DATE_VALUES:
            continue
        formatted_date = datetime.datetime.strptime(
            date, PRISON_DATE_FORMAT).strftime(OUTPUT_DATE_FORMAT)

        # Extract subset of columns we care about.
        data_row = {
            DATE_COLUMN: formatted_date,
            STATE_COLUMN: row['state'],
            FACILITY_NAME_COLUMN: row['facilities'],
            POP_TESTED_COLUMN: row['inmates_tested'],
            POP_TESTED_POSITIVE_COLUMN: row['inmates_positive'],
            POP_TESTED_NEGATIVE_COLUMN: row['inmates_negative'],
            POP_DEATHS_COLUMN: row['inmates_deaths_confirmed'],
            STAFF_TESTED_COLUMN: row['staff_tested'],
            STAFF_TESTED_POSITIVE_COLUMN: row['staff_positive'],
            STAFF_TESTED_NEGATIVE_COLUMN: row['staff_negative'],
            STAFF_DEATHS_COLUMN: row['staff_deaths']
        }

        data.append(data_row)

    return data


def _parse_ucla_workbook(ucla_workbook):
    """Parses the UCLA data Excel workbook"""
    data_sheets = []
    for sheet in ucla_workbook.sheets():
        # Sheets with data in them (as opposed to summary sheets, etc.) have a
        # name format like "04.08.20" (with inconsistent zero-padding)
        format_match = re.search(r'[0-9]+\.[0-9]+\.[0-9]+', sheet.name)
        if format_match:
            data_sheets.append(sheet)
    if not data_sheets:
        raise RuntimeError(
            'No data sheets found in UCLA source file. The sheet naming ' \
                + 'format may have changed.')

    data = []

    for sheet in data_sheets:
        # An XLRD sheet doesn't have built-in support for accessing columns by
        # their header labels, so we have to find the indices for each column.
        # This needs to be done separately for each sheet, because the order
        # isn't fixed across all sheets.
        header_row = [cell.value for cell in sheet.row(0)]
        column_indices = {
            'Date': None,
            'State': None,
            'Name': None,
            'Staff Confirmed': None,
            'Residents confirmed': None,
            'Staff Deaths': None,
            'Resident Deaths': None,
            'Staff Tested': None,
            'Residents Tested': None,
            'Website': None,
            'Add\'l Notes': None
        }
        for index, value in enumerate(header_row):
            if value in column_indices:
                column_indices[value] = index

        # Start from 1 to skip header row
        for index in range(1, sheet.nrows):
            row = \
                [_get_excel_cell_string_value(
                    cell, ucla_workbook.datemode).strip()
                 for cell in sheet.row(index)]

            date = row[column_indices['Date']]
            # Rows with missing dates should be ignored, since they can't be
            # used
            if date in MISSING_DATE_VALUES:
                continue
            formatted_date = datetime.datetime.strptime(
                date, UCLA_DATE_FORMAT).strftime(OUTPUT_DATE_FORMAT)

            # Extract subset of columns we care about. Not all columns are
            # present on every sheet, so some values will be None.
            data_row = {
                DATE_COLUMN: formatted_date,
                STATE_COLUMN: _get_cell_value_if_present(
                    'State', column_indices, row),
                FACILITY_NAME_COLUMN: _get_cell_value_if_present(
                    'Name', column_indices, row),
                POP_TESTED_COLUMN:
                    _get_cell_value_if_present(
                        'Residents Tested', column_indices, row),
                POP_TESTED_POSITIVE_COLUMN:
                    _get_cell_value_if_present(
                        'Residents confirmed', column_indices, row),
                POP_DEATHS_COLUMN:
                    _get_cell_value_if_present(
                        'Resident Deaths', column_indices, row),
                STAFF_TESTED_COLUMN:
                    _get_cell_value_if_present(
                        'Staff Tested', column_indices, row),
                STAFF_TESTED_POSITIVE_COLUMN:
                    _get_cell_value_if_present(
                        'Staff Confirmed', column_indices, row),
                STAFF_DEATHS_COLUMN:
                    _get_cell_value_if_present(
                        'Staff Deaths', column_indices, row),
                SOURCE_COLUMN: _get_cell_value_if_present(
                    'Website', column_indices, row),
                NOTES_COLUMN: _get_cell_value_if_present(
                    'Add\'l Notes', column_indices, row),
            }

            data.append(data_row)

    return data


def _parse_recidiviz_csv(recidiviz_csv_reader):
    """Parses the Recidiviz data CSV"""
    data = []

    for raw_row in recidiviz_csv_reader:
        row = {k.strip(): v.strip() for (k, v) in raw_row.items()}

        date = row['As of...? (Date)']
        # Rows with missing dates should be ignored, since they can't be used
        if date in MISSING_DATE_VALUES:
            continue
        formatted_date = datetime.datetime.strptime(
            date, RECIDIVIZ_DATE_FORMAT).strftime(OUTPUT_DATE_FORMAT)

        # Extract subset of columns we care about.
        data_row = {
            DATE_COLUMN: formatted_date,
            FACILITY_TYPE_COLUMN: row['Facility Type'],
            STATE_COLUMN: row['State'],
            FACILITY_NAME_COLUMN: row['Facility'],
            POP_TESTED_COLUMN: row['Population Tested'],
            POP_TESTED_POSITIVE_COLUMN: row['Population Tested Positive'],
            POP_TESTED_NEGATIVE_COLUMN: row['Population Tested Negative'],
            POP_DEATHS_COLUMN: row['Population Deaths'],
            STAFF_TESTED_COLUMN: row['Staff Tested'],
            STAFF_TESTED_POSITIVE_COLUMN: row['Staff Tested Positive'],
            STAFF_TESTED_NEGATIVE_COLUMN: row['Staff Tested Negative'],
            STAFF_DEATHS_COLUMN: row['Staff Deaths'],
            SOURCE_COLUMN: row['Source'],
            NOTES_COLUMN: row['Notes']
        }

        data.append(data_row)

    return data


def _fetch_facility_info_mapping():
    """Fetches facility name mappings from remote source"""
    response = requests.get(FACILITY_INFO_MAPPING_URL)
    csv_lines = response.content.decode('utf-8').splitlines()
    csv_reader = csv.reader(csv_lines, delimiter=',')

    mapping = FacilityInfoMapping()

    for raw_row in csv_reader:
        row = [cell.strip() for cell in raw_row]
        facility_type = row[0]
        state = row[1]
        canonical_facility_name = row[3]

        # Add a facility info entry for all names included in source file.
        # Index 3 is the canonical name, and all columns from index 4 onward are
        # alternate names.
        for name in row[3:]:
            if name:
                mapping.add_facility(
                    state, name, canonical_facility_name, facility_type)
                # Since some data sources treat "Federal" as an additional
                # possible value for the state field, include a second entry
                # for federal facilities with a state value of "Federal" to
                # cover those cases.
                if facility_type == 'Federal Prisons':
                    mapping.add_facility(
                        'Federal', name, canonical_facility_name, facility_type)

    return mapping


def _map_by_canonical_facility_info(source_data, facility_info_mapping):
    """Overwrites facility info with canonical facility info (if available) and
    converts to a map keyed by facility info
    """
    mapped_data = {}

    for row in source_data:
        facility_name = row[FACILITY_NAME_COLUMN]
        state = row[STATE_COLUMN]
        # Every source includes name and state, but not every source includes
        # type
        facility_type = row.get(FACILITY_TYPE_COLUMN, 'Unknown Facility Type')

        if facility_info_mapping.contains(state, facility_name):
            facility_name = facility_info_mapping.get_canonical_facility_name(
                state, facility_name)
            facility_type = facility_info_mapping.get_facility_type(
                state, facility_name)
        else:
            logging.warning(
                'No facility info for %s, %s', state, facility_name)

        # Copy row and overwrite facility name and type with canonical values
        # before setting row key
        output_row = dict(row)
        output_row[FACILITY_NAME_COLUMN] = facility_name
        output_row[FACILITY_TYPE_COLUMN] = facility_type

        row_key = _row_key(output_row[DATE_COLUMN],
                           output_row[STATE_COLUMN],
                           output_row[FACILITY_NAME_COLUMN])
        mapped_data[row_key] = output_row

    return mapped_data


def _combine_by_facility(sources):
    """Creates an aggregated data set by taking the supserset of all facilities
    present in all sources and combining the available data for each facility
    """
    all_keys = set()
    for source in sources.values():
        for key in source:
            all_keys.add(key)
    # Because of the choice of output date format and key structure, sorting by
    # key will conveniently sort by date, state, and facility, in that order.
    # This is obviously brittle, so this sort will need to be made more
    # careful if either the date format or key structure needs to be changed.
    sorted_keys = sorted(all_keys)

    aggregated_data = []

    header_row = list(OUTPUT_COLUMN_ORDER)
    aggregated_data.append(header_row)

    for key in sorted_keys:
        # The key fields and facility info fields will be the same for any
        # sources in which the key is present, so the first non-null value can
        # be used.
        date = _get_first_non_null(key, DATE_COLUMN, sources)
        facility_type = _get_first_non_null(key, FACILITY_TYPE_COLUMN, sources)
        state = _get_first_non_null(key, STATE_COLUMN, sources)
        facility_name = _get_first_non_null(key, FACILITY_NAME_COLUMN, sources)

        source = _combine_non_null_text(key, SOURCE_COLUMN, sources)
        notes = _combine_non_null_text(key, NOTES_COLUMN, sources)

        # Create initial row data, so numeric and compilation values will have
        # a place to go.
        column_values = {
            DATE_COLUMN: date,
            FACILITY_TYPE_COLUMN: facility_type,
            STATE_COLUMN: state,
            FACILITY_NAME_COLUMN: facility_name,
            SOURCE_COLUMN: source,
            NOTES_COLUMN: notes
        }

        # Easier to handle the numeric fields in a loop, since we also have to
        # keep track of the source for all of them.
        numeric_columns = [
            POP_TESTED_COLUMN,
            POP_TESTED_POSITIVE_COLUMN,
            POP_TESTED_NEGATIVE_COLUMN,
            POP_DEATHS_COLUMN,
            STAFF_TESTED_COLUMN,
            STAFF_TESTED_POSITIVE_COLUMN,
            STAFF_TESTED_NEGATIVE_COLUMN,
            STAFF_DEATHS_COLUMN
        ]
        numeric_value_sources = set()
        for column in numeric_columns:
            # For all numeric fields, the assumption is that the largest value
            # was obtained last on the given date and so should be the most
            # up-to-date value.
            value, value_source = _get_max(key, column, sources)
            # Always include value even if it's null, to ensure all required
            # columns are present.
            column_values[column] = value
            if value:
                numeric_value_sources.add(value_source)

        # Compilation column is comma-joined list of all sources from which at
        # least one numeric value was taken. The list is sorted to enable
        # easier comparison in the output data.
        column_values[COMPILATION_COLUMN] = \
            ', '.join(sorted(numeric_value_sources))

        row = [column_values[column] for column in OUTPUT_COLUMN_ORDER]
        aggregated_data.append(row)

    return aggregated_data


def _to_csv_string(rows):
    """Converts list of lists to a CSV string"""
    string_buffer = StringIO()
    csv_writer = csv.writer(string_buffer)
    csv_writer.writerows(rows)
    return string_buffer.getvalue()


def _row_key(date, state, facility_name):
    """Key format used for matching rows that represent the same facility on the
    same date
    """
    return '{}:{}:{}'.format(date, state, facility_name)


def _get_first_non_null(key, column, sources):
    """Returns first occurence of a non-null value in the provided column for a
    row with the provided key in any source
    """
    for source in sources.values():
        if key in source and column in source[key] and source[key][column]:
            return source[key][column]
    return None


def _get_max(key, column, sources):
    """Returns max value and name of max value source over all occurences of a
    value in the provided column for a row with the provided key in all sources
    """
    current_max = None
    current_max_source = None
    for source_name, source in sources.items():
        if key in source and column in source[key] and source[key][column]:
            value = None
            try:
                value = int(source[key][column])
            except ValueError:
                continue

            if not current_max or value > current_max:
                current_max = value
                current_max_source = source_name
    return current_max, current_max_source


def _combine_non_null_text(key, column, sources):
    """Returns comma-joined string of all non-null occurences of a value in the
    provided column for a row with the provided key in all sources
    """
    values = []
    for source in sources.values():
        if key in source and column in source[key] and source[key][column]:
            values.append(source[key][column])
    return ', '.join(values)


def _get_excel_cell_string_value(cell, workbook_date_mode):
    """Converts the value in an Excel cell to its corresponding string
    representation
    """
    cell_type = cell.ctype
    if cell_type == 0: # Empty cell
        return ''
    if cell_type == 1: # Text
        return cell.value
    if cell_type == 2: # Number
        return str(cell.value)
    if cell_type == 3: # Date
        # Note that this truncates any datetime values to the immediately
        # preceding integer day value
        date_fields = xlrd.xldate.xldate_as_tuple(
            cell.value, workbook_date_mode)
        date_value = datetime.date(
            date_fields[0], date_fields[1], date_fields[2])
        return date_value.strftime(UCLA_DATE_FORMAT)
    if cell_type == 4: # Boolean
        return str(cell.value == 1)
    if cell_type == 5: # Error
        return xlrd.biffh.error_text_from_code[cell.value]
    if cell_type == 6: # Blank, which is somehow different from empty
        return ''
    raise RuntimeError('Unrecognized cell type {} with value {}'
                       .format(cell_type, cell.value))


def _get_cell_value_if_present(column_label, column_indices, row):
    """Returns the value of the cell in the provided column of the row, if the
    column is present
    """
    # Column label will always be present in map. If column is not present in
    # row, the label will be mapped to None.
    if not column_indices[column_label]:
        return None
    return row[column_indices[column_label]]


class FacilityInfoMapping:
    """Map from state and facility name to info on facility"""

    def __init__(self):
        self._map = {}

    def add_facility(
            self, state, facility_name, canonical_facility_name, facility_type):
        self._map[self._key(state, facility_name)] = \
            (canonical_facility_name, facility_type)

    def get_canonical_facility_name(self, state, facility_name):
        return self._map[self._key(state, facility_name)][0]

    def get_facility_type(self, state, facility_name):
        return self._map[self._key(state, facility_name)][1]

    def contains(self, state, facility_name):
        return self._key(state, facility_name) in self._map

    def _key(self, state, facility_name):
        return '{}:{}'.format(
            state.strip().lower(), facility_name.strip().lower())
