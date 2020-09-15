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


OUTPUT_DATE_FORMAT = '%Y-%m-%d'
PRISON_DATE_FORMAT = '%Y-%m-%d'
UCLA_DATE_FORMAT = '%m/%d/%Y'
RECIDIVIZ_DATE_FORMAT = '%m/%d/%Y'

# Date column values that mean a row has no date value
MISSING_DATE_VALUES = ['', 'NA']

# Column names in output file
DATE_COLUMN = 'date'
FACILITY_TYPE_COLUMN = 'facility_type'
STATE_COLUMN = 'location_state'
FACILITY_NAME_COLUMN = 'facility_name'
POP_TESTED_COLUMN = 'pop_tested_to_date'
POP_TESTED_POSITIVE_COLUMN = 'pop_positives_to_date'
POP_TESTED_NEGATIVE_COLUMN = 'pop_negatives_to_date'
POP_PENDING_COLUMN = 'pop_pending'
POP_DEATHS_COLUMN = 'pop_deaths_to_date'
POP_ACTIVE_CASES_COLUMN = 'pop_active_cases'
POP_RECOVERED_CASES_COLUMN = 'pop_recovered_cases'
STAFF_TESTED_COLUMN = 'staff_tested_to_date'
STAFF_TESTED_POSITIVE_COLUMN = 'staff_positives_to_date'
STAFF_TESTED_NEGATIVE_COLUMN = 'staff_negatives_to_date'
STAFF_PENDING_COLUMN = 'staff_pending'
STAFF_DEATHS_COLUMN = 'staff_deaths_to_date'
STAFF_ACTIVE_CASES_COLUMN = 'staff_active_cases'
STAFF_RECOVERED_CASES_COLUMN = 'staff_recovered_cases'
SOURCE_COLUMN = 'source'
COMPILATION_COLUMN = 'compilation'
NOTES_COLUMN = 'notes'
AGGREGATION_NOTES_COLUMN = 'aggregation_notes'

OUTPUT_COLUMN_ORDER = [
    DATE_COLUMN,
    FACILITY_TYPE_COLUMN,
    STATE_COLUMN,
    FACILITY_NAME_COLUMN,
    POP_TESTED_COLUMN,
    POP_TESTED_POSITIVE_COLUMN,
    POP_TESTED_NEGATIVE_COLUMN,
    POP_DEATHS_COLUMN,
    POP_ACTIVE_CASES_COLUMN,
    POP_RECOVERED_CASES_COLUMN,
    STAFF_TESTED_COLUMN,
    STAFF_TESTED_POSITIVE_COLUMN,
    STAFF_TESTED_NEGATIVE_COLUMN,
    STAFF_DEATHS_COLUMN,
    STAFF_ACTIVE_CASES_COLUMN,
    STAFF_RECOVERED_CASES_COLUMN,
    SOURCE_COLUMN,
    COMPILATION_COLUMN,
    NOTES_COLUMN,
    AGGREGATION_NOTES_COLUMN
]

DEATH_SUMMED_NOTE = f'{POP_DEATHS_COLUMN} calculated as sum of both probable and confirmed deaths'

POP_TESTED_THREE_OPERANDS_NOTE = \
    (f'{POP_TESTED_COLUMN} calculated as sum of {POP_TESTED_POSITIVE_COLUMN}, {POP_TESTED_NEGATIVE_COLUMN}, and '
     f'{POP_PENDING_COLUMN} (not reported in output dataset)')

STAFF_TESTED_THREE_OPERANDS_NOTE = \
    (f'{STAFF_TESTED_COLUMN} calculated as sum of {STAFF_TESTED_POSITIVE_COLUMN}, {STAFF_TESTED_NEGATIVE_COLUMN}, and '
     f'{STAFF_PENDING_COLUMN} (not reported in output dataset)')

POP_TESTED_TWO_OPERANDS_NOTE = \
    f'{POP_TESTED_COLUMN} calculated as sum of {POP_TESTED_POSITIVE_COLUMN} and {POP_TESTED_NEGATIVE_COLUMN}'

STAFF_TESTED_TWO_OPERANDS_NOTE = \
    f'{STAFF_TESTED_COLUMN} calculated as sum of {STAFF_TESTED_POSITIVE_COLUMN} and {STAFF_TESTED_NEGATIVE_COLUMN}'

POP_NEGATIVES_THREE_OPERANDS_NOTE = \
    (f'{POP_TESTED_NEGATIVE_COLUMN} calculated as difference between {POP_TESTED_COLUMN} and sum of '
     f'{POP_TESTED_POSITIVE_COLUMN} and {POP_PENDING_COLUMN}')

STAFF_NEGATIVES_THREE_OPERANDS_NOTE = \
    (f'{STAFF_TESTED_NEGATIVE_COLUMN} calculated as difference between {STAFF_TESTED_COLUMN} and sum of '
     f'{STAFF_TESTED_POSITIVE_COLUMN} and {STAFF_PENDING_COLUMN}')

POP_NEGATIVES_TWO_OPERANDS_NOTE = \
    (f'{POP_TESTED_NEGATIVE_COLUMN} calculated as difference between {POP_TESTED_COLUMN} and '
     f'{POP_TESTED_POSITIVE_COLUMN}')

STAFF_NEGATIVES_TWO_OPERANDS_NOTE = \
    (f'{STAFF_TESTED_NEGATIVE_COLUMN} calculated as difference between {STAFF_TESTED_COLUMN} and '
     f'{STAFF_TESTED_POSITIVE_COLUMN}')

POP_POSITIVE_FROM_ACTIVE_AND_RECOVERED_NOTE = \
    f'{POP_TESTED_POSITIVE_COLUMN} calculated as sum of {POP_ACTIVE_CASES_COLUMN} and {POP_RECOVERED_CASES_COLUMN}'

STAFF_POSITIVE_FROM_ACTIVE_AND_RECOVERED_NOTE = \
    (f'{STAFF_TESTED_POSITIVE_COLUMN} calculated as sum of {STAFF_ACTIVE_CASES_COLUMN} and '
     f'{STAFF_RECOVERED_CASES_COLUMN}')


def aggregate(prison_csv_dicts,
              ucla_workbook,
              recidiviz_csv_dict,
              facility_mapping_csv):
    """Aggregates all COVID data source files into a single output file

    Args:
        prison_csv_dicts: list of CSV DictReaders, one for each prison file
        ucla_workbook: UCLA file as an XLRD Book
        recidiviz_csv_dict: Recidiviz file as a CSV DictReader
        facility_mapping_csv: facility mapping file as a CSV reader
    """
    if not (prison_csv_dicts
            and ucla_workbook
            and recidiviz_csv_dict
            and facility_mapping_csv):
        raise RuntimeError(
            ('COVID aggregator source missing: prison - {}, UCLA - {}, '
             + 'Recidiviz - {}, facility mapping - {}')
            .format(prison_csv_dicts,
                    ucla_workbook,
                    recidiviz_csv_dict,
                    facility_mapping_csv))

    prison_data = _parse_prison_csvs(prison_csv_dicts)
    ucla_data = _parse_ucla_workbook(ucla_workbook)
    recidiviz_data = _parse_recidiviz_csv(recidiviz_csv_dict)

    facility_info_mapping = _parse_facility_mapping_csv(
        facility_mapping_csv)

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

    amended_data = _amend_data(aggregated_data)
    formatted_output = _format_output(amended_data)
    return _to_csv_string(formatted_output)


def _parse_prison_csvs(prison_csv_dicts):
    """Parses all prison data CSVs"""
    data = []

    # Each prison file is a non-overlapping set of data, so we can just iterate
    # through them all sequentially and treat them like one large file.
    for prison_csv_dict in prison_csv_dicts:
        for raw_row in prison_csv_dict:
            row = {k.strip(): v.strip() for (k, v) in raw_row.items()}

            date = row['scrape_date']
            # Rows with missing dates should be ignored, since they can't be
            # used
            if date in MISSING_DATE_VALUES:
                continue
            # Two different date formats are used, depending on the file
            formatted_date = None
            try:
                formatted_date = datetime.datetime.strptime(
                    date, PRISON_DATE_FORMAT).strftime(OUTPUT_DATE_FORMAT)
            except ValueError:
                try:
                    # Have to strip trailing Z, because fromisoformat can't
                    # parse it
                    formatted_date = datetime.datetime.fromisoformat(
                        date.strip('Z')).strftime(OUTPUT_DATE_FORMAT)
                except ValueError:
                    # Can't use row if date can't be parsed
                    logging.warning(
                        'Failed to parse date %s in prison file', date)
                    continue

            # Two different columns can correspond to deaths. Need to convert
            # them to ints so they can be summed.
            deaths = _int_or_none(row['inmates_deaths'])
            # inmates_deaths_confirmed is not present in some earlier files
            deaths_confirmed = _int_or_none(
                row.get('inmates_deaths_confirmed', None))
            pop_deaths = None
            aggregation_notes = None
            # Explicit None checks, since 0 is a valid value
            if deaths is not None and deaths_confirmed is not None:
                pop_deaths = deaths + deaths_confirmed
                aggregation_notes = DEATH_SUMMED_NOTE
            elif deaths_confirmed is not None:
                pop_deaths = deaths_confirmed
            elif deaths is not None:
                pop_deaths = deaths

            # Extract subset of columns we care about
            data_row = {
                DATE_COLUMN: formatted_date,
                STATE_COLUMN: row['state'],
                FACILITY_NAME_COLUMN: row['facilities'],
                POP_TESTED_COLUMN: row['inmates_tested'],
                POP_TESTED_POSITIVE_COLUMN: row['inmates_positive'],
                POP_TESTED_NEGATIVE_COLUMN: row['inmates_negative'],
                POP_PENDING_COLUMN: row['inmates_pending'],
                POP_DEATHS_COLUMN: pop_deaths,
                STAFF_TESTED_COLUMN: row['staff_tested'],
                STAFF_TESTED_POSITIVE_COLUMN: row['staff_positive'],
                STAFF_TESTED_NEGATIVE_COLUMN: row['staff_negative'],
                # Staff pending is not present in some earlier files
                STAFF_PENDING_COLUMN: row.get('staff_pending', None),
                STAFF_DEATHS_COLUMN: row['staff_deaths'],
                AGGREGATION_NOTES_COLUMN: aggregation_notes
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
            'Staff.Confirmed': None,
            'Residents.Confirmed': None,
            'Staff.Deaths': None,
            'Resident.Deaths': None,
            'Staff.Recovered': None,
            'Residents.Recovered': None,
            'Website': None,
            'Notes': None,
        }
        for index, value in enumerate(header_row):
            if value in column_indices:
                column_indices[value] = index

        # If none of the numeric columns were found, something has changed with
        # the column naming
        numeric_column_found = False
        for column_name in ['Staff.Confirmed',
                            'Residents.Confirmed',
                            'Staff.Deaths',
                            'Resident.Deaths',
                            'Staff.Recovered',
                            'Residents.Recovered']:
            if column_indices[column_name]:
                numeric_column_found = True
                break
        if not numeric_column_found:
            raise RuntimeError(
                'No data columns found in UCLA source file. The column ' \
                    + 'naming format may have changed')

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
                POP_RECOVERED_CASES_COLUMN:
                    _get_cell_value_if_present(
                        'Residents.Recovered', column_indices, row),
                POP_TESTED_POSITIVE_COLUMN:
                    _get_cell_value_if_present(
                        'Residents.Confirmed', column_indices, row),
                POP_DEATHS_COLUMN:
                    _get_cell_value_if_present(
                        'Resident.Deaths', column_indices, row),
                STAFF_RECOVERED_CASES_COLUMN:
                    _get_cell_value_if_present(
                        'Staff.Recovered', column_indices, row),
                STAFF_TESTED_POSITIVE_COLUMN:
                    _get_cell_value_if_present(
                        'Staff.Confirmed', column_indices, row),
                STAFF_DEATHS_COLUMN:
                    _get_cell_value_if_present(
                        'Staff.Deaths', column_indices, row),
                SOURCE_COLUMN: _get_cell_value_if_present(
                    'Website', column_indices, row),
                NOTES_COLUMN: _get_cell_value_if_present(
                    'Notes', column_indices, row),
            }

            data.append(data_row)

    return data


def _parse_recidiviz_csv(recidiviz_csv_dict):
    """Parses the Recidiviz data CSV"""
    data = []

    for raw_row in recidiviz_csv_dict:
        row = {k.strip(): v.strip() for (k, v) in raw_row.items()}

        date = row['As of...? (Date)']
        # Rows with missing dates should be ignored, since they can't be used
        if date in MISSING_DATE_VALUES:
            continue
        formatted_date = datetime.datetime.strptime(
            date, RECIDIVIZ_DATE_FORMAT).strftime(OUTPUT_DATE_FORMAT)

        # Extract subset of columns we care about
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


def _parse_facility_mapping_csv(facility_mapping_csv):
    """Parses the facility name mappings CSV"""
    mapping = FacilityInfoMapping()

    for raw_row in facility_mapping_csv:
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
            # TODO(zdg2102): write unmapped facilities and their data out to a
            # separate file
            logging.warning(
                'No facility info for %s, %s, ignoring row',
                state,
                facility_name)
            continue

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

    aggregated_data = {}

    for key in all_keys:
        # The key fields and facility info fields will be the same for any
        # sources in which the key is present, so the first non-null value can
        # be used
        date = _get_first_non_null(key, DATE_COLUMN, sources)
        facility_type = _get_first_non_null(key, FACILITY_TYPE_COLUMN, sources)
        state = _get_first_non_null(key, STATE_COLUMN, sources)
        facility_name = _get_first_non_null(key, FACILITY_NAME_COLUMN, sources)

        source = _combine_non_null_text(key, SOURCE_COLUMN, sources)
        notes = _combine_non_null_text(key, NOTES_COLUMN, sources)
        aggregation_notes = _combine_non_null_text(
            key, AGGREGATION_NOTES_COLUMN, sources)

        # Create initial row data, so numeric and compilation values will have
        # a place to go
        combined_row = {
            DATE_COLUMN: date,
            FACILITY_TYPE_COLUMN: facility_type,
            STATE_COLUMN: state,
            FACILITY_NAME_COLUMN: facility_name,
            SOURCE_COLUMN: source,
            NOTES_COLUMN: notes,
            AGGREGATION_NOTES_COLUMN: aggregation_notes
        }

        # Easier to handle the numeric fields in a loop, since we also have to
        # keep track of the source for all of them
        numeric_columns = [
            POP_TESTED_COLUMN,
            POP_TESTED_POSITIVE_COLUMN,
            POP_TESTED_NEGATIVE_COLUMN,
            POP_PENDING_COLUMN,
            POP_DEATHS_COLUMN,
            POP_ACTIVE_CASES_COLUMN,
            POP_RECOVERED_CASES_COLUMN,
            STAFF_TESTED_COLUMN,
            STAFF_TESTED_POSITIVE_COLUMN,
            STAFF_TESTED_NEGATIVE_COLUMN,
            STAFF_PENDING_COLUMN,
            STAFF_DEATHS_COLUMN,
            STAFF_ACTIVE_CASES_COLUMN,
            STAFF_RECOVERED_CASES_COLUMN
        ]
        numeric_value_sources = set()
        numeric_value_present = False
        for column in numeric_columns:
            # For all numeric fields, the assumption is that the largest value
            # was obtained last on the given date and so should be the most
            # up-to-date value.
            #
            # Note that this step also converts all numeric fields from string
            # to int values.
            value, value_source = _get_max(key, column, sources)
            # Always include value even if it's null, to ensure all required
            # columns are present
            combined_row[column] = value
            # Check for None explicitly to avoid skipping 0 values
            if value is not None:
                numeric_value_present = True
                numeric_value_sources.add(value_source)

        # If no value was set for any numeric column in the row, the row has no
        # useful data and should be excluded from the output
        if not numeric_value_present:
            continue

        # Compilation column is comma-joined list of all sources from which at
        # least one numeric value was taken. The list is sorted to enable
        # easier comparison in the output data.
        combined_row[COMPILATION_COLUMN] = \
            ', '.join(sorted(numeric_value_sources))

        aggregated_data[key] = combined_row

    return aggregated_data


def _amend_data(data):
    """Performs corrections and additional calculations on aggregated data"""
    amended_data = {}

    for key, row in data.items():
        output_row = dict(row)
        aggregation_notes = []

        # Include any existing aggregation notes, to make sure they aren't
        # dropped
        if row[AGGREGATION_NOTES_COLUMN]:
            aggregation_notes.append(row[AGGREGATION_NOTES_COLUMN])

        # If date can't be parsed, the date is invalid and the row should be
        # skipped.
        date = None
        try:
            date = datetime.datetime.strptime(
                row[DATE_COLUMN], OUTPUT_DATE_FORMAT)
        except ValueError:
            continue

        # Pull out values to be used, to make things a little more readable than
        # bracketing into the row dict every time
        state = row[STATE_COLUMN]
        facility_type = row[FACILITY_TYPE_COLUMN]
        pop_tested = row[POP_TESTED_COLUMN]
        pop_tested_positive = row[POP_TESTED_POSITIVE_COLUMN]
        pop_tested_negative = row[POP_TESTED_NEGATIVE_COLUMN]
        pop_pending = row[POP_PENDING_COLUMN]
        pop_recovered = row[POP_RECOVERED_CASES_COLUMN]
        staff_tested = row[STAFF_TESTED_COLUMN]
        staff_tested_positive = row[STAFF_TESTED_POSITIVE_COLUMN]
        staff_tested_negative = row[STAFF_TESTED_NEGATIVE_COLUMN]
        staff_pending = row[STAFF_PENDING_COLUMN]
        staff_recovered = row[STAFF_RECOVERED_CASES_COLUMN]

        # Below logic uses None identity checks because we want to distinguish
        # missing values from zero values

        # 1. If total tested is absent, sum positive and negative (and pending
        # if available) to calculate it
        if pop_tested is None \
                and pop_tested_positive is not None \
                and pop_tested_negative is not None:
            if pop_pending is not None:
                output_row[POP_TESTED_COLUMN] = \
                    pop_tested_positive + pop_tested_negative + pop_pending
                aggregation_notes.append(POP_TESTED_THREE_OPERANDS_NOTE)
            else:
                output_row[POP_TESTED_COLUMN] = \
                    pop_tested_positive + pop_tested_negative
                aggregation_notes.append(POP_TESTED_TWO_OPERANDS_NOTE)

        if staff_tested is None \
                and staff_tested_positive is not None \
                and staff_tested_negative is not None:
            if staff_pending is not None:
                output_row[STAFF_TESTED_COLUMN] = \
                    staff_tested_positive \
                    + staff_tested_negative \
                    + staff_pending
                aggregation_notes.append(STAFF_TESTED_THREE_OPERANDS_NOTE)
            else:
                output_row[STAFF_TESTED_COLUMN] = \
                    staff_tested_positive + staff_tested_negative
                aggregation_notes.append(STAFF_TESTED_TWO_OPERANDS_NOTE)

        # 2. If negative is absent, subtract positive (and pending if available)
        # from total to calculate it
        if pop_tested_negative is None \
                and pop_tested is not None \
                and pop_tested_positive is not None:
            if pop_pending is not None:
                output_row[POP_TESTED_NEGATIVE_COLUMN] = \
                    pop_tested - (pop_tested_positive + pop_pending)
                aggregation_notes.append(POP_NEGATIVES_THREE_OPERANDS_NOTE)
            else:
                output_row[POP_TESTED_NEGATIVE_COLUMN] = \
                    pop_tested - pop_tested_positive
                aggregation_notes.append(POP_NEGATIVES_TWO_OPERANDS_NOTE)

        if staff_tested_negative is None \
                and staff_tested is not None \
                and staff_tested_positive is not None:
            if staff_pending is not None:
                output_row[STAFF_TESTED_NEGATIVE_COLUMN] = \
                    staff_tested - (staff_tested_positive + staff_pending)
                aggregation_notes.append(STAFF_NEGATIVES_THREE_OPERANDS_NOTE)
            else:
                output_row[STAFF_TESTED_NEGATIVE_COLUMN] = \
                    staff_tested - staff_tested_positive
                aggregation_notes.append(STAFF_NEGATIVES_TWO_OPERANDS_NOTE)

        # 3. Correct rows showing active as positive
        is_tn_oh_ok = state in ['Tennessee', 'Ohio', 'Oklahoma']
        is_de_on_or_after_5_20 = \
            state == 'Delaware' and date >= datetime.datetime(2020, 5, 20)
        is_federal_facility = \
            state == 'Federal' or facility_type == 'Federal Prisons'
        if is_tn_oh_ok or is_de_on_or_after_5_20 or is_federal_facility:
            if pop_tested_positive is not None:
                # Note that fixing this erroneous column mapping intentionally
                # does not have an aggregation note
                output_row[POP_ACTIVE_CASES_COLUMN] = pop_tested_positive
                output_row[POP_TESTED_POSITIVE_COLUMN] = None
                # If recovered is also available, actual positives value can be
                # calculated
                if pop_recovered is not None:
                    output_row[POP_TESTED_POSITIVE_COLUMN] = \
                        pop_tested_positive + pop_recovered
                    aggregation_notes.append(
                        POP_POSITIVE_FROM_ACTIVE_AND_RECOVERED_NOTE)
            if staff_tested_positive is not None:
                # Note that fixing this erroneous column mapping intentionally
                # does not have an aggregation note
                output_row[STAFF_ACTIVE_CASES_COLUMN] = staff_tested_positive
                output_row[STAFF_TESTED_POSITIVE_COLUMN] = None
                # If recovered is also available, actual positives value can be
                # calculated
                if staff_recovered is not None:
                    output_row[STAFF_TESTED_POSITIVE_COLUMN] = \
                        staff_tested_positive + staff_recovered
                    aggregation_notes.append(
                        STAFF_POSITIVE_FROM_ACTIVE_AND_RECOVERED_NOTE)

        if aggregation_notes:
            output_row[AGGREGATION_NOTES_COLUMN] = ', '.join(aggregation_notes)

        amended_data[key] = output_row

    return amended_data


def _format_output(data):
    """Sorts rows and columns and adds header"""
    output = []

    header_row = list(OUTPUT_COLUMN_ORDER)
    output.append(header_row)

    # Because of the choice of output date format and key structure, sorting by
    # key will conveniently sort by date, state, and facility, in that order.
    # This is obviously brittle, so this sort will need to be made more
    # careful if either the date format or key structure needs to be changed.
    sorted_keys = sorted(data.keys())

    for key in sorted_keys:
        row = data[key]
        sorted_row = [row[column] for column in OUTPUT_COLUMN_ORDER]
        output.append(sorted_row)

    return output


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
            value = _int_or_none(source[key][column])
            # Explicit None check since 0 is a valid value
            if value is not None and (not current_max or value > current_max):
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


def _int_or_none(string):
    """Converts a string to an int, or returns None if the string cannot be
    converted
    """
    if not string:
        return None
    value = None
    try:
        value = int(string)
    except ValueError:
        pass
    return value


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


# Convenience entry point for local testing and debugging
# TODO(zdg2102): remove this once the aggregation logic has settled into more of
# a finalized state
if __name__ == '__main__':
    prison_file_path = '<path here>'
    ucla_file_path = '<path here>'

    prison_file_content = None
    with open(prison_file_path, 'rt') as prison_file:
        prison_file_content = prison_file.read()
    prison_csv = csv.DictReader(
        prison_file_content.splitlines(), delimiter=',')
    prison_csvs = [prison_csv]

    ucla_wb = xlrd.open_workbook(ucla_file_path)

    recidiviz_url = 'https://docs.google.com/spreadsheets/d/e/2PACX-1vTbxP67VHDHQt4xvpNmzbsXyT0pSh_b1Pn7aY5Ac089KKYnPDT6PpskMBMvhOX_PA08Zqkxt4zNn8_y/pub?gid=0&single=true&output=csv' # pylint:disable=line-too-long
    recidiviz_response = requests.get(recidiviz_url)
    recidiviz_file_content = recidiviz_response.content.decode('utf-8')
    recidiviz_csv = csv.DictReader(
        recidiviz_file_content.splitlines(), delimiter=',')

    facility_mapping_url = 'https://docs.google.com/spreadsheets/d/e/2PACX-1vT95DUfwcHbauuuMScd1Jb9u3vLCdfCcieXrRthNowoSbrmeWF3ibv06LkfcDxl1Vd97S5aujvnHdZX/pub?gid=1112897899&single=true&output=csv' # pylint:disable=line-too-long
    facility_mapping_response = requests.get(facility_mapping_url)
    facility_mapping_file_content = \
        facility_mapping_response.content.decode('utf-8')
    facility_csv = csv.reader(
        facility_mapping_file_content.splitlines(), delimiter=',')

    aggregated_csv = aggregate(
        prison_csvs,
        ucla_wb,
        recidiviz_csv,
        facility_csv)

    print(aggregated_csv)
