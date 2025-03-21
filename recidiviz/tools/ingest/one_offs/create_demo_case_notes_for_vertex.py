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
"""
Local script for taking a CSV file of demo case notes, modifying them slightly, and saving them as an NDJSON file.

Example Usage:
    python -m recidiviz.tools.ingest.one_offs.create_demo_case_notes_for_vertex.py --input_file_path input.csv --output_file_path demo_case_notes.ndjson
"""

import argparse
import csv
import logging
import sys

# there are no type stubs for ndjson
import ndjson  # type: ignore

COLUMN_NAME_MAPPING = {
    "OffenderNoteId": "note_id",
    "NoteDate": "note_date",
    "Details": "note_body",
    "ContactModeDesc": "note_mode",
    "NoteTypeDesc": "note_type",
}


def create_demo_case_notes(input_file_path: str, output_file_path: str) -> None:
    """Creates a new NDJSON file with the demo case notes."""
    lines = []
    with open(
        input_file_path,
        mode="r",
        newline="",
        encoding="utf-8",
    ) as csvfile:  # downloaded Andrew's file locally
        reader = csv.DictReader(csvfile)
        for row in reader:
            new_row = {
                new_col: row[old_col]
                for old_col, new_col in COLUMN_NAME_MAPPING.items()
            }
            new_row["state_code"] = "US_IX"
            new_row["note_title"] = ""
            new_row["external_id"] = "003"  # demo id for Sylvester Allen
            lines.append(new_row)

    with open(output_file_path, "w", encoding="utf-8") as f:
        ndjson.dump(lines, f)


def parse_arguments() -> argparse.Namespace:
    """Parses the required arguments."""
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--input_file_path",
        required=True,
        type=str,
        help="The filepath of the input CSV file.",
    )

    parser.add_argument(
        "--output_file_path",
        required=True,
        type=str,
        help="The filepath of the output json file.",
    )

    return parser.parse_args()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(message)s", stream=sys.stdout)
    args = parse_arguments()

    create_demo_case_notes(args.input_file_path, args.output_file_path)
