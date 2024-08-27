#!/usr/bin/env bash

# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2021 Recidiviz, Inc.
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
"""
Reads case notes from the Google Sheet below and uploads them (and the metadata.json) to
the GCBucket.

Google Sheet: go/case-note-search-fake-data

You will need to make the Google Sheet publically viewable when running this script.

Note: This is an *overwrite* script - it will delete all fake_case_notes/ data before
writing new case notes.

Usage (make sure to remove the dry run flag):
python -m recidiviz.tools.prototypes.generate_fake_case_notes --sheet-id=<token> --dry-run=False

"""

import argparse
import json
from typing import Any, List
from urllib.error import HTTPError

import attr
import pandas as pd
from google.cloud import storage  # type: ignore

from recidiviz.utils.params import str_to_bool

BUCKET_NAME = "lili_scratch"
PREFIX_NAME = "fake_case_notes"


def create_parser() -> argparse.ArgumentParser:
    """Returns an argument parser for the script."""
    parser = argparse.ArgumentParser()
    parser.add_argument("--dry-run", type=str_to_bool, default=False)
    parser.add_argument("--sheet-id", type=str, required=True)
    return parser


@attr.define(kw_only=True)
class CaseNote:
    """A struct to define all the required fields of a case note."""

    text: str
    state_code: str
    external_id: str
    date: str
    contact_mode: str
    note_type: str


# Count the number of fields in a class
def count_fields(cls: Any) -> int:
    return len(cls.__annotations__)


def read_case_notes_from_public_sheet(sheet_id: str) -> List[CaseNote]:
    """Read data from the sheet and formats the data as a list of CaseNote objects."""
    url = f"https://docs.google.com/spreadsheets/d/{sheet_id}/gviz/tq?tqx=out:csv"

    try:
        df = pd.read_csv(url)
    except Exception as e:
        raise e

    documents = []
    for _, row in df.iterrows():
        if len(row) != count_fields(CaseNote):
            print("Invalid row: ", str(row))
            continue
        documents.append(
            CaseNote(
                text=str(row[0]),
                state_code=str(row[1]),
                external_id=str(row[2]),
                date=str(row[3]),
                contact_mode=str(row[4]),
                note_type=str(row[5]),
            )
        )
    return documents


def delete_existing_blobs(bucket: storage.bucket.Bucket, prefix: str) -> None:
    """Delete all blobs in the specified GCS bucket with the given prefix."""
    blobs = bucket.list_blobs(prefix=prefix)
    for blob in blobs:
        print(f"Deleting blob {blob.name}")
        blob.delete()


def create_case_notes(dry_run: bool, sheet_id: str) -> None:
    """Create case notes and write them to GCS."""
    try:
        documents = read_case_notes_from_public_sheet(
            sheet_id=sheet_id,
        )
    except HTTPError:
        print("HTTPError. Make sure that the sheet is publically viewable.")
        return
    except Exception as e:
        print(f"Failed to read from csv. error: {e}")
        return

    storage_client = storage.Client()
    bucket = storage_client.bucket(BUCKET_NAME)

    # Delete the existing fake data.
    if not dry_run:
        delete_existing_blobs(bucket=bucket, prefix=PREFIX_NAME)

    # Metadata is the file picked up by Vertex AI.
    metadatas = []
    note_id = 10000  # incremental note id.
    for document in documents:
        note_id = note_id + 1
        print(f"Uploading {note_id} to GCS")
        blob = bucket.blob(f"{PREFIX_NAME}/{note_id}")
        if not dry_run:
            blob.upload_from_string(document.text)
        else:
            print(document.text)

        metadatas.append(
            json.dumps(
                {
                    "id": str(note_id),
                    "jsonData": json.dumps(
                        {
                            "state_code": document.state_code,
                            "external_id": document.external_id,
                            "date": document.date,
                            "contact_mode": document.contact_mode,
                            "note_type": document.note_type,
                        }
                    ),
                    "content": {
                        "mimeType": "text/plain",
                        "uri": f"gs://{BUCKET_NAME}/{PREFIX_NAME}/{note_id}",
                    },
                }
            )
        )

    metadata_content = "\n".join(metadatas)
    if not dry_run:
        metadata_filename = f"{PREFIX_NAME}/metadata.json"
        print(f"Uploading {metadata_filename} to GCS")
        blob = bucket.blob(metadata_filename)
        blob.upload_from_string(metadata_content)
    else:
        print("Metadata file:")
        print(metadata_content)


if __name__ == "__main__":
    args = create_parser().parse_args()
    create_case_notes(dry_run=args.dry_run, sheet_id=args.sheet_id)
