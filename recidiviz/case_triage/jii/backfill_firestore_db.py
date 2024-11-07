#!/usr/bin/env bash

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
"""
Script to backfill the JII Firestore DB with newly stored values.
"""

import argparse
from collections import defaultdict
import logging
from typing import Dict, List
from google.api_core.retry import Retry

from recidiviz.firestore.firestore_client import FirestoreClientImpl
from recidiviz.common.constants.states import StateCode
from recidiviz.utils.params import str_to_bool


logger = logging.getLogger(__name__)


def create_parser() -> argparse.ArgumentParser:
    """Returns an argument parser for the script."""
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--dry-run",
        default=True,
        type=str_to_bool,
        help="Runs script in dry-run mode. Only prints the operations it would perform.",
    )
    parser.add_argument(
        "--update-jii-docs",
        default=False,
        type=str_to_bool,
        help="Whether or not to update the inidividual (jii) level documents.",
        required=True,
    )
    parser.add_argument(
        "--update-message-docs",
        default=False,
        type=str_to_bool,
        help="Whether or not to update the text message level documents.",
        required=True,
    )
    return parser


def backfill_firestore_db(
    dry_run: bool, update_jii_docs: bool, update_message_docs: bool
) -> None:
    """
    Backfills the JII Firestore database with values. There are 2 types of documents that we backfill.

    1. Individual (JII) level documents will be backfilled with the following values
        - state_code (str) - the jii's state_code
        - external_id (str) - the jii's external_id
        - batch_ids (Array[str]) - a list of batch_ids that the jii has been included in
        - po_names (Array[str]) - a list of po names that the jii has been associated with
        - districts (Array[str]) - a list of districts that the jii has been associated with

        Note, for individual (jii) level documents, we also want to rename the last_phone_num_update field
        to last_update. Here, we will copy the last_phone_num_update values to a new last_update field. We
        will keep the deprecated last_phone_num_update fields as is.

    2. Text message level docuents will be backfilled with the following values
        - state_code (str) - the jii's state_code
        - external_id (str) - the jii's external_id
        - batch_id (str) - the batch_id that this given text message was a part of
        - po_name (str) - the po name that this text message was associated with
        - district (str) - the district that this text message was associated with
    """
    firestore_client = FirestoreClientImpl(project_id="jii-pilots")
    state_code = StateCode.US_ID.value.lower()

    # First, grab all text message level documents
    message_ref = firestore_client.get_collection_group(
        collection_path="lsu_eligibility_messages"
    )
    # doc_id_to_batch_ids maps a jii's external_id to a list of their batch_ids
    doc_id_to_batch_ids: Dict[str, List[str]] = defaultdict(list)
    message_docs = list(message_ref.stream(retry=Retry(), timeout=1200))
    logging.info("%s total message docs", len(message_docs))
    for message_doc in message_docs:
        message_doc_dict = message_doc.to_dict()
        if message_doc_dict is None:
            continue
        message_doc_path_split = message_doc.reference.path.split("/")
        jii_external_id = message_doc_path_split[1].split("us_id_")[1]
        logging.info("jii_external_id: %s", jii_external_id)
        batch_id = message_doc_path_split[3].split("eligibility_")[1]
        logging.info("batch_id: %s", batch_id)
        doc_id_to_batch_ids[jii_external_id].append(batch_id)

        # In order to backfill po_name and district, we would have to make a call to BigQuery tables
        # from each previous launch and join these values from the BigQuery tables using the jii's external_id.
        # I don't think that this is worth the lift at the moment. For now, we will store
        # these as None, but will start storing these values in Firestore moving forward.
        po_name = None
        district = None

        updated_data = {
            "state_code": state_code,
            "external_id": jii_external_id,
            "batch_id": batch_id,
            "po_name": po_name,
            "district": district,
        }

        if update_message_docs is True and dry_run is False:
            # For each document, update the document with the new values
            # This will NOT overwrite existing values stored in the document
            firestore_client.set_document(
                document_path=message_doc.reference.path,
                data=updated_data,
                merge=True,
            )
            logging.info("Updated document: %s", message_doc.reference.path)
        else:
            logging.info(
                "Would update text message document: %s with the following data: %s",
                message_doc.reference.path,
                updated_data,
            )

    if update_jii_docs is True:
        # Grab all individual (jii) level documents
        jii_ref = firestore_client.get_collection(collection_path="twilio_messages")

        for jii_doc in jii_ref.stream(retry=Retry(), timeout=1200):
            jii_doc_dict = jii_doc.to_dict()
            if jii_doc_dict is None:
                continue

            # For each individual level document, compute all values we need to update
            last_phone_num_update = jii_doc_dict.get("last_phone_num_update")
            external_id = jii_doc.id.split("us_id_")[1]
            logging.info("external_id: %s", external_id)
            batch_ids = doc_id_to_batch_ids.get(external_id, [])
            logging.info("batch_ids: %s", batch_ids)
            # In order to backfill po_names and districts, we would have to make a call to BigQuery tables
            # from each previous launch and join these values from the BigQuery tables using the jii's external_id.
            # I don't think that this is worth the lift at the moment. For now, we will store
            # these as empty arrays, but will start storing these values in Firestore moving forward.
            po_names: List = []
            districts: List = []

            updated_data = {
                "last_update": last_phone_num_update,
                "state_code": state_code,
                "external_id": external_id,
                "batch_ids": batch_ids,
                "po_names": po_names,
                "districts": districts,
            }

            # For each document, update the document with the new values
            # This will NOT overwrite existing values stored in the document
            if dry_run is False:
                firestore_client.set_document(
                    document_path=jii_doc.reference.path,
                    data=updated_data,
                    merge=True,
                )
                logging.info("Updated document: %s", jii_doc.reference.path)
            else:
                logging.info(
                    "Would update document: %s with the following data: %s",
                    jii_doc.reference.path,
                    updated_data,
                )


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    args = create_parser().parse_args()
    backfill_firestore_db(
        dry_run=args.dry_run,
        update_jii_docs=args.update_jii_docs,
        update_message_docs=args.update_message_docs,
    )
