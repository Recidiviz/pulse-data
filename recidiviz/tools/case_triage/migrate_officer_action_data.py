# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2022 Recidiviz, Inc.
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
Tool to migrate Case Triage officer action data in the case that the officer's composite primary key changes.

python -m recidiviz.tools.case_triage.migrate_officer_action_data \
  --project-id=PROJECT_ID \
  --old-officer-external-id=OLD_OFFICER_EXTERNAL_ID \
  --new-officer-external-id=NEW_OFFICER_EXTERNAL_ID \
  --dry-run=true
"""


import argparse
import logging
import os
import sys

from recidiviz.persistence.database.schema.case_triage.schema import (
    CaseUpdate,
    OfficerMetadata,
    OfficerNote,
    OpportunityDeferral,
)
from recidiviz.persistence.database.schema_utils import SchemaType
from recidiviz.persistence.database.session_factory import SessionFactory
from recidiviz.persistence.database.sqlalchemy_database_key import SQLAlchemyDatabaseKey
from recidiviz.utils import metadata
from recidiviz.utils.environment import GCP_PROJECT_PRODUCTION, GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override
from recidiviz.utils.params import str_to_bool


def create_parser() -> argparse.ArgumentParser:
    """Returns an argument parser for the script."""
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--old-officer-external-id",
        type=str,
        help="Specifies the external id we wish to migrate.",
        required=True,
    )
    parser.add_argument(
        "--new-officer-external-id",
        type=str,
        help="Specifies the external id to migrate the officer to",
        required=True,
    )
    parser.add_argument(
        "--project-id",
        choices=[GCP_PROJECT_STAGING, GCP_PROJECT_PRODUCTION],
        help="Used to select which GCP project in which to run this script.",
        required=True,
    )
    parser.add_argument("--dry-run", type=str_to_bool, default=True)

    return parser


ENTITY_TO_EXTERNAL_ID_COLUMN_MAP = {
    CaseUpdate: CaseUpdate.officer_external_id,
    OfficerMetadata: OfficerMetadata.officer_external_id,
    OfficerNote: OfficerNote.officer_external_id,
    OpportunityDeferral: OpportunityDeferral.supervising_officer_external_id,
}


def main(
    *,
    old_officer_external_id: str,
    new_officer_external_id: str,
    dry_run: bool,
) -> None:
    """
    Migrates an officer's case triage data to a different officer.
    Useful for when an officer's external id changes.
    """
    logging.info(
        "%sMigrating officer %s to %s",
        "[DRY RUN] " if dry_run else "",
        old_officer_external_id,
        new_officer_external_id,
    )

    is_prod = metadata.project_id() == GCP_PROJECT_PRODUCTION
    ssl_cert_path = os.path.expanduser(
        f"~/{'prod' if is_prod else 'dev'}_case_triage_certs"
    )

    with SessionFactory.for_prod_data_client(
        SQLAlchemyDatabaseKey.for_schema(SchemaType.CASE_TRIAGE),
        os.path.abspath(ssl_cert_path),
    ) as session:
        for entity, external_id_column in ENTITY_TO_EXTERNAL_ID_COLUMN_MAP.items():
            query = session.query(entity).filter(
                external_id_column == old_officer_external_id
            )
            if dry_run:
                logging.info(
                    "[DRY RUN] Would update %i rows in %s", query.count(), entity
                )
            else:
                query.update({external_id_column: new_officer_external_id})


if __name__ == "__main__":
    # Route logs to stdout
    logging.basicConfig(level=logging.INFO, format="%(message)s", stream=sys.stdout)

    args = create_parser().parse_args()
    with local_project_id_override(args.project_id):
        main(
            old_officer_external_id=args.old_officer_external_id,
            new_officer_external_id=args.new_officer_external_id,
            dry_run=args.dry_run,
        )
