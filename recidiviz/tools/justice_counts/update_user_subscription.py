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
Updates the AgencyUserAccountAssociationInterface in our DB with given agency_id and user_account_id to 
either subscribe or unsubscribe to emails related to Automated Bulk Upload.

python -m recidiviz.tools.justice_counts.update_user_subscription \
  --agency_id=AGENCY_ID \
  --user_account_id=USER_ACCOUNT_ID \
  --subscribe=SUBSCRIBE \
  --project-id=PROJECT_ID \
  --dry-run=true
"""

import argparse
import logging

from recidiviz.justice_counts.agency_user_account_association import (
    AgencyUserAccountAssociationInterface,
)
from recidiviz.persistence.database.schema.justice_counts import schema
from recidiviz.persistence.database.schema_type import SchemaType
from recidiviz.persistence.database.session_factory import SessionFactory
from recidiviz.persistence.database.sqlalchemy_database_key import SQLAlchemyDatabaseKey
from recidiviz.tools.postgres.cloudsql_proxy_control import cloudsql_proxy_control
from recidiviz.utils.environment import GCP_PROJECT_PRODUCTION, GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override
from recidiviz.utils.params import str_to_bool

logger = logging.getLogger(__name__)


def create_parser() -> argparse.ArgumentParser:
    """Returns an argument parser for the script."""
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--project-id",
        choices=[GCP_PROJECT_STAGING, GCP_PROJECT_PRODUCTION],
        help="Used to select which GCP project in which to run this script.",
        required=True,
    )
    parser.add_argument(
        "--agency_id",
        type=int,
        help="Agency ID of the agency for which to update the user's subscription.",
        required=True,
    )
    parser.add_argument(
        "--user_account_id",
        type=int,
        help="User Account ID of the user for which to update the subscription for.",
        required=True,
    )
    parser.add_argument(
        "--subscribe",
        type=str_to_bool,
        help="The value to set subscribed to (true or false)",
        required=True,
    )
    parser.add_argument("--dry-run", type=str_to_bool, default=True)
    return parser


def update_user_subscription(
    agency_id: int, user_account_id: int, subscribed: bool, dry_run: bool
) -> None:
    """
    For a given user_account_id (user) and agency_id (agency) pair, update
    the subscribed column to either True or False. The subscribed column
    indicates whether or not we should attempt to send a confirmation
    email to the given user at the end of Automated Bulk Upload for the
    given agency.
    """
    schema_type = SchemaType.JUSTICE_COUNTS
    database_key = SQLAlchemyDatabaseKey.for_schema(schema_type)
    with cloudsql_proxy_control.connection(schema_type=schema_type):
        with SessionFactory.for_proxy(database_key) as session:
            associations = (
                AgencyUserAccountAssociationInterface.get_associations_by_ids(
                    user_account_ids=[user_account_id],
                    agency_id=agency_id,
                    session=session,
                )
            )

            if len(associations) == 0:
                assoc = schema.AgencyUserAccountAssociation(
                    user_id=user_account_id, agency_id=agency_id
                )
                logging.info("User is not a member of the agency; adding them.")
            else:
                assoc = associations[0]
                logging.info(
                    "User is currently a member of this agency with subscription = %s",
                    assoc.subscribed,
                )

            logging.info("Updating subscription to %s", subscribed)
            assoc.subscribed = subscribed

            if not dry_run:
                session.add(assoc)
                session.commit()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    args = create_parser().parse_args()
    with local_project_id_override(args.project_id):
        update_user_subscription(
            agency_id=args.agency_id,
            user_account_id=args.user_account_id,
            subscribed=args.subscribe,
            dry_run=args.dry_run,
        )
