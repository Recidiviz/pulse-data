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
"""Interface for working with the AgencyUserAccountAssociation model."""

import datetime
import itertools
from typing import Dict, List, Optional

from sqlalchemy.orm import Session, joinedload
from sqlalchemy.sql.expression import true

from recidiviz.auth.auth0_client import Auth0Client
from recidiviz.justice_counts.agency import AgencyInterface
from recidiviz.justice_counts.exceptions import JusticeCountsServerError
from recidiviz.justice_counts.user_account import UserAccountInterface
from recidiviz.justice_counts.utils.constants import AUTOMATIC_UPLOAD_ID
from recidiviz.persistence.database.schema.justice_counts import schema


class AgencyUserAccountAssociationInterface:
    """Contains methods for working with the AgencyUserAccountAssociation."""

    @staticmethod
    def invite_user_to_agency(
        name: str,
        email: str,
        agency_id: int,
        auth0_client: Auth0Client,
        session: Session,
    ) -> None:
        """This method does the following:
        1. Checks if the user already exists, but is part of another agency.
        2. If so, we update the users's list of agencies in Auth0, and in our DB.
        3. If the user does not exist, we create the user in Auth0 and in our DB,
        and then we add them to the agency.
        """
        # First, check if user already exists, but is part of another agency.
        existing_users = UserAccountInterface.get_users_by_email(
            session=session, emails={email}
        )
        if len(existing_users) > 1:
            raise JusticeCountsServerError(
                code="justice_counts_user_uniqueness",
                description=f"Multiple users exist with the same email: {email}",
            )

        new_agency = AgencyInterface.get_agency_by_id(
            session=session, agency_id=agency_id
        )

        child_agencies = AgencyInterface.get_child_agencies_for_agency(
            session=session, agency=new_agency
        )
        auth0_user_id = None
        if len(existing_users) == 1:
            existing_user = existing_users[0]
            existing_agency_ids = {
                assoc.agency.id for assoc in existing_user.agency_assocs
            }
            auth0_user_id = existing_user.auth0_user_id

            if agency_id in existing_agency_ids:
                # User already belongs to this agency
                raise JusticeCountsServerError(
                    code="user_reinvited_to_agency",
                    description=f"A user with the email {email} already belongs to this agency.",
                )

        # Create/Update user in our DB and Auth0
        user = UserAccountInterface.create_or_update_user(
            session=session,
            name=name,
            auth0_user_id=auth0_user_id,
            email=email,
            auth0_client=auth0_client,
        )

        # Add the user to the agency
        UserAccountInterface.add_or_update_user_agency_association(
            session=session,
            user=user,
            agencies=[new_agency] + child_agencies,
            invitation_status=schema.UserAccountInvitationStatus.PENDING,
        )

    @staticmethod
    def remove_user_from_agencies(
        email: str,
        agency_ids: List[int],
        session: Session,
    ) -> None:
        """This method removes the agency_ids from the users metadata in auth0 and
        deletes in the AgencyUserAccountAssociation between the user and the agency
        in the Justice Counts DB."""
        users = UserAccountInterface.get_users_by_email(session=session, emails={email})
        # Remove agency from users list of of agency ids in auth0.
        if len(users) > 1:
            raise JusticeCountsServerError(
                code="justice_counts_user_uniqueness",
                description=f"Multiple users exist with the same email: {email}",
            )
        user = users[0]

        # Update user in Justice Counts DB
        UserAccountInterface.remove_user_from_agencies(
            session=session,
            user=user,
            agency_ids=agency_ids,
        )

    @staticmethod
    def update_user_role(
        session: Session, role: str, agency: schema.Agency, user: schema.UserAccount
    ) -> None:
        db_role = schema.UserAccountRole(role)
        UserAccountInterface.add_or_update_user_agency_association(
            session=session, user=user, agencies=[agency], role=db_role
        )

    @staticmethod
    def get_associations_by_ids(
        user_account_ids: List[int],
        agency_id: int,
        session: Session,
    ) -> List[schema.AgencyUserAccountAssociation]:
        """This method retrieves an AgencyUserAccountAssociation based
        upon user_account_ids and agency_id."""
        return (
            session.query(schema.AgencyUserAccountAssociation)
            .filter(
                schema.AgencyUserAccountAssociation.user_account_id.in_(
                    user_account_ids
                ),
                schema.AgencyUserAccountAssociation.agency_id == agency_id,
            )
            # eagerly load the corresponding user accounts since we'll need to lookup the name
            .options(joinedload(schema.AgencyUserAccountAssociation.user_account))
            .all()
        )

    @staticmethod
    def get_association_by_ids(
        user_account_id: int,
        agency_id: int,
        session: Session,
        load_user_account: Optional[bool] = False,
    ) -> schema.AgencyUserAccountAssociation:
        """This method retrieves an AgencyUserAccountAssociation based
        upon a user_account_id and agency_id."""

        query = session.query(schema.AgencyUserAccountAssociation).filter(
            schema.AgencyUserAccountAssociation.user_account_id == user_account_id,
            schema.AgencyUserAccountAssociation.agency_id == agency_id,
        )

        if load_user_account is True:
            query = query.options(
                joinedload(schema.AgencyUserAccountAssociation.user_account)
            )

        return query.one()

    @staticmethod
    def get_editor_id_to_json(
        session: Session, reports: List[schema.Report], user: schema.UserAccount
    ) -> Dict[int, Dict[str, str]]:
        """Returns a dictionary mapping an editor's user_account_id to a
        object with their name and role. All reports will be from the same agency."""
        editor_json: Dict[int, Dict[str, str]] = {}
        if len(reports) == 0:
            return editor_json

        agency_id = reports[0].source_id

        editor_ids = list(
            itertools.chain(*[report.modified_by or [] for report in reports])
        )
        editor_assocs = AgencyUserAccountAssociationInterface.get_associations_by_ids(
            session=session,
            user_account_ids=editor_ids,
            agency_id=agency_id,
        )
        editor_ids_to_assocs = {
            k: list(v)
            for k, v in itertools.groupby(
                sorted(editor_assocs, key=lambda x: x.user_account_id),
                lambda x: x.user_account_id,
            )
        }

        user_assoc = next(
            (assoc for assoc in user.agency_assocs if assoc.agency_id == agency_id),
            None,
        )

        if not user_assoc:
            raise JusticeCountsServerError(
                code="justice_counts_agency_permission",
                description=(
                    f"User does not have permission to access agency {agency_id}."
                ),
            )

        for editor_id, editor_assocs in editor_ids_to_assocs.items():
            if len(editor_assocs) > 1:
                raise JusticeCountsServerError(
                    code="justice_counts_user_uniqueness",
                    description="Multiple user account associations exist for one user and one agency.",
                )
            assoc = editor_assocs[0]
            editor_json[editor_id] = {
                # currently this logic is duplicated in get_editor_id_to_json and get_uploader_id_to_json
                "name": (
                    "JC Admin"
                    if assoc.role == schema.UserAccountRole.JUSTICE_COUNTS_ADMIN
                    and user_assoc.role != schema.UserAccountRole.JUSTICE_COUNTS_ADMIN
                    else assoc.user_account.name
                ),
                "role": assoc.role.value if assoc.role is not None else None,
            }

        if AUTOMATIC_UPLOAD_ID in editor_ids:
            editor_json[AUTOMATIC_UPLOAD_ID] = {
                "name": "Automatic Upload",
                "role": schema.UserAccountRole.CONTRIBUTOR.value,
            }

        return editor_json

    @staticmethod
    def get_uploader_id_to_json(
        session: Session,
        spreadsheets: List[schema.Spreadsheet],
        user: schema.UserAccount,
    ) -> Dict[int, Dict[str, str]]:
        """
        Returns a dictionary mapping an editor's user_account_id to a
        object with their name and role. All reports will be from the same agency.
        """
        editor_json: Dict[int, Dict[str, str]] = {}
        if len(spreadsheets) == 0:
            return editor_json

        source_id = spreadsheets[0].agency_id
        uploader_ids = [
            spreadsheet.uploaded_by
            for spreadsheet in spreadsheets
            if spreadsheet.uploaded_by != AUTOMATIC_UPLOAD_ID
        ]

        uploaded_by_users = (
            session.query(schema.UserAccount)
            .filter(schema.UserAccount.auth0_user_id.in_(uploader_ids))
            .options(joinedload(schema.UserAccount.agency_assocs))
            .all()
        )

        uploader_ids_to_assocs = {
            user.auth0_user_id: list(
                filter(lambda a: a.agency_id == source_id, user.agency_assocs)
            )
            for user in uploaded_by_users
        }

        user_assoc = next(
            (assoc for assoc in user.agency_assocs if assoc.agency_id == source_id),
            None,
        )

        if not user_assoc:
            raise JusticeCountsServerError(
                code="justice_counts_agency_permission",
                description=(
                    f"User does not have permission to access agency {source_id}."
                ),
            )

        for editor_id, assocs in uploader_ids_to_assocs.items():
            if len(assocs) > 1:
                raise JusticeCountsServerError(
                    code="justice_counts_user_uniqueness",
                    description="Multiple user account associations exist for one user and one agency.",
                )

            if len(assocs) == 0:
                continue

            assoc = assocs[0]
            editor_json[editor_id] = {
                # currently this logic is duplicated in get_editor_id_to_json and get_uploader_id_to_json
                "name": (
                    "JC Admin"
                    if assoc.role == schema.UserAccountRole.JUSTICE_COUNTS_ADMIN
                    and user_assoc.role != schema.UserAccountRole.JUSTICE_COUNTS_ADMIN
                    else assoc.user_account.name
                ),
                "role": assoc.role.value if assoc.role is not None else None,
            }

        if AUTOMATIC_UPLOAD_ID in uploader_ids:
            editor_json[AUTOMATIC_UPLOAD_ID] = {
                "name": "Automatic Upload",
                "role": schema.UserAccountRole.CONTRIBUTOR.value,
            }

        return editor_json

    @staticmethod
    def add_child_agencies_to_super_agency_and_copy_users(
        session: Session, child_agency_ids: List[int], super_agency_id: int
    ) -> None:
        child_agencies = AgencyInterface.get_agencies_by_id(
            session=session, agency_ids=child_agency_ids
        )

        for child_agency in child_agencies:
            child_agency.super_agency_id = super_agency_id

        super_agency_users = (
            AgencyUserAccountAssociationInterface.get_users_by_agency_id(
                session=session, agency_id=super_agency_id
            )
        )
        for user_assoc in super_agency_users:
            # Add all super agency users as admins to the new child agencies
            for child_agency in child_agencies:
                new_assoc = schema.AgencyUserAccountAssociation(
                    agency_id=child_agency.id,
                    user_account_id=user_assoc.user_account_id,
                    role=user_assoc.role,
                    subscribed=user_assoc.subscribed,
                )
                session.merge(new_assoc)

    @staticmethod
    def get_users_by_agency_id(
        session: Session, agency_id: int
    ) -> List[schema.AgencyUserAccountAssociation]:
        return (
            session.query(schema.AgencyUserAccountAssociation)
            .filter(
                schema.AgencyUserAccountAssociation.agency_id == agency_id,
            )
            .all()
        )

    @staticmethod
    def delete_agency_user_acccount_associations_for_user(
        session: Session, user_account_id: int
    ) -> None:
        """Delete all AgencyUserAccountAssociation entries corresponding to the user."""
        session.query(schema.AgencyUserAccountAssociation).filter(
            schema.AgencyUserAccountAssociation.user_account_id == user_account_id,
        ).delete()
        session.commit()

    @staticmethod
    def get_subscribed_user_emails_by_agency_id(
        session: Session, agency_id: int
    ) -> List[str]:
        subscribed_user_email_tuples = (
            session.query(schema.UserAccount.email)
            .join(
                schema.AgencyUserAccountAssociation,
                schema.UserAccount.id
                == schema.AgencyUserAccountAssociation.user_account_id,
            )
            .filter(
                schema.AgencyUserAccountAssociation.agency_id == agency_id,
                schema.AgencyUserAccountAssociation.subscribed == true(),
            )
            .all()
        )

        subscribed_user_emails = [
            user_email_tuple[0] for user_email_tuple in subscribed_user_email_tuples
        ]
        return subscribed_user_emails

    @staticmethod
    def get_subscribed_user_associations_by_agency_id(
        session: Session, agency_id: int
    ) -> List[schema.AgencyUserAccountAssociation]:
        return (
            session.query(schema.AgencyUserAccountAssociation)
            .filter(
                schema.AgencyUserAccountAssociation.agency_id == agency_id,
                schema.AgencyUserAccountAssociation.subscribed == true(),
            )
            .options(joinedload(schema.AgencyUserAccountAssociation.user_account))
            .all()
        )

    @staticmethod
    def record_user_agency_page_visit(
        session: Session, user_id: int, agency_id: int
    ) -> None:
        # get association
        association = (
            session.query(schema.AgencyUserAccountAssociation)
            .filter(
                schema.AgencyUserAccountAssociation.agency_id == agency_id,
                schema.AgencyUserAccountAssociation.user_account_id == user_id,
            )
            .one()
        )
        # get today's date
        today = datetime.datetime.now(tz=datetime.timezone.utc)
        # overwrite last_visit with today's date
        association.last_visit = today
        session.add(association)
