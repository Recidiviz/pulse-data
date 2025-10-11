# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2023 Recidiviz, Inc.
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
"""Endpoints related to getting/setting information about users of Recidiviz applications."""
import csv
from datetime import datetime
from http import HTTPStatus
from typing import Any, Callable, Dict, List

from dateutil.tz import tzlocal
from flask.views import MethodView
from flask_smorest import Blueprint, abort
from psycopg2.errors import (  # pylint: disable=no-name-in-module
    NotNullViolation,
    UniqueViolation,
)
from sqlalchemy import and_, cast, func, or_, update
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.engine.row import Row
from sqlalchemy.exc import IntegrityError, ProgrammingError, StatementError
from sqlalchemy.orm import Query
from sqlalchemy.orm.exc import NoResultFound
from werkzeug.datastructures import FileStorage

from recidiviz.auth.auth_api_schemas import (
    FullUserSchema,
    PermissionsRequestSchema,
    PermissionsResponseSchema,
    ReasonSchema,
    StateCodeSchema,
    UploadSchema,
    UserRequestSchema,
    UserSchema,
)
from recidiviz.auth.auth_endpoint import _upsert_user_rows
from recidiviz.auth.helpers import (
    bulk_delete_feature_variant,
    convert_to_dict_multiple_results,
    convert_to_dict_single_result,
    generate_pseudonymized_id,
    generate_user_hash,
    log_reason,
    validate_roles,
)
from recidiviz.persistence.database.schema.case_triage.schema import (
    PermissionsOverride,
    Roster,
    StateRolePermissions,
    UserOverride,
)
from recidiviz.persistence.database.session import Session
from recidiviz.persistence.database.sqlalchemy_flask_utils import current_session


def get_users_blueprint(authentication_middleware: Callable | None) -> Blueprint:
    """Creates the users Flask Blueprint"""
    users_blueprint = Blueprint("users", "users")

    if authentication_middleware:

        @users_blueprint.before_request
        @authentication_middleware
        def auth_middleware() -> None:
            pass

    def get_users_query(session: Session) -> Query:
        """Creates a query that selects all users from Roster and UserOverride tables
        and joins to StateRolePermissions and PermissionsOverride data. If a user has
        multiple roles, permissions are aggregated before being joined to user data."""

        roles_cte = (
            session.query(
                func.coalesce(UserOverride.email_address, Roster.email_address).label(
                    "email_address"
                ),
                func.unnest(func.coalesce(UserOverride.roles, Roster.roles)).label(
                    "role"
                ),
            )
            .select_from(Roster)
            .join(
                UserOverride,
                UserOverride.email_address == Roster.email_address,
                full=True,
            )
            .cte("roles_cte")
        )

        aggregated_permissions_cte = (
            session.query(
                func.coalesce(UserOverride.email_address, Roster.email_address).label(
                    "email_address"
                ),
                func.coalesce(UserOverride.state_code, Roster.state_code).label(
                    "state_code"
                ),
                func.jsonb_agg(
                    func.coalesce(StateRolePermissions.routes, cast({}, JSONB))
                ).label("routes"),
                func.jsonb_agg(
                    func.coalesce(
                        StateRolePermissions.feature_variants, cast({}, JSONB)
                    )
                ).label("feature_variants"),
            )
            .select_from(Roster)
            .join(
                UserOverride,
                UserOverride.email_address == Roster.email_address,
                full=True,
            )
            .outerjoin(
                roles_cte,
                func.coalesce(UserOverride.email_address, Roster.email_address)
                == roles_cte.c.email_address,
            )
            .outerjoin(
                StateRolePermissions,
                (StateRolePermissions.role == roles_cte.c.role)
                & (
                    StateRolePermissions.state_code
                    == func.coalesce(UserOverride.state_code, Roster.state_code)
                ),
            )
            .group_by(
                func.coalesce(UserOverride.email_address, Roster.email_address),
                func.coalesce(UserOverride.state_code, Roster.state_code),
            )
            .cte("aggregated_permissions")
        )

        return (
            session.query(
                func.coalesce(UserOverride.state_code, Roster.state_code).label(
                    "state_code"
                ),
                func.coalesce(UserOverride.email_address, Roster.email_address).label(
                    "email_address"
                ),
                func.coalesce(UserOverride.external_id, Roster.external_id).label(
                    "external_id"
                ),
                func.coalesce(UserOverride.roles, Roster.roles).label("roles"),
                func.coalesce(UserOverride.district, Roster.district).label("district"),
                func.coalesce(UserOverride.first_name, Roster.first_name).label(
                    "first_name"
                ),
                func.coalesce(UserOverride.last_name, Roster.last_name).label(
                    "last_name"
                ),
                UserOverride.blocked_on.label("blocked_on"),
                (
                    aggregated_permissions_cte.c.routes
                    + func.coalesce(PermissionsOverride.routes, cast({}, JSONB))
                ).label("routes"),
                (
                    aggregated_permissions_cte.c.feature_variants
                    + func.coalesce(
                        PermissionsOverride.feature_variants, cast({}, JSONB)
                    )
                ).label("feature_variants"),
                func.coalesce(
                    UserOverride.user_hash,
                    Roster.user_hash,
                ).label("user_hash"),
                func.coalesce(
                    UserOverride.pseudonymized_id,
                    Roster.pseudonymized_id,
                ).label("pseudonymized_id"),
            )
            .select_from(Roster)
            .join(
                UserOverride,
                UserOverride.email_address == Roster.email_address,
                full=True,
            )
            .outerjoin(
                aggregated_permissions_cte,
                (
                    func.coalesce(UserOverride.email_address, Roster.email_address)
                    == aggregated_permissions_cte.c.email_address
                ),
            )
            .outerjoin(
                PermissionsOverride,
                func.coalesce(UserOverride.email_address, Roster.email_address)
                == PermissionsOverride.email_address,
            )
        )

    def _create_user_override(
        session: Session, user_dict: Dict[str, Any]
    ) -> UserOverride:
        """Creates a UserOverride object based on the existing UserOverride for the user represented
        in user_dict (if it exists) and any updates in user_dict."""

        user_hash = user_dict["user_hash"]

        if (attrs := _lookup_user_attrs_from_hash(session, user_hash)) is None:
            raise ValueError(
                f"User not found for email address hash {user_hash}, please file a bug"
            )
        (email, state_code) = attrs
        user_dict["email_address"] = email

        validate_roles(user_dict)

        user_dict["state_code"] = user_dict.get("state_code", state_code)
        log_reason(user_dict, f"updating user {user_dict['email_address']}")

        if "external_id" in user_dict:
            # If the user was added entirely, they won't have a pseudo id yet so create one for them.
            # Otherwise, if they were modified and external_id is present in the modification,
            # generate a new pseudo id based on their new external_id.
            user_dict["pseudonymized_id"] = generate_pseudonymized_id(
                user_dict["state_code"], user_dict["external_id"]
            )

        existing = (
            session.query(UserOverride)
            .filter(UserOverride.user_hash == user_hash)
            .first()
        )

        if existing:
            for key, value in user_dict.items():
                setattr(existing, key, value)
            return existing
        return UserOverride(**user_dict)

    def _lookup_user_attrs_from_hash(
        session: Session, user_hash: str
    ) -> tuple[str, str] | None:
        return (
            session.query(
                func.coalesce(UserOverride.email_address, Roster.email_address).label(
                    "email_address"
                ),
                func.coalesce(UserOverride.state_code, Roster.state_code).label(
                    "state_code"
                ),
            )
            .select_from(Roster)
            .join(
                UserOverride,
                UserOverride.email_address == Roster.email_address,
                full=True,
            )
            .filter(
                func.coalesce(UserOverride.user_hash, Roster.user_hash) == user_hash
            )
            .one_or_none()
        )

    @users_blueprint.route("")
    class UsersAPI(MethodView):  # pylint: disable=unused-variable
        """CRUD endpoints for /users."""

        @users_blueprint.response(HTTPStatus.OK, FullUserSchema(many=True))
        def get(self) -> List[Dict]:
            """
            This endpoint is accessed via the admin panel and our auth0 actions. It queries data from four
            Case Triage CloudSQL instance tables (roster, user_override, state_role_permissions, and
            permissions_overrides) in order to account for overrides to a user's roster data or permissions.
            Returns: JSON string with accurate information about state users and their permissions
            """
            results = get_users_query(current_session).all()
            return convert_to_dict_multiple_results(results)

        @users_blueprint.arguments(
            UserRequestSchema,
            # Return BAD_REQUEST on schema validation errors
            error_status_code=HTTPStatus.BAD_REQUEST,
        )
        @users_blueprint.response(HTTPStatus.OK, UserSchema)
        def post(self, user_dict: Dict[str, Any]) -> UserOverride:
            """Adds a new user to UserOverride and returns the created user.
            Returns an error message if a user already exists with that email address
            or if the user does not have one of the predefined roles.
            """
            try:
                user_dict["user_hash"] = generate_user_hash(user_dict["email_address"])
                user_dict["pseudonymized_id"] = generate_pseudonymized_id(
                    user_dict["state_code"], user_dict.get("external_id", None)
                )
                if (
                    current_session.query(Roster)
                    .filter(Roster.email_address == user_dict["email_address"])
                    .first()
                    is not None
                ):
                    abort(
                        HTTPStatus.UNPROCESSABLE_ENTITY,
                        message="A user with this email already exists in Roster.",
                    )

                validate_roles(user_dict)

                log_reason(user_dict, f"adding user {user_dict['email_address']}")

                user = UserOverride(**user_dict)
                current_session.add(user)
                current_session.commit()

                new_user = (
                    current_session.query(UserOverride)
                    .filter(UserOverride.email_address == user_dict["email_address"])
                    .one()
                )
                return new_user
            except IntegrityError as e:
                if isinstance(e.orig, UniqueViolation):
                    abort(
                        HTTPStatus.UNPROCESSABLE_ENTITY,
                        message="A user with this email already exists in UserOverride.",
                    )
                if isinstance(e.orig, NotNullViolation):
                    abort(HTTPStatus.BAD_REQUEST, message=f"{e}")
                raise e
            except (ProgrammingError, ValueError) as error:
                abort(HTTPStatus.BAD_REQUEST, message=f"{error}")

        @users_blueprint.arguments(
            StateCodeSchema,
            location="query",
            # Return BAD_REQUEST on schema validation errors
            error_status_code=HTTPStatus.BAD_REQUEST,
        )
        @users_blueprint.arguments(
            ReasonSchema,
            location="form",
            # Return BAD_REQUEST on schema validation errors
            error_status_code=HTTPStatus.BAD_REQUEST,
        )
        @users_blueprint.arguments(UploadSchema, location="files")
        @users_blueprint.response(HTTPStatus.OK)
        def put(
            self,
            query_args: Dict[str, str],
            form: Dict[str, str],
            files: Dict[str, FileStorage],
        ) -> str:
            """Adds/updates records in the UserOverride table for uploaded users.
            It assumes that the caller has manually formatted the CSV appropriately.
            Returns an error message if there was an error creating the records.
            """
            state_code = query_args["state_code"]
            log_reason(
                form,
                f"uploading roster for state {state_code}",
            )

            dict_reader = csv.DictReader(
                files["file"].read().decode("utf-8-sig").splitlines()
            )

            # Make sure the columns are what we expect
            expected_columns = [
                "email_address",
                "roles",
                "district",
                "external_id",
                "first_name",
                "last_name",
            ]
            if not dict_reader.fieldnames or sorted(dict_reader.fieldnames) != sorted(
                expected_columns
            ):
                abort(
                    HTTPStatus.BAD_REQUEST,
                    message=f"CSV columns must be exactly {','.join(expected_columns)}",
                )

            rows = list(dict_reader)
            # Convert "" to None for all values so missing values can still be filled in by data in Roster
            for row in rows:
                for k, v in row.items():
                    if v == "":
                        row[k] = None
            try:
                _upsert_user_rows(current_session, state_code, rows, UserOverride)

                return f"{len(rows)} users added/updated to the roster"
            except IntegrityError as e:
                if isinstance(e.orig, NotNullViolation):
                    abort(HTTPStatus.BAD_REQUEST, message=f"{e}")
                raise e
            except (ProgrammingError, ValueError) as e:
                abort(HTTPStatus.BAD_REQUEST, message=f"{e}")

        @users_blueprint.arguments(
            UserRequestSchema(many=True, partial=True),
            # Return BAD_REQUEST on schema validation errors
            error_status_code=HTTPStatus.BAD_REQUEST,
        )
        @users_blueprint.response(HTTPStatus.OK, FullUserSchema(many=True))
        def patch(self, users: List[Dict[str, Any]]) -> List[Row]:
            """Edits existing users' info by adding or updating an entry for those users in UserOverride."""
            try:
                user_hashes = []
                for user_dict in users:
                    current_session.add(
                        _create_user_override(current_session, user_dict)
                    )
                    user_hashes.append(user_dict["user_hash"])
                current_session.commit()
                updated_users = (
                    current_session.query(UserOverride)
                    .filter(UserOverride.user_hash.in_(user_hashes))
                    .all()
                )
                return updated_users
            except (ProgrammingError, ValueError) as e:
                abort(HTTPStatus.BAD_REQUEST, message=f"{e}")

    @users_blueprint.route("<path:user_hash>")
    class UsersByHashAPI(MethodView):  # pylint: disable=unused-variable
        """CRUD endpoints for /users/<user_hash>"""

        @users_blueprint.response(HTTPStatus.OK, FullUserSchema)
        def get(self, user_hash: str) -> Dict:
            """
            This endpoint is accessed via the admin panel and our auth0 actions. It queries data from four
            Case Triage CloudSQL instance tables (roster, user_override, state_role_permissions, and
            permissions_overrides) in order to account for overrides to a user's roster data or permissions.
            Returns: JSON string with accurate information about a state user and their permissions. Only
            returns a user if they are not blocked.
            """
            try:
                user = (
                    get_users_query(current_session)
                    .where(
                        and_(
                            func.coalesce(UserOverride.user_hash, Roster.user_hash)
                            == user_hash,
                            or_(
                                UserOverride.blocked_on.is_(None),
                                UserOverride.blocked_on > func.now(),
                            ),
                        )
                    )
                    .one()
                )
                return convert_to_dict_single_result(user)
            except NoResultFound:
                abort(
                    HTTPStatus.NOT_FOUND,
                    message=f"User not found for email address hash {user_hash}, please file a bug",
                )

        @users_blueprint.arguments(
            # set "partial" on schema so all fields are optional. we can derive the user from the hash.
            UserRequestSchema(partial=True),
            # Return BAD_REQUEST on schema validation errors
            error_status_code=HTTPStatus.BAD_REQUEST,
        )
        @users_blueprint.response(HTTPStatus.OK, FullUserSchema)
        def patch(self, user_dict: Dict[str, Any], user_hash: str) -> Row:
            """
            Edits an existing user's info by adding or updating an entry for that user in UserOverride.
            Returns the updated user.
            """
            try:
                user_dict["user_hash"] = user_hash
                current_session.add(_create_user_override(current_session, user_dict))
                current_session.commit()
                return self.get(user_hash)
            except (ProgrammingError, ValueError) as e:
                abort(HTTPStatus.BAD_REQUEST, message=f"{e}")

        @users_blueprint.arguments(
            ReasonSchema,
            # Return BAD_REQUEST on schema validation errors
            error_status_code=HTTPStatus.BAD_REQUEST,
        )
        @users_blueprint.response(HTTPStatus.OK)
        def delete(self, body: Dict[str, str], user_hash: str) -> None:
            """Blocks a user by setting blocked_on=today's date in the corresponding UserOverride object."""
            try:
                existing_override = current_session.query(UserOverride).filter(
                    UserOverride.user_hash == user_hash
                )
                if value := existing_override.first():
                    existing_override.update(
                        {"blocked_on": datetime.now(tzlocal())},
                        synchronize_session=False,
                    )
                    current_session.commit()
                    email = value.email_address
                else:
                    roster_user = (
                        current_session.query(Roster)
                        .filter(Roster.user_hash == user_hash)
                        .first()
                    )
                    if roster_user is None:
                        raise ValueError(
                            f"An entry for user_hash {user_hash} does not exist."
                        )
                    email = roster_user.email_address
                    user_override = UserOverride(
                        state_code=roster_user.state_code,
                        email_address=roster_user.email_address,
                        blocked_on=datetime.now(tzlocal()),
                        user_hash=roster_user.user_hash,
                    )
                    current_session.add(user_override)
                    current_session.commit()

                log_reason(
                    body,
                    f"blocking user {email}",
                )
            except ValueError as error:
                abort(HTTPStatus.BAD_REQUEST, message=f"{error}")

    @users_blueprint.route("<path:user_hash>/permissions")
    class UserPermissionsAPI(MethodView):  # pylint: disable=unused-variable
        """CRUD endpoints for /users/<user_hash>/permissions"""

        @users_blueprint.arguments(
            PermissionsRequestSchema,
            # Return BAD_REQUEST on schema validation errors
            error_status_code=HTTPStatus.BAD_REQUEST,
        )
        @users_blueprint.response(HTTPStatus.OK, PermissionsResponseSchema)
        def put(
            self,
            user_dict: Dict[str, Any],
            user_hash: str,
        ) -> PermissionsOverride:
            """Gives a state user custom permissions by adding or updating an entry for that user in Permissions Override."""
            try:
                if (
                    attrs := _lookup_user_attrs_from_hash(current_session, user_hash)
                ) is None:
                    raise ValueError(
                        f"User not found for email address hash {user_hash}, please file a bug"
                    )
                (email, _) = attrs

                user_dict["email_address"] = email
                log_reason(
                    user_dict,
                    f"updating permissions for user {user_dict['email_address']}",
                )

                if (
                    current_session.query(PermissionsOverride)
                    .filter(PermissionsOverride.email_address == email)
                    .first()
                    is None
                ):
                    new_permissions = PermissionsOverride(**user_dict)
                    current_session.add(new_permissions)
                    current_session.commit()
                    return new_permissions
                current_session.execute(
                    update(PermissionsOverride)
                    .where(PermissionsOverride.email_address == email)
                    .values(user_dict)
                )
                current_session.commit()
                return (
                    current_session.query(PermissionsOverride)
                    .filter(PermissionsOverride.email_address == email)
                    .first()
                )
            except (StatementError, TypeError, ValueError) as error:
                abort(HTTPStatus.BAD_REQUEST, message=f"{error}")

        @users_blueprint.arguments(
            ReasonSchema,
            # Return BAD_REQUEST on schema validation errors
            error_status_code=HTTPStatus.BAD_REQUEST,
        )
        @users_blueprint.response(HTTPStatus.OK)
        def delete(self, body: Dict[str, str], user_hash: str) -> None:
            """Removes state user custom permissions by deleting an entry for that user in Permissions Override."""
            try:
                if (
                    attrs := _lookup_user_attrs_from_hash(current_session, user_hash)
                ) is None:
                    raise ValueError(
                        f"User not found for email address hash {user_hash}, please file a bug"
                    )
                (email, _) = attrs

                overrides = current_session.query(PermissionsOverride).filter(
                    PermissionsOverride.email_address == email
                )
                if overrides.first() is None:
                    raise ValueError(
                        f"An entry for {email} in PermissionsOverride does not exist."
                    )

                log_reason(
                    body,
                    f"removing custom permissions for user {email}",
                )

                overrides.delete(synchronize_session=False)
                current_session.commit()
            except ValueError as error:
                abort(HTTPStatus.BAD_REQUEST, message=f"{error}")

    @users_blueprint.route("feature_variants/<path:feature_variant>")
    class FeatureVariantAPI(MethodView):  # pylint: disable=unused-variable
        """CRUD endpoints for /users/feature_variants/<feature_variant>"""

        @users_blueprint.arguments(
            ReasonSchema,
            # Return BAD_REQUEST on schema validation errors
            error_status_code=HTTPStatus.BAD_REQUEST,
        )
        @users_blueprint.response(HTTPStatus.OK)
        def delete(self, body: Dict[str, str], feature_variant: str) -> None:
            """Removes a feature variant from all custom permissions for all users."""
            try:
                num_affected_users = bulk_delete_feature_variant(
                    current_session, PermissionsOverride, feature_variant
                )

                if num_affected_users == 0:
                    abort(
                        HTTPStatus.NOT_FOUND,
                        message=f"Feature variant {feature_variant} does not exist in PermissionsOverride.",
                    )

                log_reason(
                    body,
                    f"removing feature variant {feature_variant} from {num_affected_users} user permissions overrides",
                )

                current_session.commit()

            except ValueError as error:
                abort(HTTPStatus.BAD_REQUEST, message=f"{error}")

    return users_blueprint
