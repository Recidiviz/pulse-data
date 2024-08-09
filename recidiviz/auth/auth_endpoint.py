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

"""Endpoints related to auth operations.
"""
import json
import logging
import os
from http import HTTPStatus
from typing import Any, Callable, Dict, List, Tuple, Type, Union

import pandas as pd
import sqlalchemy.orm.exc
from flask import Blueprint, Response, jsonify, request
from psycopg2.errors import (  # pylint: disable=no-name-in-module
    NotNullViolation,
    UniqueViolation,
)
from sqlalchemy import delete, func, inspect
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.exc import IntegrityError, ProgrammingError, StatementError
from sqlalchemy.sql import Update

from recidiviz.auth.helpers import (
    generate_pseudonymized_id,
    generate_user_hash,
    log_reason,
)
from recidiviz.calculator.query.state.views.reference.ingested_product_users import (
    INGESTED_PRODUCT_USERS_VIEW_BUILDER,
)
from recidiviz.cloud_storage.gcsfs_csv_reader import GcsfsCsvReader
from recidiviz.cloud_storage.gcsfs_csv_reader_delegates import (
    SimpleGcsfsCsvReaderDelegate,
)
from recidiviz.cloud_storage.gcsfs_factory import GcsfsFactory
from recidiviz.cloud_storage.gcsfs_path import GcsfsFilePath
from recidiviz.common.common_utils import convert_nested_dictionary_keys
from recidiviz.common.constants.states import StateCode
from recidiviz.common.google_cloud.single_cloud_task_queue_manager import (
    CloudTaskQueueInfo,
    SingleCloudTaskQueueManager,
    get_cloud_task_json_body,
)
from recidiviz.common.str_field_utils import to_snake_case
from recidiviz.metrics.export.export_config import (
    PRODUCT_USER_IMPORT_OUTPUT_DIRECTORY_URI,
)
from recidiviz.persistence.database.schema.case_triage.schema import (
    PermissionsOverride,
    Roster,
    StateRolePermissions,
    UserOverride,
)
from recidiviz.persistence.database.schema_type import SchemaType
from recidiviz.persistence.database.session import Session
from recidiviz.persistence.database.session_factory import SessionFactory
from recidiviz.persistence.database.sqlalchemy_database_key import SQLAlchemyDatabaseKey
from recidiviz.reporting.email_reporting_utils import validate_email_address
from recidiviz.utils import metadata
from recidiviz.utils.pubsub_helper import OBJECT_ID, extract_pubsub_message_from_json
from recidiviz.utils.string import StrictStringFormatter
from recidiviz.utils.types import assert_type

CASE_TRIAGE_DB_OPERATIONS_QUEUE = "case-triage-db-operations-queue"


def _validate_state_code(state_code: str) -> None:
    if not StateCode.is_state_code(state_code.upper()):
        raise ValueError(
            f"Unknown state_code [{state_code}] received, must be a valid state code."
        )


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
        .filter(func.coalesce(UserOverride.user_hash, Roster.user_hash) == user_hash)
        .one_or_none()
    )


def _upsert_user_rows(
    session: Session,
    state_code: str,
    rows: List[Dict[str, Any]],
    table: Type[Roster] | Type[UserOverride],
) -> None:
    """Upserts rows into a table that stores user attributes (Roster or UserOverride),
    along with some validation."""
    for row in rows:
        if not row["email_address"]:
            raise ValueError(
                "Roster contains a row that is missing an email address (required)"
            )
        validate_email_address(row["email_address"])

        email = row["email_address"].lower()
        roles = [value.strip().lower() for value in row["roles"].split(",")]

        associated_state_roles = (
            session.query(StateRolePermissions.role)
            .filter_by(state_code=f"{state_code.upper()}")
            .where(StateRolePermissions.role.in_(roles))
            .all()
        )
        if len(associated_state_roles) != len(roles):
            raise ValueError(
                f"Roster contains a row that with a role that does not exist in the default state role permissions. Offending row has email {email}"
            )

        # Enforce casing for columns where we have a preference.
        row["state_code"] = state_code.upper()
        row["email_address"] = email
        if row.get("external_id") is not None:
            row["external_id"] = row["external_id"].upper()
            if row.get("pseudonymized_id") is None:
                row["pseudonymized_id"] = generate_pseudonymized_id(
                    row["state_code"], row["external_id"]
                )
        row["roles"] = [value.strip().lower() for value in row["roles"].split(",")]
        row["role"] = row["roles"][
            0
        ]  # leaving this here until we remove 'role' from the schemas
        row["user_hash"] = generate_user_hash(row["email_address"])

        # update existing row or add new row
        existing = (
            session.query(table)
            .filter_by(state_code=f"{state_code.upper()}", email_address=f"{email}")
            .first()
        )
        if existing:
            for key, value in row.items():
                setattr(existing, key, value)
        else:
            new_row = table(**row)
            session.add(new_row)

    session.commit()


def get_auth_endpoint_blueprint(
    authentication_middleware: Callable | None,
) -> Blueprint:
    """Creates the auth Flask Blueprint"""
    auth_endpoint_blueprint = Blueprint("auth_endpoint_blueprint", __name__)

    if authentication_middleware:

        @auth_endpoint_blueprint.before_request
        @authentication_middleware
        def auth_middleware() -> None:
            pass

    @auth_endpoint_blueprint.route("/states", methods=["GET"])
    def states() -> Response:
        database_key = SQLAlchemyDatabaseKey.for_schema(
            schema_type=SchemaType.CASE_TRIAGE
        )
        with SessionFactory.using_database(database_key, autocommit=False) as session:
            state_permissions = (
                session.query(StateRolePermissions)
                .order_by(StateRolePermissions.state_code, StateRolePermissions.role)
                .all()
            )
            return jsonify(
                [
                    {
                        "stateCode": res.state_code,
                        "role": res.role,
                        "routes": res.routes,
                        "featureVariants": res.feature_variants,
                    }
                    for res in state_permissions
                ]
            )

    @auth_endpoint_blueprint.route("/state/<state_code>", methods=["GET"])
    def state_info(state_code: str) -> Response:
        """Returns the unique districts, roles, and default user permissions for a given state."""
        database_key = SQLAlchemyDatabaseKey.for_schema(
            schema_type=SchemaType.CASE_TRIAGE
        )
        with SessionFactory.using_database(database_key, autocommit=False) as session:
            districts = (
                session.query(Roster.district)
                .filter(Roster.state_code == state_code)
                .distinct()
                .all()
            )
            role_permissions = (
                session.query(StateRolePermissions)
                .filter(StateRolePermissions.state_code == state_code)
                .distinct()
                .all()
            )
            return jsonify(
                [
                    {
                        "districts": [dis.district for dis in districts],
                        "roles": [
                            {
                                "name": per.role,
                                "permissions": {
                                    "routes": per.routes,
                                    "feature_variants": per.feature_variants,
                                },
                            }
                            for per in role_permissions
                        ],
                    }
                ]
            )

    @auth_endpoint_blueprint.route(
        "/states/<state_code>/roles/<role>", methods=["POST"]
    )
    def add_state_role(
        state_code: str, role: str
    ) -> Union[tuple[Response, int], tuple[str, int]]:
        """Adds default permissions for a given role in a state."""
        if not StateCode.is_state_code(state_code.upper()):
            return (
                f"Unknown state_code [{state_code}] received, must be a valid state code.",
                HTTPStatus.BAD_REQUEST,
            )
        database_key = SQLAlchemyDatabaseKey.for_schema(
            schema_type=SchemaType.CASE_TRIAGE
        )
        try:
            with SessionFactory.using_database(database_key) as session:
                request_dict = {
                    # convert request keys to snake_case to match DB columns. Don't convert nested keys
                    # because we expect 'routes' to be JSON with keys that contain uppercase letters.
                    to_snake_case(k): v
                    for k, v in assert_type(request.json, dict).items()
                }
                request_dict["state_code"] = state_code.upper()
                request_dict["role"] = role.lower()
                if routes := request_dict.get("routes"):
                    request_dict["routes"] = assert_type(routes, dict)

                log_reason(
                    request_dict,
                    f"adding permissions for state {request_dict['state_code']}, role {request_dict['role']}",
                )

                state_role = StateRolePermissions(**request_dict)
                session.add(state_role)
                session.commit()
                return (
                    jsonify(
                        {
                            "stateCode": state_role.state_code,
                            "role": state_role.role,
                            "routes": state_role.routes,
                            "featureVariants": state_role.feature_variants,
                        }
                    ),
                    HTTPStatus.OK,
                )
        except IntegrityError as e:
            if isinstance(e.orig, UniqueViolation):
                return (
                    "A state with this role already exists in StateRolePermissions.",
                    HTTPStatus.BAD_REQUEST,
                )
            if isinstance(e.orig, NotNullViolation):
                return (
                    f"{e}",
                    HTTPStatus.BAD_REQUEST,
                )
            raise e
        except (ProgrammingError, ValueError) as error:
            return (
                f"{error}",
                HTTPStatus.BAD_REQUEST,
            )

    @auth_endpoint_blueprint.route(
        "/states/<state_code>/roles/<role>", methods=["PATCH"]
    )
    def update_state_role(
        state_code: str, role: str
    ) -> Union[tuple[Response, int], tuple[str, int]]:
        """Updates the default permissions for a given role in a state."""
        if not StateCode.is_state_code(state_code.upper()):
            return (
                f"Unknown state_code [{state_code}] received, must be a valid state code.",
                HTTPStatus.BAD_REQUEST,
            )
        database_key = SQLAlchemyDatabaseKey.for_schema(
            schema_type=SchemaType.CASE_TRIAGE
        )
        try:
            with SessionFactory.using_database(database_key) as session:
                # Read existing entry
                state_role = (
                    session.query(StateRolePermissions)
                    .filter(
                        StateRolePermissions.state_code == state_code.upper(),
                        StateRolePermissions.role == role.lower(),
                    )
                    .one()
                )

                # Transform request dict to match expected structure/casing
                request_dict = {
                    # convert request keys to snake_case to match DB columns. Don't convert nested keys
                    # because we expect 'routes' to be JSON with keys that contain uppercase letters.
                    to_snake_case(k): v
                    for k, v in assert_type(request.json, dict).items()
                }
                if rsc := request_dict.get("state_code"):
                    request_dict["state_code"] = rsc.upper()
                if rr := request_dict.get("role"):
                    request_dict["role"] = rr.lower()

                log_reason(
                    request_dict,
                    f"updating permissions for state {state_code.upper()}, role {role.lower()}",
                )

                # Perform update
                json_columns = [
                    c.name
                    for c in inspect(StateRolePermissions).columns
                    if isinstance(c.type, JSONB)
                ]
                for k, v in request_dict.items():
                    if k in json_columns:
                        continue
                    setattr(state_role, k, v)

                for col_name in json_columns:
                    if col_value := request_dict.get(col_name):
                        new_value = (
                            {**getattr(state_role, col_name), **col_value}
                            if getattr(state_role, col_name)
                            else col_value
                        )
                        # Note: JSONB does not detect in-place changes when used with the ORM.
                        # (https://docs.sqlalchemy.org/en/13/dialects/postgresql.html#sqlalchemy.dialects.postgresql.JSONB)
                        # The sqlalchemy.ext.mutable extension supposedly makes it so it does, but it didn't
                        # for me, so instead we just assign it to a new dict, which does the trick.
                        setattr(
                            state_role,
                            col_name,
                            new_value,
                        )

                session.commit()

                return (
                    jsonify(
                        {
                            "stateCode": state_role.state_code,
                            "role": state_role.role,
                            "routes": state_role.routes,
                            "featureVariants": state_role.feature_variants,
                        }
                    ),
                    HTTPStatus.OK,
                )
        except sqlalchemy.orm.exc.NoResultFound:
            return (
                f"State {state_code.upper()} with role {role.lower()} does not exist in StateRolePermissions.",
                HTTPStatus.NOT_FOUND,
            )
        except IntegrityError as e:
            if isinstance(e.orig, NotNullViolation):
                return (
                    f"{e}",
                    HTTPStatus.BAD_REQUEST,
                )
            raise e
        except (ProgrammingError, ValueError) as error:
            return (
                f"{error}",
                HTTPStatus.BAD_REQUEST,
            )

    @auth_endpoint_blueprint.route(
        "/states/<state_code>/roles/<role>", methods=["DELETE"]
    )
    def delete_state_role(
        state_code: str, role: str
    ) -> Union[tuple[Response, int], tuple[str, int]]:
        """Removes a role in a given state. Fails if there are still active users with that role in that
        state. If there are no active users, removes any blocked users with that role in that state.
        """
        if not StateCode.is_state_code(state_code.upper()):
            return (
                f"Unknown state_code [{state_code}] received, must be a valid state code.",
                HTTPStatus.BAD_REQUEST,
            )
        database_key = SQLAlchemyDatabaseKey.for_schema(
            schema_type=SchemaType.CASE_TRIAGE
        )
        try:
            with SessionFactory.using_database(database_key) as session:
                # Read existing entry
                state_role = (
                    session.query(StateRolePermissions)
                    .filter(
                        StateRolePermissions.state_code == state_code.upper(),
                        StateRolePermissions.role == role.lower(),
                    )
                    .one()
                )

                # Check if any users have access in that state/role
                state_role_users = (
                    session.query(UserOverride, Roster)
                    .select_from(Roster)
                    .join(
                        UserOverride,
                        UserOverride.email_address == Roster.email_address,
                        full=True,
                    )
                    .where(
                        func.coalesce(UserOverride.state_code, Roster.state_code)
                        == state_code,
                        func.coalesce(UserOverride.roles, Roster.roles).any(role),
                    )
                    .all()
                )
                users_with_access = [
                    (override_user or roster_user)
                    for (override_user, roster_user) in state_role_users
                    if not (override_user and override_user.blocked)
                ]
                if len(users_with_access) > 0:
                    return (
                        f"Cannot remove {role} from {state_code}. {len(users_with_access)} user(s) with access must have their access revoked first.",
                        HTTPStatus.BAD_REQUEST,
                    )

                request_json = assert_type(request.json, dict)
                request_dict = convert_nested_dictionary_keys(
                    request_json, to_snake_case
                )
                log_reason(
                    request_dict,
                    f"removing permissions and blocked users for state {state_code.upper()}, role {role.lower()}",
                )

                # Delete remaining users
                for override_user, roster_user in state_role_users:
                    if override_user:
                        session.delete(override_user)
                    if roster_user:
                        session.delete(roster_user)

                # Delete role
                session.delete(state_role)

                session.commit()

                return (
                    jsonify({}),
                    HTTPStatus.OK,
                )

        except sqlalchemy.orm.exc.NoResultFound:
            return (
                f"State {state_code.upper()} with role {role.lower()} does not exist in StateRolePermissions.",
                HTTPStatus.NOT_FOUND,
            )
        except ValueError as error:
            return (f"{error}", HTTPStatus.BAD_REQUEST)

    # "path" type annotation allows parameters containing slashes, which the user hash might
    @auth_endpoint_blueprint.route(
        "/users/<path:user_hash>/permissions", methods=["PUT"]
    )
    def update_user_permissions(
        user_hash: str,
    ) -> Union[tuple[Response, int], tuple[str, int]]:
        """Gives a state user custom permissions by adding or updating an entry for that user in Permissions Override."""
        database_key = SQLAlchemyDatabaseKey.for_schema(
            schema_type=SchemaType.CASE_TRIAGE
        )
        try:
            with SessionFactory.using_database(database_key) as session:
                request_json = assert_type(request.json, dict)
                user_dict = convert_nested_dictionary_keys(request_json, to_snake_case)
                # user_dict's value for "routes" and "feature_variants" shouldn't be in snake case
                routes_json = request_json.get("routes")
                if routes_json is not None:
                    user_dict["routes"] = assert_type(routes_json, dict)
                feature_variants_json = request_json.get("featureVariants")
                if feature_variants_json is not None:
                    user_dict["feature_variants"] = assert_type(
                        feature_variants_json, dict
                    )

                if (attrs := _lookup_user_attrs_from_hash(session, user_hash)) is None:
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
                    session.query(PermissionsOverride)
                    .filter(PermissionsOverride.email_address == email)
                    .first()
                    is None
                ):
                    new_permissions = PermissionsOverride(**user_dict)
                    session.add(new_permissions)
                    session.commit()
                    return (
                        jsonify(
                            {
                                "emailAddress": new_permissions.email_address,
                                "routes": new_permissions.routes,
                                "featureVariants": new_permissions.feature_variants,
                            }
                        ),
                        HTTPStatus.OK,
                    )
                session.execute(
                    Update(PermissionsOverride)
                    .where(PermissionsOverride.email_address == email)
                    .values(user_dict)
                )
                updated_permissions = (
                    session.query(PermissionsOverride)
                    .filter(PermissionsOverride.email_address == email)
                    .first()
                )
                return (
                    jsonify(
                        {
                            "emailAddress": updated_permissions.email_address,
                            "routes": updated_permissions.routes,
                            "featureVariants": updated_permissions.feature_variants,
                        }
                    ),
                    HTTPStatus.OK,
                )
        except (StatementError, TypeError, ValueError) as error:
            return (
                f"{error}",
                HTTPStatus.BAD_REQUEST,
            )

    # "path" type annotation allows parameters containing slashes, which the user hash might
    @auth_endpoint_blueprint.route(
        "/users/<path:user_hash>/permissions", methods=["DELETE"]
    )
    def delete_user_permissions(user_hash: str) -> tuple[str, int]:
        """Removes state user custom permissions by deleting an entry for that user in Permissions Override."""
        database_key = SQLAlchemyDatabaseKey.for_schema(
            schema_type=SchemaType.CASE_TRIAGE
        )
        try:
            with SessionFactory.using_database(database_key) as session:
                if (attrs := _lookup_user_attrs_from_hash(session, user_hash)) is None:
                    raise ValueError(
                        f"User not found for email address hash {user_hash}, please file a bug"
                    )
                (email, _) = attrs

                overrides = session.query(PermissionsOverride).filter(
                    PermissionsOverride.email_address == email
                )
                if overrides.first() is None:
                    raise ValueError(
                        f"An entry for {email} in PermissionsOverride does not exist."
                    )

                request_json = assert_type(request.json, dict)
                request_dict = convert_nested_dictionary_keys(
                    request_json, to_snake_case
                )
                log_reason(
                    request_dict,
                    f"removing custom permissions for user {email}",
                )

                overrides.delete(synchronize_session=False)
                return (
                    f"{email} has been deleted from PermissionsOverride.",
                    HTTPStatus.OK,
                )
        except ValueError as error:
            return (
                f"{error}",
                HTTPStatus.BAD_REQUEST,
            )

    # "path" type annotation allows parameters containing slashes, which the user hash might
    @auth_endpoint_blueprint.route("/users/<path:user_hash>", methods=["DELETE"])
    def delete_user(user_hash: str) -> tuple[str, int]:
        """Blocks a user by setting blocked=true in the corresponding Roster object."""
        database_key = SQLAlchemyDatabaseKey.for_schema(
            schema_type=SchemaType.CASE_TRIAGE
        )
        try:
            with SessionFactory.using_database(database_key) as session:
                request_json = assert_type(request.json, dict)
                request_dict = convert_nested_dictionary_keys(
                    request_json, to_snake_case
                )

                existing_override = session.query(UserOverride).filter(
                    UserOverride.user_hash == user_hash
                )
                if value := existing_override.first():
                    existing_override.update(
                        {"blocked": True}, synchronize_session=False
                    )
                    email = value.email_address
                else:
                    roster_user = (
                        session.query(Roster)
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
                        blocked=True,
                        user_hash=roster_user.user_hash,
                    )
                    session.add(user_override)
                    session.commit()

                log_reason(
                    request_dict,
                    f"blocking user {email}",
                )
                return (
                    f"{email} has been blocked.",
                    HTTPStatus.OK,
                )
        except ValueError as error:
            return (
                f"{error}",
                HTTPStatus.BAD_REQUEST,
            )

    @auth_endpoint_blueprint.route("/import_ingested_users_async", methods=["POST"])
    def import_ingested_users_async() -> Tuple[str, HTTPStatus]:
        """Called from a Cloud Storage Notification when a new file is created in the product user import
        bucket. It enqueues a task to import the file into Cloud SQL."""
        try:
            message = extract_pubsub_message_from_json(request.get_json())
        except Exception as e:
            return str(e), HTTPStatus.BAD_REQUEST

        if not message.attributes:
            return "Invalid Pub/Sub message", HTTPStatus.BAD_REQUEST

        attributes = message.attributes
        state_code, filename = os.path.split(attributes[OBJECT_ID])

        if not state_code:
            logging.info("Missing state code, ignoring")
            return "", HTTPStatus.OK

        # It would be nice if we could do this as a filter in the GCS notification instead of as logic
        # here, but as of Jan 2023, the available filters are not expressive enough for our needs:
        # https://cloud.google.com/pubsub/docs/filtering#filtering_syntax
        if filename != "ingested_product_users.csv":
            logging.info("Unknown filename %s, ignoring", filename)
            # We want to ignore these instead of erroring because Pub/Sub will retry the request if it
            # doesn't return a successful status code, and this is a permanent failure instead of a
            # temporary blip (https://cloud.google.com/pubsub/docs/push#receive_push).
            return "", HTTPStatus.OK

        cloud_task_manager = SingleCloudTaskQueueManager(
            queue_info_cls=CloudTaskQueueInfo,
            queue_name=CASE_TRIAGE_DB_OPERATIONS_QUEUE,
        )
        cloud_task_manager.create_task(
            relative_uri="/auth/import_ingested_users",
            body={"state_code": state_code},
        )
        logging.info(
            "Enqueued import_ingested_users task to %s",
            CASE_TRIAGE_DB_OPERATIONS_QUEUE,
        )
        return "", HTTPStatus.OK

    class ImportIngestedUsersGcsfsCsvReaderDelegate(SimpleGcsfsCsvReaderDelegate):
        """Helper class to upsert chunks of data read from GCS to the Roster table"""

        def __init__(self, session: Session, state_code: str) -> None:
            self.emails: List[str] = []
            self.session = session
            self.state_code = state_code

        def on_dataframe(self, encoding: str, chunk_num: int, df: pd.DataFrame) -> bool:
            df.columns = INGESTED_PRODUCT_USERS_VIEW_BUILDER.columns
            # df.to_dict exports missing values as 'nan', so export to json instead
            rows = json.loads(df.to_json(orient="records"))
            _upsert_user_rows(self.session, self.state_code, rows, Roster)
            self.emails.extend(r["email_address"] for r in rows)
            return True

    @auth_endpoint_blueprint.route("/import_ingested_users", methods=["POST"])
    def import_ingested_users() -> Tuple[str, HTTPStatus]:
        """This endpoint triggers the import of the ingested product users file to Cloud SQL. It is
        requested by a Cloud Task that is triggered by the import_ingested_users_async
        endpoint when a new file is created in the ingested product users bucket."""
        try:
            body = get_cloud_task_json_body()
            state_code = body.get("state_code")
            if not state_code:
                return "Missing state_code param", HTTPStatus.BAD_REQUEST

            try:
                _validate_state_code(state_code)
            except ValueError as error:
                logging.error(error)
                return str(error), HTTPStatus.BAD_REQUEST

            csv_path = GcsfsFilePath.from_absolute_path(
                os.path.join(
                    StrictStringFormatter().format(
                        PRODUCT_USER_IMPORT_OUTPUT_DIRECTORY_URI,
                        project_id=metadata.project_id(),
                    ),
                    state_code,
                    f"{INGESTED_PRODUCT_USERS_VIEW_BUILDER.view_id}.csv",
                )
            )
            database_key = SQLAlchemyDatabaseKey.for_schema(
                schema_type=SchemaType.CASE_TRIAGE
            )
            with SessionFactory.using_database(database_key) as session:
                # First, upsert users that exist in the CSV file. Then, delete any user that does not
                # exist in the CSV file. Do this instead of import_gcs_csv_to_cloud_sql in order to have
                # more accurate created/updated timestamps
                reader_delegate = ImportIngestedUsersGcsfsCsvReaderDelegate(
                    session, state_code
                )
                csv_reader = GcsfsCsvReader(fs=GcsfsFactory.build())
                csv_reader.streaming_read(
                    path=csv_path,
                    delegate=reader_delegate,
                    chunk_size=1000,
                    header=None,
                )
                delete_stmt = delete(Roster).where(
                    Roster.state_code == state_code,
                    Roster.email_address.not_in(reader_delegate.emails),
                )
                session.execute(delete_stmt)

            logging.info(
                "CSV (%s) successfully imported to Cloud SQL schema %s for region code %s",
                csv_path.blob_name,
                SchemaType.CASE_TRIAGE,
                state_code,
            )

            return (
                f"CSV {csv_path.blob_name} successfully "
                f"imported to Cloud SQL schema {SchemaType.CASE_TRIAGE} for region code {state_code}",
                HTTPStatus.OK,
            )
        except IntegrityError as e:
            if isinstance(e.orig, NotNullViolation):
                logging.error(e)
                return (
                    f"{e}",
                    HTTPStatus.BAD_REQUEST,
                )
            raise e
        except (ProgrammingError, ValueError) as error:
            logging.error(error)
            return (
                f"{error}",
                HTTPStatus.BAD_REQUEST,
            )
        except Exception as error:
            logging.error(error)
            return (str(error), HTTPStatus.INTERNAL_SERVER_ERROR)

    return auth_endpoint_blueprint
