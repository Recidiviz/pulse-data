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
from datetime import datetime, timedelta
from http import HTTPStatus
from typing import Any, Callable, Dict, List, Tuple, Type, Union

import pandas as pd
import sqlalchemy.orm.exc
from dateutil.tz import tzlocal
from flask import Blueprint, Response, jsonify, request
from psycopg2.errors import (  # pylint: disable=no-name-in-module
    NotNullViolation,
    UniqueViolation,
)
from sqlalchemy import delete, func, inspect, select, update
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.exc import IntegrityError, ProgrammingError

from recidiviz.auth.cleanup_user_overrides import cleanup_user_overrides
from recidiviz.auth.constants import PREDEFINED_ROLES
from recidiviz.auth.helpers import (
    bulk_delete_feature_variant,
    convert_user_object_to_dict,
    generate_pseudonymized_id,
    generate_user_hash,
    log_reason,
    validate_roles,
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
from recidiviz.utils.metadata import CloudRunMetadata
from recidiviz.utils.pubsub_helper import OBJECT_ID, extract_pubsub_message_from_json
from recidiviz.utils.string import StrictStringFormatter
from recidiviz.utils.types import assert_type

CASE_TRIAGE_DB_OPERATIONS_QUEUE = "case-triage-db-operations-queue"


def _validate_state_code(state_code: str) -> None:
    if not StateCode.is_state_code(state_code.upper()):
        raise ValueError(
            f"Unknown state_code [{state_code}] received, must be a valid state code."
        )


def _upsert(
    session: Session,
    row: Dict[str, Any],
    table: Type[Roster] | Type[UserOverride],
    update_null_only: bool = False,
) -> None:
    existing = (
        session.query(table)
        .filter_by(
            state_code=f"{row['state_code']}", email_address=f"{row['email_address']}"
        )
        .first()
    )
    if existing:
        for key, value in row.items():
            if not update_null_only or getattr(existing, key) is None:
                setattr(existing, key, value)
    else:
        new_row = table(**row)
        session.add(new_row)


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
        row["roles"] = roles
        validate_roles(row)

        # Experiments create arbitrary roles, and we don't want to throw an error if they aren't all configured
        non_experiment_roles = [
            role for role in roles if not role.startswith("experiment-")
        ]

        associated_state_roles = (
            session.query(StateRolePermissions.role)
            .filter_by(state_code=f"{state_code.upper()}")
            .where(StateRolePermissions.role.in_(non_experiment_roles))
            .all()
        )
        if len(associated_state_roles) != len(non_experiment_roles):
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
        row["roles"] = roles
        row["user_hash"] = generate_user_hash(row["email_address"])

        # update existing row or add new row
        _upsert(session, row, table)

    session.commit()


def get_auth_endpoint_blueprint(
    cloud_run_metadata: CloudRunMetadata,
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
                        "routes": res.routes if res.routes is not None else {},
                        "featureVariants": res.feature_variants
                        if res.feature_variants is not None
                        else {},
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
                                    "routes": per.routes
                                    if per.routes is not None
                                    else {},
                                    "feature_variants": per.feature_variants
                                    if per.feature_variants is not None
                                    else {},
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
                if role.lower() == "unknown" and (
                    request_dict.get("routes") or request_dict.get("feature_variants")
                ):
                    return (
                        "The 'unknown' role cannot be assigned permissions.",
                        HTTPStatus.BAD_REQUEST,
                    )
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
        if role.lower() == "unknown":
            return (
                "The 'unknown' role cannot be assigned permissions.",
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
                            "routes": state_role.routes
                            if state_role.routes is not None
                            else {},
                            "featureVariants": state_role.feature_variants
                            if state_role.feature_variants is not None
                            else {},
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
                    if not (
                        override_user
                        and override_user.blocked_on is not None
                        and override_user.blocked_on < datetime.now(tzlocal())
                    )
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

    @auth_endpoint_blueprint.route("/states/<state_code>/roles", methods=["POST"])
    def add_standard_state_roles(
        state_code: str,
    ) -> Union[tuple[Response, int], tuple[str, int]]:
        """Adds default permissions for all standard roles in a state."""
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

                log_reason(
                    request_dict,
                    f"adding standard role permissions for state {request_dict['state_code']}",
                )

                existing_roles = (
                    session.query(StateRolePermissions.role)
                    .where(StateRolePermissions.state_code == state_code.upper())
                    .all()
                )
                existing_role_names = [role[0] for role in existing_roles]

                state_roles = [
                    StateRolePermissions(state_code=state_code.upper(), role=role)
                    for role in PREDEFINED_ROLES
                    if role not in existing_role_names
                ]
                session.add_all(state_roles)
                session.commit()

                return (
                    jsonify(
                        [
                            {
                                "stateCode": role.state_code,
                                "role": role.role,
                                "routes": role.routes,
                                "featureVariants": role.feature_variants,
                            }
                            for role in state_roles
                        ],
                    ),
                    HTTPStatus.OK,
                )
        except (ProgrammingError, ValueError) as error:
            return (
                f"{error}",
                HTTPStatus.BAD_REQUEST,
            )

    @auth_endpoint_blueprint.route(
        "/feature_variants/<feature_variant>", methods=["DELETE"]
    )
    def delete_feature_variant_from_state_roles(
        feature_variant: str,
    ) -> Union[tuple[Response, int], tuple[str, int]]:
        """Removes a feature variant from all roles in all states."""
        database_key = SQLAlchemyDatabaseKey.for_schema(
            schema_type=SchemaType.CASE_TRIAGE
        )
        try:
            with SessionFactory.using_database(database_key) as session:
                num_affected_roles = bulk_delete_feature_variant(
                    session, StateRolePermissions, feature_variant
                )

                if num_affected_roles == 0:
                    return (
                        f"Feature variant {feature_variant} does not exist in StateRolePermissions.",
                        HTTPStatus.NOT_FOUND,
                    )

                request_json = assert_type(request.json, dict)
                request_dict = convert_nested_dictionary_keys(
                    request_json, to_snake_case
                )
                log_reason(
                    request_dict,
                    f"removing feature variant {feature_variant} from {num_affected_roles} state roles",
                )

                session.commit()

                return (
                    jsonify({}),
                    HTTPStatus.OK,
                )

        except ValueError as error:
            return (f"{error}", HTTPStatus.BAD_REQUEST)

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
            absolute_uri=f"{cloud_run_metadata.url}/auth/import_ingested_users",
            body={"state_code": state_code},
            service_account_email=cloud_run_metadata.service_account_email,
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
                # Technically this and most of the below error statuses should be BAD_REQUEST, but
                # since the caller is cloud tasks, it'll just retry the request instead of alerting
                # a user to the issue. Instead, we use INTERNAL_SERVER_ERROR so it shows up in the
                # admin panel oncall error page.
                return "Missing state_code param", HTTPStatus.INTERNAL_SERVER_ERROR

            try:
                _validate_state_code(state_code)
            except ValueError as error:
                logging.error(error)
                return str(error), HTTPStatus.INTERNAL_SERVER_ERROR

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
                ingested_emails = reader_delegate.emails

                # First remove any upcoming blocks for ingested users
                session.execute(
                    update(UserOverride)
                    .where(
                        UserOverride.state_code == state_code,
                        UserOverride.email_address.in_(ingested_emails),
                        UserOverride.blocked_on.isnot(None),
                        UserOverride.blocked_on > func.now(),
                    )
                    .values(blocked_on=None),
                    execution_options={"synchronize_session": False},
                )

                # For each user in Roster who is not ingested, copy over their info
                # to UserOverride so we don't lose anything by accident. At the same
                # time, set their upcoming block date for one week in the future.
                roster_user_deletion_criteria = [
                    Roster.state_code == state_code,
                    Roster.email_address.not_in(ingested_emails),
                ]

                roster_users_to_delete = (
                    session.execute(
                        select(Roster).where(*roster_user_deletion_criteria)
                    )
                    .scalars()
                    .all()
                )

                # If a user has a facilities role, delete them from Roster but don't
                # block them. They are still an active user but should not be in Roster
                # anymore because they are not supervision staff/being ingested through
                # roster sync
                facilities_users = (
                    session.execute(
                        select(UserOverride.email_address).where(
                            UserOverride.email_address.in_(
                                [user.email_address for user in roster_users_to_delete]
                            ),
                            func.array_to_string(UserOverride.roles, ", ").ilike(
                                "%facilities%"
                            ),
                        )
                    )
                    .scalars()
                    .all()
                )

                roster_users_as_dicts = [
                    {
                        **convert_user_object_to_dict(user),
                        "blocked_on": datetime.now(tzlocal()) + timedelta(weeks=1)
                        if user.email_address not in facilities_users
                        else None,
                    }
                    for user in roster_users_to_delete
                ]

                for user in roster_users_as_dicts:
                    _upsert(session, user, UserOverride, update_null_only=True)

                # Now we can delete the users not in the CSV file from Roster
                session.execute(
                    delete(Roster).where(*roster_user_deletion_criteria),
                    execution_options={"synchronize_session": False},
                )

                session.commit()

                # After the import, clean up any UserOverride data that is duplicative of Roster data.
                # Users with upcoming blocks set above should be unaffected because they have been
                # removed from Roster
                cleanup_user_overrides(
                    session=session, dry_run=False, state_code=state_code
                )

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
                    HTTPStatus.INTERNAL_SERVER_ERROR,
                )
            raise e
        except (ProgrammingError, ValueError) as error:
            logging.error(error)
            return (
                f"{error}",
                HTTPStatus.INTERNAL_SERVER_ERROR,
            )
        except Exception as error:
            logging.error(error)
            return (str(error), HTTPStatus.INTERNAL_SERVER_ERROR)

    return auth_endpoint_blueprint
