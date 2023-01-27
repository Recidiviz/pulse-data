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
import csv
import logging
import os
from http import HTTPStatus
from typing import Any, Dict, List, Mapping, Optional, Tuple, Union

import sqlalchemy.orm.exc
from flask import Blueprint, Response, jsonify, request
from psycopg2.errors import (  # pylint: disable=no-name-in-module
    NotNullViolation,
    UniqueViolation,
)
from sqlalchemy import func, inspect
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.exc import IntegrityError, ProgrammingError, StatementError
from sqlalchemy.sql import Update

from recidiviz.auth.auth0_client import CaseTriageAuth0AppMetadata
from recidiviz.auth.helpers import format_user_info, generate_user_hash, log_reason
from recidiviz.calculator.query.state.views.reference.dashboard_user_restrictions import (
    DASHBOARD_USER_RESTRICTIONS_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.reference.ingested_product_users import (
    INGESTED_PRODUCT_USERS_VIEW_BUILDER,
)
from recidiviz.case_triage.ops_routes import CASE_TRIAGE_DB_OPERATIONS_QUEUE
from recidiviz.cloud_sql.gcs_import_to_cloud_sql import import_gcs_csv_to_cloud_sql
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
    DASHBOARD_USER_RESTRICTIONS_OUTPUT_DIRECTORY_URI,
    PRODUCT_USER_IMPORT_OUTPUT_DIRECTORY_URI,
)
from recidiviz.persistence.database.schema.case_triage.schema import (
    DashboardUserRestrictions,
    PermissionsOverride,
    Roster,
    StateRolePermissions,
    UserOverride,
)
from recidiviz.persistence.database.schema_type import SchemaType
from recidiviz.persistence.database.session_factory import SessionFactory
from recidiviz.persistence.database.sqlalchemy_database_key import SQLAlchemyDatabaseKey
from recidiviz.reporting.email_reporting_utils import validate_email_address
from recidiviz.utils import metadata
from recidiviz.utils.auth.gae import requires_gae_auth
from recidiviz.utils.params import get_only_str_param_value
from recidiviz.utils.pubsub_helper import OBJECT_ID, extract_pubsub_message_from_json
from recidiviz.utils.string import StrictStringFormatter
from recidiviz.utils.types import assert_type

auth_endpoint_blueprint = Blueprint("auth_endpoint_blueprint", __name__)


@auth_endpoint_blueprint.route(
    "/handle_import_user_restrictions_csv_to_sql", methods=["POST"]
)
@requires_gae_auth
def handle_import_user_restrictions_csv_to_sql() -> Tuple[str, HTTPStatus]:
    """Called from a Cloud Storage Notification when a new file is created in the user restrictions
    bucket. It enqueues a task to import the file into Cloud SQL."""
    try:
        message = extract_pubsub_message_from_json(request.get_json())
    except Exception as e:
        return str(e), HTTPStatus.BAD_REQUEST

    if not message.attributes:
        return "Invalid Pub/Sub message", HTTPStatus.BAD_REQUEST

    attributes = message.attributes
    region_code, filename = os.path.split(attributes[OBJECT_ID])

    if not region_code:
        logging.info("Missing region, ignoring")
        return "", HTTPStatus.OK

    # It would be nice if we could do this as a filter in the GCS notification instead of as logic
    # here, but as of June 2022, the available filters are not expressive enough for our needs:
    # https://cloud.google.com/pubsub/docs/filtering#filtering_syntax
    if filename != "dashboard_user_restrictions.csv":
        logging.info("Unknown filename %s, ignoring", filename)
        return "", HTTPStatus.OK

    cloud_task_manager = SingleCloudTaskQueueManager(
        queue_info_cls=CloudTaskQueueInfo, queue_name=CASE_TRIAGE_DB_OPERATIONS_QUEUE
    )
    cloud_task_manager.create_task(
        relative_uri="/auth/import_user_restrictions_csv_to_sql",
        body={"region_code": region_code},
    )
    logging.info(
        "Enqueued import_user_restrictions_csv_to_sql task to %s",
        CASE_TRIAGE_DB_OPERATIONS_QUEUE,
    )
    return "", HTTPStatus.OK


@auth_endpoint_blueprint.route(
    "/import_user_restrictions_csv_to_sql", methods=["GET", "POST"]
)
@requires_gae_auth
def import_user_restrictions_csv_to_sql() -> Tuple[str, HTTPStatus]:
    """This endpoint triggers the import of the user restrictions CSV file to Cloud SQL. It is requested by a Cloud
    Function that is triggered when a new file is created in the user restrictions bucket."""
    try:
        body = get_cloud_task_json_body()
        region_code = body.get("region_code")
        if not region_code:
            return "Missing region_code param", HTTPStatus.BAD_REQUEST

        try:
            _validate_state_code(region_code)
        except ValueError as error:
            logging.error(error)
            return str(error), HTTPStatus.BAD_REQUEST

        view_builder = DASHBOARD_USER_RESTRICTIONS_VIEW_BUILDER
        csv_path = GcsfsFilePath.from_absolute_path(
            os.path.join(
                StrictStringFormatter().format(
                    DASHBOARD_USER_RESTRICTIONS_OUTPUT_DIRECTORY_URI,
                    project_id=metadata.project_id(),
                ),
                region_code,
                f"{view_builder.view_id}.csv",
            )
        )

        import_gcs_csv_to_cloud_sql(
            database_key=SQLAlchemyDatabaseKey.for_schema(SchemaType.CASE_TRIAGE),
            model=DashboardUserRestrictions,
            gcs_uri=csv_path,
            columns=view_builder.columns,
            region_code=region_code.upper(),
        )
        logging.info(
            "CSV (%s) successfully imported to Cloud SQL schema %s for region code %s",
            csv_path.blob_name,
            SchemaType.CASE_TRIAGE,
            region_code,
        )

        return (
            f"CSV {csv_path.blob_name} successfully "
            f"imported to Cloud SQL schema {SchemaType.CASE_TRIAGE} for region code {region_code}",
            HTTPStatus.OK,
        )
    except Exception as error:
        logging.error(error)
        return (str(error), HTTPStatus.INTERNAL_SERVER_ERROR)


@auth_endpoint_blueprint.route("/dashboard_user_restrictions_by_email", methods=["GET"])
@requires_gae_auth
def dashboard_user_restrictions_by_email() -> Tuple[
    Union[CaseTriageAuth0AppMetadata, str], HTTPStatus
]:
    """This endpoint is accessed by a service account used by an Auth0 hook that is called at the pre-registration when
    a user first signs up for an account. Given a user email address in the request, it responds with
    the app_metadata that the hook will save on the user so that the UP dashboards can apply the appropriate
    restrictions.

    Query parameters:
        email_address: (required) The email address that requires a user restrictions lookup
        region_code: (required) The region code to use to lookup the user restrictions

    Returns:
         JSON response of the app_metadata associated with the given email address and an HTTP status

    Raises:
        Nothing. Catch everything so that we can always return a response to the request
    """
    email_address = get_only_str_param_value("email_address", request.args)
    region_code = get_only_str_param_value(
        "region_code", request.args, preserve_case=True
    )

    try:
        if not email_address:
            return "Missing email_address param", HTTPStatus.BAD_REQUEST
        if not region_code:
            return "Missing region_code param", HTTPStatus.BAD_REQUEST
        _validate_state_code(region_code)
        validate_email_address(email_address)
    except ValueError as error:
        logging.error(error)
        return str(error), HTTPStatus.BAD_REQUEST

    database_key = SQLAlchemyDatabaseKey.for_schema(schema_type=SchemaType.CASE_TRIAGE)
    # TODO(#8046): Don't use the deprecated session fetcher
    session = SessionFactory.deprecated__for_database(database_key=database_key)
    try:
        user_restrictions = (
            session.query(
                DashboardUserRestrictions.allowed_supervision_location_ids,
                DashboardUserRestrictions.allowed_supervision_location_level,
                DashboardUserRestrictions.can_access_leadership_dashboard,
                DashboardUserRestrictions.can_access_case_triage,
                DashboardUserRestrictions.should_see_beta_charts,
                DashboardUserRestrictions.routes,
                DashboardUserRestrictions.user_hash,
            )
            .filter(
                DashboardUserRestrictions.state_code == region_code.upper(),
                func.lower(DashboardUserRestrictions.restricted_user_email)
                == email_address.lower(),
            )
            .one()
        )

        restrictions = _format_db_results(user_restrictions)

        return (restrictions, HTTPStatus.OK)

    except sqlalchemy.orm.exc.NoResultFound:
        return (
            f"User not found for email address {email_address} and region code {region_code}.",
            HTTPStatus.NOT_FOUND,
        )

    except Exception as error:
        logging.error(error)
        return (
            f"An error occurred while fetching dashboard user restrictions with the email {email_address} for "
            f"region_code {region_code}: {error}",
            HTTPStatus.INTERNAL_SERVER_ERROR,
        )

    finally:
        session.close()


def _format_db_results(
    user_restrictions: Dict[str, Any],
) -> CaseTriageAuth0AppMetadata:
    return {
        "allowed_supervision_location_ids": _format_allowed_supervision_location_ids(
            user_restrictions["allowed_supervision_location_ids"]
        ),
        "allowed_supervision_location_level": user_restrictions[
            "allowed_supervision_location_level"
        ],
        "can_access_leadership_dashboard": user_restrictions[
            "can_access_leadership_dashboard"
        ],
        "can_access_case_triage": user_restrictions["can_access_case_triage"],
        "should_see_beta_charts": user_restrictions["should_see_beta_charts"],
        "routes": user_restrictions["routes"],
        "user_hash": user_restrictions["user_hash"],
    }


def _normalize_current_restrictions(
    current_app_metadata: Mapping[str, Any],
) -> Mapping[str, Any]:
    return {
        "allowed_supervision_location_ids": current_app_metadata.get(
            "allowed_supervision_location_ids", []
        ),
        "allowed_supervision_location_level": current_app_metadata.get(
            "allowed_supervision_location_level", None
        ),
        "can_access_leadership_dashboard": current_app_metadata.get(
            "can_access_leadership_dashboard", False
        ),
        "can_access_case_triage": current_app_metadata.get(
            "can_access_case_triage", False
        ),
        "should_see_beta_charts": current_app_metadata.get(
            "should_see_beta_charts", False
        ),
        "routes": current_app_metadata.get("routes", {}),
        "user_hash": current_app_metadata.get("user_hash"),
    }


def _format_allowed_supervision_location_ids(
    allowed_supervision_location_ids: str,
) -> List[str]:
    if not allowed_supervision_location_ids:
        return []
    return [
        restriction.strip()
        for restriction in allowed_supervision_location_ids.split(",")
        if restriction.strip()
    ]


def _validate_state_code(state_code: str) -> None:
    if not StateCode.is_state_code(state_code.upper()):
        raise ValueError(
            f"Unknown state_code [{state_code}] received, must be a valid state code."
        )


@auth_endpoint_blueprint.route("/dashboard_user_restrictions", methods=["GET"])
@requires_gae_auth
def dashboard_user_restrictions() -> Response:
    database_key = SQLAlchemyDatabaseKey.for_schema(schema_type=SchemaType.CASE_TRIAGE)
    # TODO(#8046): Don't use the deprecated session fetcher
    session = SessionFactory.deprecated__for_database(database_key=database_key)
    user_restrictions = session.query(DashboardUserRestrictions).all()
    return jsonify(
        [
            {
                "restrictedUserEmail": res.restricted_user_email,
                "stateCode": res.state_code,
                "allowedSupervisionLocationIds": res.allowed_supervision_location_ids,
                "allowedSupervisionLocationLevel": res.allowed_supervision_location_level,
                "canAccessLeadershipDashboard": res.can_access_leadership_dashboard,
                "canAccessCaseTriage": res.can_access_case_triage,
                "shouldSeeBetaCharts": res.should_see_beta_charts,
                "routes": res.routes,
            }
            for res in user_restrictions
        ]
    )


@auth_endpoint_blueprint.route("/users", defaults={"email": None}, methods=["GET"])
@auth_endpoint_blueprint.route("/users/<email>", methods=["GET"])
@requires_gae_auth
def users(email: Optional[str] = None) -> Tuple[Union[str, Response], HTTPStatus]:
    """
    This endpoint is accessed via the admin panel. It queries data from four Case Triage CloudSQL instance tables
    (roster, user_override, state_role_permissions, and permissions_overrides) in order to account for overrides to
    a user's roster data or permissions.

    Returns: JSON string with accurate information about state user/users and their permissions
    """
    database_key = SQLAlchemyDatabaseKey.for_schema(schema_type=SchemaType.CASE_TRIAGE)
    with SessionFactory.using_database(database_key, autocommit=False) as session:
        try:
            query = (
                session.query(
                    func.coalesce(UserOverride.state_code, Roster.state_code).label(
                        "state_code"
                    ),
                    func.coalesce(
                        UserOverride.email_address, Roster.email_address
                    ).label("email_address"),
                    func.coalesce(UserOverride.external_id, Roster.external_id).label(
                        "external_id"
                    ),
                    func.coalesce(UserOverride.role, Roster.role).label("role"),
                    func.coalesce(UserOverride.district, Roster.district).label(
                        "district"
                    ),
                    func.coalesce(UserOverride.first_name, Roster.first_name).label(
                        "first_name"
                    ),
                    func.coalesce(UserOverride.last_name, Roster.last_name).label(
                        "last_name"
                    ),
                    func.coalesce(UserOverride.blocked, False).label("blocked"),
                    func.coalesce(
                        PermissionsOverride.can_access_leadership_dashboard,
                        StateRolePermissions.can_access_leadership_dashboard,
                        False,
                    ).label("can_access_leadership_dashboard"),
                    func.coalesce(
                        PermissionsOverride.can_access_case_triage,
                        StateRolePermissions.can_access_case_triage,
                        False,
                    ).label("can_access_case_triage"),
                    func.coalesce(
                        PermissionsOverride.should_see_beta_charts,
                        StateRolePermissions.should_see_beta_charts,
                        False,
                    ).label("should_see_beta_charts"),
                    func.coalesce(
                        PermissionsOverride.routes,
                        StateRolePermissions.routes,
                    ).label("routes"),
                    func.coalesce(
                        PermissionsOverride.feature_variants,
                        StateRolePermissions.feature_variants,
                    ).label("feature_variants"),
                    func.coalesce(
                        UserOverride.user_hash,
                        Roster.user_hash,
                    ).label("user_hash"),
                )
                .select_from(Roster)
                .join(
                    UserOverride,
                    UserOverride.email_address == Roster.email_address,
                    full=True,
                )
                .outerjoin(
                    StateRolePermissions,
                    (
                        func.coalesce(UserOverride.state_code, Roster.state_code)
                        == StateRolePermissions.state_code
                    )
                    & (
                        func.coalesce(UserOverride.role, Roster.role)
                        == StateRolePermissions.role
                    ),
                )
                .outerjoin(
                    PermissionsOverride,
                    func.coalesce(UserOverride.email_address, Roster.email_address)
                    == PermissionsOverride.user_email,
                )
            )
            user_info = (
                query.all()
                if email is None
                else query.where(
                    func.coalesce(UserOverride.email_address, Roster.email_address)
                    == email,
                ).one()
            )
            permissions = (
                jsonify(format_user_info(user_info))
                if email is not None
                else jsonify([format_user_info(user) for user in user_info])
            )
            return (permissions, HTTPStatus.OK)

        except sqlalchemy.orm.exc.NoResultFound:
            return (
                f"User not found for email address {email}",
                HTTPStatus.NOT_FOUND,
            )


@auth_endpoint_blueprint.route("/users/<email>", methods=["POST"])
@requires_gae_auth
def add_user(email: str) -> Union[tuple[Response, int], tuple[str, int]]:
    """Adds a new user to UserOverride and returns the created user.
    Returns an error message if a user already exists with that email address.
    """
    try:
        with SessionFactory.using_database(
            SQLAlchemyDatabaseKey.for_schema(SchemaType.CASE_TRIAGE)
        ) as session:
            user_dict = convert_nested_dictionary_keys(
                assert_type(request.json, dict), to_snake_case
            )
            if user_dict["state_code"] is None:
                raise ValueError("Request is missing the state_code param")
            # Enforce casing for columns where we have a preference.
            user_dict["email_address"] = email.lower()
            user_dict["state_code"] = user_dict["state_code"].upper()
            user_dict["external_id"] = (
                user_dict["external_id"].upper()
                if user_dict.get("external_id") is not None
                else None
            )
            user_dict["user_hash"] = generate_user_hash(user_dict["email_address"])
            if (
                session.query(Roster).filter(Roster.email_address == email).first()
                is not None
            ):
                return (
                    "A user with this email already exists in Roster.",
                    HTTPStatus.UNPROCESSABLE_ENTITY,
                )

            log_reason(user_dict, f"adding user {user_dict['email_address']}")

            user = UserOverride(**user_dict)
            session.add(user)
            session.commit()
            return (
                jsonify(
                    {
                        "stateCode": user.state_code,
                        "emailAddress": user.email_address,
                        "externalId": user.external_id,
                        "role": user.role,
                        "district": user.district,
                        "firstName": user.first_name,
                        "lastName": user.last_name,
                        "userHash": user.user_hash,
                    }
                ),
                HTTPStatus.OK,
            )
    except IntegrityError as e:
        if isinstance(e.orig, UniqueViolation):
            return (
                "A user with this email already exists in UserOverride.",
                HTTPStatus.UNPROCESSABLE_ENTITY,
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


@auth_endpoint_blueprint.route("/users", methods=["PUT"])
@requires_gae_auth
def upload_roster() -> Tuple[str, HTTPStatus]:
    """Adds records to the "roster" table if existing record is not found,
    otherwise updates the existing record and returns the number of records updated.
    It assumes that the caller has manually formatted the CSV appropriately.
    Returns an error message if there was an error creating the records.
    """
    state_code = get_only_str_param_value("state_code", request.args)

    try:
        if state_code is None:
            raise ValueError("Request is missing the state_code param")

        request_dict = convert_nested_dictionary_keys(
            assert_type(request.form, dict), to_snake_case
        )
        log_reason(
            request_dict,
            f"uploading roster for state {state_code}",
        )

        with SessionFactory.using_database(
            SQLAlchemyDatabaseKey.for_schema(SchemaType.CASE_TRIAGE)
        ) as session:
            dict_reader = csv.DictReader(
                request.files["file"].read().decode("utf-8-sig").splitlines()
            )
            rows = list(dict_reader)

            for row in rows:
                if not row["email_address"]:
                    raise ValueError(
                        "Roster contains a row that is missing an email address (required)"
                    )
                role = row["role"].lower()
                email = row["email_address"].lower()
                associated_state_role = (
                    session.query(StateRolePermissions)
                    .filter_by(state_code=f"{state_code.upper()}", role=f"{role}")
                    .first()
                )
                if not associated_state_role:
                    raise ValueError(
                        f"Roster contains a row that with a role that does not exist in the default state role permissions. Offending row has email {email}"
                    )

                # Enforce casing for columns where we have a preference.
                row["state_code"] = state_code.upper()
                row["email_address"] = email
                row["external_id"] = row["external_id"].upper()
                row["role"] = row["role"].lower()
                row["user_hash"] = generate_user_hash(row["email_address"])

                # update existing roster or create add new roster
                existing = (
                    session.query(Roster)
                    .filter_by(
                        state_code=f"{state_code.upper()}", email_address=f"{email}"
                    )
                    .first()
                )
                if existing:
                    for key, value in row.items():
                        setattr(existing, key, value)
                else:
                    roster = Roster(**row)
                    session.add(roster)

            session.commit()

            return (
                f"{len(rows)} users added/updated to the roster",
                HTTPStatus.OK,
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


@auth_endpoint_blueprint.route("/states", methods=["GET"])
@requires_gae_auth
def states() -> Response:
    database_key = SQLAlchemyDatabaseKey.for_schema(schema_type=SchemaType.CASE_TRIAGE)
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
                    "canAccessLeadershipDashboard": res.can_access_leadership_dashboard,
                    "canAccessCaseTriage": res.can_access_case_triage,
                    "shouldSeeBetaCharts": res.should_see_beta_charts,
                    "routes": res.routes,
                    "featureVariants": res.feature_variants,
                }
                for res in state_permissions
            ]
        )


@auth_endpoint_blueprint.route("/state/<state_code>", methods=["GET"])
@requires_gae_auth
def state_info(state_code: str) -> Response:
    """Returns the unique districts, roles, and default user permissions for a given state."""
    database_key = SQLAlchemyDatabaseKey.for_schema(schema_type=SchemaType.CASE_TRIAGE)
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
                                "can_access_leadership_dashboard": per.can_access_leadership_dashboard,
                                "can_access_case_triage": per.can_access_case_triage,
                                "should_see_beta_charts": per.should_see_beta_charts,
                                "routes": per.routes,
                                "feature_variants": per.feature_variants,
                            },
                        }
                        for per in role_permissions
                    ],
                }
            ]
        )


@auth_endpoint_blueprint.route("/states/<state_code>/roles/<role>", methods=["POST"])
@requires_gae_auth
def add_state_role(
    state_code: str, role: str
) -> Union[tuple[Response, int], tuple[str, int]]:
    """Adds default permissions for a given role in a state."""
    if not StateCode.is_state_code(state_code.upper()):
        return (
            f"Unknown state_code [{state_code}] received, must be a valid state code.",
            HTTPStatus.BAD_REQUEST,
        )
    database_key = SQLAlchemyDatabaseKey.for_schema(schema_type=SchemaType.CASE_TRIAGE)
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
                        "canAccessCaseTriage": state_role.can_access_case_triage,
                        "canAccessLeadershipDashboard": state_role.can_access_leadership_dashboard,
                        "shouldSeeBetaCharts": state_role.should_see_beta_charts,
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


@auth_endpoint_blueprint.route("/states/<state_code>/roles/<role>", methods=["PATCH"])
@requires_gae_auth
def update_state_role(
    state_code: str, role: str
) -> Union[tuple[Response, int], tuple[str, int]]:
    """Updates the default permissions for a given role in a state."""
    if not StateCode.is_state_code(state_code.upper()):
        return (
            f"Unknown state_code [{state_code}] received, must be a valid state code.",
            HTTPStatus.BAD_REQUEST,
        )
    database_key = SQLAlchemyDatabaseKey.for_schema(schema_type=SchemaType.CASE_TRIAGE)
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
                    # Note: JSONB does not detect in-place changes when used with the ORM.
                    # (https://docs.sqlalchemy.org/en/13/dialects/postgresql.html#sqlalchemy.dialects.postgresql.JSONB)
                    # The sqlalchemy.ext.mutable extension supposedly makes it so it does, but it didn't
                    # for me, so instead we just assign it to a new dict, which does the trick.
                    setattr(
                        state_role,
                        col_name,
                        {**getattr(state_role, col_name), **col_value},
                    )

            session.commit()

            return (
                jsonify(
                    {
                        "stateCode": state_role.state_code,
                        "role": state_role.role,
                        "canAccessCaseTriage": state_role.can_access_case_triage,
                        "canAccessLeadershipDashboard": state_role.can_access_leadership_dashboard,
                        "shouldSeeBetaCharts": state_role.should_see_beta_charts,
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


@auth_endpoint_blueprint.route("/states/<state_code>/roles/<role>", methods=["DELETE"])
@requires_gae_auth
def delete_state_role(
    state_code: str, role: str
) -> Union[tuple[Response, int], tuple[str, int]]:
    """Removes a role in a given state. Fails if there are still active users with that role in that
    state. If there are no active users, removes any blocked users with that role in that state."""
    if not StateCode.is_state_code(state_code.upper()):
        return (
            f"Unknown state_code [{state_code}] received, must be a valid state code.",
            HTTPStatus.BAD_REQUEST,
        )
    database_key = SQLAlchemyDatabaseKey.for_schema(schema_type=SchemaType.CASE_TRIAGE)
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
                    func.coalesce(UserOverride.role, Roster.role) == role,
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
            request_dict = convert_nested_dictionary_keys(request_json, to_snake_case)
            log_reason(
                request_dict,
                f"removing permissions and blocked users for state {state_code.upper()}, role {role.lower()}",
            )

            # Delete remaining users
            for (override_user, roster_user) in state_role_users:
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


@auth_endpoint_blueprint.route("/users/<email>", methods=["PATCH"])
@requires_gae_auth
def update_user(email: str) -> Union[tuple[Response, int], tuple[str, int]]:
    """Edits an existing user's info by adding or updating an entry for that user in UserOverride."""
    database_key = SQLAlchemyDatabaseKey.for_schema(schema_type=SchemaType.CASE_TRIAGE)
    try:
        with SessionFactory.using_database(database_key) as session:
            user_dict = convert_nested_dictionary_keys(
                assert_type(request.json, dict), to_snake_case
            )
            if "state_code" not in user_dict:
                raise ValueError("Must provide a state_code when updating a user")
            user_dict["email_address"] = email
            log_reason(user_dict, f"updating user {user_dict['email_address']}")
            if (
                session.query(UserOverride)
                .filter(UserOverride.email_address == email)
                .first()
                is None
            ):
                user_dict["user_hash"] = generate_user_hash(
                    user_dict["email_address"].lower()
                )
                user = UserOverride(**user_dict)
                session.add(user)
                session.commit()
                return (
                    jsonify(
                        {
                            "stateCode": user.state_code,
                            "emailAddress": user.email_address,
                            "externalId": user.external_id,
                            "role": user.role,
                            "district": user.district,
                            "firstName": user.first_name,
                            "lastName": user.last_name,
                            "userHash": user.user_hash,
                        }
                    ),
                    HTTPStatus.OK,
                )
            session.execute(
                Update(UserOverride)
                .where(UserOverride.email_address == email)
                .values(user_dict)
            )
            updated_user = (
                session.query(UserOverride)
                .filter(UserOverride.email_address == email)
                .first()
            )
            return (
                jsonify(
                    {
                        "stateCode": updated_user.state_code,
                        "emailAddress": updated_user.email_address,
                        "externalId": updated_user.external_id,
                        "role": updated_user.role,
                        "district": updated_user.district,
                        "firstName": updated_user.first_name,
                        "lastName": updated_user.last_name,
                        "userHash": updated_user.user_hash,
                    }
                ),
                HTTPStatus.OK,
            )
    except (IntegrityError, ValueError) as error:
        return (
            f"{error}",
            HTTPStatus.BAD_REQUEST,
        )


@auth_endpoint_blueprint.route("/users/<email>/permissions", methods=["PUT"])
@requires_gae_auth
def update_user_permissions(email: str) -> Union[tuple[Response, int], tuple[str, int]]:
    """Gives a state user custom permissions by adding or updating an entry for that user in Permissions Override."""
    database_key = SQLAlchemyDatabaseKey.for_schema(schema_type=SchemaType.CASE_TRIAGE)
    try:
        with SessionFactory.using_database(database_key) as session:
            request_json = assert_type(request.json, dict)
            user_dict = convert_nested_dictionary_keys(request_json, to_snake_case)
            user_dict["user_email"] = email
            # user_dict's value for "routes" and "feature_variants" shouldn't be in snake case
            routes_json = request_json.get("routes")
            if routes_json is not None:
                user_dict["routes"] = assert_type(routes_json, dict)
            feature_variants_json = request_json.get("featureVariants")
            if feature_variants_json is not None:
                user_dict["feature_variants"] = assert_type(feature_variants_json, dict)

            log_reason(
                user_dict, f"updating permissions for user {user_dict['user_email']}"
            )

            if (
                session.query(PermissionsOverride)
                .filter(PermissionsOverride.user_email == email)
                .first()
                is None
            ):
                new_permissions = PermissionsOverride(**user_dict)
                session.add(new_permissions)
                session.commit()
                return (
                    jsonify(
                        {
                            "emailAddress": new_permissions.user_email,
                            "canAccessLeadershipDashboard": new_permissions.can_access_leadership_dashboard,
                            "canAccessCaseTriage": new_permissions.can_access_case_triage,
                            "shouldSeeBetaCharts": new_permissions.should_see_beta_charts,
                            "routes": new_permissions.routes,
                            "featureVariants": new_permissions.feature_variants,
                        }
                    ),
                    HTTPStatus.OK,
                )
            session.execute(
                Update(PermissionsOverride)
                .where(PermissionsOverride.user_email == email)
                .values(user_dict)
            )
            updated_permissions = (
                session.query(PermissionsOverride)
                .filter(PermissionsOverride.user_email == email)
                .first()
            )
            return (
                jsonify(
                    {
                        "emailAddress": updated_permissions.user_email,
                        "canAccessLeadershipDashboard": updated_permissions.can_access_leadership_dashboard,
                        "canAccessCaseTriage": updated_permissions.can_access_case_triage,
                        "shouldSeeBetaCharts": updated_permissions.should_see_beta_charts,
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


@auth_endpoint_blueprint.route("/users/<email>/permissions", methods=["DELETE"])
@requires_gae_auth
def delete_user_permissions(email: str) -> tuple[str, int]:
    database_key = SQLAlchemyDatabaseKey.for_schema(schema_type=SchemaType.CASE_TRIAGE)
    try:
        with SessionFactory.using_database(database_key) as session:
            overrides = session.query(PermissionsOverride).filter(
                PermissionsOverride.user_email == email
            )
            if overrides.first() is None:
                raise ValueError(
                    f"An entry for {email} in PermissionsOverride does not exist."
                )

            request_json = assert_type(request.json, dict)
            request_dict = convert_nested_dictionary_keys(request_json, to_snake_case)
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


@auth_endpoint_blueprint.route("/users/<email>", methods=["DELETE"])
@requires_gae_auth
def delete_user(email: str) -> tuple[str, int]:
    """Blocks a user by setting blocked=true in the corresponding Roster object."""
    database_key = SQLAlchemyDatabaseKey.for_schema(schema_type=SchemaType.CASE_TRIAGE)
    try:
        with SessionFactory.using_database(database_key) as session:
            request_json = assert_type(request.json, dict)
            request_dict = convert_nested_dictionary_keys(request_json, to_snake_case)
            log_reason(
                request_dict,
                f"blocking user {email}",
            )

            existing_override = session.query(UserOverride).filter(
                UserOverride.email_address == email
            )
            if existing_override.first():
                existing_override.update({"blocked": True}, synchronize_session=False)
            else:
                roster_user = (
                    session.query(Roster).filter(Roster.email_address == email).first()
                )
                if roster_user is None:
                    raise ValueError(f"An entry for {email} does not exist.")
                user_override = UserOverride(
                    state_code=roster_user.state_code,
                    email_address=email,
                    blocked=True,
                    user_hash=roster_user.user_hash,
                )
                session.add(user_override)
                session.commit()
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
@requires_gae_auth
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
        queue_info_cls=CloudTaskQueueInfo, queue_name=CASE_TRIAGE_DB_OPERATIONS_QUEUE
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


@auth_endpoint_blueprint.route("/import_ingested_users", methods=["POST"])
@requires_gae_auth
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

        view_builder = INGESTED_PRODUCT_USERS_VIEW_BUILDER
        csv_path = GcsfsFilePath.from_absolute_path(
            os.path.join(
                StrictStringFormatter().format(
                    PRODUCT_USER_IMPORT_OUTPUT_DIRECTORY_URI,
                    project_id=metadata.project_id(),
                ),
                state_code,
                f"{view_builder.view_id}.csv",
            )
        )

        import_gcs_csv_to_cloud_sql(
            database_key=SQLAlchemyDatabaseKey.for_schema(SchemaType.CASE_TRIAGE),
            model=Roster,
            gcs_uri=csv_path,
            columns=view_builder.columns,
            region_code=state_code.upper(),
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
    except Exception as error:
        logging.error(error)
        return (str(error), HTTPStatus.INTERNAL_SERVER_ERROR)
