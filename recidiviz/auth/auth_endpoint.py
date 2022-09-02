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
import logging
import os
from http import HTTPStatus
from typing import Any, Dict, List, Mapping, Tuple, Union

import sqlalchemy.orm.exc
from flask import Blueprint, Response, jsonify, request
from psycopg2.errors import (  # pylint: disable=no-name-in-module
    NotNullViolation,
    UniqueViolation,
)
from sqlalchemy import func
from sqlalchemy.exc import IntegrityError, ProgrammingError, StatementError
from sqlalchemy.sql import Update

from recidiviz.auth.auth0_client import CaseTriageAuth0AppMetadata
from recidiviz.calculator.query.state.views.reference.dashboard_user_restrictions import (
    DASHBOARD_USER_RESTRICTIONS_VIEW_BUILDER,
)
from recidiviz.case_triage.ops_routes import CASE_TRIAGE_DB_OPERATIONS_QUEUE
from recidiviz.cloud_sql.gcs_import_to_cloud_sql import import_gcs_csv_to_cloud_sql
from recidiviz.cloud_storage.gcsfs_path import GcsfsFilePath
from recidiviz.common.common_utils import convert_nested_dictionary_keys
from recidiviz.common.constants.states import StateCode
from recidiviz.common.google_cloud.cloud_task_queue_manager import (
    CloudTaskQueueInfo,
    CloudTaskQueueManager,
    get_cloud_task_json_body,
)
from recidiviz.common.str_field_utils import to_snake_case
from recidiviz.metrics.export.export_config import (
    DASHBOARD_USER_RESTRICTIONS_OUTPUT_DIRECTORY_URI,
)
from recidiviz.persistence.database.schema.case_triage.schema import (
    DashboardUserRestrictions,
    PermissionsOverride,
    Roster,
    StateRolePermissions,
    UserOverride,
)
from recidiviz.persistence.database.schema_utils import SchemaType
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

    cloud_task_manager = CloudTaskQueueManager(
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
            _validate_region_code(region_code)
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
        _validate_region_code(region_code)
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


def _validate_region_code(region_code: str) -> None:
    if not StateCode.is_state_code(region_code.upper()):
        raise ValueError(
            f"Unknown region_code [{region_code}] received, must be a valid state code."
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


@auth_endpoint_blueprint.route("/users", methods=["GET"])
@requires_gae_auth
def users() -> Union[str, Response]:
    """
    This endpoint is accessed via the admin panel. It queries data from four Case Triage CloudSQL instance tables
    (roster, user_override, state_role_permissions, and permissions_overrides) in order to account for overrides to
    a user's roster data or permissions.

    Returns: JSON string with accurate information about state users and their permissions
    """
    database_key = SQLAlchemyDatabaseKey.for_schema(schema_type=SchemaType.CASE_TRIAGE)
    with SessionFactory.using_database(database_key, autocommit=False) as session:
        user_info = (
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
                func.coalesce(UserOverride.role, Roster.role).label("role"),
                func.coalesce(UserOverride.district, Roster.district).label("district"),
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
            .all()
        )
        return jsonify(
            [
                {
                    "emailAddress": user.email_address,
                    "stateCode": user.state_code,
                    "externalId": user.external_id,
                    "role": user.role,
                    "district": user.district,
                    "firstName": user.first_name,
                    "lastName": user.last_name,
                    "allowedSupervisionLocationIds": user.district
                    if user.state_code == "US_MO"
                    else "",
                    "allowedSupervisionLocationLevel": "level_1_supervision_location"
                    if user.state_code == "US_MO" and user.district is not None
                    else "",
                    "canAccessLeadershipDashboard": user.can_access_leadership_dashboard,
                    "canAccessCaseTriage": user.can_access_case_triage,
                    "shouldSeeBetaCharts": user.should_see_beta_charts,
                    "routes": user.routes,
                    "blocked": user.blocked,
                }
                for user in user_info
            ]
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
            user_dict["email_address"] = email
            if (
                session.query(Roster).filter(Roster.email_address == email).first()
                is not None
            ):
                return (
                    "A user with this email already exists in Roster.",
                    HTTPStatus.UNPROCESSABLE_ENTITY,
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
    except ProgrammingError as error:
        return (
            f"{error}",
            HTTPStatus.BAD_REQUEST,
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
                            },
                        }
                        for per in role_permissions
                    ],
                }
            ]
        )


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
            user_dict["email_address"] = email
            if (
                session.query(UserOverride)
                .filter(UserOverride.email_address == email)
                .first()
                is None
            ):
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
                    }
                ),
                HTTPStatus.OK,
            )
    except IntegrityError as error:
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
            routes_json = request_json.get(
                "routes"
            )  # user_dict's value for "routes" shouldn't be in snake case
            if routes_json is not None:
                user_dict["routes"] = assert_type(routes_json, dict)
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
                raise ValueError
            overrides.delete(synchronize_session=False)
            return (
                f"{email} has been deleted from PermissionsOverride.",
                HTTPStatus.OK,
            )
    except ValueError:
        return (
            f"An entry for {email} in PermissionsOverride does not exist.",
            HTTPStatus.BAD_REQUEST,
        )


@auth_endpoint_blueprint.route("/users/<email>", methods=["DELETE"])
@requires_gae_auth
def delete_user(email: str) -> tuple[str, int]:
    database_key = SQLAlchemyDatabaseKey.for_schema(schema_type=SchemaType.CASE_TRIAGE)
    try:
        with SessionFactory.using_database(database_key) as session:
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
                    raise ValueError
                user_override = UserOverride(
                    state_code=roster_user.state_code,
                    email_address=email,
                    blocked=True,
                )
                session.add(user_override)
                session.commit()
            return (
                f"{email} has been blocked.",
                HTTPStatus.OK,
            )
    except ValueError:
        return (
            f"An entry for {email} does not exist.",
            HTTPStatus.BAD_REQUEST,
        )
