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
from typing import Any, Dict, List, Mapping, Optional, Tuple, Union

import sqlalchemy.orm.exc
from flask import Blueprint, request
from sqlalchemy import func

from recidiviz.auth.auth0_client import Auth0AppMetadata, Auth0Client
from recidiviz.calculator.query.state.views.reference.dashboard_user_restrictions import (
    DASHBOARD_USER_RESTRICTIONS_VIEW_BUILDER,
)
from recidiviz.case_triage.ops_routes import CASE_TRIAGE_DB_OPERATIONS_QUEUE
from recidiviz.cloud_sql.gcs_import_to_cloud_sql import import_gcs_csv_to_cloud_sql
from recidiviz.cloud_storage.gcsfs_path import GcsfsFilePath
from recidiviz.common.constants.states import StateCode
from recidiviz.common.google_cloud.cloud_task_queue_manager import (
    CloudTaskQueueInfo,
    CloudTaskQueueManager,
    get_cloud_task_json_body,
)
from recidiviz.metrics.export.export_config import (
    DASHBOARD_USER_RESTRICTIONS_OUTPUT_DIRECTORY_URI,
)
from recidiviz.persistence.database.schema.case_triage.schema import (
    DashboardUserRestrictions,
)
from recidiviz.persistence.database.schema_utils import SchemaType
from recidiviz.persistence.database.session_factory import SessionFactory
from recidiviz.persistence.database.sqlalchemy_database_key import SQLAlchemyDatabaseKey
from recidiviz.reporting.email_reporting_utils import validate_email_address
from recidiviz.utils import metadata
from recidiviz.utils.auth.gae import requires_gae_auth
from recidiviz.utils.params import get_only_str_param_value

auth_endpoint_blueprint = Blueprint("auth_endpoint_blueprint", __name__)


@auth_endpoint_blueprint.route(
    "/handle_import_user_restrictions_csv_to_sql", methods=["GET"]
)
@requires_gae_auth
def handle_import_user_restrictions_csv_to_sql() -> Tuple[str, HTTPStatus]:
    region_code = get_only_str_param_value(
        "region_code", request.args, preserve_case=True
    )
    if not region_code:
        return "Missing region_code param", HTTPStatus.BAD_REQUEST

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
                DASHBOARD_USER_RESTRICTIONS_OUTPUT_DIRECTORY_URI.format(
                    project_id=metadata.project_id()
                ),
                region_code,
                f"{view_builder.view_id}.csv",
            )
        )

        import_gcs_csv_to_cloud_sql(
            schema_type=SchemaType.CASE_TRIAGE,
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
    Union[Auth0AppMetadata, str], HTTPStatus
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
                DashboardUserRestrictions.routes,
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


@auth_endpoint_blueprint.route("/update_auth0_user_metadata", methods=["GET"])
@requires_gae_auth
def update_auth0_user_metadata() -> Tuple[str, HTTPStatus]:
    """This endpoint is triggered from a GCS bucket when a new user restrictions file is created. It downloads the
     user restrictions file and updates each user's app_metadata with the restrictions from the file.

    This function first queries the Auth0 Management API to request all users and their user_ids by the
    list of email addresses that exist in the downloaded file. Then it iterates over those users and updates their
    app_metadata.

    Query parameters:
        region_code: (required) The region code that has the updated user restrictions file, e.g. US_MO

    Returns:
         Text indicating the results of the run and an HTTP status

    Raises:
        Nothing. Catch everything so that we can always return a response to the request
    """
    region_code = get_only_str_param_value(
        "region_code", request.args, preserve_case=True
    )

    if not region_code:
        return (
            "Missing required region_code param",
            HTTPStatus.BAD_REQUEST,
        )

    try:
        _validate_region_code(region_code)
    except ValueError as error:
        logging.error(error)
        return str(error), HTTPStatus.BAD_REQUEST

    database_key = SQLAlchemyDatabaseKey.for_schema(schema_type=SchemaType.CASE_TRIAGE)
    with SessionFactory.using_database(
        database_key=database_key, autocommit=False
    ) as session:
        try:
            all_user_restrictions = (
                session.query(
                    DashboardUserRestrictions.restricted_user_email,
                    DashboardUserRestrictions.allowed_supervision_location_ids,
                    DashboardUserRestrictions.allowed_supervision_location_level,
                    DashboardUserRestrictions.can_access_leadership_dashboard,
                    DashboardUserRestrictions.can_access_case_triage,
                    DashboardUserRestrictions.routes,
                )
                .filter(DashboardUserRestrictions.state_code == region_code.upper())
                .order_by(DashboardUserRestrictions.restricted_user_email)
                .all()
            )
            user_restrictions_by_email: Dict[str, Auth0AppMetadata] = {}
            for user_restrictions in all_user_restrictions:
                email = user_restrictions["restricted_user_email"].lower()
                user_restrictions_by_email[email] = _format_db_results(
                    user_restrictions
                )

            auth0 = Auth0Client()
            email_addresses = list(user_restrictions_by_email.keys())
            users = auth0.get_all_users_by_email_addresses(email_addresses)
            num_updated_users = 0

            for user in users:
                email = user.get("email", "")
                current_app_metadata = user.get("app_metadata", {})
                new_restrictions: Optional[
                    Auth0AppMetadata
                ] = user_restrictions_by_email.get(email)
                current_restrictions = _normalize_current_restrictions(
                    current_app_metadata
                )

                if (
                    new_restrictions is not None
                    and current_restrictions != new_restrictions
                ):
                    num_updated_users += 1
                    auth0.update_user_app_metadata(
                        user_id=user.get("user_id", ""), app_metadata=new_restrictions
                    )

            return (
                f"Finished updating {num_updated_users} auth0 users with restrictions for region {region_code}",
                HTTPStatus.OK,
            )

        except Exception as error:
            logging.error(error)
            return (
                f"Error using Auth0 management API to update users: {error}",
                HTTPStatus.INTERNAL_SERVER_ERROR,
            )


def _format_db_results(
    user_restrictions: Dict[str, Any],
) -> Auth0AppMetadata:
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
        "routes": user_restrictions["routes"],
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
        "routes": current_app_metadata.get("routes", {}),
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
