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
import collections
import json
import logging
import os
from http import HTTPStatus
from typing import Any, Dict, List, Optional, Tuple, Union

import sqlalchemy.orm.exc
from flask import Blueprint, request
from sqlalchemy import func

from recidiviz.auth.auth0_client import Auth0AppMetadata, Auth0Client
from recidiviz.calculator.query.state.views.reference.dashboard_user_restrictions import (
    DASHBOARD_USER_RESTRICTIONS_VIEW_BUILDER,
)
from recidiviz.cloud_sql.gcs_import_to_cloud_sql import import_gcs_csv_to_cloud_sql
from recidiviz.cloud_storage.gcs_file_system import GcsfsFileContentsHandle
from recidiviz.cloud_storage.gcsfs_factory import GcsfsFactory
from recidiviz.cloud_storage.gcsfs_path import GcsfsFilePath
from recidiviz.common.constants.states import StateCode
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


@auth_endpoint_blueprint.route("/import_user_restrictions_csv_to_sql", methods=["GET"])
@requires_gae_auth
def import_user_restrictions_csv_to_sql() -> Tuple[str, HTTPStatus]:
    """This endpoint triggers the import of the user restrictions CSV file to Cloud SQL. It is requested by a Cloud
    Function that is triggered when a new file is created in the user restrictions bucket."""
    try:
        region_code = get_only_str_param_value(
            "region_code", request.args, preserve_case=True
        )
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
            destination_table=DashboardUserRestrictions.__tablename__,
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
    Union[str, Dict[str, Any]], HTTPStatus
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

    try:
        database_key = SQLAlchemyDatabaseKey.for_schema(
            schema_type=SchemaType.CASE_TRIAGE
        )
        session = SessionFactory.for_database(database_key=database_key)

        user_restrictions = (
            session.query(
                DashboardUserRestrictions.allowed_supervision_location_ids,
                DashboardUserRestrictions.allowed_supervision_location_level,
            )
            .filter(
                DashboardUserRestrictions.state_code == region_code.upper(),
                func.lower(DashboardUserRestrictions.restricted_user_email)
                == email_address.lower(),
            )
            .one()
        )

        restrictions = {
            "allowed_supervision_location_ids": _format_user_restrictions(
                user_restrictions.allowed_supervision_location_ids
            ),
            "allowed_supervision_location_level": user_restrictions.allowed_supervision_location_level,
        }

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


@auth_endpoint_blueprint.route("/update_auth0_user_metadata", methods=["GET"])
@requires_gae_auth
def update_auth0_user_metadata() -> Tuple[str, HTTPStatus]:
    """This endpoint is triggered from a GCS bucket when a new user restrictions file is created. It downloads the
     user restrictions file and updates each user's app_metadata with the restrictions from the file.

    This function first queries the Auth0 Management API to request all users and their user_ids by the
    list of email addresses that exist in the downloaded file. Then it iterates over those users and updates their
    app_metadata.

    Query parameters:
        bucket: (required) The bucket where the restrictions files are exported to,
            e.g. recidiviz-{env}-dashboard-user-restrictions
        region_code: (required) The region code that has the updated user restrictions file, e.g. US_MO
        filename: (required) The filename for the user restrictions file,
            e.g. dashboard_user_restrictions.json

    Returns:
         Text indicating the results of the run and an HTTP status

    Raises:
        Nothing. Catch everything so that we can always return a response to the request
    """

    bucket = get_only_str_param_value("bucket", request.args)
    region_code = get_only_str_param_value(
        "region_code", request.args, preserve_case=True
    )
    filename = get_only_str_param_value("filename", request.args)

    if filename != "dashboard_user_restrictions.json":
        return (
            f"Auth endpoint update_auth0_user_metadata called with unexpected filename: {filename}",
            HTTPStatus.BAD_REQUEST,
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

    try:
        path = GcsfsFilePath.from_absolute_path(f"{bucket}/{region_code}/{filename}")
        gcsfs = GcsfsFactory.build()
        temp_file_handler = gcsfs.download_to_temp_file(path)

        if not temp_file_handler:
            return (
                f"GCS path does not exist: bucket={bucket}, region_code={region_code}, filename={filename}",
                HTTPStatus.BAD_REQUEST,
            )

        user_restrictions: List[Dict[str, Any]] = _load_json_lines(temp_file_handler)
        user_restrictions_by_email: Dict[str, Tuple[List[str], Optional[str]]] = {}

        for user_restriction in user_restrictions:
            email = user_restriction.get("restricted_user_email", "").lower()
            allowed_ids = _format_user_restrictions(
                user_restriction.get("allowed_supervision_location_ids", "")
            )
            allowed_level = user_restriction.get(
                "allowed_supervision_location_level", None
            )
            user_restrictions_by_email[email] = (allowed_ids, allowed_level)

        auth0 = Auth0Client()
        email_addresses = list(user_restrictions_by_email.keys())
        users = auth0.get_all_users_by_email_addresses(email_addresses)
        num_updated_users = 0

        for user in users:
            email = user.get("email", "")
            current_app_metadata = user.get("app_metadata", {})
            new_restrictions: Tuple[
                List[str], Optional[str]
            ] = user_restrictions_by_email.get(email, ([], None))

            new_restrictions_ids, new_restrictions_level = new_restrictions

            current_restrictions_ids: List[str] = current_app_metadata.get(
                "allowed_supervision_location_ids", []
            )
            current_restrictions_level: Optional[str] = current_app_metadata.get(
                "allowed_supervision_location_level", None
            )

            if _should_update_restrictions(
                current_restrictions_ids,
                current_restrictions_level,
                new_restrictions_ids,
                new_restrictions_level,
            ):
                num_updated_users += 1
                app_metadata: Auth0AppMetadata = {
                    "allowed_supervision_location_ids": new_restrictions_ids,
                    "allowed_supervision_location_level": new_restrictions_level,
                }
                auth0.update_user_app_metadata(
                    user_id=user.get("user_id", ""), app_metadata=app_metadata
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


def _format_user_restrictions(user_restrictions: str) -> List[str]:
    if not user_restrictions:
        return []
    return [
        restriction.strip()
        for restriction in user_restrictions.split(",")
        if restriction.strip()
    ]


def _validate_region_code(region_code: str) -> None:
    if not StateCode.is_state_code(region_code.upper()):
        raise ValueError(
            f"Unknown region_code [{region_code}] received, must be a valid state code."
        )


def _should_update_restrictions(
    current_restrictions_ids: List[str],
    current_restrictions_level: Optional[str],
    new_restrictions_ids: List[str],
    new_restrictions_level: Optional[str],
) -> bool:
    return bool(
        len(new_restrictions_ids) > 0
        and new_restrictions_level is not None
        and (
            collections.Counter(current_restrictions_ids)
            != collections.Counter(new_restrictions_ids)
            or current_restrictions_level != new_restrictions_level
        )
    )


def _load_json_lines(
    file_handler: GcsfsFileContentsHandle,
) -> List[Dict[str, Union[str, List[str]]]]:
    file_contents = file_handler.get_contents_iterator()
    loaded_json = []
    for json_line in file_contents:
        loaded_json.append(json.loads(json_line))
    return loaded_json
