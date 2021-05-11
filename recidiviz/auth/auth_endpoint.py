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
from http import HTTPStatus
from typing import Tuple, List, Dict

from flask import Blueprint, request

from recidiviz.cloud_storage.gcs_file_system import GcsfsFileContentsHandle
from recidiviz.cloud_storage.gcsfs_factory import GcsfsFactory
from recidiviz.cloud_storage.gcsfs_path import GcsfsFilePath
from recidiviz.utils.auth.gae import requires_gae_auth
from recidiviz.utils.params import get_only_str_param_value
from recidiviz.auth.auth0_client import Auth0Client, Auth0AppMetadata

auth_endpoint_blueprint = Blueprint("auth_endpoint_blueprint", __name__)


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
            e.g. supervision_location_restricted_access_emails.json

    Returns:
         Text indicating the results of the run and an HTTP status

    Raises:
        Nothing. Catch everything so that we can always return a response to the request
    """
    try:
        bucket = get_only_str_param_value("bucket", request.args)
        region_code = get_only_str_param_value(
            "region_code", request.args, preserve_case=True
        )
        filename = get_only_str_param_value("filename", request.args)

        if filename != "supervision_location_restricted_access_emails.json":
            return (
                f"Auth endpoint update_auth0_user_metadata called with unexpected filename: {filename}",
                HTTPStatus.BAD_REQUEST,
            )

        path = GcsfsFilePath.from_absolute_path(f"{bucket}/{region_code}/{filename}")
        gcsfs = GcsfsFactory.build()
        temp_file_handler = gcsfs.download_to_temp_file(path)

        if not temp_file_handler:
            return (
                f"GCS path does not exist: bucket={bucket}, region_code={region_code}, filename={filename}",
                HTTPStatus.BAD_REQUEST,
            )

        user_restrictions: List[Dict[str, str]] = _load_json_lines(temp_file_handler)
        user_restrictions_by_email: Dict[str, List[str]] = {}

        for user_restriction in user_restrictions:
            email = user_restriction["restricted_user_email"]
            user_restrictions_by_email[email] = _format_user_restrictions(
                user_restriction["allowed_level_1_supervision_location_ids"]
            )

        auth0 = Auth0Client()
        email_addresses = list(user_restrictions_by_email.keys())
        users = auth0.get_all_users_by_email_addresses(email_addresses)
        num_updated_users = 0

        for user in users:
            email = user["email"]
            current_app_metadata = user.get("app_metadata", {})
            current_restrictions = current_app_metadata.get(
                "allowed_supervision_location_ids", []
            )
            new_restrictions = user_restrictions_by_email[email]

            if _should_update_restrictions(current_restrictions, new_restrictions):
                num_updated_users += 1
                app_metadata: Auth0AppMetadata = {
                    "allowed_supervision_location_ids": new_restrictions,
                    "allowed_supervision_location_level": "level_1_supervision_location",
                }
                auth0.update_user_app_metadata(
                    user_id=user["user_id"], app_metadata=app_metadata
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


def _should_update_restrictions(
    current_restrictions: List[str], new_restrictions: List[str]
) -> bool:
    return bool(
        new_restrictions
        and (
            collections.Counter(current_restrictions)
            != collections.Counter(new_restrictions)
        )
    )


def _format_user_restrictions(user_restrictions: str) -> List[str]:
    return [
        restriction
        for restriction in map(lambda r: r.strip(), user_restrictions.split(","))
        if restriction
    ]


def _load_json_lines(file_handler: GcsfsFileContentsHandle) -> List[Dict[str, str]]:
    file_contents = file_handler.get_contents_iterator()
    loaded_json = []
    for json_line in file_contents:
        loaded_json.append(json.loads(json_line))
    return loaded_json
