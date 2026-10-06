# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2026 Recidiviz, Inc.
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
"""Exchanges Linear OAuth client credentials for an access token.

Reads LINEAR_CLIENT_ID and LINEAR_CLIENT_SECRET from environment variables,
exchanges them for an access token, and prints the token to stdout.

Usage:
    LINEAR_CLIENT_ID=my_client_id
    LINEAR_CLIENT_SECRET=my_client_secret
    TOKEN=$(uv run python -m recidiviz.tools.linear.get_linear_oauth_token --scope read)
"""
import argparse
import os

from recidiviz.issue_tracking.linear.linear_client import get_linear_oauth_access_token

LINEAR_CLIENT_ID_ENV_VAR = "LINEAR_CLIENT_ID"
LINEAR_CLIENT_SECRET_ENV_VAR = "LINEAR_CLIENT_SECRET"  # nosec B105


def get_linear_oauth_token(*, scope: str) -> str:
    """Exchanges Linear OAuth client credentials for an access token.

    Reads LINEAR_CLIENT_ID and LINEAR_CLIENT_SECRET from environment variables,
    exchanges them for an access token, and returns it.
    """
    client_id = os.environ.get(LINEAR_CLIENT_ID_ENV_VAR)
    if not client_id:
        raise ValueError(f"Must set {LINEAR_CLIENT_ID_ENV_VAR} environment variable.")
    client_secret = os.environ.get(LINEAR_CLIENT_SECRET_ENV_VAR)
    if not client_secret:
        raise ValueError(
            f"Must set {LINEAR_CLIENT_SECRET_ENV_VAR} environment variable."
        )
    return get_linear_oauth_access_token(
        client_id=client_id, client_secret=client_secret, scope=scope
    )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--scope",
        default="read",
        help="OAuth scope to request (e.g. 'read' or 'read,write')",
    )
    args = parser.parse_args()

    print(get_linear_oauth_token(scope=args.scope))
