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
"""Constants for the Identity Service."""

from recidiviz.common.constants.identity import ProductApp

IAP_BACKEND_SERVICE_ID_SECRET_NAME = (
    "iap_identity_service_load_balancer_service_id"  # nosec
)

DEV_CALLER_SERVICE_ACCOUNT = "fake-acct@fake-project.iam.gserviceaccount.com"

# Maps a caller's authenticated service account email (extracted from the
# `email` claim of the IAP JWT) to the `source_product_app` value it is
# allowed to write under. A `None` value means the caller is permitted to
# call the service but does not write PRODUCT_APP-sourced attributes (e.g.
# the batch identity clustering DAG, which writes EXTERNAL_DATA_SYSTEM
# records). Callers whose email is not in this mapping are rejected with 403.
PRODUCT_APP_BY_SERVICE_ACCOUNT: dict[str, ProductApp | None] = {
    DEV_CALLER_SERVICE_ACCOUNT: ProductApp.ADMIN_PANEL,
}
