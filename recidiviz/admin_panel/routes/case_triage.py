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
"""Defines routes for the Case Triage API endpoints in the admin panel."""
import json
import logging
import os
from http import HTTPStatus
from json import JSONDecodeError
from typing import List, Optional, Tuple

from flask import Blueprint, jsonify, request

import recidiviz.reporting.data_retrieval as data_retrieval
from recidiviz.admin_panel.admin_stores import fetch_state_codes
from recidiviz.admin_panel.case_triage_helpers import (
    columns_for_case_triage_view,
    get_importable_csvs,
)
from recidiviz.case_triage.views.view_config import CASE_TRIAGE_EXPORTED_VIEW_BUILDERS
from recidiviz.cloud_sql.cloud_sql_export_to_gcs import export_from_cloud_sql_to_gcs_csv
from recidiviz.cloud_sql.gcs_import_to_cloud_sql import import_gcs_csv_to_cloud_sql
from recidiviz.cloud_storage.gcsfs_path import GcsfsFilePath
from recidiviz.common.constants.states import StateCode
from recidiviz.common.results import MultiRequestResult
from recidiviz.metrics.export.export_config import (
    CASE_TRIAGE_VIEWS_OUTPUT_DIRECTORY_URI,
)
from recidiviz.persistence.database.schema.case_triage.schema import CaseUpdate
from recidiviz.persistence.database.schema_utils import SchemaType
from recidiviz.persistence.database.session_factory import SessionFactory
from recidiviz.persistence.database.sqlalchemy_database_key import SQLAlchemyDatabaseKey
from recidiviz.reporting.context.po_monthly_report.constants import ReportType
from recidiviz.reporting.email_reporting_utils import (
    generate_batch_id,
    validate_email_address,
)
from recidiviz.reporting.region_codes import REGION_CODES, InvalidRegionCodeException
from recidiviz.utils import metadata
from recidiviz.utils.auth.gae import requires_gae_auth
from recidiviz.utils.environment import GCP_PROJECT_STAGING, in_development
from recidiviz.utils.metadata import local_project_id_override
from recidiviz.utils.params import get_only_str_param_value

EMAIL_STATE_CODES = [StateCode.US_ID, StateCode.US_PA]


def add_case_triage_routes(bp: Blueprint) -> None:
    """Adds the relevant Case Triage API routes to an input Blueprint."""

    # Fetch ETL View Ids for GCS -> Cloud SQL Import
    @bp.route("/api/case_triage/fetch_etl_view_ids", methods=["POST"])
    @requires_gae_auth
    def _fetch_etl_view_ids() -> Tuple[str, HTTPStatus]:
        override_project_id: Optional[str] = None
        if in_development():
            override_project_id = GCP_PROJECT_STAGING
        return (
            jsonify(
                [builder.view_id for builder in CASE_TRIAGE_EXPORTED_VIEW_BUILDERS]
                + list(
                    get_importable_csvs(override_project_id=override_project_id).keys()
                )
            ),
            HTTPStatus.OK,
        )

    # Generate Case Updates export from Cloud SQL -> GCS
    @bp.route("/api/case_triage/generate_case_updates_export", methods=["POST"])
    @requires_gae_auth
    def _generate_case_updates_export() -> Tuple[str, HTTPStatus]:
        export_from_cloud_sql_to_gcs_csv(
            SchemaType.CASE_TRIAGE,
            "case_update_actions",
            GcsfsFilePath.from_absolute_path(
                os.path.join(
                    CASE_TRIAGE_VIEWS_OUTPUT_DIRECTORY_URI.format(
                        project_id=metadata.project_id()
                    ),
                    "exported",
                    "case_update_actions.csv",
                )
            ),
            [col.name for col in CaseUpdate.__table__.columns],
        )

        return "", HTTPStatus.OK

    # Run GCS -> Cloud SQL Import
    @bp.route("/api/case_triage/run_gcs_import", methods=["POST"])
    @requires_gae_auth
    def _run_gcs_import() -> Tuple[str, HTTPStatus]:
        """Executes an import of data from Google Cloud Storage into Cloud SQL,
        based on the query parameters in the request."""
        if "viewIds" not in request.json:
            return "`viewIds` must be present in arugment list", HTTPStatus.BAD_REQUEST

        known_view_builders = {
            builder.view_id: builder for builder in CASE_TRIAGE_EXPORTED_VIEW_BUILDERS
        }
        importable_csvs = get_importable_csvs()

        for view_id in request.json["viewIds"]:
            if view_id in importable_csvs:
                # CSVs put in to_import override ones from known view builders
                csv_path = importable_csvs[view_id]
                try:
                    columns = columns_for_case_triage_view(view_id)
                except ValueError:
                    logging.warning(
                        "View_id (%s) found in to_import/ folder but does not have corresponding columns",
                        view_id,
                    )
                    continue
            elif view_id in known_view_builders:
                csv_path = GcsfsFilePath.from_absolute_path(
                    os.path.join(
                        CASE_TRIAGE_VIEWS_OUTPUT_DIRECTORY_URI.format(
                            project_id=metadata.project_id()
                        ),
                        f"{view_id}.csv",
                    )
                )
                columns = known_view_builders[view_id].columns
            else:
                logging.warning(
                    "Unexpected view_id (%s) found in call to run_gcs_import", view_id
                )
                continue

            # NOTE: We are currently taking advantage of the fact that the destination table name
            # matches the view id of the corresponding builder here. This invariant isn't enforced
            # in code (yet), but the aim is to preserve this invariant for as long as possible.
            import_gcs_csv_to_cloud_sql(
                SchemaType.CASE_TRIAGE,
                view_id,
                csv_path,
                columns,
            )
            logging.info("View (%s) successfully imported", view_id)

        return "", HTTPStatus.OK

    @bp.route("/api/case_triage/get_po_feedback", methods=["POST"])
    @requires_gae_auth
    def _fetch_po_user_feedback() -> Tuple[str, HTTPStatus]:
        session = SessionFactory.for_database(
            SQLAlchemyDatabaseKey.for_schema(SchemaType.CASE_TRIAGE)
        )
        results = session.query(CaseUpdate).filter(CaseUpdate.comment.isnot(None)).all()

        return (
            jsonify(
                [
                    {
                        "personExternalId": res.person_external_id,
                        "officerExternalId": res.officer_external_id,
                        "actionType": res.action_type,
                        "comment": res.comment,
                        "timestamp": str(res.action_ts),
                    }
                    for res in results
                ]
            ),
            HTTPStatus.OK,
        )

    @bp.route("/api/line_staff_tools/fetch_email_state_codes", methods=["POST"])
    @requires_gae_auth
    def _fetch_email_state_codes() -> Tuple[str, HTTPStatus]:
        # hard coding Idaho and Pennsylvania for now
        state_code_info = fetch_state_codes(EMAIL_STATE_CODES)
        return jsonify(state_code_info), HTTPStatus.OK

    @bp.route(
        "/api/line_staff_tools/<state_code_str>/generate_emails", methods=["POST"]
    )
    @requires_gae_auth
    def _generate_emails(state_code_str: str) -> Tuple[str, HTTPStatus]:
        try:
            data = request.json
            state_code = StateCode(state_code_str)
            if state_code not in EMAIL_STATE_CODES:
                raise ValueError("State code is invalid for PO monthly reports")
            # report_type hardcoded for now, need to change when different reports are created
            report_type = ReportType.POMonthlyReport
            test_address = data.get("testAddress")
            region_code = data.get("regionCode")
            message_body_override = data.get("messageBodyOverride")
            raw_email_allowlist = get_only_str_param_value(
                "email_allowlist", request.args
            )

            validate_email_address(test_address)

            email_allowlist: Optional[List[str]] = (
                json.loads(raw_email_allowlist) if raw_email_allowlist else None
            )

        except (ValueError, JSONDecodeError) as error:
            logging.error(error)
            return str(error), HTTPStatus.BAD_REQUEST

        if email_allowlist is not None:
            for recipient_email in email_allowlist:
                validate_email_address(recipient_email)

        if test_address == "":
            test_address = None
        if region_code not in REGION_CODES:
            region_code = None

        try:
            batch_id = generate_batch_id()
            if in_development():
                with local_project_id_override(GCP_PROJECT_STAGING):
                    result: MultiRequestResult[str, str] = data_retrieval.start(
                        state_code=state_code,
                        report_type=report_type,
                        batch_id=batch_id,
                        test_address=test_address,
                        region_code=region_code,
                        email_allowlist=email_allowlist,
                        message_body_override=message_body_override,
                    )
            else:
                result = data_retrieval.start(
                    state_code=state_code,
                    report_type=report_type,
                    batch_id=batch_id,
                    test_address=test_address,
                    region_code=region_code,
                    email_allowlist=email_allowlist,
                    message_body_override=message_body_override,
                )
        except InvalidRegionCodeException:
            return "Invalid region code provided", HTTPStatus.BAD_REQUEST

        new_batch_text = f"New batch started for {state_code} and {report_type}. Batch id = {batch_id}."
        test_address_text = (
            f"Emails generated for test address: {test_address}" if test_address else ""
        )
        counts_text = f"Successfully generate {len(result.successes)} email(s)"
        success_text = f"{new_batch_text} {test_address_text} {counts_text}"
        if result.failures and not result.successes:
            return (
                f"{success_text}"
                f" Failed to generate all emails. Retry the request again."
            ), HTTPStatus.INTERNAL_SERVER_ERROR
        if result.failures:
            return (
                f"{success_text}"
                f" Failed to generate {len(result.failures)} email(s): {', '.join(result.failures)}"
            ), HTTPStatus.MULTI_STATUS

        return (f"{success_text}"), HTTPStatus.OK
