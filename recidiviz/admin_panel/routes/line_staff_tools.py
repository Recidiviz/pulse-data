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
import csv
import logging
import os
from http import HTTPStatus
from json import JSONDecodeError
from typing import Optional, Tuple

from flask import Blueprint, jsonify, request

from recidiviz.admin_panel.admin_stores import fetch_state_codes
from recidiviz.admin_panel.case_triage_helpers import (
    columns_for_case_triage_view,
    get_importable_csvs,
)
from recidiviz.admin_panel.line_staff_tools.constants import (
    EMAIL_STATE_CODES,
    ROSTER_STATE_CODES,
)
from recidiviz.admin_panel.line_staff_tools.rosters import RosterManager
from recidiviz.case_triage.views.view_config import (
    CASE_TRIAGE_EXPORTED_VIEW_BUILDERS,
    ETL_TABLES,
)
from recidiviz.cloud_sql.cloud_sql_export_to_gcs import export_from_cloud_sql_to_gcs_csv
from recidiviz.cloud_sql.gcs_import_to_cloud_sql import import_gcs_csv_to_cloud_sql
from recidiviz.cloud_storage.gcsfs_path import GcsfsFilePath
from recidiviz.common.constants.states import StateCode
from recidiviz.common.results import MultiRequestResult
from recidiviz.metrics.export.export_config import (
    CASE_TRIAGE_VIEWS_OUTPUT_DIRECTORY_URI,
)
from recidiviz.persistence.database.schema.case_triage import (
    schema as case_triage_schema,
)
from recidiviz.persistence.database.schema.case_triage.schema import CaseUpdate
from recidiviz.persistence.database.schema_utils import (
    SchemaType,
    get_case_triage_table_classes,
    get_database_entity_by_table_name,
)
from recidiviz.persistence.database.session_factory import SessionFactory
from recidiviz.persistence.database.sqlalchemy_database_key import SQLAlchemyDatabaseKey
from recidiviz.reporting import data_retrieval, email_delivery
from recidiviz.reporting.context.po_monthly_report.constants import ReportType
from recidiviz.reporting.email_reporting_handler import (
    EmailMetadataReportDateError,
    EmailReportingHandler,
    InvalidReportTypeError,
)
from recidiviz.reporting.email_reporting_utils import (
    Batch,
    generate_batch_id,
    validate_email_address,
)
from recidiviz.reporting.region_codes import REGION_CODES, InvalidRegionCodeException
from recidiviz.utils import metadata
from recidiviz.utils.auth.gae import requires_gae_auth
from recidiviz.utils.environment import GCP_PROJECT_STAGING, in_development
from recidiviz.utils.metadata import local_project_id_override
from recidiviz.utils.string import StrictStringFormatter


def add_line_staff_tools_routes(bp: Blueprint) -> None:
    """Adds the relevant Case Triage API routes to an input Blueprint."""

    email_handler = EmailReportingHandler()

    # Fetch ETL View Ids for GCS -> Cloud SQL Import
    @bp.route("/api/line_staff_tools/fetch_etl_view_ids", methods=["POST"])
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
    @bp.route("/api/line_staff_tools/generate_non_etl_exports", methods=["POST"])
    @requires_gae_auth
    def _generate_non_etl_exports() -> Tuple[str, HTTPStatus]:
        etl_table_classes = [t.__table__ for t in ETL_TABLES]
        for table_class in get_case_triage_table_classes():
            if table_class in etl_table_classes:
                continue

            export_from_cloud_sql_to_gcs_csv(
                SchemaType.CASE_TRIAGE,
                table_class.name,
                GcsfsFilePath.from_absolute_path(
                    os.path.join(
                        StrictStringFormatter().format(
                            CASE_TRIAGE_VIEWS_OUTPUT_DIRECTORY_URI,
                            project_id=metadata.project_id(),
                        ),
                        "exported",
                        f"{table_class.name}.csv",
                    )
                ),
                [col.name for col in table_class.columns],
            )

        return "", HTTPStatus.OK

    # Run GCS -> Cloud SQL Import
    @bp.route("/api/line_staff_tools/run_gcs_import", methods=["POST"])
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
                        StrictStringFormatter().format(
                            CASE_TRIAGE_VIEWS_OUTPUT_DIRECTORY_URI,
                            project_id=metadata.project_id(),
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
                get_database_entity_by_table_name(case_triage_schema, view_id),
                csv_path,
                columns,
            )
            logging.info("View (%s) successfully imported", view_id)

        return "", HTTPStatus.OK

    # fetch PO monthly report states
    @bp.route("/api/line_staff_tools/get_po_feedback", methods=["POST"])
    @requires_gae_auth
    def _fetch_po_user_feedback() -> Tuple[str, HTTPStatus]:
        with SessionFactory.using_database(
            SQLAlchemyDatabaseKey.for_schema(SchemaType.CASE_TRIAGE), autocommit=False
        ) as session:
            results = (
                session.query(CaseUpdate)
                .filter(
                    CaseUpdate.comment.isnot(None),
                    CaseUpdate.officer_external_id.notlike("demo::%"),
                )
                .all()
            )
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

    @bp.route("/api/line_staff_tools/fetch_roster_state_codes", methods=["POST"])
    @requires_gae_auth
    def _fetch_roster_state_codes() -> Tuple[str, HTTPStatus]:
        state_code_info = fetch_state_codes(ROSTER_STATE_CODES)
        return jsonify(state_code_info), HTTPStatus.OK

    # Generate monthly report emails
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
            # TODO(#7790): Support more email types.
            report_type = ReportType(data.get("reportType"))
            if report_type != ReportType.POMonthlyReport:
                raise ValueError(f"{report_type.value} is not a valid ReportType")
            test_address = data.get("testAddress")
            region_code = data.get("regionCode")
            message_body_override = data.get("messageBodyOverride")
            email_allowlist = data.get("emailAllowlist")

            if not test_address:
                test_address = None
            validate_email_address(test_address)

            if not region_code:
                region_code = None

            if not message_body_override:
                message_body_override = None

            if email_allowlist is not None:
                for recipient_email in email_allowlist:
                    validate_email_address(recipient_email)

        except (ValueError, JSONDecodeError) as error:
            logging.error(error)
            return str(error), HTTPStatus.BAD_REQUEST

        if region_code not in REGION_CODES:
            region_code = None

        try:
            batch_id = generate_batch_id()
            batch = Batch(
                state_code=state_code,
                batch_id=batch_id,
                report_type=report_type,
            )

            if in_development():
                with local_project_id_override(GCP_PROJECT_STAGING):
                    result: MultiRequestResult[str, str] = data_retrieval.start(
                        batch=batch,
                        test_address=test_address,
                        region_code=region_code,
                        email_allowlist=email_allowlist,
                        message_body_override=message_body_override,
                    )
            else:
                result = data_retrieval.start(
                    batch=batch,
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
        counts_text = f"Successfully generated {len(result.successes)} email(s)"
        success_text = f"{new_batch_text} {test_address_text} {counts_text}."
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

        return (
            jsonify(
                {
                    "batchId": batch_id,
                    "statusText": f"{success_text}",
                }
            ),
            HTTPStatus.OK,
        )

    # Send monthly report emails
    @bp.route("/api/line_staff_tools/<state_code_str>/send_emails", methods=["POST"])
    @requires_gae_auth
    def _send_emails(state_code_str: str) -> Tuple[str, HTTPStatus]:
        try:
            data = request.json
            state_code = StateCode(state_code_str)
            if state_code not in EMAIL_STATE_CODES:
                raise ValueError("State code is invalid for the monthly reports")
            batch_id = data.get("batchId")
            redirect_address = data.get("redirectAddress")
            cc_addresses = data.get("ccAddresses")
            subject_override = data.get("subjectOverride")
            email_allowlist = data.get("emailAllowlist")
            report_type = ReportType(data.get("reportType"))

            if not subject_override:
                subject_override = None

            if not redirect_address:
                redirect_address = None
            validate_email_address(redirect_address)

            if cc_addresses is not None:
                for cc_address in cc_addresses:
                    validate_email_address(cc_address)

            if email_allowlist is not None:
                for recipient_email in email_allowlist:
                    validate_email_address(recipient_email)

        except ValueError as error:
            logging.error(error)
            return str(error), HTTPStatus.BAD_REQUEST

        if not state_code:
            msg = "Query parameter 'state_code' not received"
            logging.error(msg)
            return msg, HTTPStatus.BAD_REQUEST

        if not batch_id:
            msg = "Query parameter 'batch_id' not received"
            logging.error(msg)
            return msg, HTTPStatus.BAD_REQUEST

        if not report_type:
            msg = "Query parameter 'report_type' not received"
            logging.error(msg)
            return msg, HTTPStatus.BAD_REQUEST

        batch = Batch(state_code=state_code, batch_id=batch_id, report_type=report_type)

        # TODO(#7790): Support more email types.
        try:
            if report_type != ReportType.POMonthlyReport:
                raise InvalidReportTypeError(
                    f"Invalid report type: Sending emails with {report_type} is not implemented yet."
                )
        except InvalidReportTypeError as error:
            logging.error(error)
            return str(error), HTTPStatus.NOT_IMPLEMENTED

        try:
            report_date = email_handler.generate_report_date(batch)
        except EmailMetadataReportDateError as error:
            logging.error(error)
            return str(error), HTTPStatus.BAD_REQUEST

        result = email_delivery.deliver(
            batch=batch,
            redirect_address=redirect_address,
            cc_addresses=cc_addresses,
            subject_override=subject_override,
            email_allowlist=email_allowlist,
            report_date=report_date,
        )

        redirect_text = (
            f"to the redirect email address {redirect_address}"
            if redirect_address
            else ""
        )
        cc_addresses_text = (
            f"CC'd {','.join(email for email in cc_addresses)}." if cc_addresses else ""
        )
        success_text = (
            f"Sent {len(result.successes)} emails {redirect_text}. {cc_addresses_text} "
        )

        if result.failures and not result.successes:
            return (
                f"{success_text} " f"All emails failed to send",
                HTTPStatus.INTERNAL_SERVER_ERROR,
            )

        if result.failures:
            return (
                f"{success_text} "
                f"{len(result.failures)} emails failed to send: {','.join(result.failures)}",
                HTTPStatus.MULTI_STATUS,
            )
        return (f"{success_text}"), HTTPStatus.OK

    @bp.route("/api/line_staff_tools/list_batch_info", methods=["POST"])
    @requires_gae_auth
    def _list_batch_info() -> Tuple[str, HTTPStatus]:
        try:
            data = request.json
            state_code = StateCode(data.get("stateCode"))
            if state_code not in EMAIL_STATE_CODES:
                raise ValueError("State code is invalid for retrieving batch ids")

        except ValueError as error:
            logging.error(error)
            return str(error), HTTPStatus.BAD_REQUEST
        batch_info = email_handler.get_batch_info(state_code)

        return (
            jsonify({"batchInfo": batch_info}),
            HTTPStatus.OK,
        )

    @bp.route("/api/line_staff_tools/<state_code_str>/upload_roster", methods=["POST"])
    @requires_gae_auth
    def _upload_roster(state_code_str: str) -> Tuple[str, HTTPStatus]:
        """This method handles uploading rosters for line staff tools access.
        It assumes that the caller has manually formatted the CSV appropriately."""

        state_code = StateCode(state_code_str)
        if state_code not in ROSTER_STATE_CODES:
            return (
                f"Rosters are not supported for {state_code.value}",
                HTTPStatus.BAD_REQUEST,
            )

        dict_reader = csv.DictReader(
            request.files["file"].read().decode("utf-8-sig").splitlines()
        )
        rows = list(dict_reader)

        manager = RosterManager(state_code, rows)

        try:
            manager.validate_roster_upload()
        except ValueError as e:
            return str(e), HTTPStatus.BAD_REQUEST

        manager.normalize_roster_values()

        manager.grant_access()

        # Update roster after access is fully granted.
        manager.store_roster()

        return "", HTTPStatus.OK
